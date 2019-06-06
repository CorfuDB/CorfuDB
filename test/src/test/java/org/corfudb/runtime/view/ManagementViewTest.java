package org.corfudb.runtime.view;


import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.Range;
import com.google.common.reflect.TypeToken;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.SequencerServer;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.ServerContextBuilder;
import org.corfudb.infrastructure.TestLayoutBuilder;
import org.corfudb.infrastructure.TestServerRouter;
import org.corfudb.infrastructure.management.FailureDetector;
import org.corfudb.infrastructure.management.NetworkStretcher;
import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.LayoutCommittedRequest;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.NodeState;
import org.corfudb.protocols.wireprotocol.ReadResponse;
import org.corfudb.protocols.wireprotocol.SequencerMetrics.SequencerStatus;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity.NodeConnectivityType;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.TestRule;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.collections.ISMRMap;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.exceptions.AbortCause;
import org.corfudb.runtime.exceptions.OutrankedException;
import org.corfudb.runtime.exceptions.ServerNotReadyException;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.view.ClusterStatusReport.ClusterStatus;
import org.corfudb.runtime.view.ClusterStatusReport.ClusterStatusReliability;
import org.corfudb.runtime.view.ClusterStatusReport.ConnectivityStatus;
import org.corfudb.runtime.view.ClusterStatusReport.NodeStatus;
import org.corfudb.runtime.view.stream.IStreamView;
import org.corfudb.util.Sleep;
import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.fail;

/**
 * Test to verify the Management Server functionalities.
 *
 * Created by zlokhandwala on 11/9/16.
 */
@Slf4j
public class ManagementViewTest extends AbstractViewTest {

    @Getter
    protected CorfuRuntime corfuRuntime = null;

    /**
     * Sets aggressive timeouts for all the router endpoints on all the runtimes.
     * <p>
     *
     * @param layout        Layout to get all server endpoints.
     * @param corfuRuntimes All runtimes whose routers' timeouts are to be set.
     */
    public void setAggressiveTimeouts(Layout layout, CorfuRuntime... corfuRuntimes) {
        layout.getAllServers().forEach(routerEndpoint -> {
            for (CorfuRuntime runtime : corfuRuntimes) {
                runtime.getRouter(routerEndpoint).setTimeoutConnect(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());
                runtime.getRouter(routerEndpoint).setTimeoutResponse(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());
                runtime.getRouter(routerEndpoint).setTimeoutRetry(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());
            }
        });
    }

    private void waitForSequencerToBootstrap(int primarySequencerPort) {
        // Waiting for sequencer to be bootstrapped
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_MODERATE; i++) {
            if (getSequencer(primarySequencerPort).getSequencerEpoch() != Layout.INVALID_EPOCH) {
                return;
            }
            Sleep.sleepUninterruptibly(PARAMETERS.TIMEOUT_SHORT);
        }
        Assert.fail();
    }

    /**
     * Refreshes the layout and waits for a limited time for the refreshed layout to
     * satisfy the expected verifier.
     *
     * @param verifier     Layout predicate to test the refreshed layout.
     * @param corfuRuntime corfu runtime.
     */
    public static void waitForLayoutChange(Predicate<Layout> verifier, CorfuRuntime corfuRuntime) {
        corfuRuntime.invalidateLayout();
        Layout refreshedLayout = corfuRuntime.getLayoutView().getLayout();

        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_MODERATE; i++) {
            if (verifier.test(refreshedLayout)) {
                break;
            }
            corfuRuntime.invalidateLayout();
            refreshedLayout = corfuRuntime.getLayoutView().getLayout();
            Sleep.sleepUninterruptibly(PARAMETERS.TIMEOUT_VERY_SHORT);
        }
        assertThat(verifier.test(refreshedLayout)).isTrue();
    }

    /**
     * Scenario with 2 nodes: SERVERS.PORT_0 and SERVERS.PORT_1.
     * We fail SERVERS.PORT_0 and then listen to intercept the message
     * sent by SERVERS.PORT_1's client to the server to handle the failure.
     *
     * @throws Exception
     */
    @Test
    public void invokeFailureHandler()
            throws Exception {

        // Boolean flag turned to true when the MANAGEMENT_FAILURE_DETECTED message
        // is sent by the Management client to its server.
        final Semaphore failureDetected = new Semaphore(1, true);

        addServer(SERVERS.PORT_0);
        addServer(SERVERS.PORT_1);
        Layout l = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(SERVERS.PORT_0)
                .addLayoutServer(SERVERS.PORT_1)
                .addSequencer(SERVERS.PORT_1)
                .buildSegment()
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addLogUnit(SERVERS.PORT_1)
                .addToSegment()
                .addToLayout()
                .build();
        bootstrapAllServers(l);

        // Shutting down causes loss of heartbeat requests and responses from this node.
        getManagementServer(SERVERS.PORT_0).shutdown();

        CorfuRuntime corfuRuntime = getRuntime(l).connect();

        // Set aggressive timeouts.
        setAggressiveTimeouts(l, corfuRuntime,
                getManagementServer(SERVERS.PORT_1).getManagementAgent().getCorfuRuntime());

        failureDetected.acquire();

        // Adding a rule on SERVERS.PORT_0 to drop all packets
        addServerRule(SERVERS.PORT_0, new TestRule().always().drop());

        // Adding a rule on SERVERS.PORT_1 to toggle the flag when it sends the
        // MANAGEMENT_FAILURE_DETECTED message.
        addClientRule(getManagementServer(SERVERS.PORT_1).getManagementAgent().getCorfuRuntime(),
                new TestRule().matches(corfuMsg -> {
                    if (corfuMsg.getMsgType().equals(CorfuMsgType
                            .MANAGEMENT_FAILURE_DETECTED)) {
                        failureDetected.release();
                    }
                    return true;
                }));

        assertThat(failureDetected.tryAcquire(PARAMETERS.TIMEOUT_LONG.toNanos(),
                TimeUnit.NANOSECONDS)).isEqualTo(true);
    }

    /**
     * Scenario with 3 nodes: SERVERS.PORT_0, SERVERS.PORT_1 and SERVERS.PORT_2.
     * We fail SERVERS.PORT_1 and then wait for one of the other two servers to
     * handle this failure, propose a new layout. The test asserts on a stable
     * layout. The failure is handled by removing the failed node.
     */
    @Test
    public void removeSingleNodeFailure() {
        addServer(SERVERS.PORT_0);
        addServer(SERVERS.PORT_1);
        addServer(SERVERS.PORT_2);

        Layout l = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(SERVERS.PORT_0)
                .addLayoutServer(SERVERS.PORT_1)
                .addLayoutServer(SERVERS.PORT_2)
                .addSequencer(SERVERS.PORT_0)
                .buildSegment()
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addLogUnit(SERVERS.PORT_2)
                .addToSegment()
                .addToLayout()
                .build();
        bootstrapAllServers(l);

        CorfuRuntime corfuRuntime = getRuntime(l).connect();

        // Setting aggressive timeouts for connect, retry, response timeouts
        setAggressiveTimeouts(l, corfuRuntime,
                getManagementServer(SERVERS.PORT_0).getManagementAgent().getCorfuRuntime(),
                getManagementServer(SERVERS.PORT_1).getManagementAgent().getCorfuRuntime(),
                getManagementServer(SERVERS.PORT_2).getManagementAgent().getCorfuRuntime());

        // Setting aggressive timeouts for failure and healing detectors
        setAggressiveDetectorTimeouts(SERVERS.PORT_0, SERVERS.PORT_1, SERVERS.PORT_2);

        // Adding a rule on SERVERS.PORT_1 to drop all packets
        addServerRule(SERVERS.PORT_1, new TestRule().always().drop());
        getManagementServer(SERVERS.PORT_1).shutdown();

        // Waiting until a stable layout is committed
        waitForLayoutChange(layout -> layout.getUnresponsiveServers().contains(SERVERS.ENDPOINT_1) &&
                        layout.getUnresponsiveServers().size() == 1,
                corfuRuntime);

        // Verifying layout and remove of failed server
        Layout l2 = corfuRuntime.getLayoutView().getLayout();
        assertThat(l2.getEpoch()).isGreaterThan(l.getEpoch());
        assertThat(l2.getLayoutServers().size()).isEqualTo(l.getAllServers().size());
        assertThat(l2.getAllActiveServers().size()).isEqualTo(l.getAllServers().size() - 1);
        assertThat(l2.getUnresponsiveServers()).contains(SERVERS.ENDPOINT_1);
    }

    private void setAggressiveDetectorTimeouts(int... managementServersPorts) {
        Arrays.stream(managementServersPorts).forEach(port -> {
            NetworkStretcher stretcher = NetworkStretcher.builder()
                    .periodDelta(PARAMETERS.TIMEOUT_VERY_SHORT)
                    .maxPeriod(PARAMETERS.TIMEOUT_VERY_SHORT)
                    .initialPollInterval(PARAMETERS.TIMEOUT_VERY_SHORT)
                    .build();

            FailureDetector failureDetector = getManagementServer(port)
                    .getManagementAgent()
                    .getRemoteMonitoringService()
                    .getFailureDetector();
            failureDetector.setNetworkStretcher(stretcher);
        });
    }

    private Layout getManagementTestLayout() {
        addServer(SERVERS.PORT_0);
        addServer(SERVERS.PORT_1);
        addServer(SERVERS.PORT_2);

        Layout l = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(SERVERS.PORT_0)
                .addLayoutServer(SERVERS.PORT_1)
                .addLayoutServer(SERVERS.PORT_2)
                .addSequencer(SERVERS.PORT_0)
                .addSequencer(SERVERS.PORT_1)
                .addSequencer(SERVERS.PORT_2)
                .buildSegment()
                .buildStripe()
                .addLogUnit(SERVERS.PORT_1)
                .addLogUnit(SERVERS.PORT_2)
                .addToSegment()
                .addToLayout()
                .setClusterId(UUID.randomUUID())
                .build();
        bootstrapAllServers(l);
        corfuRuntime = getRuntime(l).connect();

        waitForSequencerToBootstrap(SERVERS.PORT_0);

        // Setting aggressive timeouts
        setAggressiveTimeouts(l, corfuRuntime,
                getManagementServer(SERVERS.PORT_0).getManagementAgent().getCorfuRuntime(),
                getManagementServer(SERVERS.PORT_1).getManagementAgent().getCorfuRuntime(),
                getManagementServer(SERVERS.PORT_2).getManagementAgent().getCorfuRuntime());

        setAggressiveDetectorTimeouts(SERVERS.PORT_0, SERVERS.PORT_1, SERVERS.PORT_2);

        return l;
    }

    private Layout get3NodeLayout() {
        addServer(SERVERS.PORT_0);
        addServer(SERVERS.PORT_1);
        addServer(SERVERS.PORT_2);

        Layout l = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(SERVERS.PORT_0)
                .addLayoutServer(SERVERS.PORT_1)
                .addLayoutServer(SERVERS.PORT_2)
                .addSequencer(SERVERS.PORT_0)
                .addSequencer(SERVERS.PORT_1)
                .addSequencer(SERVERS.PORT_2)
                .buildSegment()
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addLogUnit(SERVERS.PORT_1)
                .addLogUnit(SERVERS.PORT_2)
                .addToSegment()
                .addToLayout()
                .setClusterId(UUID.randomUUID())
                .build();
        bootstrapAllServers(l);
        corfuRuntime = getRuntime(l).connect();

        waitForSequencerToBootstrap(SERVERS.PORT_0);

        // Setting aggressive timeouts
        setAggressiveTimeouts(l, corfuRuntime,
                getManagementServer(SERVERS.PORT_0).getManagementAgent().getCorfuRuntime(),
                getManagementServer(SERVERS.PORT_1).getManagementAgent().getCorfuRuntime(),
                getManagementServer(SERVERS.PORT_2).getManagementAgent().getCorfuRuntime());

        setAggressiveDetectorTimeouts(SERVERS.PORT_0, SERVERS.PORT_1, SERVERS.PORT_2);

        return l;
    }

    /**
     * Scenario with 1 node: SERVERS.PORT_0
     * The node is setup, bootstrapped and then requested for a
     * heartbeat. This is responded with the nodeMetrics which is
     * asserted with expected values.
     *
     * @throws Exception
     */
    @Test
    public void checkNodeState()
            throws Exception {
        addServer(SERVERS.PORT_0);

        Layout l = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(SERVERS.PORT_0)
                .addSequencer(SERVERS.PORT_0)
                .buildSegment()
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addToSegment()
                .addToLayout()
                .build();
        bootstrapAllServers(l);


        CorfuRuntime corfuRuntime = getRuntime(l).connect();

        // Set aggressive timeouts.
        setAggressiveTimeouts(l, corfuRuntime,
                getManagementServer(SERVERS.PORT_0).getManagementAgent().getCorfuRuntime());

        NodeState nodeState= null;

        // Send heartbeat requests and wait until we get a valid response.
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_LOW; i++) {

            nodeState = corfuRuntime.getLayoutView().getRuntimeLayout()
                    .getManagementClient(SERVERS.ENDPOINT_0).sendNodeStateRequest().get();

            if (nodeState.getConnectivity().getType() == NodeConnectivityType.CONNECTED
                    && !nodeState.getConnectivity().getConnectedNodes().isEmpty()) {
                break;
            }
            Sleep.sleepUninterruptibly(PARAMETERS.TIMEOUT_VERY_SHORT);
        }
        assertThat(nodeState).isNotNull();
        assertThat(nodeState.getConnectivity()).isNotNull();
        assertThat(nodeState.getConnectivity().getConnectedNodes()).contains(SERVERS.ENDPOINT_0);
    }

    /**
     * Scenario with 3 nodes: SERVERS.PORT_0, SERVERS.PORT_1 and SERVERS.PORT_2.
     * Simulate transient failure of a server leading to a partial seal.
     * Allow the management server to detect the partial seal and correct this.
     * <p>
     * Part 1.
     * The partial seal causes SERVERS.PORT_0 to be at epoch 2 whereas,
     * SERVERS.PORT_1 & SERVERS.PORT_2 fail to receive this message and are stuck at epoch 1.
     * <p>
     * Part 2.
     * All the 3 servers are now functional and receive all messages.
     * <p>
     * Part 3.
     * The PING message gets rejected by the partially sealed router (WrongEpoch)
     * and the management server realizes of the partial seal and corrects this
     * by issuing another failure detected message.
     *
     * @throws Exception
     */
    @Test
    public void handleTransientFailure() throws Exception {
        log.info("Boolean flag turned to true when the MANAGEMENT_FAILURE_DETECTED message " +
                "is sent by the Management client to its server"
        );
        final Semaphore failureDetected = new Semaphore(2, true);

        addServer(SERVERS.PORT_0);
        addServer(SERVERS.PORT_1);
        addServer(SERVERS.PORT_2);

        Layout l = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(SERVERS.PORT_0)
                .addLayoutServer(SERVERS.PORT_1)
                .addLayoutServer(SERVERS.PORT_2)
                .addSequencer(SERVERS.PORT_0)
                .addSequencer(SERVERS.PORT_1)
                .addSequencer(SERVERS.PORT_2)
                .buildSegment()
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addLogUnit(SERVERS.PORT_1)
                .addLogUnit(SERVERS.PORT_2)
                .addToSegment()
                .addToLayout()
                .setClusterId(UUID.randomUUID())
                .build();
        bootstrapAllServers(l);
        corfuRuntime = getRuntime(l).connect();

        waitForSequencerToBootstrap(SERVERS.PORT_0);

        log.info("Setting aggressive timeouts");
        setAggressiveTimeouts(l, corfuRuntime,
                getManagementServer(SERVERS.PORT_0).getManagementAgent().getCorfuRuntime(),
                getManagementServer(SERVERS.PORT_1).getManagementAgent().getCorfuRuntime(),
                getManagementServer(SERVERS.PORT_2).getManagementAgent().getCorfuRuntime());

        setAggressiveDetectorTimeouts(SERVERS.PORT_0, SERVERS.PORT_1, SERVERS.PORT_2);

        failureDetected.acquire(2);

        log.info("Only allow SERVERS.PORT_0 to manage failures. Prevent the other servers from handling failures.");
        TestRule testRule = new TestRule()
                .matches(corfuMsg -> corfuMsg.getMsgType().equals(CorfuMsgType.SET_EPOCH)
                        || corfuMsg.getMsgType().equals(CorfuMsgType.MANAGEMENT_FAILURE_DETECTED))
                .drop();

        addClientRule(getManagementServer(SERVERS.PORT_0).getManagementAgent().getCorfuRuntime(),
                SERVERS.ENDPOINT_1, testRule);
        addClientRule(getManagementServer(SERVERS.PORT_0).getManagementAgent().getCorfuRuntime(),
                SERVERS.ENDPOINT_2, testRule);
        addClientRule(getManagementServer(SERVERS.PORT_1).getManagementAgent().getCorfuRuntime(),
                SERVERS.ENDPOINT_1, testRule);
        addClientRule(getManagementServer(SERVERS.PORT_1).getManagementAgent().getCorfuRuntime(),
                SERVERS.ENDPOINT_2, testRule);
        addClientRule(getManagementServer(SERVERS.PORT_2).getManagementAgent().getCorfuRuntime(),
                SERVERS.ENDPOINT_1, testRule);
        addClientRule(getManagementServer(SERVERS.PORT_2).getManagementAgent().getCorfuRuntime(),
                SERVERS.ENDPOINT_2, testRule);

        // PART 1.
        log.info("Prevent ENDPOINT_1 from sealing.");
        addClientRule(getManagementServer(SERVERS.PORT_0).getManagementAgent().getCorfuRuntime(),
                SERVERS.ENDPOINT_1, new TestRule()
                        .matches(corfuMsg -> corfuMsg.getMsgType().equals(CorfuMsgType.SET_EPOCH))
                        .drop());
        log.info("Simulate ENDPOINT_2 failure from ENDPOINT_1 (only Management Server)");
        addClientRule(getManagementServer(SERVERS.PORT_1).getManagementAgent().getCorfuRuntime(),
                SERVERS.ENDPOINT_2, new TestRule().always().drop());

        log.info("Adding a rule on SERVERS.PORT_1 to toggle the flag when it " +
                "sends the MANAGEMENT_FAILURE_DETECTED message."
        );
        addClientRule(getManagementServer(SERVERS.PORT_0).getManagementAgent().getCorfuRuntime(),
                new TestRule().matches(corfuMsg -> {
                    if (corfuMsg.getMsgType().equals(CorfuMsgType.MANAGEMENT_FAILURE_DETECTED)) {
                        failureDetected.release();
                    }
                    return true;
                }));

        log.info("Go ahead when sealing of ENDPOINT_0 takes place.");
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_MODERATE; i++) {
            if (getServerRouter(SERVERS.PORT_0).getServerEpoch() == 2L) {
                failureDetected.release();
                break;
            }
            Thread.sleep(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());
        }

        assertThat(failureDetected.tryAcquire(2, PARAMETERS.TIMEOUT_NORMAL.toNanos(),
                TimeUnit.NANOSECONDS)).isEqualTo(true);

        addClientRule(getManagementServer(SERVERS.PORT_0).getManagementAgent().getCorfuRuntime(),
                new TestRule().matches(corfuMsg ->
                        corfuMsg.getMsgType().equals(CorfuMsgType.MANAGEMENT_FAILURE_DETECTED))
                        .drop());

        log.info("Assert that only a partial seal was successful. " +
                "ENDPOINT_0 sealed. ENDPOINT_1 & ENDPOINT_2 not sealed."
        );
        assertThat(getServerRouter(SERVERS.PORT_0).getServerEpoch()).isEqualTo(2L);
        assertThat(getServerRouter(SERVERS.PORT_1).getServerEpoch()).isEqualTo(1L);
        assertThat(getServerRouter(SERVERS.PORT_2).getServerEpoch()).isEqualTo(1L);
        assertThat(getLayoutServer(SERVERS.PORT_0).getCurrentLayout().getEpoch()).isEqualTo(1L);
        assertThat(getLayoutServer(SERVERS.PORT_1).getCurrentLayout().getEpoch()).isEqualTo(1L);
        assertThat(getLayoutServer(SERVERS.PORT_2).getCurrentLayout().getEpoch()).isEqualTo(1L);

        // PART 2.
        log.info("Simulate normal operations for all servers and clients.");
        clearClientRules(getManagementServer(SERVERS.PORT_0).getManagementAgent().getCorfuRuntime());

        // PART 3.
        log.info("Allow management server to detect partial seal and correct this issue.");
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_MODERATE; i++) {
            Thread.sleep(PARAMETERS.TIMEOUT_SHORT.toMillis());
            log.info("Assert successful seal of all servers.");

            long server0Epoch = getServerRouter(SERVERS.PORT_0).getServerEpoch();
            long server1Epoch = getServerRouter(SERVERS.PORT_1).getServerEpoch();
            long server2Epoch = getServerRouter(SERVERS.PORT_2).getServerEpoch();
            long server0LayoutEpoch = getLayoutServer(SERVERS.PORT_0).getCurrentLayout().getEpoch();
            long server1LayoutEpoch = getLayoutServer(SERVERS.PORT_1).getCurrentLayout().getEpoch();
            long server2LayoutEpoch = getLayoutServer(SERVERS.PORT_2).getCurrentLayout().getEpoch();

            List<Long> epochs = Arrays.asList(
                    server0Epoch, server1Epoch, server2Epoch,
                    server0LayoutEpoch, server1LayoutEpoch, server2LayoutEpoch
            );

            if (epochs.stream().allMatch(epoch -> epoch == 2)) {
                return;
            }
            log.warn("The seal is not complete yet. Wait for next iteration. Servers epochs: {}", epochs);
        }
        fail();

    }

    private void induceSequencerFailureAndWait() {

        long currentEpoch = getCorfuRuntime().getLayoutView().getLayout().getEpoch();

        // induce a failure to the server on PORT_0, where the current sequencer is active
        //
        getManagementServer(SERVERS.PORT_0).shutdown();
        addServerRule(SERVERS.PORT_0, new TestRule().always().drop());

        // wait for failover to install a new epoch (and a new layout)
        waitForLayoutChange(layout ->
                        layout.getEpoch() > currentEpoch && !layout.getPrimarySequencer().equals(SERVERS.ENDPOINT_0),
                getCorfuRuntime());
    }

    /**
     * Scenario with 3 nodes: SERVERS.PORT_0, SERVERS.PORT_1 and SERVERS.PORT_2.
     * We fail SERVERS.PORT_1 and then wait for one of the other two servers to
     * handle this failure, propose a new layout and we assert on the epoch change.
     * The failure is handled by ConserveFailureHandlerPolicy.
     * No nodes are removed from the layout, but are marked unresponsive.
     * A sequencer failover takes place where the next working sequencer is reset
     * and made the primary.
     */
    @Test
    public void testSequencerFailover() {
        getManagementTestLayout();

        final long beforeFailure = 5L;
        final long afterFailure = 10L;

        IStreamView sv = getCorfuRuntime().getStreamsView().get(CorfuRuntime.getStreamID("streamA"));
        byte[] testPayload = "hello world".getBytes();
        sv.append(testPayload);
        sv.append(testPayload);
        sv.append(testPayload);
        sv.append(testPayload);
        sv.append(testPayload);

        assertThat(getSequencer(SERVERS.PORT_0).getGlobalLogTail()).isEqualTo(beforeFailure);
        assertThat(getSequencer(SERVERS.PORT_1).getGlobalLogTail()).isEqualTo(0L);

        induceSequencerFailureAndWait();

        waitForLayoutChange(layout -> layout.getUnresponsiveServers().size() == 1
                && layout.getUnresponsiveServers().contains(SERVERS.ENDPOINT_0), getCorfuRuntime());
        Layout newLayout = new Layout(getCorfuRuntime().getLayoutView().getLayout());

        // Block until new sequencer reaches READY state.
        TokenResponse tokenResponse = getCorfuRuntime().getSequencerView().query();
        // verify that a failover sequencer was started with the correct starting-tail
        assertThat(tokenResponse.getSequence()).isEqualTo(beforeFailure - 1);

        sv.append(testPayload);
        sv.append(testPayload);
        sv.append(testPayload);
        sv.append(testPayload);
        sv.append(testPayload);

        assertThat(newLayout.getUnresponsiveServers()).containsExactly(SERVERS.ENDPOINT_0);

        tokenResponse = getCorfuRuntime().getSequencerView().query();
        assertThat(tokenResponse.getSequence()).isEqualTo(afterFailure - 1);
    }

    protected <T> Object instantiateCorfuObject(TypeToken<T> tType, String name) {
        return getCorfuRuntime().getObjectsView()
                .build()
                .setStreamName(name)     // stream name
                .setTypeToken(tType)    // a TypeToken of the specified class
                .open();                // instantiate the object!
    }


    protected ISMRMap<Integer, String> getMap() {
        ISMRMap<Integer, String> testMap;

        testMap = (ISMRMap<Integer, String>) instantiateCorfuObject(
                new TypeToken<SMRMap<Integer, String>>() {
                }, "test stream"
        );

        return testMap;
    }

    protected void TXBegin() {
        getCorfuRuntime().getObjectsView().TXBegin();
    }

    protected void TXEnd() {
        getCorfuRuntime().getObjectsView().TXEnd();
    }

    /**
     * Check that transaction conflict resolution works properly in face of sequencer failover
     */
    @Test
    public void ckSequencerFailoverTXResolution() {
        // setup 3-Corfu node cluster
        getManagementTestLayout();

        Map<Integer, String> map = getMap();

        // start a transaction and force it to obtain snapshot timestamp
        // preceding the sequencer failover
        t(0, () -> {
            TXBegin();
            map.get(0);
        });

        final String payload = "hello";
        final int nUpdates = 5;

        // in another thread, fill the log with a few entries
        t(1, () -> {
            for (int i = 0; i < nUpdates; i++)
                map.put(i, payload);
        });

        // now, the tail of the log is at nUpdates;
        // kill the sequencer, wait for a failover,
        // and then resume the transaction above; it should abort
        // (unnecessarily, but we are being conservative)
        //
        induceSequencerFailureAndWait();
        t(0, () -> {
            boolean commit = true;
            map.put(nUpdates + 1, payload); // should not conflict
            try {
                TXEnd();
            } catch (TransactionAbortedException ta) {
                assertThat(ta.getAbortCause()).isEqualTo(AbortCause.NEW_SEQUENCER);
                commit = false;
            }
            assertThat(commit)
                    .isFalse();
        });

        // now, check that the same scenario, starting a new, can succeed
        t(0, () -> {
            TXBegin();
            map.get(0);
        });

        // in another thread, fill the log with a few entries
        t(1, () -> {
            for (int i = 0; i < nUpdates; i++)
                map.put(i, payload + 1);
        });

        t(0, () -> {
            boolean commit = true;
            map.put(nUpdates + 1, payload); // should not conflict
            try {
                TXEnd();
            } catch (TransactionAbortedException ta) {
                commit = false;
            }
            assertThat(commit)
                    .isTrue();
        });
    }


    /**
     * small variant on the above : don't start the first TX at the start of the log.
     */
    @Test
    public void ckSequencerFailoverTXResolution1() {
        getManagementTestLayout();

        Map<Integer, String> map = getMap();
        final String payload = "hello";
        final int nUpdates = 5;

        for (int i = 0; i < nUpdates; i++)
            map.put(i, payload);

        // start a transaction and force it to obtain snapshot timestamp
        // preceding the sequencer failover
        t(0, () -> {
            TXBegin();
            map.get(0);
        });

        // in another thread, fill the log with a few entries
        t(1, () -> {
            for (int i = 0; i < nUpdates; i++)
                map.put(i, payload + 1);
        });

        // now, the tail of the log is at nUpdates;
        // kill the sequencer, wait for a failover,
        // and then resume the transaction above; it should abort
        // (unnecessarily, but we are being conservative)
        //
        induceSequencerFailureAndWait();

        t(0, () -> {
            boolean commit = true;
            map.put(nUpdates + 1, payload); // should not conflict
            try {
                TXEnd();
            } catch (TransactionAbortedException ta) {
                assertThat(ta.getAbortCause()).isEqualTo(AbortCause.NEW_SEQUENCER);
                commit = false;
            }
            assertThat(commit)
                    .isFalse();
        });

        // now, check that the same scenario, starting anew, can succeed
        t(0, () -> {
            TXBegin();
            map.get(0);
        });

        // in another thread, fill the log with a few entries
        t(1, () -> {
            for (int i = 0; i < nUpdates; i++)
                map.put(i, payload + 2);
        });

        t(0, () -> {
            boolean commit = true;
            map.put(nUpdates + 1, payload); // should not conflict
            try {
                TXEnd();
            } catch (TransactionAbortedException ta) {
                commit = false;
            }
            assertThat(commit)
                    .isTrue();
        });
    }

    /**
     * When a stream is seen for the first time by the sequencer it returns a -1
     * in the backpointer map.
     * After failover, the new sequencer returns a null in the backpointer map
     * forcing it to single step backwards and get the last backpointer for the
     * given stream.
     * An example is shown below:
     * <p>
     * Index  :  0  1  2  3  |          | 4  5  6  7  8
     * Stream :  A  B  A  B  | failover | A  C  A  B  B
     * B.P    : -1 -1  0  1  |          | 2 -1  4  3  7
     * <p>
     * -1 : New StreamID so empty backpointers
     * X : (null) Unknown backpointers as this is a failed-over sequencer.
     * <p>
     */
    @Test
    public void sequencerFailoverBackpointerCheck() {
        getManagementTestLayout();

        UUID streamA = UUID.nameUUIDFromBytes("stream A".getBytes());
        UUID streamB = UUID.nameUUIDFromBytes("stream B".getBytes());
        UUID streamC = UUID.nameUUIDFromBytes("stream C".getBytes());

        final long streamA_backpointerRecovered = 2L;
        final long streamB_backpointerRecovered = 3L;

        final long streamA_backpointerFinal = 4L;
        final long streamB_backpointerFinal = 7L;

        getTokenWriteAndAssertBackPointer(streamA, Address.NON_EXIST);
        getTokenWriteAndAssertBackPointer(streamB, Address.NON_EXIST);
        getTokenWriteAndAssertBackPointer(streamA, 0L);
        getTokenWriteAndAssertBackPointer(streamB, 1L);

        induceSequencerFailureAndWait();

        getTokenWriteAndAssertBackPointer(streamA, streamA_backpointerRecovered);
        getTokenWriteAndAssertBackPointer(streamC, Address.NON_EXIST);
        getTokenWriteAndAssertBackPointer(streamA, streamA_backpointerFinal);
        getTokenWriteAndAssertBackPointer(streamB, streamB_backpointerRecovered);
        getTokenWriteAndAssertBackPointer(streamB, streamB_backpointerFinal);
    }

    /**
     * Requests for a token for the given stream ID.
     * Asserts the backpointer map in the token response with the specified backpointer location.
     * Writes test data to the log unit servers using the tokenResponse.
     *
     * @param streamID                 Stream ID to request token for.
     * @param expectedBackpointerValue Expected backpointer for given stream.
     */
    private void getTokenWriteAndAssertBackPointer(UUID streamID, Long expectedBackpointerValue) {
        TokenResponse tokenResponse =
                corfuRuntime.getSequencerView().next(streamID);
        if (expectedBackpointerValue == null) {
            assertThat(tokenResponse.getBackpointerMap()).isEmpty();
        } else {
            assertThat(tokenResponse.getBackpointerMap()).containsEntry(streamID, expectedBackpointerValue);
        }
        corfuRuntime.getAddressSpaceView().write(tokenResponse,
                "test".getBytes());
    }

    /**
     * Asserts that we cannot fetch a token after a failure but before sequencer reset.
     *
     * @throws Exception
     */
    @Test
    public void blockRecoverySequencerUntilReset() throws Exception {

        final Semaphore resetDetected = new Semaphore(1);
        Layout layout = getManagementTestLayout();

        UUID streamA = UUID.nameUUIDFromBytes("stream A".getBytes());

        getTokenWriteAndAssertBackPointer(streamA, Address.NON_EXIST);

        resetDetected.acquire();
        // Allow only SERVERS.PORT_0 to handle the failure.
        // Preventing PORT_2 from bootstrapping the sequencer.
        addClientRule(getManagementServer(SERVERS.PORT_2).getManagementAgent().getCorfuRuntime(),
                new TestRule().matches(msg -> msg.getMsgType().equals(CorfuMsgType.BOOTSTRAP_SEQUENCER)).drop());
        addClientRule(getManagementServer(SERVERS.PORT_1).getManagementAgent().getCorfuRuntime(),
                new TestRule().matches(msg -> {
                    if (msg.getMsgType().equals(CorfuMsgType.BOOTSTRAP_SEQUENCER)) {
                        try {
                            // There is a failure but the BOOTSTRAP_SEQUENCER message has not yet been
                            // sent. So if we request a token now, we should be denied as the
                            // server is sealed and we get a WrongEpochException.
                            corfuRuntime.getLayoutView().getRuntimeLayout(layout)
                                    .getSequencerClient(SERVERS.ENDPOINT_1)
                                    .nextToken(Collections.singletonList(CorfuRuntime
                                            .getStreamID("testStream")), 1).get();
                            fail();
                        } catch (InterruptedException | ExecutionException e) {
                            resetDetected.release();
                        }
                    }
                    return false;
                }));
        // Inducing failure on PORT_1
        induceSequencerFailureAndWait();

        assertThat(resetDetected
                .tryAcquire(PARAMETERS.TIMEOUT_NORMAL.toMillis(), TimeUnit.MILLISECONDS))
                .isTrue();

        // We should be able to request a token now.
        corfuRuntime.getSequencerView().next(CorfuRuntime.getStreamID("testStream"));
    }

    @Test
    public void sealDoesNotModifyClientEpoch() {
        Layout l = getManagementTestLayout();

        // Seal
        Layout newLayout = new Layout(l);
        newLayout.setEpoch(newLayout.getEpoch() + 1);
        corfuRuntime.getLayoutView().getRuntimeLayout(newLayout).sealMinServerSet();
        assertThat(corfuRuntime.getLayoutView().getLayout().getEpoch()).isEqualTo(l.getEpoch());
    }

    /**
     * Checks for updates trailing layout servers.
     * The layout is partially committed with epoch 2 except for ENDPOINT_0.
     * All commit messages from the cluster are intercepted.
     * The test checks whether at least one of the 3 management agents patches the layout server with the latest
     * layout.
     * If a commit message with any other epoch is sent, the test fails.
     */
    @Test
    public void updateTrailingLayoutServers() throws Exception {

        Layout layout = new Layout(getManagementTestLayout());

        AtomicBoolean commitWithDifferentEpoch = new AtomicBoolean(false);

        final CountDownLatch latch = new CountDownLatch(1);
        TestRule interceptCommit = new TestRule().matches(corfuMsg -> {
            if (corfuMsg.getMsgType().equals(CorfuMsgType.LAYOUT_COMMITTED)) {
                if (((CorfuPayloadMsg<LayoutCommittedRequest>) corfuMsg).getPayload().getLayout().getEpoch() == 2) {
                    latch.countDown();
                } else {
                    commitWithDifferentEpoch.set(true);
                }
            }
            return true;
        });

        addClientRule(getManagementServer(SERVERS.PORT_0).getManagementAgent().getCorfuRuntime(), interceptCommit);
        addClientRule(getManagementServer(SERVERS.PORT_1).getManagementAgent().getCorfuRuntime(), interceptCommit);
        addClientRule(getManagementServer(SERVERS.PORT_2).getManagementAgent().getCorfuRuntime(), interceptCommit);

        final long highRank = 10L;

        addClientRule(corfuRuntime, SERVERS.ENDPOINT_0, new TestRule().always().drop());
        layout.setEpoch(2L);
        corfuRuntime.getLayoutView().getRuntimeLayout(layout).sealMinServerSet();
        // We increase to a higher rank to avoid being outranked. We could be outranked if the management
        // agent attempts to fill in the epoch slot before we update.
        corfuRuntime.getLayoutView().updateLayout(layout, highRank);

        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_MODERATE; i++) {
            Thread.sleep(PARAMETERS.TIMEOUT_SHORT.toMillis());
            if (getLayoutServer(SERVERS.PORT_0).getCurrentLayout().equals(layout))
                break;

        }

        assertThat(getLayoutServer(SERVERS.PORT_0).getCurrentLayout().getEpoch()).isEqualTo(2L);
        assertThat(getLayoutServer(SERVERS.PORT_0).getCurrentLayout()).isEqualTo(layout);
        latch.await();
        assertThat(commitWithDifferentEpoch.get()).isFalse();
    }

    /**
     * Tests a 3 node cluster.
     * All Prepare messages are first blocked. Then a seal is issued for epoch 2.
     * The test then ensures that no layout is committed for the epoch 2.
     * We ensure that no layout is committed other than the Paxos path.
     */
    @Test
    public void blockLayoutUpdateAfterSeal() {

        Layout layout = new Layout(getManagementTestLayout());

        TestRule dropPrepareMsg = new TestRule()
                .matches(corfuMsg -> corfuMsg.getMsgType().equals(CorfuMsgType.LAYOUT_PREPARE))
                .drop();

        // Block Paxos round by blocking all prepare methods.
        addClientRule(getManagementServer(SERVERS.PORT_0).getManagementAgent().getCorfuRuntime(), dropPrepareMsg);
        addClientRule(getManagementServer(SERVERS.PORT_1).getManagementAgent().getCorfuRuntime(), dropPrepareMsg);
        addClientRule(getManagementServer(SERVERS.PORT_2).getManagementAgent().getCorfuRuntime(), dropPrepareMsg);

        // Seal the layout.
        layout.setEpoch(2L);
        corfuRuntime.getLayoutView().getRuntimeLayout(layout).sealMinServerSet();

        // Wait for the cluster to move the layout with epoch 2 without the Paxos round.
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_LOW; i++) {
            Sleep.sleepUninterruptibly(PARAMETERS.TIMEOUT_VERY_SHORT);
            if (corfuRuntime.getLayoutView().getLayout().getEpoch() == 2L) {
                fail();
            }
            corfuRuntime.invalidateLayout();
        }
        assertThat(corfuRuntime.getLayoutView().getLayout().getEpoch()).isEqualTo(1L);
    }

    /**
     * A single node cluster is sealed but the client crashes before it can propose a layout.
     * The management server now have to detect this state and fill up this slot with an existing
     * layout in order to unblock the data plane operations.
     *
     * @throws Exception
     */
    @Test
    public void unblockSealedCluster() throws Exception {
        CorfuRuntime corfuRuntime = getDefaultRuntime();
        Layout l = new Layout(corfuRuntime.getLayoutView().getLayout());
        setAggressiveDetectorTimeouts(SERVERS.PORT_0);

        waitForSequencerToBootstrap(SERVERS.PORT_0);

        l.setEpoch(l.getEpoch() + 1);
        corfuRuntime.getLayoutView().getRuntimeLayout(l).sealMinServerSet();

        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_MODERATE; i++) {
            Thread.sleep(PARAMETERS.TIMEOUT_SHORT.toMillis());
            if (corfuRuntime.getLayoutView().getLayout().getEpoch() == l.getEpoch()) {
                break;
            }
            corfuRuntime.invalidateLayout();
        }
        assertThat(corfuRuntime.getLayoutView().getLayout().getEpoch()).isEqualTo(l.getEpoch());
    }

    /**
     * Tests healing of an unresponsive node now responding to pings.
     * A rule is added on PORT_2 to drop all messages.
     * The other 2 nodes PORT_0 and PORT_1 will detect this failure and mark it as unresponsive.
     * The rule is then removed simulating a normal functioning PORT_2. The other nodes will now
     * be able to successfully ping PORT_2. They then remove the node PORT_2 from the unresponsive
     * servers list and mark as active.
     *
     * @throws Exception
     */
    @Test
    public void testNodeHealing() {
        CorfuRuntime corfuRuntime = null;
        try {
            addServer(SERVERS.PORT_0);
            addServer(SERVERS.PORT_1);
            addServer(SERVERS.PORT_2);
            Layout l = new TestLayoutBuilder()
                    .setEpoch(1L)
                    .addLayoutServer(SERVERS.PORT_0)
                    .addLayoutServer(SERVERS.PORT_1)
                    .addLayoutServer(SERVERS.PORT_2)
                    .addSequencer(SERVERS.PORT_0)
                    .buildSegment()
                    .buildStripe()
                    .addLogUnit(SERVERS.PORT_0)
                    .addToSegment()
                    .addToLayout()
                    .build();
            bootstrapAllServers(l);

            corfuRuntime = getRuntime(l).connect();

            setAggressiveTimeouts(l, corfuRuntime,
                    getManagementServer(SERVERS.PORT_0).getManagementAgent().getCorfuRuntime());
            setAggressiveDetectorTimeouts(SERVERS.PORT_0);

            addServerRule(SERVERS.PORT_2, new TestRule().always().drop());
            waitForLayoutChange(layout -> layout.getUnresponsiveServers()
                    .equals(Collections.singletonList(SERVERS.ENDPOINT_2)), corfuRuntime);

            clearServerRules(SERVERS.PORT_2);
            waitForLayoutChange(layout -> layout.getUnresponsiveServers().isEmpty()
                    && layout.getSegments().size() == 1, corfuRuntime);
        } finally {
            if (corfuRuntime != null) {
                corfuRuntime.shutdown();
            }
        }
    }

    /**
     * Add a new node with layout, sequencer and log unit components.
     * The new log unit node is open to reads and writes only in the new segment and no
     * catchup or replication of old data is performed.
     *
     * @throws Exception
     */
    @Test
    public void testAddNodeWithoutCatchup() throws Exception {
        CorfuRuntime rt = null;
        try {
            addServer(SERVERS.PORT_0);

            Layout l1 = new TestLayoutBuilder()
                    .setEpoch(0L)
                    .addLayoutServer(SERVERS.PORT_0)
                    .addSequencer(SERVERS.PORT_0)
                    .buildSegment()
                    .buildStripe()
                    .addLogUnit(SERVERS.PORT_0)
                    .addToSegment()
                    .addToLayout()
                    .build();
            bootstrapAllServers(l1);

            ServerContext sc1 = new ServerContextBuilder()
                    .setSingle(false)
                    .setServerRouter(new TestServerRouter(SERVERS.PORT_1))
                    .setPort(SERVERS.PORT_1)
                    .build();
            addServer(SERVERS.PORT_1, sc1);

            rt = getNewRuntime(getDefaultNode())
                    .connect();

            // Write to address space 0
            rt.getStreamsView().get(CorfuRuntime.getStreamID("test"))
                    .append("testPayload".getBytes());

            rt.getLayoutManagementView().addNode(l1, SERVERS.ENDPOINT_1,
                    true,
                    true,
                    true,
                    false,
                    0);

            rt.invalidateLayout();
            Layout layoutPhase2 = rt.getLayoutView().getLayout();

            Layout l2 = new TestLayoutBuilder()
                    .setEpoch(1L)
                    .addLayoutServer(SERVERS.PORT_0)
                    .addLayoutServer(SERVERS.PORT_1)
                    .addSequencer(SERVERS.PORT_0)
                    .addSequencer(SERVERS.PORT_1)
                    .buildSegment()
                    .setStart(0L)
                    .setEnd(1L)
                    .buildStripe()
                    .addLogUnit(SERVERS.PORT_0)
                    .addToSegment()
                    .addToLayout()
                    .buildSegment()
                    .setStart(1L)
                    .setEnd(-1L)
                    .buildStripe()
                    .addLogUnit(SERVERS.PORT_0)
                    .addLogUnit(SERVERS.PORT_1)
                    .addToSegment()
                    .addToLayout()
                    .build();
            assertThat(l2.asJSONString()).isEqualTo(layoutPhase2.asJSONString());
        } finally {
            if (rt != null) {
                rt.shutdown();
            }
        }
    }

    private Map<Long, LogData> getAllNonEmptyData(CorfuRuntime corfuRuntime,
                                                  String endpoint, long end) throws Exception {
        ReadResponse readResponse = corfuRuntime.getLayoutView().getRuntimeLayout()
                .getLogUnitClient(endpoint)
                .readAll(ContiguousSet.create(Range.closed(0L, end), DiscreteDomain.longs()).asList())
                .get();
        return readResponse.getAddresses().entrySet()
                .stream()
                .filter(longLogDataEntry -> !longLogDataEntry.getValue().isEmpty())
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    }

    /**
     * The test first creates a layout with 2 segments.
     * Segment 1: 0 -> 3 (exclusive) Node 0
     * Segment 2: 3 -> infinity (exclusive) Node 0, Node 1
     * Now a new node, Node 2 is added to the layout which splits the last segment, replicates
     * and merges the previous segment to produce the following:
     * Segment 1: 0 -> 3 (exclusive) Node 0
     * Segment 2: 3 -> infinity (exclusive) Node 0, Node 1, Node 2
     * Finally the state transfer is verified by asserting Node 1's data with Node 2's data.
     */
    @Test
    public void verifyStateTransferAndMerge() throws Exception {
        CorfuRuntime rt = null;
        try {
            addServer(SERVERS.PORT_0);
            addServer(SERVERS.PORT_1);

            final long writtenAddressesBatch1 = 3L;
            final long writtenAddressesBatch2 = 6L;
            Layout l1 = new TestLayoutBuilder()
                    .setEpoch(1L)
                    .addLayoutServer(SERVERS.PORT_0)
                    .addLayoutServer(SERVERS.PORT_1)
                    .addSequencer(SERVERS.PORT_0)
                    .addSequencer(SERVERS.PORT_1)
                    .buildSegment()
                    .setStart(0L)
                    .setEnd(writtenAddressesBatch1)
                    .buildStripe()
                    .addLogUnit(SERVERS.PORT_0)
                    .addToSegment()
                    .addToLayout()
                    .buildSegment()
                    .setStart(writtenAddressesBatch1)
                    .setEnd(-1L)
                    .buildStripe()
                    .addLogUnit(SERVERS.PORT_0)
                    .addLogUnit(SERVERS.PORT_1)
                    .addToSegment()
                    .addToLayout()
                    .build();
            bootstrapAllServers(l1);

            rt = getNewRuntime(getDefaultNode()).connect();

            IStreamView testStream = rt.getStreamsView().get(CorfuRuntime.getStreamID("test"));
            // Write to address spaces 0 to 2 (inclusive) to SERVER 0 only.
            testStream.append("testPayload".getBytes());
            testStream.append("testPayload".getBytes());
            testStream.append("testPayload".getBytes());

            // Write to address spaces 3 to 5 (inclusive) to SERVER 0 and SERVER 1.
            testStream.append("testPayload".getBytes());
            testStream.append("testPayload".getBytes());
            testStream.append("testPayload".getBytes());

            addServer(SERVERS.PORT_2);
            final int addNodeRetries = 3;
            rt.getManagementView()
                    .addNode(SERVERS.ENDPOINT_2, addNodeRetries, Duration.ofMinutes(1L), Duration.ofSeconds(1));
            rt.invalidateLayout();
            final long epochAfterAdd = 3L;
            Layout expectedLayout = new TestLayoutBuilder()
                    .setEpoch(epochAfterAdd)
                    .addLayoutServer(SERVERS.PORT_0)
                    .addLayoutServer(SERVERS.PORT_1)
                    .addLayoutServer(SERVERS.PORT_2)
                    .addSequencer(SERVERS.PORT_0)
                    .addSequencer(SERVERS.PORT_1)
                    .addSequencer(SERVERS.PORT_2)
                    .buildSegment()
                    .setStart(0L)
                    .setEnd(writtenAddressesBatch1)
                    .buildStripe()
                    .addLogUnit(SERVERS.PORT_0)
                    .addLogUnit(SERVERS.PORT_2)
                    .addToSegment()
                    .addToLayout()
                    .buildSegment()
                    .setStart(writtenAddressesBatch1)
                    .setEnd(writtenAddressesBatch2)
                    .buildStripe()
                    .addLogUnit(SERVERS.PORT_0)
                    .addLogUnit(SERVERS.PORT_1)
                    .addLogUnit(SERVERS.PORT_2)
                    .addToSegment()
                    .addToLayout()
                    .buildSegment()
                    .setStart(writtenAddressesBatch2)
                    .setEnd(-1L)
                    .buildStripe()
                    .addLogUnit(SERVERS.PORT_0)
                    .addLogUnit(SERVERS.PORT_1)
                    .addLogUnit(SERVERS.PORT_2)
                    .addToSegment()
                    .addToLayout()
                    .build();

            ClusterStatusReport clusterStatus = rt.getManagementView().getClusterStatus();
            Map<String, ConnectivityStatus> nodeConnectivityMap = clusterStatus.getClientServerConnectivityStatusMap();
            Map<String, NodeStatus> nodeStatusMap = clusterStatus.getClusterNodeStatusMap();
            ClusterStatusReliability clusterStatusReliability = clusterStatus.getClusterStatusReliability();
            assertThat(nodeConnectivityMap.get(SERVERS.ENDPOINT_0)).isEqualTo(ConnectivityStatus.RESPONSIVE);
            assertThat(nodeConnectivityMap.get(SERVERS.ENDPOINT_1)).isEqualTo(ConnectivityStatus.RESPONSIVE);
            assertThat(nodeConnectivityMap.get(SERVERS.ENDPOINT_2)).isEqualTo(ConnectivityStatus.RESPONSIVE);
            assertThat(nodeStatusMap.get(SERVERS.ENDPOINT_0)).isEqualTo(NodeStatus.UP);
            assertThat(nodeStatusMap.get(SERVERS.ENDPOINT_1)).isEqualTo(NodeStatus.DB_SYNCING);
            assertThat(nodeStatusMap.get(SERVERS.ENDPOINT_2)).isEqualTo(NodeStatus.UP);
            assertThat(clusterStatus.getClusterStatus()).isEqualTo(ClusterStatus.DB_SYNCING);
            assertThat(clusterStatusReliability).isEqualTo(ClusterStatusReliability.STRONG_QUORUM);
            assertThat(rt.getLayoutView().getLayout()).isEqualTo(expectedLayout);

            TokenResponse tokenResponse = rt.getSequencerView().query(CorfuRuntime.getStreamID("test"));
            long lastAddress = tokenResponse.getSequence();

            Map<Long, LogData> map_0 = getAllNonEmptyData(rt, SERVERS.ENDPOINT_0, lastAddress);
            Map<Long, LogData> map_2 = getAllNonEmptyData(rt, SERVERS.ENDPOINT_2, lastAddress);

            assertThat(map_2.entrySet()).containsOnlyElementsOf(map_0.entrySet());
        } finally {
            if (rt != null) {
                rt.shutdown();
            }
        }
    }

    /**
     * The test first creates a layout with 3 segments.
     * Initial layout:
     * Segment 1: 0 -> 3 (exclusive) Node 0
     * Segment 2: 3 -> 6 (exclusive) Node 0, Node 1
     * Segment 3: 6 -> infinity (exclusive) Node 0, Node 1
     *
     * Now a failed node, Node 2 is healed back which results in the following intermediary
     * states.
     *
     * First, last segment will be split.
     * Segment 1: 0 -> 3 (exclusive) Node 0
     * Segment 2: 3 -> 6 (exclusive) Node 0, Node 1
     * Segment 3: 6 -> 9 (exclusive) Node 0, Node 1
     * Segment 4: 9 -> infinity (exclusive) Node 0, Node 1, Node 2
     *
     * Then, healing carries out a cleaning task of merging the segments.
     * (transfer segment 1 to Node 1 and merge segments 1 and 2.)
     * Segment 1: 0 -> 6 (exclusive) Node 0, Node 1
     * Segment 2: 6 -> 9 (exclusive) Node 0, Node 1
     * Segment 3: 9 -> infinity (exclusive) Node 0, Node 1, Node 2
     *
     * And then:
     * Segment 1: 0 -> 9 (exclusive) Node 0, Node 1
     * Segment 2: 9 -> infinity (exclusive) Node 0, Node 1, Node 2
     *
     * At the end, the stable layout will be the following:
     * Segment 1: 0 -> infinity (exclusive) Node 0, Node 1, Node 2
     * Finally the stable layout is verified as well as the state transfer is verified by asserting
     * all 3 nodes' data.
     */
    @Test
    public void verifyStateTransferAndMergeInHeal() throws Exception {
        // Add three servers
        addServer(SERVERS.PORT_0);
        addServer(SERVERS.PORT_1);
        addServer(SERVERS.PORT_2);

        addServerRule(SERVERS.PORT_2, new TestRule().matches(
                msg -> !msg.getMsgType().equals(CorfuMsgType.LAYOUT_BOOTSTRAP)
                        && !msg.getMsgType().equals(CorfuMsgType.MANAGEMENT_BOOTSTRAP_REQUEST))
                .drop());

        final long writtenAddressesBatch1 = 3L;
        final long writtenAddressesBatch2 = 6L;
        Layout l1 = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(SERVERS.PORT_0)
                .addLayoutServer(SERVERS.PORT_1)
                .addLayoutServer(SERVERS.PORT_2)
                .addSequencer(SERVERS.PORT_0)
                .addSequencer(SERVERS.PORT_1)
                .addSequencer(SERVERS.PORT_2)
                .buildSegment()
                .setStart(0L)
                .setEnd(writtenAddressesBatch1)
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addToSegment()
                .addToLayout()
                .buildSegment()
                .setStart(writtenAddressesBatch1)
                .setEnd(writtenAddressesBatch2)
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addLogUnit(SERVERS.PORT_1)
                .addToSegment()
                .addToLayout()
                .buildSegment()
                .setStart(writtenAddressesBatch2)
                .setEnd(-1L)
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addLogUnit(SERVERS.PORT_1)
                .addToSegment()
                .addToLayout()
                .addUnresponsiveServer(SERVERS.PORT_2)
                .build();
        bootstrapAllServers(l1);

        CorfuRuntime rt = getNewRuntime(getDefaultNode()).connect();
        setAggressiveTimeouts(l1, rt,
                getManagementServer(SERVERS.PORT_0).getManagementAgent().getCorfuRuntime());
        setAggressiveDetectorTimeouts(SERVERS.PORT_0);
        IStreamView testStream = rt.getStreamsView().get(CorfuRuntime.getStreamID("test"));

        // Write to address spaces 0 to 2 (inclusive) to SERVER 0 only.
        testStream.append("testPayload".getBytes());
        testStream.append("testPayload".getBytes());
        testStream.append("testPayload".getBytes());

        // Write to address spaces 3 to 5 (inclusive) to SERVER 0 and SERVER 1.
        testStream.append("testPayload".getBytes());
        testStream.append("testPayload".getBytes());
        testStream.append("testPayload".getBytes());

        // Write to address spaces 6 to 9 (inclusive) to SERVER 0 and SERVER 1.
        testStream.append("testPayload".getBytes());
        testStream.append("testPayload".getBytes());
        testStream.append("testPayload".getBytes());

        // Allow node 2 to be healed.
        clearServerRules(SERVERS.PORT_2);

        rt.invalidateLayout();

        // Wait until a stable and merged layout is observed
        waitForLayoutChange(layout -> layout.getUnresponsiveServers().isEmpty() &&
                                      layout.segments.size() == 1,
                            rt);

        final String[] expectedNodes = new String[]{SERVERS.ENDPOINT_0,
                                                    SERVERS.ENDPOINT_1,
                                                    SERVERS.ENDPOINT_2};
        final Layout actualLayout = rt.getLayoutView().getLayout();

        // Verify Node 1, Node 2, and Node 3 are active without considering order
        assertThat(actualLayout.getAllActiveServers()).containsExactlyInAnyOrder(expectedNodes);

        // Verify segments are merged into one segment
        assertThat(actualLayout.getSegments().size()).isEqualTo(1);

        // Verify start and end of the segment
        assertThat(actualLayout.getSegments().get(0).start).isEqualTo(0);
        assertThat(actualLayout.getSegments().get(0).end).isEqualTo(-1);

        // Verify no unresponsive server are in the layout
        assertThat(actualLayout.getUnresponsiveServers()).isEmpty();

        // Verify layout servers in the layout and their order
        assertThat(actualLayout.getLayoutServers()).containsExactly(expectedNodes);

        // Verify sequencers in the layout and their order
        assertThat(actualLayout.getSequencers()).containsExactly(expectedNodes);

        final TokenResponse tokenResponse = rt.getSequencerView().query(CorfuRuntime.getStreamID(
                "test"));
        final long lastAddress = tokenResponse.getSequence();

        // Verify Nodes' data
        Map<Long, LogData> map_0 = getAllNonEmptyData(rt, SERVERS.ENDPOINT_0, lastAddress);
        Map<Long, LogData> map_2 = getAllNonEmptyData(rt, SERVERS.ENDPOINT_2, lastAddress);
        assertThat(map_2.entrySet()).containsOnlyElementsOf(map_0.entrySet());
    }

    /**
     * This test verifies that if the adjacent segments have same number of servers,
     * state transfer is not happened, while merge segments can succeed.
     *
     * The test first creates a layout with 3 segments.
     *
     * Segment 1: 0 -> 5 (exclusive) Node 0, Node 1
     * Segment 2: 5 -> 10 (exclusive) Node 0, Node 1
     * Segment 3: 10 -> infinity (exclusive) Node 0, Node 1
     *
     * Now drop all the read response from Node 1, so that we make sure state transfer
     * will fail if it happens.
     *
     * Finally verify merge segments succeed with only one segment in the new layout.
     */
    @Test
    public void verifyStateTransferNotHappenButMergeSucceeds() throws Exception {
        addServer(SERVERS.PORT_0);
        addServer(SERVERS.PORT_1);

        final long segmentSize = 5L;
        final int numSegments = 3;

        Layout layout = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(SERVERS.PORT_0)
                .addLayoutServer(SERVERS.PORT_1)
                .addSequencer(SERVERS.PORT_0)
                .addSequencer(SERVERS.PORT_1)
                .buildSegment()
                .setStart(0L)
                .setEnd(segmentSize)
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addLogUnit(SERVERS.PORT_1)
                .addToSegment()
                .addToLayout()
                .buildSegment()
                .setStart(segmentSize)
                .setEnd(segmentSize * 2)
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addLogUnit(SERVERS.PORT_1)
                .addToSegment()
                .addToLayout()
                .buildSegment()
                .setStart(segmentSize * 2)
                .setEnd(-1L)
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addLogUnit(SERVERS.PORT_1)
                .addToSegment()
                .addToLayout()
                .build();

        // Drop read responses to make sure state transfer will fail if it happens
        addServerRule(SERVERS.PORT_1, new TestRule().matches(m ->
                m.getMsgType().equals(CorfuMsgType.READ_RESPONSE)).drop());

        bootstrapAllServers(layout);

        CorfuRuntime rt = getNewRuntime(getDefaultNode()).connect();

        IStreamView testStream = rt.getStreamsView().get(CorfuRuntime.getStreamID("test"));
        for (int i = 0; i < segmentSize * numSegments; i++) {
            testStream.append("testPayload".getBytes());
        }

        // Verify that segments merged without state transfer
        waitForLayoutChange(l -> l.getSegments().size() == 1, rt);
    }

    /**
     * This test starts with a cluster of 3 at epoch 1 with PORT_0 as the primary sequencer.
     * Now runtime_1 writes 5 entries to streams A and B each.
     * The token count has increased from 0-9.
     *
     * The cluster now reconfigures to mark PORT_1 as the primary sequencer for epoch 2.
     * A stale client still observing epoch 1 requests 10 tokens from PORT_0.
     * However 2 tokens are requested from PORT_1.
     *
     * The cluster again reconfigures to mark PORT_0 as the primary sequencer for epoch 3.
     * The state of the new primary sequencer should now be recreated using the FastObjectLoader
     * and should NOT reflect the 10 invalid tokens requested by the stale client.
     */
    @Test
    public void regressTokenCountToValidDispatchedTokens() throws Exception {
        final UUID streamA = CorfuRuntime.getStreamID("streamA");
        final UUID streamB = CorfuRuntime.getStreamID("streamB");
        byte[] payload = "test_payload".getBytes();

        Layout layout_1 = new Layout(getManagementTestLayout());

        // In case any management agent is capable of detecting a failure (one node shutdown) before all
        // three nodes go down, we will drop all messages to prevent reporting failures which could move
        // the epoch, before the client actually moves it (leading to a wrongEpochException)
        TestRule mngtAgentDropAll = new TestRule().always().drop();
        addClientRule(getManagementServer(SERVERS.PORT_0).getManagementAgent().getCorfuRuntime(),
                mngtAgentDropAll);
        addClientRule(getManagementServer(SERVERS.PORT_1).getManagementAgent().getCorfuRuntime(),
                mngtAgentDropAll);
        addClientRule(getManagementServer(SERVERS.PORT_2).getManagementAgent().getCorfuRuntime(),
                mngtAgentDropAll);

        // Shut down management servers to prevent auto-reconfiguration.
        getManagementServer(SERVERS.PORT_0).shutdown();
        getManagementServer(SERVERS.PORT_1).shutdown();
        getManagementServer(SERVERS.PORT_2).shutdown();

        SequencerServer server0 = getSequencer(SERVERS.PORT_0);
        SequencerServer server1 = getSequencer(SERVERS.PORT_1);

        CorfuRuntime runtime_1 = getNewRuntime(getDefaultNode()).connect();
        CorfuRuntime runtime_2 = getNewRuntime(getDefaultNode()).connect();

        IStreamView streamViewA = runtime_1.getStreamsView().get(streamA);
        IStreamView streamViewB = runtime_1.getStreamsView().get(streamB);

        // Write 10 entries to the log using runtime_1.
        // Stream A 0-4
        streamViewA.append(payload);
        streamViewA.append(payload);
        streamViewA.append(payload);
        streamViewA.append(payload);
        streamViewA.append(payload);
        // Stream B 5-9
        streamViewB.append(payload);
        streamViewB.append(payload);
        streamViewB.append(payload);
        streamViewB.append(payload);
        streamViewB.append(payload);

        // Add a rule to drop Seal and Paxos messages on PORT_0 (the current primary sequencer).
        addClientRule(runtime_1, SERVERS.ENDPOINT_0, new TestRule().drop().always());
        // Trigger reconfiguration and failover the sequencer to PORT_1
        Layout layout_2 = new LayoutBuilder(layout_1)
                .assignResponsiveSequencerAsPrimary(Collections.singleton(SERVERS.ENDPOINT_0))
                .build();
        layout_2.setEpoch(layout_2.getEpoch() + 1);
        runtime_1.getLayoutView().getRuntimeLayout(layout_2).sealMinServerSet();
        runtime_1.getLayoutView().updateLayout(layout_2, 1L);
        runtime_1.getLayoutManagementView().reconfigureSequencerServers(layout_1, layout_2, false);
        waitForLayoutChange(layout -> layout.getEpoch() == layout_2.getEpoch(), runtime_1);

        clearClientRules(runtime_1);

        // Using the stale client with view of epoch 1, request 10 tokens.
        final int tokenCount = 5;
        for (int x = 0; x < tokenCount; x++) {
            runtime_2.getSequencerView().next(streamA);
            runtime_2.getSequencerView().next(streamB);
        }
        // Using the new client request 2 tokens and write to the log.
        streamViewA.append(payload);
        streamViewA.append(payload);

        final int expectedServer0Tokens = 20;
        final int expectedServer1Tokens = 12;
        assertThat(server0.getSequencerEpoch()).isEqualTo(layout_1.getEpoch());
        assertThat(server1.getSequencerEpoch()).isEqualTo(layout_2.getEpoch());
        assertThat(server0.getGlobalLogTail()).isEqualTo(expectedServer0Tokens);
        assertThat(server1.getGlobalLogTail()).isEqualTo(expectedServer1Tokens);

        // Trigger reconfiguration to failover back to PORT_0.
        Layout layout_3 = new LayoutBuilder(layout_2)
                .assignResponsiveSequencerAsPrimary(Collections.singleton(SERVERS.ENDPOINT_1))
                .build();
        layout_3.setEpoch(layout_3.getEpoch() + 1);
        runtime_1.getLayoutView().getRuntimeLayout(layout_3).sealMinServerSet();
        runtime_1.getLayoutView().updateLayout(layout_3, 1L);
        runtime_1.getLayoutManagementView().reconfigureSequencerServers(layout_2, layout_3, false);

        // Assert that the token count does not reflect the 10 tokens requested by the stale
        // client on PORT_0.
        assertThat(server0.getSequencerEpoch()).isEqualTo(layout_3.getEpoch());
        assertThat(server1.getSequencerEpoch()).isEqualTo(layout_2.getEpoch());
        assertThat(server0.getGlobalLogTail()).isEqualTo(expectedServer1Tokens);
        assertThat(server1.getGlobalLogTail()).isEqualTo(expectedServer1Tokens);

        // Assert that the streamTailMap has been reset and returns the correct backpointer.
        final long expectedBackpointerStreamA = 11;
        TokenResponse tokenResponse = runtime_1.getSequencerView().next(streamA);
        assertThat(tokenResponse.getBackpointerMap().get(streamA))
                .isEqualTo(expectedBackpointerStreamA);
    }

    /**
     * Starts a cluster with 3 nodes.
     * The epoch is then incremented and a layout proposed and accepted for the new epoch.
     * This leaves the sequencer un-bootstrapped causing token requests to hang.
     * The heartbeats should convey this primary sequencer NOT_READY state to the failure
     * detector which bootstraps the sequencer.
     */
    @Test
    public void handleUnBootstrappedSequencer() throws Exception {
        Layout layout = new Layout(getManagementTestLayout());
        final long highRank = 10L;
        // We increment the epoch and propose the same layout for the new epoch.
        // Due to the router and sequencer epoch mismatch, the sequencer becomes NOT_READY.
        // Note that this reconfiguration is not followed by the explicit sequencer bootstrap step.
        layout.setEpoch(layout.getEpoch() + 1);
        corfuRuntime.getLayoutView().getRuntimeLayout(layout).sealMinServerSet();
        // We increase to a higher rank to avoid being outranked. We could be outranked if the management
        // agent attempts to fill in the epoch slot before we update.
        corfuRuntime.getLayoutView().updateLayout(layout, highRank);
        // Assert that the primary sequencer is not ready.
        assertThatThrownBy(() -> corfuRuntime.getLayoutView().getRuntimeLayout()
                .getPrimarySequencerClient()
                .requestMetrics().get()).hasCauseInstanceOf(ServerNotReadyException.class);

        // Wait for the management service to detect and bootstrap the sequencer.
        corfuRuntime.getSequencerView().query();

        // Assert that the primary sequencer is bootstrapped.
        assertThat(corfuRuntime.getLayoutView().getRuntimeLayout().getPrimarySequencerClient()
                .requestMetrics().get().getSequencerStatus()).isEqualTo(SequencerStatus.READY);
    }

    /**
     * Tests the Cluster Status Query API.
     * The test starts with setting up a 3 node cluster:
     * Layout Servers = PORT_0, PORT_1, PORT_2.
     * Sequencer Servers = PORT_0, PORT_1, PORT_2.
     * LogUnit Servers = PORT_0, PORT_1, PORT_2.
     *
     * STEP 1: First status query:
     * All nodes up. Cluster status: STABLE.
     *
     * STEP 2: In this step the client is partitioned from the 2 nodes in the cluster.
     * The cluster however is healthy. Status query:
     * PORT_0 and PORT_1 are UNRESPONSIVE. Cluster status: STABLE
     * (Since the cluster is table it is just the client that cannot reach all healthy servers)
     *
     * A few entries are appended on a Stream. This data is written to PORT_0, PORT_1 and PORT_2.
     *
     * STEP 3: Client connections are restored from the previous step. PORT_0 is failed.
     * This causes sequencer failover. The cluster is still reachable. Status query:
     * PORT_0 is DOWN. Cluster status: DEGRADED.
     *
     * STEP 4: PORT_1 is failed. The cluster is non-operational now. Status query:
     * PORT_0 and PORT_1 UNRESPONSIVE, however the Cluster status: DEGRADED, as layout cannot
     * converge to a new layout (no consensus).
     *
     * STEP 5: All nodes are failed. The cluster is non-operational now. Status query:
     * PORT_0, PORT_1 and PORT_2 DOWN/UNRESPONSIVE. Cluster status: UNAVAILABLE.
     *
     */
    @Test
    public void queryClusterStatus() throws Exception {
        get3NodeLayout();
        getCorfuRuntime().getLayoutView().getLayout().getAllServers().forEach(endpoint ->
                getCorfuRuntime().getRouter(endpoint)
                        .setTimeoutResponse(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis()));

        // STEP 1.
        ClusterStatusReport clusterStatus = getCorfuRuntime().getManagementView().getClusterStatus();
        Map<String, ConnectivityStatus> nodeConnectivityMap = clusterStatus.getClientServerConnectivityStatusMap();
        Map<String, NodeStatus> nodeStatusMap = clusterStatus.getClusterNodeStatusMap();
        ClusterStatusReport.ClusterStatusReliability clusterStatusReliability = clusterStatus.getClusterStatusReliability();
        assertThat(nodeConnectivityMap.get(SERVERS.ENDPOINT_0)).isEqualTo(ConnectivityStatus.RESPONSIVE);
        assertThat(nodeConnectivityMap.get(SERVERS.ENDPOINT_1)).isEqualTo(ConnectivityStatus.RESPONSIVE);
        assertThat(nodeConnectivityMap.get(SERVERS.ENDPOINT_2)).isEqualTo(ConnectivityStatus.RESPONSIVE);
        assertThat(nodeStatusMap.get(SERVERS.ENDPOINT_0)).isEqualTo(NodeStatus.UP);
        assertThat(nodeStatusMap.get(SERVERS.ENDPOINT_1)).isEqualTo(NodeStatus.UP);
        assertThat(nodeStatusMap.get(SERVERS.ENDPOINT_2)).isEqualTo(NodeStatus.UP);
        assertThat(clusterStatusReliability).isEqualTo(ClusterStatusReport.ClusterStatusReliability.STRONG_QUORUM);
        assertThat(clusterStatus.getClusterStatus()).isEqualTo(ClusterStatus.STABLE);

        // STEP 2.
        // Because we are explicitly dropping PING messages only (which are used to verify
        // client's connectivity) on ENDPOINT_0 and ENDPOINT_1, this test will show both nodes
        // unresponsive, despite of their actual node status being UP.
        TestRule rule = new TestRule()
                .matches(corfuMsg -> corfuMsg.getMsgType().equals(CorfuMsgType.PING))
                .drop();
        addClientRule(getCorfuRuntime(), SERVERS.ENDPOINT_0, rule);
        addClientRule(getCorfuRuntime(), SERVERS.ENDPOINT_1, rule);
        clusterStatus = getCorfuRuntime().getManagementView().getClusterStatus();
        nodeConnectivityMap = clusterStatus.getClientServerConnectivityStatusMap();
        nodeStatusMap = clusterStatus.getClusterNodeStatusMap();
        clusterStatusReliability = clusterStatus.getClusterStatusReliability();
        assertThat(nodeConnectivityMap.get(SERVERS.ENDPOINT_0)).isEqualTo(ConnectivityStatus.UNRESPONSIVE);
        assertThat(nodeConnectivityMap.get(SERVERS.ENDPOINT_1)).isEqualTo(ConnectivityStatus.UNRESPONSIVE);
        assertThat(nodeConnectivityMap.get(SERVERS.ENDPOINT_2)).isEqualTo(ConnectivityStatus.RESPONSIVE);
        assertThat(nodeStatusMap.get(SERVERS.ENDPOINT_0)).isEqualTo(NodeStatus.UP);
        assertThat(nodeStatusMap.get(SERVERS.ENDPOINT_1)).isEqualTo(NodeStatus.UP);
        assertThat(nodeStatusMap.get(SERVERS.ENDPOINT_2)).isEqualTo(NodeStatus.UP);
        assertThat(clusterStatusReliability).isEqualTo(ClusterStatusReport.ClusterStatusReliability.STRONG_QUORUM);
        assertThat(clusterStatus.getClusterStatus()).isEqualTo(ClusterStatus.STABLE);

        // Write 10 entries. 0-9.
        IStreamView streamView = getCorfuRuntime().getStreamsView()
                .get(CorfuRuntime.getStreamID("testStream"));
        final int entriesCount = 10;
        final byte[] payload = "payload".getBytes();
        for (int i = 0; i < entriesCount; i++) {
            streamView.append(payload);
        }

        // STEP 3.
        clearClientRules(getCorfuRuntime());
        addServerRule(SERVERS.PORT_0, new TestRule().drop().always());
        waitForLayoutChange(layout -> layout.getUnresponsiveServers().size() == 1
                        && layout.getUnresponsiveServers().contains(SERVERS.ENDPOINT_0)
                        && layout.getSegments().size() == 1,
                getCorfuRuntime());
        clusterStatus = getCorfuRuntime().getManagementView().getClusterStatus();
        nodeConnectivityMap = clusterStatus.getClientServerConnectivityStatusMap();
        nodeStatusMap = clusterStatus.getClusterNodeStatusMap();
        clusterStatusReliability = clusterStatus.getClusterStatusReliability();
        assertThat(nodeConnectivityMap.get(SERVERS.ENDPOINT_0)).isEqualTo(ConnectivityStatus.UNRESPONSIVE);
        assertThat(nodeConnectivityMap.get(SERVERS.ENDPOINT_1)).isEqualTo(ConnectivityStatus.RESPONSIVE);
        assertThat(nodeConnectivityMap.get(SERVERS.ENDPOINT_2)).isEqualTo(ConnectivityStatus.RESPONSIVE);
        assertThat(nodeStatusMap.get(SERVERS.ENDPOINT_0)).isEqualTo(NodeStatus.DOWN);
        assertThat(nodeStatusMap.get(SERVERS.ENDPOINT_1)).isEqualTo(NodeStatus.UP);
        assertThat(nodeStatusMap.get(SERVERS.ENDPOINT_2)).isEqualTo(NodeStatus.UP);
        assertThat(clusterStatusReliability).isEqualTo(ClusterStatusReport.ClusterStatusReliability.STRONG_QUORUM);
        assertThat(clusterStatus.getClusterStatus()).isEqualTo(ClusterStatus.DEGRADED);

        // STEP 4.
        // Since there will be no epoch change as majority of servers are down, we cannot obtain
        // a reliable state of the cluster and report unavailable, still responsiveness to each node
        // in the cluster is reported.
        Semaphore latch1 = new Semaphore(1);
        latch1.acquire();
        addClientRule(getManagementServer(SERVERS.PORT_2).getManagementAgent().getCorfuRuntime(),
                new TestRule().matches(corfuMsg -> {
                    if (corfuMsg.getMsgType().equals(CorfuMsgType.MANAGEMENT_FAILURE_DETECTED)) {
                        latch1.release();
                        return true;
                    }
                    return false;
                }).drop());
        addServerRule(SERVERS.PORT_1, new TestRule().drop().always());
        addClientRule(getManagementServer(SERVERS.PORT_1).getManagementAgent().getCorfuRuntime(),
                new TestRule().always().drop());
        assertThat(latch1.tryAcquire(PARAMETERS.TIMEOUT_LONG.toMillis(), TimeUnit.MILLISECONDS))
                .isTrue();
        clusterStatus = getCorfuRuntime().getManagementView().getClusterStatus();
        nodeConnectivityMap = clusterStatus.getClientServerConnectivityStatusMap();
        nodeStatusMap = clusterStatus.getClusterNodeStatusMap();
        clusterStatusReliability = clusterStatus.getClusterStatusReliability();
        assertThat(nodeConnectivityMap.get(SERVERS.ENDPOINT_0)).isEqualTo(ConnectivityStatus.UNRESPONSIVE);
        assertThat(nodeConnectivityMap.get(SERVERS.ENDPOINT_1)).isEqualTo(ConnectivityStatus.UNRESPONSIVE);
        assertThat(nodeConnectivityMap.get(SERVERS.ENDPOINT_2)).isEqualTo(ConnectivityStatus.RESPONSIVE);
        assertThat(nodeStatusMap.get(SERVERS.ENDPOINT_0)).isEqualTo(NodeStatus.NA);
        assertThat(nodeStatusMap.get(SERVERS.ENDPOINT_1)).isEqualTo(NodeStatus.NA);
        assertThat(nodeStatusMap.get(SERVERS.ENDPOINT_2)).isEqualTo(NodeStatus.NA);
        assertThat(clusterStatusReliability).isEqualTo(ClusterStatusReport.ClusterStatusReliability.WEAK_NO_QUORUM);
        assertThat(clusterStatus.getClusterStatus()).isEqualTo(ClusterStatus.UNAVAILABLE);


        // STEP 5.
        clearClientRules(getCorfuRuntime());
        addServerRule(SERVERS.PORT_2, new TestRule().drop().always());
        clusterStatus = getCorfuRuntime().getManagementView().getClusterStatus();
        nodeConnectivityMap = clusterStatus.getClientServerConnectivityStatusMap();
        nodeStatusMap = clusterStatus.getClusterNodeStatusMap();
        clusterStatusReliability = clusterStatus.getClusterStatusReliability();
        assertThat(nodeConnectivityMap.get(SERVERS.ENDPOINT_0)).isEqualTo(ConnectivityStatus.UNRESPONSIVE);
        assertThat(nodeConnectivityMap.get(SERVERS.ENDPOINT_1)).isEqualTo(ConnectivityStatus.UNRESPONSIVE);
        assertThat(nodeConnectivityMap.get(SERVERS.ENDPOINT_2)).isEqualTo(ConnectivityStatus.UNRESPONSIVE);
        assertThat(nodeStatusMap.get(SERVERS.ENDPOINT_0)).isEqualTo(NodeStatus.NA);
        assertThat(nodeStatusMap.get(SERVERS.ENDPOINT_1)).isEqualTo(NodeStatus.NA);
        assertThat(nodeStatusMap.get(SERVERS.ENDPOINT_2)).isEqualTo(NodeStatus.NA);
        assertThat(clusterStatusReliability).isEqualTo(ClusterStatusReport.ClusterStatusReliability.UNAVAILABLE);
        assertThat(clusterStatus.getClusterStatus()).isEqualTo(ClusterStatus.UNAVAILABLE);
    }

    /**
     * Tests that if the cluster gets stuck in a live-lock the systemDownHandler is invoked.
     * Scenario: Cluster of 2 nodes - Nodes 0 and 1
     * Some data (10 appends) is written into the cluster.
     * Then rules are added on both the nodes' management agents so that they cannot reconfigure
     * the system. Another rule is added to the tail of the chain to drop all READ_RESPONSES.
     * The epoch is incremented and the new layout is pushed to both the nodes.
     * NOTE: The sequencer is not bootstrapped for the new epoch.
     * Now, both the management agents attempt to bootstrap the new sequencer but the
     * FastObjectLoaders should stall due to the READ_RESPONSE drop rule.
     * This triggers the systemDownHandler.
     */
    @Test
    public void triggerSystemDownHandlerInDeadlock() throws Exception {
        // Cluster Setup.
        addServer(SERVERS.PORT_0);
        addServer(SERVERS.PORT_1);

        Layout layout = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(SERVERS.PORT_0)
                .addLayoutServer(SERVERS.PORT_1)
                .addSequencer(SERVERS.PORT_0)
                .addSequencer(SERVERS.PORT_1)
                .buildSegment()
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addLogUnit(SERVERS.PORT_1)
                .addToSegment()
                .addToLayout()
                .setClusterId(UUID.randomUUID())
                .build();
        bootstrapAllServers(layout);
        corfuRuntime = getRuntime(layout).connect();

        CorfuRuntime managementRuntime0 = getManagementServer(SERVERS.PORT_0)
                .getManagementAgent().getCorfuRuntime();
        CorfuRuntime managementRuntime1 = getManagementServer(SERVERS.PORT_1)
                .getManagementAgent().getCorfuRuntime();

        waitForSequencerToBootstrap(SERVERS.PORT_0);

        // Setting aggressive timeouts
        setAggressiveTimeouts(layout, corfuRuntime, managementRuntime0, managementRuntime1);
        setAggressiveDetectorTimeouts(SERVERS.PORT_0, SERVERS.PORT_1);

        // Append data.
        IStreamView streamView = corfuRuntime.getStreamsView()
                .get(CorfuRuntime.getStreamID("testStream"));
        final byte[] payload = "test".getBytes();
        final int num = 10;
        for (int i = 0; i < num; i++) {
            streamView.append(payload);
        }

        // Register custom systemDownHandler to detect live-lock.
        final Semaphore semaphore = new Semaphore(2);
        semaphore.acquire(2);

        final int sysDownTriggerLimit = 3;
        managementRuntime0.getParameters().setSystemDownHandlerTriggerLimit(sysDownTriggerLimit);
        managementRuntime1.getParameters().setSystemDownHandlerTriggerLimit(sysDownTriggerLimit);

        TestRule testRule = new TestRule()
                .matches(m -> m.getMsgType().equals(CorfuMsgType.SET_EPOCH))
                .drop();
        addClientRule(managementRuntime0, testRule);
        addClientRule(managementRuntime1, testRule);

        // Since the fast loader will retrieve the tails from the head node,
        // we need to drop all tail requests to hang the FastObjectLoaders
        addServerRule(SERVERS.PORT_0, new TestRule().matches(m -> {
            if (m.getMsgType().equals(CorfuMsgType.LOG_ADDRESS_SPACE_RESPONSE)) {
                semaphore.release();
                return true;
            }
            return false;
        }).drop());

        // Trigger an epoch change to trigger FastObjectLoader to run for sequencer bootstrap.
        Layout layout1 = new Layout(layout);
        layout1.setEpoch(layout1.getEpoch() + 1);
        corfuRuntime.getLayoutView().getRuntimeLayout(layout1).sealMinServerSet();
        corfuRuntime.getLayoutView().updateLayout(layout1, 1L);

        assertThat(semaphore
                .tryAcquire(2, PARAMETERS.TIMEOUT_LONG.toMillis(), TimeUnit.MILLISECONDS))
                .isTrue();

        // Create a fault - Epoch instability by just sealing the cluster but not filling the
        // layout slot.
        corfuRuntime.invalidateLayout();
        Layout layout2 = new Layout(corfuRuntime.getLayoutView().getLayout());
        layout2.setEpoch(layout2.getEpoch() + 1);
        corfuRuntime.getLayoutView().getRuntimeLayout(layout2).sealMinServerSet();


        clearClientRules(managementRuntime0);
        clearClientRules(managementRuntime1);

        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_MODERATE; i++) {
            if (corfuRuntime.getLayoutView().getLayout().getEpoch() == layout2.getEpoch()) {
                break;
            }
            corfuRuntime.invalidateLayout();
            Sleep.sleepUninterruptibly(PARAMETERS.TIMEOUT_SHORT);
        }
        // Assert that the DetectionWorker threads are freed from the deadlock and are able to fill
        // up the layout slot and stabilize the cluster.
        assertThat(corfuRuntime.getLayoutView().getLayout().getEpoch())
                .isEqualTo(layout2.getEpoch());

        clearServerRules(SERVERS.PORT_0);
        // Once the rules are cleared, the detectors should resolve the epoch instability,
        // bootstrap the sequencer and fetch a new token.
        assertThat(corfuRuntime.getSequencerView().query()).isNotNull();
    }

    /**
     * Tests the triggerSequencerReconfiguration method. The READ_RESPONSE messages are blocked by
     * adding a rule to drop these. The reconfiguration task unblocks with the help of the
     * systemDownHandler.
     */
    @Test
    public void unblockSequencerRecoveryOnDeadlock() throws Exception {
        CorfuRuntime corfuRuntime = getDefaultRuntime();
        final Layout layout = corfuRuntime.getLayoutView().getLayout();
        // Setting aggressive timeouts
        setAggressiveTimeouts(layout, corfuRuntime,
                getManagementServer(SERVERS.PORT_0).getManagementAgent().getCorfuRuntime());
        setAggressiveDetectorTimeouts(SERVERS.PORT_0);

        final int sysDownTriggerLimit = 3;
        getManagementServer(SERVERS.PORT_0).getManagementAgent().getCorfuRuntime().getParameters()
                .setSystemDownHandlerTriggerLimit(sysDownTriggerLimit);

        // Add rule to drop all read responses to hang the FastObjectLoaders.
        addServerRule(SERVERS.PORT_0, new TestRule().matches(m -> m.getMsgType()
                .equals(CorfuMsgType.READ_RESPONSE)).drop());

        getManagementServer(SERVERS.PORT_0).getManagementAgent()
                .getCorfuRuntime().getLayoutManagementView()
                .asyncSequencerBootstrap(layout,
                        getManagementServer(SERVERS.PORT_0).getManagementAgent()
                                .getRemoteMonitoringService().getFailureDetectorWorker())
                .get();
    }

    /**
     * Tests that a degraded cluster heals a sealed cluster.
     * NOTE: A sealed cluster without a layout causes the system to halt as none of the clients can
     * perform data operations until the new epoch is filled in with a layout.
     * Scenario: 3 nodes - PORT_0, PORT_1 and PORT_2.
     * A server rule is added to simulate PORT_2 as unresponsive.
     * First, the degraded cluster moves from epoch 1 to epoch 2 to mark PORT_2 unresponsive.
     * Now, PORT_0 and PORT_1 are sealed to epoch 3.
     * The fault detectors detect this and fills the epoch 3 with a layout.
     */
    @Test
    public void testSealedDegradedClusterHealing() {
        get3NodeLayout();
        CorfuRuntime corfuRuntime = getDefaultRuntime();

        addServerRule(SERVERS.PORT_2, new TestRule().always().drop());

        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_MODERATE; i++) {
            corfuRuntime.invalidateLayout();
            if (!corfuRuntime.getLayoutView().getLayout().getUnresponsiveServers().isEmpty()) {
                break;
            }
            Sleep.sleepUninterruptibly(PARAMETERS.TIMEOUT_VERY_SHORT);
        }

        Layout layout = new Layout(corfuRuntime.getLayoutView().getLayout());
        layout.setEpoch(layout.getEpoch() + 1);
        corfuRuntime.getLayoutView().getRuntimeLayout(layout).sealMinServerSet();

        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_MODERATE; i++) {
            corfuRuntime.invalidateLayout();
            if (corfuRuntime.getLayoutView().getLayout().getEpoch() == layout.getEpoch()) {
                break;
            }
            Sleep.sleepUninterruptibly(PARAMETERS.TIMEOUT_VERY_SHORT);
        }

        assertThat(corfuRuntime.getLayoutView().getLayout()).isEqualTo(layout);
    }

    /**
     * Write a random entry to the specified CorfuTable.
     *
     * @param table CorfuTable to populate.
     */
    private void writeRandomEntryToTable(CorfuTable table) {
        Random r = new Random();
        corfuRuntime.getObjectsView().TXBegin();
        table.put(r.nextInt(), r.nextInt());
        corfuRuntime.getObjectsView().TXEnd();
    }

    /**
     * Increment the cluster layout epoch by 1.
     *
     * @return New committed layout
     * @throws OutrankedException If layout proposal is outranked.
     */
    private Layout incrementClusterEpoch() throws OutrankedException {
        corfuRuntime.invalidateLayout();
        Layout layout = new Layout(corfuRuntime.getLayoutView().getLayout());
        layout.setEpoch(layout.getEpoch() + 1);
        corfuRuntime.getLayoutView().getRuntimeLayout(layout).sealMinServerSet();
        corfuRuntime.getLayoutView().updateLayout(layout, 1L);
        return layout;
    }

    /**
     * Test scenario where the sequencer bootstrap triggers cache cleanup causing maxConflictWildcard to be reset.
     * The runtime requests for 2 tokens but persists only 1 log entry. On an epoch change, the failover sequencer
     * (in this case, itself) is bootstrapped by running the fastObjectLoader.
     * This bootstrap sets the token to 1 and maxConflictWildcard to 0. This test asserts that the maxConflictWildcard
     * stays 0 even after the cache eviction and does not abort transactions with SEQUENCER_OVERFLOW cause.
     */
    @Test
    public void testSequencerCacheOverflowOnFailover() throws Exception {
        corfuRuntime = getDefaultRuntime();

        CorfuTable<String, String> table = corfuRuntime.getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {
                })
                .setStreamName("test")
                .open();

        writeRandomEntryToTable(table);
        // Block the writes so that we only fetch a sequencer token but not persist the entry on the LogUnit.
        addClientRule(corfuRuntime, new TestRule().matches(corfuMsg -> corfuMsg.getMsgType() == CorfuMsgType.WRITE)
                .drop());
        CompletableFuture<Boolean> future = CompletableFuture.supplyAsync(() -> {
            writeRandomEntryToTable(table);
            return true;
        });
        // Block any sequencer bootstrap attempts.
        addClientRule(getManagementServer(SERVERS.PORT_0).getManagementAgent().getCorfuRuntime(), new TestRule()
                .matches(corfuMsg -> corfuMsg.getMsgType() == CorfuMsgType.BOOTSTRAP_SEQUENCER).drop());

        // Increment the sequencer epoch twice so that a full sequencer bootstrap is required.
        incrementClusterEpoch();
        Layout layout = incrementClusterEpoch();

        // Clear rules to now allow sequencer bootstrap.
        clearClientRules(getManagementServer(SERVERS.PORT_0).getManagementAgent().getCorfuRuntime());
        while (getSequencer(SERVERS.PORT_0).getSequencerEpoch() != layout.getEpoch()) {
            Sleep.sleepUninterruptibly(PARAMETERS.TIMEOUT_VERY_SHORT);
        }
        clearClientRules(corfuRuntime);

        // Attempt data operation.
        // The data operation should fail if the maxConflictWildcard is updated on cache invalidation causing the
        // the value to change.
        writeRandomEntryToTable(table);

        future.cancel(true);
    }

    /**
     * Tests the partial bootstrap scenario while adding a new node.
     * Starts with cluster of 1 node: PORT_0.
     * We then bootstrap the layout server of PORT_1 with a layout.
     * Then the bootstrapNewNode is triggered. This should detect the same layout and complete the bootstrap.
     */
    @Test
    public void testPartialBootstrapNodeSuccess() throws ExecutionException, InterruptedException {
        corfuRuntime = getDefaultRuntime();
        addServer(SERVERS.PORT_1);
        Layout layout = corfuRuntime.getLayoutView().getLayout();

        // Bootstrap the layout server.
        corfuRuntime.getLayoutView().getRuntimeLayout().getLayoutClient(SERVERS.ENDPOINT_1).bootstrapLayout(layout);
        // Attempt bootstrapping the node. The node should attempt bootstrapping both the components Layout Server and
        // Management Server.
        assertThat(corfuRuntime.getLayoutManagementView().bootstrapNewNode(SERVERS.ENDPOINT_1).get()).isTrue();
    }

    /**
     * Tests the partial bootstrap scenario while adding a new node.
     * Starts with cluster of 1 node: PORT_0.
     * We then bootstrap the layout server of PORT_1 with a layout.
     * A rule is added to prevent the management server from being bootstrapped. Then the bootstrapNewNode is
     * triggered. This should fail and throw a timeout exception.
     */
    @Test
    public void testPartialBootstrapNodeFailure() {
        corfuRuntime = getDefaultRuntime();
        addServer(SERVERS.PORT_1);
        Layout layout = corfuRuntime.getLayoutView().getLayout();

        // Bootstrap the layout server.
        corfuRuntime.getLayoutView().getRuntimeLayout().getLayoutClient(SERVERS.ENDPOINT_1).bootstrapLayout(layout);

        addClientRule(corfuRuntime, new TestRule().matches(corfuMsg ->
                corfuMsg.getMsgType().equals(CorfuMsgType.MANAGEMENT_BOOTSTRAP_REQUEST)).drop());

        // Attempt bootstrapping the node. The node should attempt bootstrapping both the components Layout Server and
        // Management Server.
        assertThatThrownBy(corfuRuntime.getLayoutManagementView().bootstrapNewNode(SERVERS.ENDPOINT_1)::get)
                .hasRootCauseInstanceOf(TimeoutException.class);
    }
}
