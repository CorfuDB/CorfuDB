package org.corfudb.runtime.view;


import com.google.common.reflect.TypeToken;

import lombok.Getter;

import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.ServerContextBuilder;
import org.corfudb.infrastructure.TestLayoutBuilder;
import org.corfudb.infrastructure.TestServerRouter;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.ManagementClient;
import org.corfudb.runtime.clients.SequencerClient;
import org.corfudb.runtime.clients.TestRule;
import org.corfudb.runtime.collections.ISMRMap;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.view.stream.IStreamView;
import org.corfudb.util.CFUtils;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

/**
 * Test to verify the Management Server functionality.
 * <p>
 * Created by zlokhandwala on 11/9/16.
 */
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

        CorfuRuntime corfuRuntime = new CorfuRuntime();
        l.getLayoutServers().forEach(corfuRuntime::addLayoutServer);
        corfuRuntime.connect();
        corfuRuntime.getRouter(SERVERS.ENDPOINT_1).getClient(ManagementClient.class).initiateFailureHandler().get();


        // Set aggressive timeouts.
        setAggressiveTimeouts(l, corfuRuntime,
                getManagementServer(SERVERS.PORT_0).getCorfuRuntime(),
                getManagementServer(SERVERS.PORT_1).getCorfuRuntime());

        failureDetected.acquire();

        // Adding a rule on SERVERS.PORT_0 to drop all packets
        addServerRule(SERVERS.PORT_0, new TestRule().always().drop());
        getManagementServer(SERVERS.PORT_0).shutdown();

        // Adding a rule on SERVERS.PORT_1 to toggle the flag when it sends the
        // MANAGEMENT_FAILURE_DETECTED message.
        addClientRule(getManagementServer(SERVERS.PORT_1).getCorfuRuntime(),
                new TestRule().matches(corfuMsg -> {
                    if (corfuMsg.getMsgType().equals(CorfuMsgType
                            .MANAGEMENT_FAILURE_DETECTED)) {
                        failureDetected.release();
                    }
                    return true;
                }));

        assertThat(failureDetected.tryAcquire(PARAMETERS.TIMEOUT_NORMAL
                        .toNanos(),
                TimeUnit.NANOSECONDS)).isEqualTo(true);
    }

    /**
     * Scenario with 3 nodes: SERVERS.PORT_0, SERVERS.PORT_1 and SERVERS.PORT_2.
     * We fail SERVERS.PORT_1 and then wait for one of the other two servers to
     * handle this failure, propose a new layout and we assert on the epoch change.
     * The failure is handled by removing the failed node.
     *
     * @throws Exception
     */
    @Test
    public void removeSingleNodeFailure()
            throws Exception {

        // Creating server contexts with PurgeFailurePolicies.
        ServerContext sc0 = new ServerContextBuilder().setSingle(false).setServerRouter(new TestServerRouter(SERVERS.PORT_0)).setPort(SERVERS.PORT_0).build();
        ServerContext sc1 = new ServerContextBuilder().setSingle(false).setServerRouter(new TestServerRouter(SERVERS.PORT_1)).setPort(SERVERS.PORT_1).build();
        ServerContext sc2 = new ServerContextBuilder().setSingle(false).setServerRouter(new TestServerRouter(SERVERS.PORT_2)).setPort(SERVERS.PORT_2).build();
        sc0.setFailureHandlerPolicy(new PurgeFailurePolicy());
        sc1.setFailureHandlerPolicy(new PurgeFailurePolicy());
        sc2.setFailureHandlerPolicy(new PurgeFailurePolicy());

        addServer(SERVERS.PORT_0, sc0);
        addServer(SERVERS.PORT_1, sc1);
        addServer(SERVERS.PORT_2, sc2);

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

        CorfuRuntime corfuRuntime = new CorfuRuntime();
        l.getLayoutServers().forEach(corfuRuntime::addLayoutServer);
        corfuRuntime.connect();
        // Initiating all failure handlers.
        for (String server : l.getAllServers()) {
            corfuRuntime.getRouter(server).getClient(ManagementClient.class).initiateFailureHandler().get();
        }

        // Setting aggressive timeouts
        setAggressiveTimeouts(l, corfuRuntime,
                getManagementServer(SERVERS.PORT_0).getCorfuRuntime(),
                getManagementServer(SERVERS.PORT_1).getCorfuRuntime(),
                getManagementServer(SERVERS.PORT_2).getCorfuRuntime());

        // Adding a rule on SERVERS.PORT_1 to drop all packets
        addServerRule(SERVERS.PORT_1, new TestRule().always().drop());
        getManagementServer(SERVERS.PORT_1).shutdown();

        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_LOW; i++) {
            corfuRuntime.invalidateLayout();
            if (corfuRuntime.getLayoutView().getLayout().getEpoch() == 2L) {
                break;
            }
            Thread.sleep(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());
        }
        Layout l2 = corfuRuntime.getLayoutView().getLayout();
        assertThat(l2.getEpoch()).isEqualTo(2L);
        assertThat(l2.getLayoutServers().size()).isEqualTo(2);
        assertThat(l2.getLayoutServers().contains(SERVERS.ENDPOINT_1)).isFalse();
    }

    protected Layout getManagementTestLayout()
            throws Exception {
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
        l.setRuntime(corfuRuntime);

        // Initiating all failure handlers.
        for (String server : corfuRuntime.getLayoutView().getLayout().getAllServers()) {
            corfuRuntime.getRouter(server).getClient(ManagementClient.class).initiateFailureHandler().get();
        }

        // Waiting for management servers to send the bootstrap sequencer request and be ready
        // to detect failures to speed up test time.
        CFUtils.within(
                CompletableFuture.allOf(
                        getManagementServer(SERVERS.PORT_0).getSequencerBootstrappedFuture(),
                        getManagementServer(SERVERS.PORT_1).getSequencerBootstrappedFuture(),
                        getManagementServer(SERVERS.PORT_2).getSequencerBootstrappedFuture()),
                PARAMETERS.TIMEOUT_NORMAL
        ).join();

        // Setting aggressive timeouts
        setAggressiveTimeouts(l, corfuRuntime,
                getManagementServer(SERVERS.PORT_0).getCorfuRuntime(),
                getManagementServer(SERVERS.PORT_1).getCorfuRuntime(),
                getManagementServer(SERVERS.PORT_2).getCorfuRuntime());

        return l;
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
    public void handleTransientFailure()
            throws Exception {
        // Boolean flag turned to true when the MANAGEMENT_FAILURE_DETECTED message
        // is sent by the Management client to its server.
        final Semaphore failureDetected = new Semaphore(2, true);

        getManagementTestLayout();

        failureDetected.acquire(2);

        // Only allow SERVERS.PORT_0 to manage failures.
        getManagementServer(SERVERS.PORT_1).shutdown();
        getManagementServer(SERVERS.PORT_2).shutdown();

        // PART 1.
        // Prevent ENDPOINT_1 from sealing.
        addClientRule(getManagementServer(SERVERS.PORT_0).getCorfuRuntime(), SERVERS.ENDPOINT_1,
                new TestRule()
                        .matches(corfuMsg -> corfuMsg.getMsgType().equals(CorfuMsgType.SET_EPOCH))
                        .drop());
        // Simulate ENDPOINT_2 failure from ENDPOINT_0 (only Management Server)
        addClientRule(getManagementServer(SERVERS.PORT_0).getCorfuRuntime(), SERVERS.ENDPOINT_2,
                new TestRule().matches(corfuMsg -> true).drop());

        // Adding a rule on SERVERS.PORT_1 to toggle the flag when it sends the
        // MANAGEMENT_FAILURE_DETECTED message.
        addClientRule(getManagementServer(SERVERS.PORT_0).getCorfuRuntime(),
                new TestRule().matches(corfuMsg -> {
                    if (corfuMsg.getMsgType().equals(CorfuMsgType.MANAGEMENT_FAILURE_DETECTED)) {
                        failureDetected.release();
                    }
                    return true;
                }));

        // Go ahead when sealing of ENDPOINT_0 takes place.
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_LOW; i++) {
            if (getServerRouter(SERVERS.PORT_0).getServerEpoch() == 2L) {
                failureDetected.release();
                break;
            }
            Thread.sleep(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());
        }

        assertThat(failureDetected.tryAcquire(2, PARAMETERS.TIMEOUT_NORMAL.toNanos(),
                TimeUnit.NANOSECONDS)).isEqualTo(true);

        addClientRule(getManagementServer(SERVERS.PORT_0).getCorfuRuntime(),
                new TestRule().matches(corfuMsg -> corfuMsg.getMsgType().equals(CorfuMsgType.MANAGEMENT_FAILURE_DETECTED)
                ).drop());

        // Assert that only a partial seal was successful.
        // ENDPOINT_0 sealed. ENDPOINT_1 & ENDPOINT_2 not sealed.
        assertThat(getServerRouter(SERVERS.PORT_0).getServerEpoch()).isEqualTo(2L);
        assertThat(getServerRouter(SERVERS.PORT_1).getServerEpoch()).isEqualTo(1L);
        assertThat(getServerRouter(SERVERS.PORT_2).getServerEpoch()).isEqualTo(1L);
        assertThat(getLayoutServer(SERVERS.PORT_0).getCurrentLayout().getEpoch()).isEqualTo(1L);
        assertThat(getLayoutServer(SERVERS.PORT_1).getCurrentLayout().getEpoch()).isEqualTo(1L);
        assertThat(getLayoutServer(SERVERS.PORT_2).getCurrentLayout().getEpoch()).isEqualTo(1L);

        // PART 2.
        // Simulate normal operations for all servers and clients.
        clearClientRules(getManagementServer(SERVERS.PORT_0).getCorfuRuntime());

        // PART 3.
        // Allow management server to detect partial seal and correct this issue.
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_MODERATE; i++) {
            Thread.sleep(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());
            // Assert successful seal of all servers.
            if (getServerRouter(SERVERS.PORT_0).getServerEpoch() == 2L &&
                    getServerRouter(SERVERS.PORT_1).getServerEpoch() == 2L &&
                    getServerRouter(SERVERS.PORT_2).getServerEpoch() == 2L &&
                    getLayoutServer(SERVERS.PORT_0).getCurrentLayout().getEpoch() == 2L &&
                    getLayoutServer(SERVERS.PORT_1).getCurrentLayout().getEpoch() == 2L &&
                    getLayoutServer(SERVERS.PORT_2).getCurrentLayout().getEpoch() == 2L) {
                return;
            }
        }
        fail();

    }

    private void induceSequencerFailureAndWait() throws Exception {

        long currentEpoch = getCorfuRuntime().getLayoutView().getLayout().getEpoch();

        // induce a failure to the server on PORT_0, where the current sequencer is active
        //
        getManagementServer(SERVERS.PORT_0).shutdown();
        addServerRule(SERVERS.PORT_0, new TestRule().always().drop());

        // wait for failover to install a new epoch (and a new layout)
        //
        while (getCorfuRuntime().getLayoutView().getLayout().getEpoch() == currentEpoch) {
            getCorfuRuntime().invalidateLayout();
            Thread.sleep(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());
        }

    }

    /**
     * Scenario with 3 nodes: SERVERS.PORT_0, SERVERS.PORT_1 and SERVERS.PORT_2.
     * We fail SERVERS.PORT_1 and then wait for one of the other two servers to
     * handle this failure, propose a new layout and we assert on the epoch change.
     * The failure is handled by ConserveFailureHandlerPolicy.
     * No nodes are removed from the layout, but are marked unresponsive.
     * A sequencer failover takes place where the next working sequencer is reset
     * and made the primary.
     *
     * @throws Exception
     */
    @Test
    public void testSequencerFailover() throws Exception {
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

        assertThat(getSequencer(SERVERS.PORT_0).getGlobalLogTail().get()).isEqualTo(beforeFailure);
        assertThat(getSequencer(SERVERS.PORT_1).getGlobalLogTail().get()).isEqualTo(0L);

        induceSequencerFailureAndWait();

        // Block until new sequencer reaches READY state.
        getCorfuRuntime().getSequencerView().nextToken(
                Collections.singleton(CorfuRuntime.getStreamID("streamA")),
                0);
        // verify that a failover sequencer was started with the correct starting-tail
        //
        assertThat(getSequencer(SERVERS.PORT_1).getGlobalLogTail().get()).isEqualTo(beforeFailure);

        sv.append(testPayload);
        sv.append(testPayload);
        sv.append(testPayload);
        sv.append(testPayload);
        sv.append(testPayload);

        // verify the failover layout
        //
        Layout expectedLayout = new TestLayoutBuilder()
                .setEpoch(2L)
                .addLayoutServer(SERVERS.PORT_0)
                .addLayoutServer(SERVERS.PORT_1)
                .addLayoutServer(SERVERS.PORT_2)
                .addSequencer(SERVERS.PORT_1)
                .addSequencer(SERVERS.PORT_0)
                .addSequencer(SERVERS.PORT_2)
                .buildSegment()
                .buildStripe()
                .addLogUnit(SERVERS.PORT_1)
                .addLogUnit(SERVERS.PORT_2)
                .addToSegment()
                .addToLayout()
                .addUnresponsiveServer(SERVERS.PORT_0)
                .setClusterId(getCorfuRuntime().getLayoutView().getLayout().getClusterId())
                .build();

        assertThat(getCorfuRuntime().getLayoutView().getLayout()).isEqualTo(expectedLayout);

        // verify that the new sequencer is advancing the tail properly
        assertThat(getSequencer(SERVERS.PORT_1).getGlobalLogTail().get()).isEqualTo(afterFailure);

        // sanity check that no other sequencer is active
        assertThat(getSequencer(SERVERS.PORT_2).getGlobalLogTail().get()).isEqualTo(0L);

    }

    protected <T> Object instantiateCorfuObject(TypeToken<T> tType, String name) {
        return (T)
                getCorfuRuntime().getObjectsView()
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
     * check that transcation conflict resolution works properly in face of sequencer failover
     */
    @Test
    public void ckSequencerFailoverTXResolution()
            throws Exception {
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
    public void ckSequencerFailoverTXResolution1()
            throws Exception {
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
     *
     * @throws Exception
     */
    @Test
    public void sequencerFailoverBackpointerCheck() throws Exception {
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
                corfuRuntime.getSequencerView().nextToken(Collections.singleton(streamID), 1);
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
        getManagementTestLayout();

        UUID streamA = UUID.nameUUIDFromBytes("stream A".getBytes());

        getTokenWriteAndAssertBackPointer(streamA, Address.NON_EXIST);

        resetDetected.acquire();
        // Allow only SERVERS.PORT_0 to handle the failure.
        // Shutting down PORT_2
        getManagementServer(SERVERS.PORT_2).shutdown();
        addClientRule(getManagementServer(SERVERS.PORT_1).getCorfuRuntime(),
                new TestRule().matches(msg -> {
                    if (msg.getMsgType().equals(CorfuMsgType.BOOTSTRAP_SEQUENCER)) {
                        try {
                            // There is a failure but the BOOTSTRAP_SEQUENCER message has not yet been
                            // sent. So if we request a token now, we should be denied as the
                            // server is sealed and we get a WrongEpochException.
                            corfuRuntime
                                    .getRouter(SERVERS.ENDPOINT_1)
                                    .getClient(SequencerClient.class)
                                    .nextToken(Collections.singleton(CorfuRuntime
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
        corfuRuntime.getSequencerView().nextToken(Collections.singleton(CorfuRuntime
                .getStreamID("testStream")), 1);
    }

    @Test
    public void sealDoesNotModifyClientRouterEpoch() throws Exception {
        Layout l = getManagementTestLayout();

        // Seal
        Layout currentLayout = corfuRuntime.getLayoutView().getCurrentLayout();
        currentLayout.setEpoch(currentLayout.getEpoch() + 1);
        currentLayout.moveServersToEpoch();

        for (String router : l.getAllServers()) {
            assertThat(corfuRuntime.getRouter(router).getEpoch()).isEqualTo(1L);
        }

    }

    @Test
    public void updateTrailingLayoutServers() throws Exception {

        Layout layout = getManagementTestLayout();

        addClientRule(corfuRuntime, SERVERS.ENDPOINT_0, new TestRule().always().drop());
        layout.setEpoch(2L);
        layout.moveServersToEpoch();
        corfuRuntime.getLayoutView().updateLayout(layout, 1L);

        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_MODERATE; i++) {
            Thread.sleep(PARAMETERS.TIMEOUT_SHORT.toMillis());
            if (getLayoutServer(SERVERS.PORT_0).getCurrentLayout().equals(layout))
                break;

        }

        assertThat(getLayoutServer(SERVERS.PORT_0).getCurrentLayout().getEpoch()).isEqualTo(2L);
        assertThat(getLayoutServer(SERVERS.PORT_0).getCurrentLayout()).isEqualTo(layout);
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

        corfuRuntime.getRouter(SERVERS.ENDPOINT_0).getClient(ManagementClient.class)
                .initiateFailureHandler().get();

        CFUtils.within(
                CompletableFuture.allOf(
                        getManagementServer(SERVERS.PORT_0).getSequencerBootstrappedFuture()),
                PARAMETERS.TIMEOUT_NORMAL
        ).join();

        l.setRuntime(corfuRuntime);
        l.setEpoch(l.getEpoch() + 1);
        l.moveServersToEpoch();

        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_MODERATE; i++) {
            Thread.sleep(PARAMETERS.TIMEOUT_SHORT.toMillis());
            if (corfuRuntime.getLayoutView().getLayout().getEpoch() == l.getEpoch())
                break;
            corfuRuntime.invalidateLayout();
        }
        assertThat(corfuRuntime.getLayoutView().getLayout().getEpoch()).isEqualTo(l.getEpoch());
    }

    /**
     * Tests healing of an unresponsive node now responding to pings.
     * PORT_2 is marked as unresponsive initially. The other 2 nodes PORT_0 and PORT_1 will
     * be able to successfully ping PORT_2. They then remove the node PORT_2 from the unresponsive
     * servers list and mark as active.å
     *
     * @throws Exception
     */
    @Test
    public void testNodeHealing() throws Exception {

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
                // Starting with a scenario with PORT_2 marked as unresponsive.
                .addUnresponsiveServer(SERVERS.PORT_2)
                .build();
        bootstrapAllServers(l);
        CorfuRuntime corfuRuntime = getRuntime(l).connect();

        setAggressiveTimeouts(l, corfuRuntime,
                getManagementServer(SERVERS.PORT_0).getCorfuRuntime(),
                getManagementServer(SERVERS.PORT_1).getCorfuRuntime(),
                getManagementServer(SERVERS.PORT_2).getCorfuRuntime());

        corfuRuntime.getRouter(SERVERS.ENDPOINT_0).getClient(ManagementClient.class)
                .initiateFailureHandler().get();
        corfuRuntime.getRouter(SERVERS.ENDPOINT_1).getClient(ManagementClient.class)
                .initiateFailureHandler().get();
        corfuRuntime.getRouter(SERVERS.ENDPOINT_2).getClient(ManagementClient.class)
                .initiateFailureHandler().get();

        while (corfuRuntime.getLayoutView().getLayout().getEpoch() == l.getEpoch()) {
            Thread.sleep(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());
            corfuRuntime.invalidateLayout();
        }
        long newEpoch = 2L;
        assertThat(corfuRuntime.getLayoutView().getLayout().getEpoch()).isEqualTo(newEpoch);
        assertThat(corfuRuntime.getLayoutView().getLayout().getUnresponsiveServers()).isEmpty();
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

        CorfuRuntime rt = getNewRuntime(getDefaultNode())
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
        Layout layoutPhase2 = rt.layout.get();

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
    }
}
