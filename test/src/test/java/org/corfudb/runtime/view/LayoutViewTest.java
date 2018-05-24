package org.corfudb.runtime.view;

import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.TestLayoutBuilder;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.TestRule;
import org.corfudb.runtime.exceptions.OutrankedException;
import org.corfudb.runtime.exceptions.QuorumUnreachableException;
import org.corfudb.runtime.exceptions.WrongClusterException;
import org.corfudb.runtime.view.stream.IStreamView;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Created by mwei on 1/6/16.
 */
@Slf4j
public class LayoutViewTest extends AbstractViewTest {

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

    //@Test
    public void canGetLayout() {
        CorfuRuntime r = getDefaultRuntime().connect();
        Layout l = r.getLayoutView().getCurrentLayout();
        assertThat(l.asJSONString())
                .isNotNull();
    }

    @Test
    public void canSetLayout()
            throws Exception {
        CorfuRuntime r = getDefaultRuntime().connect();
        Layout l = new TestLayoutBuilder()
                .setEpoch(1)
                .addLayoutServer(SERVERS.PORT_0)
                .addSequencer(SERVERS.PORT_0)
                .buildSegment()
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addToSegment()
                .addToLayout()
                .setClusterId(r.clusterId)
                .build();
        r.getLayoutView().getRuntimeLayout(l).moveServersToEpoch();
        r.getLayoutView().updateLayout(l, 1L);
        r.invalidateLayout();
        assertThat(r.getLayoutView().getLayout().epoch)
                .isEqualTo(1L);
    }

    /** Make sure that trying to set a layout with the wrong cluster id results
     *  in a wrong epoch exception.
     */
    @Test
    public void cannotSetLayoutWithWrongId()
        throws Exception {
        CorfuRuntime r = getDefaultRuntime().connect();
        Layout l = new TestLayoutBuilder()
            .setEpoch(1)
            .addLayoutServer(SERVERS.PORT_0)
            .addSequencer(SERVERS.PORT_0)
            .buildSegment()
            .buildStripe()
            .addLogUnit(SERVERS.PORT_0)
            .addToSegment()
            .addToLayout()
            .setClusterId(UUID.nameUUIDFromBytes("wrong cluster".getBytes()))
            .build();
        r.getLayoutView().getRuntimeLayout(l).moveServersToEpoch();
        r.invalidateLayout();
        assertThatThrownBy(() -> r.getLayoutView().updateLayout(l, 1L))
            .isInstanceOf(WrongClusterException.class);
    }

    @Test
    public void canTolerateLayoutServerFailure()
            throws Exception {
        addServer(SERVERS.PORT_0);
        addServer(SERVERS.PORT_1);

        Layout l = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(SERVERS.PORT_0)
                .addLayoutServer(SERVERS.PORT_1)
                .addSequencer(SERVERS.PORT_0)
                .buildSegment()
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addToSegment()
                .addToLayout()
                .build();
        bootstrapAllServers(l);

        CorfuRuntime r = getRuntime().connect();
        getManagementServer(SERVERS.PORT_0).shutdown();
        getManagementServer(SERVERS.PORT_1).shutdown();
        setAggressiveTimeouts(l, r);

        // Fail the network link between the client and test server
        addServerRule(SERVERS.PORT_1, new TestRule()
                .always()
                .drop());

        r.invalidateLayout();

        r.getStreamsView().get(CorfuRuntime.getStreamID("hi")).hasNext();
    }

    /**
     * Fail a server and reconfigure
     * while data operations are going on.
     * Details:
     * Start with a configuration of 3 servers SERVERS.PORT_0, SERVERS.PORT_1, SERVERS.PORT_2.
     * Perform data operations. Fail SERVERS.PORT_1 and reconfigure to have only SERVERS.PORT_0 and SERVERS.PORT_2.
     * Perform data operations while the reconfiguration is going on. The operations should
     * be stuck till the new configuration is chosen and then complete after that.
     * FIXME: We cannot failover the server with the primary sequencer yet.
     *
     * @throws Exception
     */
    @Test
    public void reconfigurationDuringDataOperations()
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
                .addLogUnit(SERVERS.PORT_0)
                .addLogUnit(SERVERS.PORT_1)
                .addLogUnit(SERVERS.PORT_2)
                .addToSegment()
                .addToLayout()
                .build();
        bootstrapAllServers(l);
        CorfuRuntime corfuRuntime = getRuntime(l).connect();

        getManagementServer(SERVERS.PORT_0).shutdown();
        getManagementServer(SERVERS.PORT_1).shutdown();
        getManagementServer(SERVERS.PORT_2).shutdown();
        setAggressiveTimeouts(l, corfuRuntime);

        // Thread to reconfigure the layout
        CountDownLatch startReconfigurationLatch = new CountDownLatch(1);
        CountDownLatch layoutReconfiguredLatch = new CountDownLatch(1);

        Thread t = new Thread(() -> {
            try {
                startReconfigurationLatch.await();
                corfuRuntime.invalidateLayout();

                // Fail the network link between the client and test server
                addServerRule(SERVERS.PORT_1, new TestRule().always().drop());
                // New layout removes the failed server SERVERS.PORT_0
                Layout newLayout = new TestLayoutBuilder()
                        .setEpoch(l.getEpoch() + 1)
                        .addLayoutServer(SERVERS.PORT_0)
                        .addLayoutServer(SERVERS.PORT_2)
                        .addSequencer(SERVERS.PORT_0)
                        .addSequencer(SERVERS.PORT_2)
                        .buildSegment()
                        .buildStripe()
                        .addLogUnit(SERVERS.PORT_0)
                        .addLogUnit(SERVERS.PORT_2)
                        .addToSegment()
                        .addToLayout()
                        .build();
                //TODO need to figure out if we can move to
                //update layout
                corfuRuntime.getLayoutView().getRuntimeLayout(newLayout).moveServersToEpoch();

                corfuRuntime.getLayoutView().updateLayout(newLayout, newLayout.getEpoch());
                corfuRuntime.invalidateLayout();
                corfuRuntime.getLayoutView().getRuntimeLayout()
                        .getSequencerClient(SERVERS.ENDPOINT_0)
                        .bootstrap(0L, Collections.emptyMap(), newLayout.getEpoch(), true).get();
                log.debug("layout updated new layout {}", corfuRuntime.getLayoutView().getLayout());
                layoutReconfiguredLatch.countDown();
            } catch (Exception e) {
                log.error("GOT ERROR HERE !!");
                e.printStackTrace();
            }
        });
        t.start();

        // verify writes and reads happen before and after the reconfiguration
        IStreamView sv = corfuRuntime.getStreamsView().get(CorfuRuntime.getStreamID("streamA"));
        // This append will happen before the reconfiguration while the read for this append
        // will happen after reconfiguration
        writeAndReadStream(corfuRuntime, sv, startReconfigurationLatch, layoutReconfiguredLatch);
        // Write and read after reconfiguration.
        writeAndReadStream(corfuRuntime, sv, startReconfigurationLatch, layoutReconfiguredLatch);
        t.join();
    }

    private void writeAndReadStream(CorfuRuntime corfuRuntime, IStreamView sv, CountDownLatch startReconfigurationLatch, CountDownLatch layoutReconfiguredLatch) throws InterruptedException {
        byte[] testPayload = "hello world".getBytes();
        sv.append(testPayload);
        startReconfigurationLatch.countDown();
        layoutReconfiguredLatch.await();
        assertThat(sv.next().getPayload(corfuRuntime)).isEqualTo("hello world".getBytes());
        assertThat(sv.next()).isEqualTo(null);
    }

    /**
     * Want to ensure that consensus is taken only from the members of the existing layout.
     * If we take consensus on new layout, the test should fail as we would receive a
     * wrong epoch exception from SERVERS.PORT_0.
     *
     * @throws Exception
     */
    @Test
    public void getConsensusFromCurrentMembers1Node() throws Exception {
        addServer(SERVERS.PORT_0);
        addServer(SERVERS.PORT_1);

        Layout l = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(SERVERS.PORT_1)
                .addSequencer(SERVERS.PORT_1)
                .buildSegment()
                .buildStripe()
                .addLogUnit(SERVERS.PORT_1)
                .addToSegment()
                .addToLayout()
                .build();
        bootstrapAllServers(l);
        CorfuRuntime corfuRuntime = getRuntime(l).connect();


        getManagementServer(SERVERS.PORT_0).shutdown();
        getManagementServer(SERVERS.PORT_1).shutdown();

        Layout newLayout = new TestLayoutBuilder()
                .setEpoch(2L)
                .addLayoutServer(SERVERS.PORT_0)
                .addLayoutServer(SERVERS.PORT_1)
                .addSequencer(SERVERS.PORT_1)
                .buildSegment()
                .buildStripe()
                .addLogUnit(SERVERS.PORT_1)
                .addToSegment()
                .addToLayout()
                .build();

        l.setEpoch(l.getEpoch() + 1);
        corfuRuntime.getLayoutView().getRuntimeLayout(l).moveServersToEpoch();
        corfuRuntime.getLayoutView().updateLayout(newLayout, 1L);

        assertThat(getLayoutServer(SERVERS.PORT_0).getCurrentLayout()).isEqualTo(newLayout);
        assertThat(getLayoutServer(SERVERS.PORT_1).getCurrentLayout()).isEqualTo(newLayout);
    }

    /**
     * Moving from a cluster of 3 nodes {Node 0, Node 1, Node 2} -> {Node 1, Node 2, Node 3}
     * We add a rule to make Node 1 unresponsive.
     * To move to the next epoch we would need a majority consensus from {0, 1, 2} and we receive
     * responses from {0, 2} and successfully update the layout.
     * NOTE: We DO NOT await consensus from {1, 2, 3}. If we do, Node 1 does not respond and
     * Node 3 should throw a WrongEpochException and our updateLayout should fail.
     *
     * @throws Exception
     */
    @Test
    public void getConsensusFromCurrentMembers3Nodes() throws Exception {
        addServer(SERVERS.PORT_0);
        addServer(SERVERS.PORT_1);
        addServer(SERVERS.PORT_2);
        addServer(SERVERS.PORT_3);

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
        Layout copy = new Layout(l);
        bootstrapAllServers(copy);
        CorfuRuntime corfuRuntime = getRuntime(l).connect();


        getManagementServer(SERVERS.PORT_0).shutdown();
        getManagementServer(SERVERS.PORT_1).shutdown();
        getManagementServer(SERVERS.PORT_2).shutdown();
        getManagementServer(SERVERS.PORT_3).shutdown();

        addServerRule(SERVERS.PORT_1, new TestRule().always().drop());

        Layout newLayout = new TestLayoutBuilder()
                .setEpoch(2L)
                .addLayoutServer(SERVERS.PORT_1)
                .addLayoutServer(SERVERS.PORT_2)
                .addLayoutServer(SERVERS.PORT_3)
                .addSequencer(SERVERS.PORT_0)
                .buildSegment()
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addToSegment()
                .addToLayout()
                .build();

        // Keep old layout untouched for assertion
        Layout oldLayout = new Layout(l);
        l.setEpoch(l.getEpoch() + 1);
        corfuRuntime.getLayoutView().getRuntimeLayout(l).moveServersToEpoch();

        // We receive responses from PORT_0 and PORT_2
        corfuRuntime.getLayoutView().updateLayout(newLayout, 1L);

        assertThat(getLayoutServer(SERVERS.PORT_0).getCurrentLayout()).isEqualTo(oldLayout);
        assertThat(getLayoutServer(SERVERS.PORT_1).getCurrentLayout()).isEqualTo(newLayout);
        assertThat(getLayoutServer(SERVERS.PORT_2).getCurrentLayout()).isEqualTo(newLayout);
        assertThat(getLayoutServer(SERVERS.PORT_3).getCurrentLayout()).isEqualTo(newLayout);
    }

    private final Map<String, Map<CorfuMsgType, List<Semaphore>>> messageLocks =
            new ConcurrentHashMap<>();

    private void holdMessage(CorfuRuntime corfuRuntime, String clientRouterEndpoint,
                             CorfuMsgType corfuMsgType) {
        Semaphore lock = new Semaphore(0);
        addClientRule(corfuRuntime, clientRouterEndpoint, new TestRule().matches(msg -> {
            if (msg.getMsgType().equals(corfuMsgType)) {
                Map<CorfuMsgType, List<Semaphore>> lockMap =
                        messageLocks.getOrDefault(clientRouterEndpoint, new HashMap<>());
                List<Semaphore> lockList = lockMap.getOrDefault(corfuMsgType, new ArrayList<>());
                lockList.add(lock);

                lockMap.put(corfuMsgType, lockList);
                messageLocks.put(clientRouterEndpoint, lockMap);

                try {
                    lock.acquire();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            return true;
        }));
    }

    private void releaseMessages(String clientRouterEndpoint, CorfuMsgType corfuMsgType) {
        messageLocks.computeIfPresent(clientRouterEndpoint, (s, corfuMsgTypeListMap) -> {
            corfuMsgTypeListMap.computeIfPresent(corfuMsgType, (msgType, semaphores) -> {
                semaphores.forEach(Semaphore::release);
                return null;
            });
            return corfuMsgTypeListMap;
        });
    }

    /**
     * Prepare should return the layout with the highest proposed rank.
     * The scenario is tested in 3 steps as shown below.
     * PORT_0         PORT_1         PORT_2
     *
     * prepare(1)     prepare(1)     X
     * propose(1,L1)  X              X
     *
     * X              prepare(2)     prepare(2)
     * X              propose(2,L2)  X
     *
     * prepare(3)     prepare(3)     prepare(3)
     * [Returns L1]   [Returns L2]   null
     *
     * The final prepare attempt should return L2 as it has the highest rank of 2.
     *
     * @throws Exception
     */
    @Test
    public void prepareReturnLayoutWithHighestRank() throws Exception {

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
        CorfuRuntime corfuRuntime = getRuntime(l).connect();

        getManagementServer(SERVERS.PORT_0).shutdown();
        getManagementServer(SERVERS.PORT_1).shutdown();
        getManagementServer(SERVERS.PORT_2).shutdown();
        setAggressiveTimeouts(l, corfuRuntime);

        Layout l1 = new TestLayoutBuilder()
                .setEpoch(2L)
                .addLayoutServer(SERVERS.PORT_0)
                .addLayoutServer(SERVERS.PORT_1)
                .addLayoutServer(SERVERS.PORT_2)
                .addSequencer(SERVERS.PORT_0)
                .addSequencer(SERVERS.PORT_1)
                .buildSegment()
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addToSegment()
                .addToLayout()
                .build();

        Layout l2 = new TestLayoutBuilder()
                .setEpoch(2L)
                .addLayoutServer(SERVERS.PORT_0)
                .addLayoutServer(SERVERS.PORT_1)
                .addLayoutServer(SERVERS.PORT_2)
                .addSequencer(SERVERS.PORT_0)
                .addSequencer(SERVERS.PORT_2)
                .buildSegment()
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addToSegment()
                .addToLayout()
                .build();

        l.setEpoch(l.getEpoch() + 1);
        corfuRuntime.getLayoutView().getRuntimeLayout(l).moveServersToEpoch();

        // STEP 1
        final long rank1 = 1L;
        addClientRule(corfuRuntime, SERVERS.ENDPOINT_2, new TestRule().drop().always());
        Layout alreadyProposedLayout1 = corfuRuntime.getLayoutView().prepare(l1.getEpoch(), rank1);
        assertThat(alreadyProposedLayout1).isNull();
        addClientRule(corfuRuntime, SERVERS.ENDPOINT_1, new TestRule().drop().always());
        try {
            alreadyProposedLayout1 = corfuRuntime.getLayoutView().propose(l1.getEpoch(), rank1, l1);
        } catch (QuorumUnreachableException ignore) {
        }
        assertThat(alreadyProposedLayout1).isNull();
        clearClientRules(corfuRuntime);

        // STEP 2
        final long rank2 = 2L;
        addClientRule(corfuRuntime, SERVERS.ENDPOINT_0, new TestRule().drop().always());
        Layout alreadyProposedLayout2 = corfuRuntime.getLayoutView().prepare(l2.getEpoch(), rank2);
        assertThat(alreadyProposedLayout2).isNull();
        addClientRule(corfuRuntime, SERVERS.ENDPOINT_2, new TestRule().drop().always());
        try {
            alreadyProposedLayout2 = corfuRuntime.getLayoutView().propose(l2.getEpoch(), rank2, l2);
        } catch (QuorumUnreachableException ignore) {
        }
        assertThat(alreadyProposedLayout2).isNull();
        clearClientRules(corfuRuntime);

        // STEP 3
        final long rank3 = 3L;
        addClientRule(corfuRuntime, SERVERS.ENDPOINT_2, new TestRule().drop().always());
        Layout alreadyProposedLayout3 = corfuRuntime.getLayoutView().prepare(l.getEpoch(), rank3);
        assertThat(alreadyProposedLayout3).isEqualTo(l2);
        clearClientRules(corfuRuntime);

        final long rank4 = 4L;
        addClientRule(corfuRuntime, SERVERS.ENDPOINT_1, new TestRule().drop().always());
        alreadyProposedLayout3 = corfuRuntime.getLayoutView().prepare(l.getEpoch(), rank4);
        assertThat(alreadyProposedLayout3).isEqualTo(l1);
        clearClientRules(corfuRuntime);

        final long rank5 = 5L;
        addClientRule(corfuRuntime, SERVERS.ENDPOINT_0, new TestRule().drop().always());
        alreadyProposedLayout3 = corfuRuntime.getLayoutView().prepare(l.getEpoch(), rank5);
        assertThat(alreadyProposedLayout3).isEqualTo(l2);
        clearClientRules(corfuRuntime);
    }

    /**
     * Propose messages should be rejected if a higher rank epoch has already been accepted.
     *
     * PORT_0             PORT_1                 PORT_2
     *
     * prepare(1)         prepare(1)             prepare(1)
     * propose(1,L1)      ? (delayed)            X
     *
     * X                  prepare(2)             prepare(2)
     * ---                propose(1,L1)(reject)  --
     * X                  propose(2,L2)          propose(2,L2)
     *
     * prepare(3)         prepare(3)             prepare(3)
     * [Returns L1]       [Returns L2]           [Returns L2]
     *
     * The final prepare attempt should return L2 as it has been accepted by the quorum.
     *
     * @throws Exception
     */
    @Test
    public void delayedProposeTest() throws Exception {

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
        CorfuRuntime corfuRuntime1 = getRuntime(l).connect();
        CorfuRuntime corfuRuntime2 = getRuntime(l).connect();

        getManagementServer(SERVERS.PORT_0).shutdown();
        getManagementServer(SERVERS.PORT_1).shutdown();
        getManagementServer(SERVERS.PORT_2).shutdown();
        setAggressiveTimeouts(l, corfuRuntime1, corfuRuntime2);

        Layout l1 = new TestLayoutBuilder()
                .setEpoch(2L)
                .addLayoutServer(SERVERS.PORT_0)
                .addLayoutServer(SERVERS.PORT_1)
                .addLayoutServer(SERVERS.PORT_2)
                .addSequencer(SERVERS.PORT_0)
                .addSequencer(SERVERS.PORT_1)
                .buildSegment()
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addToSegment()
                .addToLayout()
                .build();

        Layout l2 = new TestLayoutBuilder()
                .setEpoch(2L)
                .addLayoutServer(SERVERS.PORT_0)
                .addLayoutServer(SERVERS.PORT_1)
                .addLayoutServer(SERVERS.PORT_2)
                .addSequencer(SERVERS.PORT_0)
                .addSequencer(SERVERS.PORT_2)
                .buildSegment()
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addToSegment()
                .addToLayout()
                .build();

        l.setEpoch(l.getEpoch() + 1);
        corfuRuntime1.getLayoutView().getRuntimeLayout(l).moveServersToEpoch();

        Semaphore proposeLock = new Semaphore(0);
        ExecutorService executorService = Executors.newSingleThreadExecutor();

        // STEP 1: Send prepare to all 3 nodes with rank 1.
        final long rank1 = 1L;
        Layout alreadyProposedLayout1 = corfuRuntime1.getLayoutView().prepare(l1.getEpoch(), rank1);
        assertThat(alreadyProposedLayout1).isNull();

        // PORT_2 drops the oncoming propose message.
        addClientRule(corfuRuntime1, SERVERS.ENDPOINT_2, new TestRule().drop().always());

        // Add rule to hold the propose message sent to PORT_1.
        addClientRule(corfuRuntime1, SERVERS.ENDPOINT_1, new TestRule().matches(msg -> {
            if (msg.getMsgType().equals(CorfuMsgType.LAYOUT_PROPOSE)) {
                proposeLock.release();
            }
            return true;
        }));
        holdMessage(corfuRuntime1, SERVERS.ENDPOINT_1, CorfuMsgType.LAYOUT_PROPOSE);

        // Asynchronously send a propose message to PORT_0 and PORT_1 (PORT_2 is disabled)
        Future<Boolean> future = executorService.submit(() -> {
            try {
                corfuRuntime1.getLayoutView().propose(l1.getEpoch(), rank1, l1);
            } catch (QuorumUnreachableException e) {
                return true;
            } catch (OutrankedException ignore) {
            }
            return false;
        });

        AtomicBoolean proposalRejected = new AtomicBoolean(false);
        addServerRule(SERVERS.PORT_1, new TestRule().matches(msg -> {
            if (msg.getMsgType().equals(CorfuMsgType.LAYOUT_PROPOSE_REJECT)) {
                proposalRejected.set(true);
            }
            return true;
        }));

        proposeLock.tryAcquire(PARAMETERS.TIMEOUT_NORMAL.toMillis(), TimeUnit.MILLISECONDS);
        // After propose delayed, send a prepare to PORT_1 and PORT_2.
        final long rank2 = 2L;
        addClientRule(corfuRuntime2, SERVERS.ENDPOINT_0, new TestRule().drop().always());
        Layout alreadyProposedLayout2 = corfuRuntime2.getLayoutView().prepare(l2.getEpoch(), rank2);
        assertThat(alreadyProposedLayout2).isNull();

        while (messageLocks.isEmpty()) {
            Thread.sleep(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());
        }
        // release the held propose message
        releaseMessages(SERVERS.ENDPOINT_1, CorfuMsgType.LAYOUT_PROPOSE);
        try {
            alreadyProposedLayout2 = corfuRuntime2.getLayoutView().propose(l2.getEpoch(), rank2, l2);
            assertThat(alreadyProposedLayout2).isEqualTo(l2);
        } catch (QuorumUnreachableException ignore) {
        }

        // Assert that the proposal with rank 1 and layout l1 is rejected and a
        // LAYOUT_PROPOSE_REJECTED is returned by the server.
        assertThat(future.get()).isTrue();
        assertThat(proposalRejected.get()).isTrue();

        clearClientRules(corfuRuntime1);

        final long rank3 = 3L;
        Layout alreadyProposedLayout3 = corfuRuntime1.getLayoutView().prepare(l.getEpoch(), rank3);
        assertThat(alreadyProposedLayout3).isEqualTo(l2);

    }
}
