package org.corfudb.runtime.view;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.TestLayoutBuilder;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.LayoutBootstrapRequest;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.SequencerClient;
import org.corfudb.runtime.clients.LayoutClient;
import org.corfudb.runtime.clients.TestRule;
import org.corfudb.runtime.exceptions.WrongClusterIdException;
import org.corfudb.runtime.view.stream.IStreamView;
import org.junit.Test;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Created by mwei on 1/6/16.
 */
@Slf4j
public class LayoutViewTest extends AbstractViewTest {
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
                .setClusterId(r.getLayoutView().getLayout().getClusterId())
                .setEpoch(1)
                .addLayoutServer(SERVERS.PORT_0)
                .addSequencer(SERVERS.PORT_0)
                .buildSegment()
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addToSegment()
                .addToLayout()
                .build();
        l.setRuntime(r);
        l.moveServersToEpoch();
        r.getLayoutView().updateLayout(l, 1L);
        r.invalidateLayout();
        assertThat(r.getLayoutView().getLayout().epoch)
                .isEqualTo(1L);
    }

    @Test
    public void canTolerateLayoutServerFailure()
            throws Exception {
        addServer(SERVERS.PORT_0);
        addServer(SERVERS.PORT_1);

        bootstrapAllServers(new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(SERVERS.PORT_0)
                .addLayoutServer(SERVERS.PORT_1)
                .addSequencer(SERVERS.PORT_0)
                .buildSegment()
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addToSegment()
                .addToLayout()
                .build());

        CorfuRuntime r = getRuntime().connect();

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
        UUID clusterID = UUID.randomUUID();
        Layout l = new TestLayoutBuilder()
                .setClusterId(clusterID)
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
                        .setClusterId(clusterID)
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
                newLayout.setRuntime(corfuRuntime);
                //TODO need to figure out if we can move to
                //update layout
                newLayout.moveServersToEpoch();

                corfuRuntime.getLayoutView().updateLayout(newLayout, newLayout.getEpoch());
                corfuRuntime.invalidateLayout();
                corfuRuntime.layout.get();
                corfuRuntime.getRouter(SERVERS.ENDPOINT_0).getClient(SequencerClient.class)
                        .bootstrap(0L, Collections.EMPTY_MAP, newLayout.getEpoch()).get();
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

        UUID clusterId = UUID.randomUUID();

        Layout l = new TestLayoutBuilder()
                .setClusterId(clusterId)
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
                .setClusterId(clusterId)
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

        l.setRuntime(corfuRuntime);
        newLayout.setRuntime(corfuRuntime);
        corfuRuntime.getLayoutView().getLayout();

        l.setEpoch(l.getEpoch() + 1);
        l.moveServersToEpoch();
        log.warn("{}", corfuRuntime.nodeRouters);
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

        UUID clusterId = UUID.randomUUID();

        Layout l = new TestLayoutBuilder()
                .setClusterId(clusterId)
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
        getManagementServer(SERVERS.PORT_3).shutdown();

        addServerRule(SERVERS.PORT_1, new TestRule().always().drop());

        Layout newLayout = new TestLayoutBuilder()
                .setClusterId(clusterId)
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

        l.setRuntime(corfuRuntime);
        newLayout.setRuntime(corfuRuntime);

        // Keep old layout untouched for assertion
        Layout oldLayout = (Layout) l.clone();
        l.setEpoch(l.getEpoch() + 1);
        l.moveServersToEpoch();

        // We receive responses from PORT_0 and PORT_2
        corfuRuntime.getLayoutView().updateLayout(newLayout, 1L);

        assertThat(getLayoutServer(SERVERS.PORT_0).getCurrentLayout()).isEqualTo(oldLayout);
        assertThat(getLayoutServer(SERVERS.PORT_1).getCurrentLayout()).isEqualTo(newLayout);
        assertThat(getLayoutServer(SERVERS.PORT_2).getCurrentLayout()).isEqualTo(newLayout);
        assertThat(getLayoutServer(SERVERS.PORT_3).getCurrentLayout()).isEqualTo(newLayout);
    }

    /**
     * Set up 2 clusters with one node each and completely bootstrapped.
     * We then attempt to send a phase 1 paxos message and see that it gets rejected
     * at the router and returns back a wrong cluster id exception.
     * @throws Exception
     */
    @Test
    public void rejectInterClusterControlMsgs()
            throws Exception {
        addServer(SERVERS.PORT_0);
        addServer(SERVERS.PORT_1);

        Layout layout1 = new TestLayoutBuilder()
                .setClusterId(UUID.randomUUID())
                .setEpoch(1L)
                .addLayoutServer(SERVERS.PORT_0)
                .addSequencer(SERVERS.PORT_0)
                .buildSegment()
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addToSegment()
                .addToLayout()
                .build();
        getLayoutServer(SERVERS.PORT_0).handleMessage(CorfuMsgType.LAYOUT_BOOTSTRAP.payloadMsg(new LayoutBootstrapRequest(layout1)),
                null, getServerRouter(SERVERS.PORT_0));
        getManagementServer(SERVERS.PORT_0).handleMessage(CorfuMsgType.MANAGEMENT_BOOTSTRAP_REQUEST.payloadMsg(layout1),
                null, getServerRouter(SERVERS.PORT_0));

        Layout layout2 = new TestLayoutBuilder()
                .setClusterId(UUID.randomUUID())
                .setEpoch(1L)
                .addLayoutServer(SERVERS.PORT_1)
                .addSequencer(SERVERS.PORT_1)
                .buildSegment()
                .buildStripe()
                .addLogUnit(SERVERS.PORT_1)
                .addToSegment()
                .addToLayout()
                .build();
        getLayoutServer(SERVERS.PORT_1).handleMessage(CorfuMsgType.LAYOUT_BOOTSTRAP.payloadMsg(new LayoutBootstrapRequest(layout2)),
                null, getServerRouter(SERVERS.PORT_1));
        getManagementServer(SERVERS.PORT_1).handleMessage(CorfuMsgType.MANAGEMENT_BOOTSTRAP_REQUEST.payloadMsg(layout2),
                null, getServerRouter(SERVERS.PORT_1));

        CorfuRuntime corfuRuntime = getRuntime(layout1).connect();
        layout1.setRuntime(corfuRuntime);
        layout1.setEpoch(layout1.getEpoch() + 1);
        layout1.moveServersToEpoch();
        layout1.getLayoutServers().add(SERVERS.ENDPOINT_1);
        assertThat(corfuRuntime.getRouter(SERVERS.ENDPOINT_0).getClusterId()).isEqualTo(layout1.getClusterId());
        assertThatThrownBy(() ->
                corfuRuntime.getRouter(SERVERS.ENDPOINT_1).getClient(LayoutClient.class).prepare(2L, 1L).get()
        ).hasCause(new WrongClusterIdException(layout2.getClusterId()));
    }
}
