package org.corfudb.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.integration.Harness.run;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.integration.cluster.Harness.Node;
import org.corfudb.recovery.FastObjectLoader;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.MultiCheckpointWriter;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.LayoutBuilder;
import org.corfudb.util.Sleep;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.List;

/**
 * This integration test verifies the behaviour of the add node workflow. In particular, a single node
 * cluster is created and then populated with data, then a new node is added to the cluster,
 * making it of size 2. Checkpointing is then triggered so that the new second node starts servicing
 * new writes, then a 3rd node is added. The third node will have the checkpoints of the CorfuTable
 * that were populated and checkpointed when the cluster was only 2 nodes. Finally, a client reads
 * back the data generated while growing the cluster to verify that it is correct and can be read
 * from a three node cluster.
 * <p>
 * Created by Maithem on 12/1/17.
 */
@Slf4j
public class WorkflowIT extends AbstractIT {

    final String host = "localhost";

    String getConnectionString(int port) {
        return host + ":" + port;
    }

    final Duration timeout = Duration.ofMinutes(5);
    final Duration pollPeriod = Duration.ofMillis(50);
    final int workflowNumRetry = 3;

    private Process runServer(int port, boolean single) throws IOException {
        return new CorfuServerRunner()
                .setHost(host)
                .setPort(port)
                .setSingle(single)
                .runServer();
    }

    @Test
    public void addAndRemoveNodeIT() throws Exception {
        final String streamName = "s1";
        final int n1Port = 9000;
        final int numIter = 11_000;

        // Start node one and populate it with data
        Process server_1 = runServer(n1Port, true);

        // start a second node
        final int n2Port = 9001;
        Process server_2 = runServer(n2Port, false);

        // start a third node
        final int n3Port = 9002;
        Process server_3 = runServer(n3Port, false);

        CorfuRuntime n1Rt = new CorfuRuntime(getConnectionString(n1Port))
                .setCacheDisabled(true).connect();

        CorfuTable table = n1Rt.getObjectsView()
                .build()
                .setType(CorfuTable.class)
                .setStreamName(streamName)
                .open();

        for (int x = 0; x < numIter; x++) {
            table.put(String.valueOf(x), String.valueOf(x));
        }

        n1Rt.getManagementView().addNode(getConnectionString(n2Port), workflowNumRetry,
                timeout, pollPeriod);

        n1Rt.invalidateLayout();
        final int clusterSizeN2 = 2;
        assertThat(n1Rt.getLayoutView().getLayout().getAllServers().size()).isEqualTo(clusterSizeN2);

        MultiCheckpointWriter mcw = new MultiCheckpointWriter();
        mcw.addMap(table);

        long prefix = mcw.appendCheckpoints(n1Rt, "Maithem");

        n1Rt.getAddressSpaceView().prefixTrim(prefix - 1);

        n1Rt.getAddressSpaceView().invalidateClientCache();
        n1Rt.getAddressSpaceView().invalidateServerCaches();
        n1Rt.getAddressSpaceView().gc();

        // Add a third node after compaction
        n1Rt.getManagementView().addNode(getConnectionString(n3Port), workflowNumRetry,
                timeout, pollPeriod);

        // Verify that the third node has been added and data can be read back
        n1Rt.invalidateLayout();
        final int clusterSizeN3 = 3;
        assertThat(n1Rt.getLayoutView().getLayout().getAllServers().size()).isEqualTo(clusterSizeN3);

        Layout n3Layout = new Layout(n1Rt.getLayoutView().getLayout());

        Layout expectedLayout = new LayoutBuilder(n3Layout)
                .removeLayoutServer(getConnectionString(n2Port))
                .removeSequencerServer(getConnectionString(n2Port))
                .removeLogunitServer(getConnectionString(n2Port))
                .removeUnresponsiveServer(getConnectionString(n2Port))
                .setEpoch(n3Layout.getEpoch() + 1)
                .build();

        // Remove node 2
        n1Rt.getManagementView().removeNode(getConnectionString(n2Port), workflowNumRetry,
                timeout, pollPeriod);
        n1Rt.invalidateLayout();
        assertThat(n1Rt.getLayoutView().getLayout().getAllServers().size()).isEqualTo(clusterSizeN2);


        // Remove node 2 again and verify that the epoch doesn't change
        n1Rt.getManagementView().removeNode(getConnectionString(n2Port), workflowNumRetry,
                timeout, pollPeriod);
        shutdownCorfuServer(server_2);

        n1Rt.invalidateLayout();
        // Verify that the layout epoch hasn't changed after the second remove and that
        // the sequencers/layouts/segments nodes include the first and third node
        assertThat(n1Rt.getLayoutView().getLayout()).isEqualTo(expectedLayout);

        // Force remove node 3
        n1Rt.getManagementView().forceRemoveNode(getConnectionString(n3Port), workflowNumRetry,
                timeout, pollPeriod);
        shutdownCorfuServer(server_3);

        n1Rt.invalidateLayout();
        assertThat(n1Rt.getLayoutView().getLayout().getAllServers().size()).isEqualTo(1);

        server_2 = runServer(n2Port, false);
        // Re-add node 2
        n1Rt.getManagementView().addNode(getConnectionString(n2Port), workflowNumRetry,
                timeout, pollPeriod);

        n1Rt.invalidateLayout();
        assertThat(n1Rt.getLayoutView().getLayout().getAllServers().size()).isEqualTo(clusterSizeN2);

        for (int x = 0; x < numIter; x++) {
            String v = (String) table.get(String.valueOf(x));
            assertThat(v).isEqualTo(String.valueOf(x));
        }

        shutdownCorfuServer(server_1);
        shutdownCorfuServer(server_2);
    }

    /**
     * This tests will resize the cluster according to the following order,
     * create a cluster of size 2, then force remove one node. Then, it will
     * regrow the cluster to 3 nodes and remove one node.
     */
    @Test
    public void clusterResizingTest1() throws Exception {
        final int n0Port = 9000;
        final int n1Port = 9001;
        final int n2Port = 9002;

        final int clusterSizeN1 = 1;
        final int clusterSizeN2 = 2;
        final int clusterSizeN3 = 3;

        Process p0 = runServer(n0Port, true);
        Process p1 = runServer(n1Port, false);
        Process p2 = runServer(n2Port, false);

        CorfuRuntime n0Rt = new CorfuRuntime(getConnectionString(n0Port)).connect();
        CorfuTable table = n0Rt.getObjectsView().build()
                .setType(CorfuTable.class)
                .setStreamName("table1").open();

        final int iter = 1000;
        for (int x = 0; x < iter; x++) {
            table.put(String.valueOf(x), String.valueOf(x));
        }

        n0Rt.getManagementView().addNode(getConnectionString(n1Port), workflowNumRetry,
                timeout, pollPeriod);
        n0Rt.invalidateLayout();

        assertThat(n0Rt.getLayoutView().getLayout().getAllServers().size()).isEqualTo(clusterSizeN2);

        n0Rt.getManagementView().forceRemoveNode(getConnectionString(n1Port), workflowNumRetry,
                timeout, pollPeriod);
        n0Rt.invalidateLayout();

        assertThat(n0Rt.getLayoutView().getLayout().getAllServers().size()).isEqualTo(clusterSizeN1);

        n0Rt.getManagementView().addNode(getConnectionString(n1Port), workflowNumRetry,
                timeout, pollPeriod);

        n0Rt.getManagementView().addNode(getConnectionString(n2Port), workflowNumRetry,
                timeout, pollPeriod);

        n0Rt.invalidateLayout();
        assertThat(n0Rt.getLayoutView().getLayout().getAllServers().size()).isEqualTo(clusterSizeN3);

        n0Rt.getManagementView().removeNode(getConnectionString(n1Port), workflowNumRetry,
                timeout, pollPeriod);
        n0Rt.invalidateLayout();
        assertThat(n0Rt.getLayoutView().getLayout().getAllServers().size()).isEqualTo(clusterSizeN2);

        for (int x = 0; x < iter; x++) {
            assertThat(table.get(String.valueOf(x))).isEqualTo(String.valueOf(x));
        }

        shutdownCorfuServer(p0);
        shutdownCorfuServer(p1);
        shutdownCorfuServer(p2);
    }

    @Test
    public void clusterResizingTest2() throws Exception {
        // This test will create a 3 node cluster, then simulate loss of quorum by terminating
        // two of the three nodes, then the failed nodes are forcefully removed from the cluster.
        final int n0Port = 9000;
        final int n1Port = 9001;
        final int n2Port = 9002;

        final int clusterSizeN1 = 1;
        final int clusterSizeN3 = 3;

        Process p0 = runServer(n0Port, true);
        Process p1 = runServer(n1Port, false);
        Process p2 = runServer(n2Port, false);

        CorfuRuntime n0Rt = new CorfuRuntime(getConnectionString(n0Port)).connect();
        CorfuTable table = n0Rt.getObjectsView().build()
                .setType(CorfuTable.class)
                .setStreamName("table1").open();

        final int iter = 100;
        for (int x = 0; x < iter; x++) {
            table.put(String.valueOf(x), String.valueOf(x));
        }

        n0Rt.getManagementView().addNode(getConnectionString(n1Port), workflowNumRetry,
                timeout, pollPeriod);

        n0Rt.getManagementView().addNode(getConnectionString(n2Port), workflowNumRetry,
                timeout, pollPeriod);

        n0Rt.invalidateLayout();
        assertThat(n0Rt.getLayoutView().getLayout().getAllServers().size()).isEqualTo(clusterSizeN3);

        // Kill two nodes from a three node cluster
        shutdownCorfuServer(p1);
        shutdownCorfuServer(p2);

        // Force remove the "failed" node
        n0Rt.getManagementView().forceRemoveNode(getConnectionString(n1Port), workflowNumRetry,
                timeout, pollPeriod);
        n0Rt.getManagementView().forceRemoveNode(getConnectionString(n2Port), workflowNumRetry,
                timeout, pollPeriod);
        n0Rt.invalidateLayout();
        assertThat(n0Rt.getLayoutView().getLayout().getAllServers().size()).isEqualTo(clusterSizeN1);

        for (int x = 0; x < iter; x++) {
            assertThat(table.get(String.valueOf(x))).isEqualTo(String.valueOf(x));
        }

        shutdownCorfuServer(p0);
    }

    @Test
    public void forceRemoveTwoDeadNodes() throws Exception {
        // Create a 3 node cluster and attempt to force remove two
        // dead nodes. This will test the case where the layout doesn't
        // reflect the set of unresponsive servers and thus needs to rely
        // on selecting an available orchestrator to force remove the dead nodes.
        Harness harness = Harness.getDefaultHarness();

        final int numNodes = 3;
        List<Node> nodeList = harness.deployCluster(numNodes);
        Node n0 = nodeList.get(0);
        Node n1 = nodeList.get(1);
        Node n2 = nodeList.get(2);

        CorfuRuntime rt = harness.createRuntimeForNode(n2);

        assertThat(rt.getLayoutView().getLayout().getAllServers().size()).isEqualTo(numNodes);

        // Shutdown two nodes
        run(n0.shutdown, n1.shutdown);

        rt.getManagementView().forceRemoveNode(n0.getAddress(), workflowNumRetry, timeout, pollPeriod);
        rt.getManagementView().forceRemoveNode(n1.getAddress(), workflowNumRetry, timeout, pollPeriod);

        assertThat(rt.getLayoutView().getLayout().getAllServers().size()).isEqualTo(1);
        assertThat(rt.getLayoutView().getLayout().getAllServers()).containsExactly(n2.getAddress());
    }

    /**
     * Tests whether the trimMark is transferred during stateTransfer.
     * Scenario: Setup a cluster of 1 node.
     * 1000 entries are written to node_1. These are then checkpointed and trimmed.
     * Another 1000 entries are added. Now node_1 has a trimMark at address 1000.
     * Now 2 new nodes node_2 and node_3 are added to the cluster after which node_1 is shutdown.
     * Finally we should see that the trimMark should be updated on the new nodes and the
     * FastObjectLoader trying to recreate the state from these 2 nodes should be able to do so.
     */
    @Test
    public void addNodeWithTrim() throws Exception {
        Harness harness = Harness.getDefaultHarness();

        final int numNodes = 3;
        final int PORT_1 = 9001;
        final int PORT_2 = 9002;
        List<Node> nodeList = harness.deployCluster(1);
        Node n0 = nodeList.get(0);
        Node n1 = harness.deployUnbootstrappedNode(PORT_1);
        Node n2 = harness.deployUnbootstrappedNode(PORT_2);

        CorfuRuntime rt = harness.createRuntimeForNode(n0);
        final String streamName = "test";
        CorfuTable<String, Integer> table = rt.getObjectsView()
                .build()
                .setType(CorfuTable.class)
                .setStreamName(streamName)
                .open();
        final int entriesCount = 1_000;

        // Write 1_000 entries.
        for (int i = 0; i < entriesCount; i++) {
            table.put(Integer.toString(i), i);
        }

        // Checkpoint and trim the entries.
        MultiCheckpointWriter mcw = new MultiCheckpointWriter();
        mcw.addMap(table);
        long prefixTrimAddress = mcw.appendCheckpoints(rt, "author");
        rt.getAddressSpaceView().prefixTrim(prefixTrimAddress);
        rt.getAddressSpaceView().invalidateServerCaches();
        rt.getAddressSpaceView().invalidateClientCache();
        rt.getAddressSpaceView().gc();

        assertThat(rt.getAddressSpaceView().getTrimMark()).isEqualTo(entriesCount);

        // 2 Checkpoint entries for the start and end.
        // 1000 entries being checkpointed = 20 checkpoint entries due to batch size of 50.
        final int checkpointEntriesCount = 22;

        // Write another batch of 1_000 entries.
        for (int i = 0; i < entriesCount; i++) {
            table.put(Integer.toString(i), i);
        }
        final long streamTail = entriesCount + checkpointEntriesCount + entriesCount - 1;

        // Add 2 new nodes.
        final int retries = 3;
        final Duration timeout = PARAMETERS.TIMEOUT_LONG;
        final Duration pollPeriod = PARAMETERS.TIMEOUT_VERY_SHORT;
        rt.getManagementView().addNode("localhost:9001", retries, timeout, pollPeriod);
        rt.getManagementView().addNode("localhost:9002", retries, timeout, pollPeriod);

        rt.invalidateLayout();
        Layout layoutAfterAdds = rt.getLayoutView().getLayout();
        assertThat(layoutAfterAdds.getSegments().stream()
                .allMatch(s -> s.getAllLogServers().size() == numNodes)).isTrue();

        run(n0.shutdown);

        assertThat(rt.getAddressSpaceView().getTrimMark()).isEqualTo(entriesCount);

        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_MODERATE; i++) {
            if (rt.getLayoutView().getLayout().getEpoch() > layoutAfterAdds.getEpoch()) {
                break;
            }
            rt.invalidateLayout();
            Sleep.sleepUninterruptibly(PARAMETERS.TIMEOUT_SHORT);
        }

        // Assert that the new nodes should have the correct trimMark.
        assertThat(rt.getLayoutView().getRuntimeLayout().getLogUnitClient("localhost:9001").getTrimMark().get())
                .isEqualTo(prefixTrimAddress + 1);
        assertThat(rt.getLayoutView().getRuntimeLayout().getLogUnitClient("localhost:9002").getTrimMark().get())
                .isEqualTo(prefixTrimAddress + 1);
        FastObjectLoader fastObjectLoader = new FastObjectLoader(rt);
        fastObjectLoader.setRecoverSequencerMode(true);
        fastObjectLoader.loadMaps();
        assertThat(fastObjectLoader.getStreamTails().get(CorfuRuntime.getStreamID(streamName)))
                .isEqualTo(streamTail);

        // Shutdown two nodes
        run(n1.shutdown, n2.shutdown);
    }
}
