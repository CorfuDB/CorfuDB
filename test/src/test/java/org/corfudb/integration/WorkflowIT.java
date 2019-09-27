package org.corfudb.integration;

import static org.assertj.core.api.Assertions.*;

import com.google.common.reflect.TypeToken;
import lombok.extern.slf4j.Slf4j;

import static org.corfudb.integration.Harness.run;
import org.corfudb.integration.cluster.Harness.Node;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.MultiCheckpointWriter;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.Sleep;

import org.junit.Test;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * This integration test verifies the behaviour of the add node workflow. In particular, a single node
 * cluster is created and then populated with data, then a new node is added to the cluster,
 * making it of size 2. Checkpointing is then triggered so that the new second node starts servicing
 * new writes, then a 3rd node is added. The third node will have the checkpoints of the CorfuTable
 * that were populated and checkpointed when the cluster was only 2 nodes. Finally, a client reads
 * back the data generated while growing the cluster to verify that it is correct and can be read
 * from a three node cluster.
 * <p>
 *
 * Created by Maithem on 12/1/17.
 */
@Slf4j
public class WorkflowIT extends AbstractIT {

    private final String host = "localhost";

    String getConnectionString(int port) {
        return host + ":" + port;
    }

    final Duration timeout = Duration.ofMinutes(5);
    final Duration pollPeriod = Duration.ofMillis(50);
    final int workflowNumRetry = 3;

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

        runtime = new CorfuRuntime(getConnectionString(n1Port))
                .setCacheDisabled(true).connect();

        CorfuTable<String, String> table = runtime.getObjectsView()
                .build()
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {})
                .setStreamName(streamName)
                .open();

        for (int x = 0; x < numIter; x++) {
            table.put(String.valueOf(x), String.valueOf(x));
        }

        runtime.getManagementView().addNode(getConnectionString(n2Port), workflowNumRetry,
                timeout, pollPeriod);

        // Wait for servers
        final int clusterSizeN2 = 2;
        waitForLayoutChange(layout -> layout.getAllServers().size() == clusterSizeN2
                && layout.getSegments().size() == 1, runtime);

        MultiCheckpointWriter mcw = new MultiCheckpointWriter();
        mcw.addMap(table);

        Token prefix = mcw.appendCheckpoints(runtime, "Maithem");

        runtime.getAddressSpaceView().invalidateClientCache();
        runtime.getAddressSpaceView().invalidateServerCaches();

        // Add a third node after compaction
        runtime.getManagementView().addNode(getConnectionString(n3Port), workflowNumRetry,
                timeout, pollPeriod);

        // Verify that the third node has been added and data can be read back
        final int clusterSizeN3 = 3;
        waitForLayoutChange(layout -> layout.getAllServers().size() == clusterSizeN3
                && layout.getSegments().size() == 1, runtime);

        // Remove node 2
        runtime.getManagementView().removeNode(getConnectionString(n2Port), workflowNumRetry,
                timeout, pollPeriod);
        runtime.invalidateLayout();
        assertThat(runtime.getLayoutView().getLayout().getAllServers().size()).isEqualTo(clusterSizeN2);


        // Remove node 2 again and verify that the epoch doesn't change
        runtime.getManagementView().removeNode(getConnectionString(n2Port), workflowNumRetry,
                timeout, pollPeriod);
        shutdownCorfuServer(server_2);

        // Verify that the layout epoch hasn't changed after the second remove and that
        // the sequencers/layouts/segments nodes include the first and third node
        waitForLayoutChange(layout ->
                layout.getAllServers().size() == 2
                        && !layout.getAllServers().contains(getConnectionString(n2Port)), runtime);

        // Force remove node 3
        runtime.getManagementView().forceRemoveNode(getConnectionString(n3Port), workflowNumRetry,
                timeout, pollPeriod);
        shutdownCorfuServer(server_3);

        runtime.invalidateLayout();
        assertThat(runtime.getLayoutView().getLayout().getAllServers().size()).isEqualTo(1);

        server_2 = runServer(n2Port, false);
        // Re-add node 2
        runtime.getManagementView().addNode(getConnectionString(n2Port), workflowNumRetry,
                timeout, pollPeriod);

        waitForLayoutChange(layout -> layout.getAllServers().size() == clusterSizeN2
                && layout.getSegments().size() == 1, runtime);

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

        runtime = new CorfuRuntime(getConnectionString(n0Port)).connect();
        CorfuTable<String, String> table = runtime.getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {})
                .setStreamName("table1").open();

        final int iter = 1000;
        for (int x = 0; x < iter; x++) {
            table.put(String.valueOf(x), String.valueOf(x));
        }

        runtime.getManagementView().addNode(getConnectionString(n1Port), workflowNumRetry,
                timeout, pollPeriod);
        runtime.invalidateLayout();

        assertThat(runtime.getLayoutView().getLayout().getAllServers().size()).isEqualTo(clusterSizeN2);

        runtime.getManagementView().forceRemoveNode(getConnectionString(n1Port), workflowNumRetry,
                timeout, pollPeriod);
        runtime.invalidateLayout();

        assertThat(runtime.getLayoutView().getLayout().getAllServers().size()).isEqualTo(clusterSizeN1);

        runtime.getManagementView().addNode(getConnectionString(n1Port), workflowNumRetry,
                timeout, pollPeriod);

        runtime.getManagementView().addNode(getConnectionString(n2Port), workflowNumRetry,
                timeout, pollPeriod);

        runtime.invalidateLayout();
        assertThat(runtime.getLayoutView().getLayout().getAllServers().size()).isEqualTo(clusterSizeN3);

        runtime.getManagementView().removeNode(getConnectionString(n1Port), workflowNumRetry,
                timeout, pollPeriod);
        runtime.invalidateLayout();
        assertThat(runtime.getLayoutView().getLayout().getAllServers().size()).isEqualTo(clusterSizeN2);

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

        runtime = new CorfuRuntime(getConnectionString(n0Port)).connect();
        CorfuTable<String, String> table = runtime.getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {})
                .setStreamName("table1").open();

        final int iter = 100;
        for (int x = 0; x < iter; x++) {
            table.put(String.valueOf(x), String.valueOf(x));
        }

        runtime.getManagementView().addNode(getConnectionString(n1Port), workflowNumRetry,
                timeout, pollPeriod);

        runtime.getManagementView().addNode(getConnectionString(n2Port), workflowNumRetry,
                timeout, pollPeriod);

        runtime.invalidateLayout();
        assertThat(runtime.getLayoutView().getLayout().getAllServers().size()).isEqualTo(clusterSizeN3);

        // Kill two nodes from a three node cluster
        shutdownCorfuServer(p1);
        shutdownCorfuServer(p2);

        // Force remove the "failed" node
        runtime.getManagementView().forceRemoveNode(getConnectionString(n1Port), workflowNumRetry,
                timeout, pollPeriod);
        runtime.getManagementView().forceRemoveNode(getConnectionString(n2Port), workflowNumRetry,
                timeout, pollPeriod);
        runtime.invalidateLayout();
        assertThat(runtime.getLayoutView().getLayout().getAllServers().size()).isEqualTo(clusterSizeN1);

        for (int x = 0; x < iter; x++) {
            assertThat(table.get(String.valueOf(x))).isEqualTo(String.valueOf(x));
        }

        shutdownCorfuServer(p0);
    }
}
