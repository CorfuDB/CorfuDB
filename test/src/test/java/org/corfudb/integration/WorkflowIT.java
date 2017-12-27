package org.corfudb.integration;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.orchestrator.CreateWorkflowResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.MultiCheckpointWriter;
import org.corfudb.runtime.clients.ManagementClient;
import org.corfudb.runtime.collections.CorfuTable;
import org.junit.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 *
 * This integration test verifies the behaviour of the add node workflow. In particular, a single node
 * cluster is created and then populated with data, then a new node is added to the cluster,
 * making it of size 2. Checkpointing is then triggered so that the new second node starts servicing
 * new writes, then a 3rd node is added. The third node will have the checkpoints of the CorfuTable
 * that were populated and checkpointed when the cluster was only 2 nodes. Finally, a client reads
 * back the data generated while growing the cluster to verify that it is correct and can be read
 * from a three node cluster.
 *
 * Created by Maithem on 12/1/17.
 */
@Slf4j
public class WorkflowIT extends AbstractIT {

    final String host = "localhost";

    final int maxTries = 10;

    final int sleepTime = 5_000;

    String getConnectionString(int port) {
        return host + ":" + port;
    }

    @Test
    public void AddRemoveNodeTestIT() throws Exception {
        final String host = "localhost";
        final String streamName = "s1";
        final int n1Port = 9000;

        // Start node one and populate it with data
        new CorfuServerRunner()
                .setHost(host)
                .setPort(n1Port)
                .setSingle(true)
                .runServer();

        CorfuRuntime n1Rt = new CorfuRuntime(getConnectionString(n1Port)).connect();

        CorfuTable table = n1Rt.getObjectsView()
                .build()
                .setType(CorfuTable.class)
                .setStreamName(streamName)
                .open();

        final int numEntries = 12_000;
        for (int x = 0; x < numEntries; x++) {
            table.put(String.valueOf(x), String.valueOf(x));
        }

        // Add a second node
        final int n2Port = 9001;
        new CorfuServerRunner()
                .setHost(host)
                .setPort(n2Port)
                .runServer();

        ManagementClient mgmt = n1Rt.getRouter(getConnectionString(n1Port))
                .getClient(ManagementClient.class);

        CreateWorkflowResponse resp = mgmt.addNodeRequest(getConnectionString(n2Port));

        assertThat(resp.getWorkflowId()).isNotNull();

        waitForWorkflow(resp.getWorkflowId(), n1Rt, n1Port);

        n1Rt.invalidateLayout();
        final int clusterSizeN2 = 2;
        assertThat(n1Rt.getLayoutView().getLayout().getAllServers().size()).isEqualTo(clusterSizeN2);

        // Verify that the workflow ID for node 2 is no longer active
        assertThat(mgmt.queryRequest(resp.getWorkflowId()).isActive()).isFalse();

        MultiCheckpointWriter mcw = new MultiCheckpointWriter();
        mcw.addMap(table);

        long prefix = mcw.appendCheckpoints(n1Rt, "Maithem");

        n1Rt.getAddressSpaceView().prefixTrim(prefix - 1);

        n1Rt.getAddressSpaceView().invalidateClientCache();
        n1Rt.getAddressSpaceView().invalidateServerCaches();
        n1Rt.getAddressSpaceView().gc();

        // Add a third node after compaction

        final int n3Port = 9002;
        new CorfuServerRunner()
                .setHost(host)
                .setPort(n3Port)
                .runServer();

        CreateWorkflowResponse resp2 = mgmt.addNodeRequest(getConnectionString(n3Port));
        assertThat(resp2.getWorkflowId()).isNotNull();

        waitForWorkflow(resp2.getWorkflowId(), n1Rt, n1Port);

        // Verify that the third node has been added and data can be read back
        n1Rt.invalidateLayout();

        final int clusterSizeN3 = 3;
        assertThat(n1Rt.getLayoutView().getLayout().getAllServers().size()).isEqualTo(clusterSizeN3);
        // Verify that the workflow ID for node 3 is no longer active
        assertThat(mgmt.queryRequest(resp2.getWorkflowId()).isActive()).isFalse();

        // Remove node 2
        CreateWorkflowResponse  resp3 = mgmt.removeNode(getConnectionString(n2Port));
        waitForWorkflow(resp3.getWorkflowId(), n1Rt, n1Port);
        n1Rt.invalidateLayout();
        assertThat(n1Rt.getLayoutView().getLayout().getAllServers().size()).isEqualTo(clusterSizeN2);

        // Force remove node 3
        CreateWorkflowResponse resp4 = mgmt.forceRemoveNode(getConnectionString(n3Port));
        waitForWorkflow(resp4.getWorkflowId(), n1Rt, n1Port);
        n1Rt.invalidateLayout();
        assertThat(n1Rt.getLayoutView().getLayout().getAllServers().size()).isEqualTo(1);

        // Re-add node 2
        CreateWorkflowResponse resp5 = mgmt.addNodeRequest(getConnectionString(n2Port));
        waitForWorkflow(resp5.getWorkflowId(), n1Rt, n1Port);
        n1Rt.invalidateLayout();
        assertThat(n1Rt.getLayoutView().getLayout().getAllServers().size()).isEqualTo(clusterSizeN2);

        for (int x = 0; x < numEntries; x++) {
            String v = (String) table.get(String.valueOf(x));
            assertThat(v).isEqualTo(String.valueOf(x));
        }
    }

    @Test
    public void clusterResizingTest1() throws Exception {
        // This tests will resize the cluster according to the following
        // order: add node -> add node -> remove node -> add node -> add node
        final int n0Port = 9000;
        final int n1Port = 9001;
        final int n2Port = 9002;

        final int clusterSizeN1 = 1;
        final int clusterSizeN2 = 2;
        final int clusterSizeN3 = 3;

        new CorfuServerRunner()
                .setHost(host)
                .setPort(n0Port)
                .setSingle(true)
                .runServer();

        new CorfuServerRunner()
                .setHost(host)
                .setPort(n1Port)
                .runServer();

        new CorfuServerRunner()
                .setHost(host)
                .setPort(n2Port)
                .runServer();

        CorfuRuntime n0Rt = new CorfuRuntime(getConnectionString(n0Port)).connect();
        CorfuTable table = n0Rt.getObjectsView().build()
                .setType(CorfuTable.class)
                .setStreamName("table1").open();

        final int iter = 100;
        for (int x = 0; x < iter; x++) {
            table.put(String.valueOf(x), String.valueOf(x));
        }

        ManagementClient mgmt = n0Rt.getRouter(getConnectionString(n0Port))
                .getClient(ManagementClient.class);

        UUID id1 = mgmt.addNodeRequest(getConnectionString(n1Port)).getWorkflowId();
        waitForWorkflow(id1, n0Rt, n0Port);

        n0Rt.invalidateLayout();
        assertThat(n0Rt.getLayoutView().getLayout().getAllServers().size()).isEqualTo(clusterSizeN2);

        UUID id2 = mgmt.forceRemoveNode(getConnectionString(n1Port)).getWorkflowId();
        waitForWorkflow(id2, n0Rt, n0Port);
        assertThat(n0Rt.getLayoutView().getLayout().getAllServers().size()).isEqualTo(clusterSizeN1);

        UUID id3 = mgmt.addNodeRequest(getConnectionString(n1Port)).getWorkflowId();
        waitForWorkflow(id3, n0Rt, n0Port);

        UUID id4 = mgmt.addNodeRequest(getConnectionString(n2Port)).getWorkflowId();
        waitForWorkflow(id4, n0Rt, n0Port);

        n0Rt.invalidateLayout();
        assertThat(n0Rt.getLayoutView().getLayout().getAllServers().size()).isEqualTo(clusterSizeN3);

        UUID id5 = mgmt.removeNode(getConnectionString(n1Port)).getWorkflowId();
        waitForWorkflow(id5, n0Rt, n0Port);
        assertThat(n0Rt.getLayoutView().getLayout().getAllServers().size()).isEqualTo(clusterSizeN2);

        n0Rt.invalidateLayout();

        for (int x = 0; x < iter; x++) {
            assertThat(table.get(String.valueOf(x))).isEqualTo(String.valueOf(x));
        }
    }

    @Test
    public void clusterResizingTest2() throws Exception {
        final int n0Port = 9000;
        final int n1Port = 9001;

        final int clusterSizeN1 = 1;
        final int clusterSizeN2 = 2;

        new CorfuServerRunner()
                .setHost(host)
                .setPort(n0Port)
                .setSingle(true)
                .runServer();

        Process p2 = new CorfuServerRunner()
                .setHost(host)
                .setPort(n1Port)
                .runServer();

        CorfuRuntime n0Rt = new CorfuRuntime(getConnectionString(n0Port)).connect();
        CorfuTable table = n0Rt.getObjectsView().build()
                .setType(CorfuTable.class)
                .setStreamName("table1").open();

        final int iter = 100;
        for (int x = 0; x < iter; x++) {
            table.put(String.valueOf(x), String.valueOf(x));
        }

        ManagementClient mgmt = n0Rt.getRouter(getConnectionString(n0Port))
                .getClient(ManagementClient.class);

        UUID id1 = mgmt.addNodeRequest(getConnectionString(n1Port)).getWorkflowId();
        waitForWorkflow(id1, n0Rt, n0Port);

        n0Rt.invalidateLayout();
        assertThat(n0Rt.getLayoutView().getLayout().getAllServers().size()).isEqualTo(clusterSizeN2);

        // Kill one node from a two node cluster
        p2.destroy();

        // Force remove the "failed" node
        CreateWorkflowResponse resp4 = mgmt.forceRemoveNode(getConnectionString(n1Port));
        waitForWorkflow(resp4.getWorkflowId(), n0Rt, n0Port);
        n0Rt.invalidateLayout();
        assertThat(n0Rt.getLayoutView().getLayout().getAllServers().size()).isEqualTo(clusterSizeN1);

        for (int x = 0; x < iter; x++) {
            assertThat(table.get(String.valueOf(x))).isEqualTo(String.valueOf(x));
        }
    }

    void waitForWorkflow(UUID id, CorfuRuntime rt, int port) throws Exception {
        ManagementClient mgmt = rt.getRouter(getConnectionString(port))
                .getClient(ManagementClient.class);
        for (int x = 0; x < maxTries; x++) {
            try {
                if (mgmt.queryRequest(id).isActive()) {
                    Thread.sleep(sleepTime);
                } else {
                    break;
                }
            } catch (Exception e) {
                rt.invalidateLayout();
                Thread.sleep(sleepTime);
            }
        }
    }
}
