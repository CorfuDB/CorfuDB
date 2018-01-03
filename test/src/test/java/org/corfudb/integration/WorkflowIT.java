package org.corfudb.integration;

import com.google.common.collect.Range;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.orchestrator.CreateWorkflowResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.MultiCheckpointWriter;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.clients.ManagementClient;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.util.CFUtils;
import org.junit.Test;

import java.util.Map;
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
    public void MergeTwoSingleNodeClustersIT() throws Exception {
        final String host = "localhost";
        final String streamName = "s1";
        final int n1Port = 9000;
        final int n2Port = 9001;

        // Start node one and populate it with data
        new CorfuServerRunner()
                .setHost(host)
                .setPort(n1Port)
                .setLogPath(getCorfuServerLogPath(host, n1Port))
                .setSingle(true)
                .runServer();

        CorfuRuntime n1Rt = new CorfuRuntime(getConnectionString(n1Port)).connect();

        CorfuTable table = n1Rt.getObjectsView()
                .build()
                .setType(CorfuTable.class)
                .setStreamName(streamName)
                .open();

        final long numEntries = 100;
        for (int x = 0; x < numEntries; x++) {
            table.put(String.valueOf(x), String.valueOf(x));
        }

        // Start node two and populate it with data
        new CorfuServerRunner()
                .setHost(host)
                .setPort(n2Port)
                .setLogPath(getCorfuServerLogPath(host, n2Port))
                .setSingle(true)
                .runServer();

        CorfuRuntime n2Rt = new CorfuRuntime(getConnectionString(n2Port)).connect();

        CorfuTable table2 = n2Rt.getObjectsView()
                .build()
                .setType(CorfuTable.class)
                .setStreamName(streamName)
                .open();

        for (int x = 0; x < numEntries; x++) {
            table2.put(String.valueOf(x + n2Port), String.valueOf(x));
        }

        // Add node two to the node one's cluster
        ManagementClient mgmt = n1Rt.getRouter(getConnectionString(n1Port))
                .getClient(ManagementClient.class);

        CreateWorkflowResponse resp = mgmt.addNodeRequest(getConnectionString(n2Port));

        waitForWorkflow(resp.getWorkflowId(), n1Rt, n1Port);

        n1Rt.invalidateLayout();
        final int clusterSizeN2 = 2;
        assertThat(n1Rt.getLayoutView().getLayout().getAllServers().size()).isEqualTo(clusterSizeN2);

        // Verify that the workflow ID for node 2 is no longer active
        assertThat(mgmt.queryRequest(resp.getWorkflowId()).isActive()).isFalse();


        n1Rt.getAddressSpaceView().invalidateClientCache();
        n1Rt.getAddressSpaceView().invalidateServerCaches();

        // Verify that node two's data was cleared and has the same data as node one after
        // the add node workflow completes
        Range<Long> range = Range.closed(0L, numEntries);

        Map<Long, LogData> readRes1 = CFUtils.getUninterruptibly(n1Rt
                .getRouter(getConnectionString(n1Port))
                .getClient(LogUnitClient.class).read(range))
                .getAddresses();

        Map<Long, LogData> readRes2 = CFUtils.getUninterruptibly(n1Rt
                .getRouter(getConnectionString(n2Port))
                .getClient(LogUnitClient.class).read(range))
                .getAddresses();

        for (long x = 0; x < numEntries; x++) {
            assertThat(readRes1.get(x).getData()).isEqualTo(readRes2.get(x).getData());
        }
    }

    @Test
    public void AddNodeIT() throws Exception {
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

        for (int x = 0; x < numEntries; x++) {
            String v = (String) table.get(String.valueOf(x));
            assertThat(v).isEqualTo(String.valueOf(x));
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
