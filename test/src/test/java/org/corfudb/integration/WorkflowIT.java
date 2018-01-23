package org.corfudb.integration;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.MultiCheckpointWriter;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.LayoutBuilder;
import org.junit.Test;

import java.time.Duration;

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

    String getConnectionString(int port) {
        return host + ":" + port;
    }

    @Test
    public void AddAndRemoveNodeIT() throws Exception {
        final String host = "localhost";
        final String streamName = "s1";
        final int n1Port = 9000;
        final int numIter = 11_000;
        final Duration timeout = Duration.ofMinutes(5);
        final Duration pollPeriod = Duration.ofSeconds(5);
        final int workflowNumRetry = 3;

        // Start node one and populate it with data
        Process server_1 = new CorfuServerRunner()
                .setHost(host)
                .setPort(n1Port)
                .setSingle(true)
                .runServer();

        // start a second node
        final int n2Port = 9001;
        Process server_2 = new CorfuServerRunner()
                .setHost(host)
                .setPort(n2Port)
                .setSingle(false)
                .runServer();

        // start a third node
        final int n3Port = 9002;
        Process server_3 = new CorfuServerRunner()
                .setHost(host)
                .setPort(n3Port)
                .setSingle(false)
                .runServer();

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
        assertThat(n1Rt.getLayoutView().getLayout().getAllActiveServers().size()).isEqualTo(clusterSizeN2);

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
        assertThat(n1Rt.getLayoutView().getLayout().getAllActiveServers().size()).isEqualTo(clusterSizeN3);

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

        n1Rt.invalidateLayout();
        // Verify that the layout epoch hasn't changed after the second remove and that
        // the sequencers/layouts/segments nodes include the first and third node
        assertThat(n1Rt.getLayoutView().getLayout()).isEqualTo(expectedLayout);

        for (int x = 0; x < numIter; x++) {
            String v = (String) table.get(String.valueOf(x));
            assertThat(v).isEqualTo(String.valueOf(x));
        }

        shutdownCorfuServer(server_1);
        shutdownCorfuServer(server_2);
        shutdownCorfuServer(server_3);
    }
}
