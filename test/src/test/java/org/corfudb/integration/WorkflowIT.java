package org.corfudb.integration;

import static org.assertj.core.api.Assertions.*;
import static org.corfudb.integration.Harness.run;

import com.google.common.reflect.TypeToken;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.integration.cluster.Harness.Node;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.MultiCheckpointWriter;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.TrimmedException;
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

    final String host = "localhost";

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

        // Wait for servers
        final int clusterSizeN2 = 2;
        waitForLayoutChange(layout -> layout.getAllServers().size() == clusterSizeN2
                && layout.getSegments().size() == 1, n1Rt);

        MultiCheckpointWriter mcw = new MultiCheckpointWriter();
        mcw.addMap(table);

        Token prefix = mcw.appendCheckpoints(n1Rt, "Maithem");

        n1Rt.getAddressSpaceView().prefixTrim(prefix);

        n1Rt.getAddressSpaceView().invalidateClientCache();
        n1Rt.getAddressSpaceView().invalidateServerCaches();
        n1Rt.getAddressSpaceView().gc();

        // Add a third node after compaction
        n1Rt.getManagementView().addNode(getConnectionString(n3Port), workflowNumRetry,
                timeout, pollPeriod);

        // Verify that the third node has been added and data can be read back
        final int clusterSizeN3 = 3;
        waitForLayoutChange(layout -> layout.getAllServers().size() == clusterSizeN3
                && layout.getSegments().size() == 1, n1Rt);

        // Remove node 2
        n1Rt.getManagementView().removeNode(getConnectionString(n2Port), workflowNumRetry,
                timeout, pollPeriod);
        n1Rt.invalidateLayout();
        assertThat(n1Rt.getLayoutView().getLayout().getAllServers().size()).isEqualTo(clusterSizeN2);


        // Remove node 2 again and verify that the epoch doesn't change
        n1Rt.getManagementView().removeNode(getConnectionString(n2Port), workflowNumRetry,
                timeout, pollPeriod);
        shutdownCorfuServer(server_2);

        // Verify that the layout epoch hasn't changed after the second remove and that
        // the sequencers/layouts/segments nodes include the first and third node
        waitForLayoutChange(layout ->
                layout.getAllServers().size() == 2
                        && !layout.getAllServers().contains(getConnectionString(n2Port)), n1Rt);

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

        waitForLayoutChange(layout -> layout.getAllServers().size() == clusterSizeN2
                && layout.getSegments().size() == 1, n1Rt);

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
        Token prefixTrimAddress = mcw.appendCheckpoints(rt, "author");
        rt.getAddressSpaceView().prefixTrim(prefixTrimAddress);
        rt.getAddressSpaceView().invalidateServerCaches();
        rt.getAddressSpaceView().invalidateClientCache();
        rt.getAddressSpaceView().gc();

        assertThat(rt.getAddressSpaceView().getTrimMark().getSequence()).isEqualTo(entriesCount);

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

        assertThat(rt.getAddressSpaceView().getTrimMark().getSequence()).isEqualTo(entriesCount);

        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_MODERATE; i++) {
            if (rt.getLayoutView().getLayout().getEpoch() > layoutAfterAdds.getEpoch()) {
                break;
            }
            rt.invalidateLayout();
            Sleep.sleepUninterruptibly(PARAMETERS.TIMEOUT_SHORT);
        }

        // Assert that the new nodes should have the correct trimMark.
        assertThat(rt.getLayoutView().getRuntimeLayout().getLogUnitClient("localhost:9001").getTrimMark().get())
                .isEqualTo(prefixTrimAddress.getSequence() + 1);
        assertThat(rt.getLayoutView().getRuntimeLayout().getLogUnitClient("localhost:9002").getTrimMark().get())
                .isEqualTo(prefixTrimAddress.getSequence() + 1);
        assertThat(rt.getAddressSpaceView().getAllTails().getStreamTails().get(CorfuRuntime.getStreamID(streamName)))
                .isEqualTo(streamTail);

        // Shutdown two nodes
        run(n1.shutdown, n2.shutdown);
    }

    /**
     * This test checks that data is not lost if the runtime GC is triggered right after the stream
     * has synced back to a version that falls in the space of trimmed addresses.
     * To this end, we verify that after GC syncing to the most recent version of the stream does not end in data loss.
     *
     * The steps performed in this test are the following:
     *
     * 1. Create a table.
     * 2. Within a transaction write (numDataEntries - 2) entries to table.
     * 3. Checkpoint table.
     * 4. Write 2 more entries to table.
     *
     * Log should look like this:
     *
     * [         'table' Stream        ]            [ 'table' ]
     * +-------------------------------------------------------+
     * | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 | 11 | 12  |
     * +-------------------------------------------------------+
     *                                 [ checkpoint ]       ^
     *                                                     Global
     *                                                     Pointer
     *
     *  5. Initiate a snapshot transaction in a version that moves the pointer to the space of addresses to trim.
     *     We initiate a snapshot transaction to version 1.
     *
     * [         'table' Stream        ]            [ 'table' ]
     * +-------------------------------------------------------+
     * | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 | 11 | 12  |
     * +-------------------------------------------------------+
     *       ^                         [ checkpoint ]
     *      Global
     *      Pointer
     *
     *  6. Trim the log. On the server side, addresses 0-7 will be trimmed.
     *  7. Trigger runtime GC.
     *  8. Assert that after running GC we are not losing data.
     *  9. Trigger runtime GC for the second time (since GC is deferred in one cycle).
     *  10. Assert that initiating a snapshot transaction on a trimmed address is aborted with the right cause.
     *  11. Assert again that pointer was not moved and data is not lost.
     *
     */
    @Test
    public void testRuntimeGCForActiveTransactionsInTrimRangeSingleThread() throws Exception {
        // Run single node server and create runtime
        runDefaultServer();
        CorfuRuntime corfuRuntime = createDefaultRuntime();

        final int numDataEntries = 10;

        // (1)
        CorfuTable<Integer, String> table = corfuRuntime.getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<Integer, String>>() {
                })
                .setStreamName("test")
                .open();

        // (2)
        for (int i = 0; i < numDataEntries - 2; i++) {
            corfuRuntime.getObjectsView().TXBuild().type(TransactionType.WRITE_AFTER_WRITE).build().begin();
            table.put(i, String.valueOf(i));
            corfuRuntime.getObjectsView().TXEnd();
        }

        // (3)
        MultiCheckpointWriter mcw = new MultiCheckpointWriter();
        mcw.addMap(table);
        Token prefixTrim = mcw.appendCheckpoints(corfuRuntime, "author");

        // (4)
        for (int i = numDataEntries - 2; i < numDataEntries; i++) {
            corfuRuntime.getObjectsView().TXBuild().type(TransactionType.WRITE_AFTER_WRITE).build().begin();
            table.put(i, String.valueOf(i));
            corfuRuntime.getObjectsView().TXEnd();
        }

        // (5)
        // Force the global pointer to move back to the space of addresses to be trimmed.
        corfuRuntime.getObjectsView().TXBuild().type(TransactionType.SNAPSHOT)
                .snapshot(new Token(0,1))
                .build()
                .begin();
        table.get(0);
        corfuRuntime.getObjectsView().TXEnd();

        // (6)
        corfuRuntime.getAddressSpaceView().prefixTrim(prefixTrim);

        // (7)
        corfuRuntime.getGarbageCollector().runRuntimeGC();

        // (8)
        assertThat(table).hasSize(numDataEntries);
        for(int i = 0; i < numDataEntries; i++) {
            assertThat(table.get(i)).isEqualTo(String.valueOf(i));
        }

        // (9)
        corfuRuntime.getGarbageCollector().runRuntimeGC();

        // (10)
        assertThatThrownBy(() -> {
            corfuRuntime.getObjectsView().TXBuild().type(TransactionType.SNAPSHOT)
                .snapshot(new Token(0,1))
                .build()
                .begin();
            table.get(0);
            corfuRuntime.getObjectsView().TXEnd();})
                .isInstanceOf(TransactionAbortedException.class)
                .hasCauseInstanceOf(TrimmedException.class);

        // (11)
        assertThat(table).hasSize(numDataEntries);
        for(int i = 0; i < numDataEntries; i++) {
            assertThat(table.get(i)).isEqualTo(String.valueOf(i));
        }
    }

    /**
     * This test ensures that any ongoing transaction which snapshot is positioned
     * in the space of trimmed addresses does not incur in data loss. Steps of this test:
     *
     * 1. Create a table
     * 2. On T0 (thread 0, main thread) write one entry into the table. Await.
     *
     *    ['table']
     *     +---+
     *     | 0 |
     *     +---+
     *
     * 3. Start T1 (Thread 1). Begin write_after_write transaction, write one entry to set the tx snapshot to @0. Await.
     *    Note: this is not reflected in the log as the tx has not ended.
     * 4. On T0 write (numElements - 2) entries.
     *
     * [         'table' Stream        ]
     * +-------------------------------+
     * | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 |
     * +-------------------------------+
     *
     * 5. On T0 checkpoint.
     *
     * [         'table' Stream        ]
     * +--------------------------------------------+
     * | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 |
     * +--------------------------------------------+
     *                                 [ checkpoint ]
     *
     * 6. On T0 add 3 more entries to the log, log should look like this: (await)
     *
     *
     * [         'table' Stream        ]            [    'table'   ]
     * +-----------------------------------------------------------+
     * | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 | 11 | 12 | 13 |
     * +-----------------------------------------------------------+
     *                                 [ checkpoint ]            ^
     *                                                         Global
     *                                                         Pointer
     *
     * 7. On T1, do an access on table, to force globalPointer to move to snapshot position @0
     * 8. On T0, prefixTrim @7
     * 9. On T0, trigger runtimeGC
     * 10. Attempt to access table, we should not have any data loss.
     * 11. Initiate a snapshot transaction in the space of trimmed addresses, should be aborted.
     * 12. Attempt to access table, we should not have any data loss.
     *
     * @throws Exception
     */
    @Test
    public void testRuntimeGCForActiveTransactionsInTrimRangeMultiThread() throws Exception {
        // Run single node server and create runtime
        runDefaultServer();
        CorfuRuntime corfuRuntime = createDefaultRuntime();

        int initKey = 0;
        final int numDataEntries = 10;

        // (1)
        CorfuTable<Integer, String> table = corfuRuntime.getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<Integer, String>>() {
                })
                .setStreamName("test")
                .open();

        // (2)
        corfuRuntime.getObjectsView().TXBuild().type(TransactionType.WRITE_AFTER_WRITE).build().begin();
        table.put(initKey, String.valueOf(initKey));
        corfuRuntime.getObjectsView().TXEnd();

        initKey++;

        CountDownLatch countDownLatch1 = new CountDownLatch(1);
        CountDownLatch countDownLatch2 = new CountDownLatch(1);
        CountDownLatch countDownLatch3 = new CountDownLatch(1);

        Thread t = new Thread(() -> {
            // (3)
            corfuRuntime.getObjectsView().TXBuild().type(TransactionType.WRITE_AFTER_WRITE).build().begin();
            table.put(numDataEntries, String.valueOf(numDataEntries));
                    countDownLatch1.countDown();
            try {
                countDownLatch2.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            // TODO(Anny): when debugging found that on this access the VLO is reset, not sure if this is
            // efficient or there is a bug, further look into this...
            // (7)
            table.get(0);
            corfuRuntime.getObjectsView().TXEnd();
            countDownLatch3.countDown();
        });
        t.start();

        countDownLatch1.await();

        // (4)
        for (int i = initKey; i < numDataEntries - 2; i++) {
            corfuRuntime.getObjectsView().TXBuild().type(TransactionType.WRITE_AFTER_WRITE).build().begin();
            table.put(i, String.valueOf(i));
            corfuRuntime.getObjectsView().TXEnd();
        }

        // (5)
        MultiCheckpointWriter mcw = new MultiCheckpointWriter();
        mcw.addMap(table);
        Token prefixTrim = mcw.appendCheckpoints(corfuRuntime, "author");

        // (6)
        for (int i = numDataEntries - 2; i < numDataEntries; i++) {
            corfuRuntime.getObjectsView().TXBuild().type(TransactionType.WRITE_AFTER_WRITE).build().begin();
            table.put(i, String.valueOf(i));
            corfuRuntime.getObjectsView().TXEnd();
        }

        countDownLatch2.countDown();
        countDownLatch3.await();

        // (8)
        corfuRuntime.getAddressSpaceView().prefixTrim(prefixTrim);

        // (9) Note: run it twice so we enforce the trim at stream layer which is deferred in one cycle
        corfuRuntime.getGarbageCollector().runRuntimeGC();
        corfuRuntime.getGarbageCollector().runRuntimeGC();

        // (10)
        assertThat(table).hasSize(numDataEntries + 1);
        for(int i = 0; i <= numDataEntries; i++) {
            assertThat(table.get(i)).isEqualTo(String.valueOf(i));
        }

        t.join();

        // (11)
        assertThatThrownBy(() -> {
            final int snapshotTxAddress = 4;
            corfuRuntime.getObjectsView().TXBuild().type(TransactionType.SNAPSHOT)
                    .snapshot(new Token(0, snapshotTxAddress))
                    .build().begin();
            table.get(0);
            corfuRuntime.getObjectsView().TXEnd();
        }).isInstanceOf(TransactionAbortedException.class).hasCauseInstanceOf((TrimmedException.class));

        // (12)
        assertThat(table).hasSize(numDataEntries + 1);
        for(int i = 0; i <= numDataEntries; i++) {
            assertThat(table.get(i)).isEqualTo(String.valueOf(i));
        }
    }
}
