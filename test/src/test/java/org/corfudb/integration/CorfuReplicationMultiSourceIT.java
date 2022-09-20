package org.corfudb.integration;

import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterManager;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata;
import org.corfudb.infrastructure.logreplication.proto.Sample;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.ExampleSchemas;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStreamEntry;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.test.SampleSchema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager.LR_STATUS_STREAM_TAG;

/**
 * This IT uses a topology of 3 Source clusters and 1 Sink cluster, each with its own Corfu Server.  The 3 Source
 * clusters replicate to the same Sink cluster.  The set of streams to replicate as received from the
 * LogReplicationConfig is the same for all clusters.
 */
@Slf4j
@RunWith(Parameterized.class)
public class CorfuReplicationMultiSourceIT extends CorfuReplicationMultiSourceSinkIT {

    private Table<Sample.StringKey, SampleSchema.ValueFieldTagOne, Message> table1;
    private Table<Sample.StringKey, SampleSchema.ValueFieldTagOne, Message> table2;
    private Table<Sample.StringKey, SampleSchema.ValueFieldTagOne, Message> table3;
    private Table<Sample.StringKey, SampleSchema.ValueFieldTagOne, Message> table4Source1;
    private Table<Sample.StringKey, SampleSchema.ValueFieldTagOne, Message> table4Source2;
    private Table<Sample.StringKey, SampleSchema.ValueFieldTagOne, Message> table4Source3;
    private Table<Sample.StringKey, SampleSchema.ValueFieldTagOne, Message> table4Sink1;

    private ReplicatedStreamsListener streamsListener;
    private ReplicatedStreamsListener sourceStreamsListener1;
    private ReplicatedStreamsListener sourceStreamsListener2;
    private ReplicatedStreamsListener sourceStreamsListener3;

    public CorfuReplicationMultiSourceIT(String pluginConfigFilePath) {
        this.pluginConfigFilePath = pluginConfigFilePath;
    }

    // Static method that generates and returns test data (automatically test for two transport protocols: netty and
    // GRPC)
    @Parameterized.Parameters
    public static Collection input() {

        List<String> transportPlugins = Arrays.asList(
            "src/test/resources/transport/grpcConfig.properties",
            "src/test/resources/transport/nettyConfig.properties");

        List<String> absolutePathPlugins = new ArrayList<>();
        transportPlugins.forEach(plugin -> {
            File f = new File(plugin);
            absolutePathPlugins.add(f.getAbsolutePath());
        });
        return absolutePathPlugins;
    }

    @Before
    public void setUp() throws Exception {
        // Setup Corfu on 3 LR Source Sites and 1 LR Sink Site
        setupSourceAndSinkCorfu();
    }

    /**
     * The test verifies snapshot and log entry sync on a topology with 3 Source clusters and 1 Sink cluster.
     * The 3 Source clusters replicate to the same Sink cluster.  The set of streams to replicate as received from the
     * LogReplicationConfig is the same for all clusters.
     * However, Source Cluster 1 has data in Table001, Source Cluster 2 in Table002 and Source Cluster 3 in Table003.
     *
     * During snapshot sync, a 'clear' on Table002 from Source Cluster 1 can overwrite the updates written by Source
     * Cluster 2 because the streams to replicate set is the same.  In the test, a listener listens to updates on
     * each table and filters out 'single clear' entries during snapshot sync and collects the other updates.  It then
     * verifies that the required number of updates were received on the expected table.
     *
     * Verification of entries being replicated during log entry sync is straightforward and needs no such filtering.
     *
     * The test also verifies that the expected number of writes were made to the ReplicationStatus table on the Sink.
     * The number of updates depends on the number of available Source clusters(3 in this case).
     *
     */
    @Test
    public void testUpdatesOnReplicatedTables() throws Exception {
        verifySnapshotAndLogEntrySink();
    }

    private void verifySnapshotAndLogEntrySink() throws Exception {
        int numSourceClusters = MAX_REMOTE_CLUSTERS;

        // Open maps on the Source and Destination Sites
        openMaps();
        log.debug("Write 3 records locally on each Source Cluster");

        // Write data to all the source sites
        writeData(corfuStoreSource1, TABLE_1, table1, 0, NUM_RECORDS_IN_TABLE);
        writeData(corfuStoreSource2, TABLE_2, table2, 0, NUM_RECORDS_IN_TABLE);
        writeData(corfuStoreSource3, TABLE_3, table3, 0, NUM_RECORDS_IN_TABLE);

        // Register a stream listener on the Sink cluster to listen for incoming replication data writes
        // During snapshot sync, there will be 1 write(with all entries) for each replicated table with data.  The
        // listener ignores 'single clear' writes for the countdown latch so there will be 1 write per Source
        // cluster, i.e., Source Cluster 1's writes to Table001 and so on.
        CountDownLatch snapshotWritesLatch = new CountDownLatch(numSourceClusters);
        streamsListener = new ReplicatedStreamsListener(snapshotWritesLatch, true);
        corfuStoreSink1.subscribeListener(streamsListener, NAMESPACE, STREAM_TAG);

        // Subscribe to updates to the Replication Status table on Sink
        corfuStoreSink1.openTable(LogReplicationMetadataManager.NAMESPACE,
            LogReplicationMetadataManager.REPLICATION_STATUS_TABLE, LogReplicationMetadata.ReplicationStatusKey.class,
            LogReplicationMetadata.ReplicationStatusVal.class, null,
            TableOptions.fromProtoSchema(LogReplicationMetadata.ReplicationStatusVal.class));

        // On startup, an initial default replication status is written for each
        // remote cluster(NUM_INITIAL_REPLICATION_STATUS_UPDATES).  Subsequently, the table will be updated on
        // snapshot sync from each Source cluster.
        int numExpectedUpdates = NUM_INITIAL_REPLICATION_STATUS_UPDATES +
            calculateSnapshotSyncUpdatesOnSinkStatusTable(numSourceClusters);
        CountDownLatch statusUpdateLatch = new CountDownLatch(numExpectedUpdates);
        sinkListener1 = new ReplicationStatusListener(statusUpdateLatch);
        corfuStoreSink1.subscribeListener(sinkListener1, LogReplicationMetadataManager.NAMESPACE, LR_STATUS_STREAM_TAG);

        startReplicationServers();
        statusUpdateLatch.await();
        snapshotWritesLatch.await();

        // There will be 1 Clear + NUM_RECORDS_IN_TABLE Update records for each table
        Assert.assertEquals(NUM_RECORDS_IN_TABLE + 1, streamsListener.getTableToOpTypeMap().get(TABLE_1).size());
        Assert.assertEquals(NUM_RECORDS_IN_TABLE + 1, streamsListener.getTableToOpTypeMap().get(TABLE_2).size());
        Assert.assertEquals(NUM_RECORDS_IN_TABLE + 1, streamsListener.getTableToOpTypeMap().get(TABLE_3).size());

        // Verify the operations are as expected and in the right order
        List<CorfuStreamEntry.OperationType> expectedOpsList = Arrays.asList(CorfuStreamEntry.OperationType.CLEAR,
            CorfuStreamEntry.OperationType.UPDATE, CorfuStreamEntry.OperationType.UPDATE,
            CorfuStreamEntry.OperationType.UPDATE);

        Assert.assertTrue(Objects.equals(expectedOpsList, streamsListener.getTableToOpTypeMap().get(TABLE_1)));
        Assert.assertTrue(Objects.equals(expectedOpsList, streamsListener.getTableToOpTypeMap().get(TABLE_2)));
        Assert.assertTrue(Objects.equals(expectedOpsList, streamsListener.getTableToOpTypeMap().get(TABLE_3)));

        // Verify LogEntry Sync by writing and deleting a record each (2 updates)
        // Set the expected countdown latch to 2
        CountDownLatch logEntryWritesLatch = new CountDownLatch(2);
        streamsListener.setCountdownLatch(logEntryWritesLatch);
        streamsListener.clearTableToOpTypeMap();
        streamsListener.setSnapshotSync(false);

        log.debug("Add a record on table on Sender-1, Table-1");
        writeData(corfuStoreSource1, TABLE_1, table1, NUM_RECORDS_IN_TABLE, 1);

        log.debug("Delete a record from Sender-2, Table-2");
        log.debug("Delete Key2");
        deleteRecord(corfuStoreSource2, TABLE_2, 2);

        logEntryWritesLatch.await();
        Assert.assertEquals(1, streamsListener.getTableToOpTypeMap().get(TABLE_1).size());
        Assert.assertEquals(CorfuStreamEntry.OperationType.UPDATE,
            streamsListener.getTableToOpTypeMap().get(TABLE_1).get(0));

        Assert.assertEquals(1, streamsListener.getTableToOpTypeMap().get(TABLE_2).size());
        Assert.assertEquals(CorfuStreamEntry.OperationType.DELETE,
            streamsListener.getTableToOpTypeMap().get(TABLE_2).get(0));
    }

    /**
     * Verify snapshot and log entry sync in a topology with 3 Source clusters and 1 Sink cluster.
     * Then perform a role switch by changing the topology to 3 Sink clusters, 1 Source cluster and verify that the new
     * Sink clusters successfully complete a snapshot sync.
     * @throws Exception
     */
    @Test
    public void testRoleChange() throws Exception {
        verifySnapshotAndLogEntrySink();

        // After a role change, there will be 1 Source cluster
        int numSourceClusters = 1;

        // Write data to a different table(Table004) on the current Sink.  This table did not have any replicated data
        // and must be empty.
        Assert.assertEquals(0, table4Sink1.count());
        writeData(corfuStoreSink1, TABLE_4, table4Sink1, 0, NUM_RECORDS_IN_TABLE);

        // Subscribe the to-be Sink, now-Source Clusters to replication writes on Table004
        CountDownLatch snapshotWritesLatch1 = new CountDownLatch(numSourceClusters);
        sourceStreamsListener1 = new ReplicatedStreamsListener(snapshotWritesLatch1, true);
        corfuStoreSource1.subscribeListener(sourceStreamsListener1, NAMESPACE, STREAM_TAG, Arrays.asList(TABLE_4));

        CountDownLatch snapshotWritesLatch2 = new CountDownLatch(numSourceClusters);
        sourceStreamsListener2 = new ReplicatedStreamsListener(snapshotWritesLatch2, true);
        corfuStoreSource2.subscribeListener(sourceStreamsListener2, NAMESPACE, STREAM_TAG, Arrays.asList(TABLE_4));

        CountDownLatch snapshotWritesLatch3 = new CountDownLatch(numSourceClusters);
        sourceStreamsListener3 = new ReplicatedStreamsListener(snapshotWritesLatch3, true);
        corfuStoreSource3.subscribeListener(sourceStreamsListener3, NAMESPACE, STREAM_TAG, Arrays.asList(TABLE_4));


        // Verify no data exists on this table on any current Source cluster
        Assert.assertEquals(0, table4Source1.count());
        Assert.assertEquals(0, table4Source2.count());
        Assert.assertEquals(0, table4Source3.count());

        // Perform a role switch.  This will change the topology (3 Source, 1 Sink) -> (3 Sink, 1 Source)
        Table<ExampleSchemas.ClusterUuidMsg, ExampleSchemas.ClusterUuidMsg, ExampleSchemas.ClusterUuidMsg> configTable =
            corfuStoreSource1.openTable(DefaultClusterManager.CONFIG_NAMESPACE, DefaultClusterManager.CONFIG_TABLE_NAME,
                ExampleSchemas.ClusterUuidMsg.class, ExampleSchemas.ClusterUuidMsg.class,
                ExampleSchemas.ClusterUuidMsg.class, TableOptions.fromProtoSchema(ExampleSchemas.ClusterUuidMsg.class));

        try (TxnContext txn = corfuStoreSource1.txn(DefaultClusterManager.CONFIG_NAMESPACE)) {
            txn.putRecord(configTable, DefaultClusterManager.OP_SWITCH, DefaultClusterManager.OP_SWITCH,
                DefaultClusterManager.OP_SWITCH);
            txn.commit();
        }
        assertThat(configTable.count()).isOne();
        snapshotWritesLatch1.await();
        snapshotWritesLatch2.await();
        snapshotWritesLatch3.await();

        // Also verify that Table004 now has replicated data
        Assert.assertEquals(NUM_RECORDS_IN_TABLE, table4Source1.count());
        Assert.assertEquals(NUM_RECORDS_IN_TABLE, table4Source2.count());
        Assert.assertEquals(NUM_RECORDS_IN_TABLE, table4Source3.count());
    }

    private void setupSourceAndSinkCorfu() throws Exception {
        sourceCorfu1 = runServer(sourceSiteCorfuPort1, true);
        sourceCorfu2 = runServer(sourceSiteCorfuPort2, true);
        sourceCorfu3 = runServer(sourceSiteCorfuPort3, true);

        sinkCorfu1 = runServer(sinkSiteCorfuPort1, true);

        // Setup the runtimes to each Corfu server
        CorfuRuntime.CorfuRuntimeParameters params = CorfuRuntime.CorfuRuntimeParameters
            .builder()
            .build();

        sourceRuntime1 = CorfuRuntime.fromParameters(params);
        sourceRuntime1.parseConfigurationString(sourceEndpoint1);
        sourceRuntime1.connect();

        sourceRuntime2 = CorfuRuntime.fromParameters(params);
        sourceRuntime2.parseConfigurationString(sourceEndpoint2);
        sourceRuntime2.connect();

        sourceRuntime3 = CorfuRuntime.fromParameters(params);
        sourceRuntime3.parseConfigurationString(sourceEndpoint3);
        sourceRuntime3.connect();

        sinkRuntime1 = CorfuRuntime.fromParameters(params);
        sinkRuntime1.parseConfigurationString(sinkEndpoint1);
        sinkRuntime1.connect();

        corfuStoreSource1 = new CorfuStore(sourceRuntime1);
        corfuStoreSource2 = new CorfuStore(sourceRuntime2);
        corfuStoreSource3 = new CorfuStore(sourceRuntime3);
        corfuStoreSink1 = new CorfuStore(sinkRuntime1);
    }

    private void openMaps() throws Exception {
        // Source tables
        table1 = corfuStoreSource1.openTable(NAMESPACE, TABLE_1, Sample.StringKey.class,
            SampleSchema.ValueFieldTagOne.class, null,
            TableOptions.fromProtoSchema(SampleSchema.ValueFieldTagOne.class));

        table2 = corfuStoreSource2.openTable(NAMESPACE, TABLE_2, Sample.StringKey.class,
            SampleSchema.ValueFieldTagOne.class, null,
            TableOptions.fromProtoSchema(SampleSchema.ValueFieldTagOne.class));

        table3 = corfuStoreSource3.openTable(NAMESPACE, TABLE_3, Sample.StringKey.class,
            SampleSchema.ValueFieldTagOne.class, null,
            TableOptions.fromProtoSchema(SampleSchema.ValueFieldTagOne.class));

        table4Source1 = corfuStoreSource1.openTable(NAMESPACE, TABLE_4, Sample.StringKey.class,
            SampleSchema.ValueFieldTagOne.class, null,
            TableOptions.fromProtoSchema(SampleSchema.ValueFieldTagOne.class));

        table4Source2 = corfuStoreSource2.openTable(NAMESPACE, TABLE_4, Sample.StringKey.class,
            SampleSchema.ValueFieldTagOne.class, null,
            TableOptions.fromProtoSchema(SampleSchema.ValueFieldTagOne.class));

        table4Source3 = corfuStoreSource3.openTable(NAMESPACE, TABLE_4, Sample.StringKey.class,
            SampleSchema.ValueFieldTagOne.class, null,
            TableOptions.fromProtoSchema(SampleSchema.ValueFieldTagOne.class));

        // Sink tables
        corfuStoreSink1.openTable(NAMESPACE, TABLE_1, Sample.StringKey.class, SampleSchema.ValueFieldTagOne.class,
            null, TableOptions.fromProtoSchema(SampleSchema.ValueFieldTagOne.class));

        corfuStoreSink1.openTable(NAMESPACE, TABLE_2, Sample.StringKey.class, SampleSchema.ValueFieldTagOne.class,
            null, TableOptions.fromProtoSchema(SampleSchema.ValueFieldTagOne.class));

        corfuStoreSink1.openTable(NAMESPACE, TABLE_3, Sample.StringKey.class, SampleSchema.ValueFieldTagOne.class,
            null, TableOptions.fromProtoSchema(SampleSchema.ValueFieldTagOne.class));

        table4Sink1 = corfuStoreSink1.openTable(NAMESPACE, TABLE_4, Sample.StringKey.class,
            SampleSchema.ValueFieldTagOne.class, null,
            TableOptions.fromProtoSchema(SampleSchema.ValueFieldTagOne.class));
    }

    private void startReplicationServers() throws Exception {
        sourceReplicationServer1 = runReplicationServer(sourceReplicationPort1, pluginConfigFilePath);
        sourceReplicationServer2 = runReplicationServer(sourceReplicationPort2, pluginConfigFilePath);
        sourceReplicationServer3 = runReplicationServer(sourceReplicationPort3, pluginConfigFilePath);
        sinkReplicationServer1 = runReplicationServer(sinkReplicationPort1, pluginConfigFilePath);
    }

    @After
    public void tearDown() {
        super.tearDown();
        corfuStoreSink1.unsubscribeListener(streamsListener);

    }
}
