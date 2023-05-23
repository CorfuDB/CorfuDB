package org.corfudb.integration;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterManager;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * This IT uses a topology of 3 Sink clusters and 1 Source cluster, each with its own Corfu Server.  The Source
 * cluster replicates to all 3 Sink clusters.  The set of streams to replicate as received from the
 * LogReplicationConfig is the same for all clusters.
 */
@Slf4j
@RunWith(Parameterized.class)
public class CorfuReplicationMultiSinkIT extends CorfuReplicationMultiSourceSinkIT {

    public CorfuReplicationMultiSinkIT(String pluginConfigFilePath) {
        this.pluginConfigFilePath = pluginConfigFilePath;
    }

    // Static method that generates and returns test data (automatically test for two transport protocols: netty and
    // GRPC)
    @Parameterized.Parameters
    public static Collection input() {

        List<String> transportPlugins = Arrays.asList(
            "src/test/resources/transport/pluginConfig.properties"
        );

        List<String> absolutePathPlugins = new ArrayList<>();
        transportPlugins.forEach(plugin -> {
            File f = new File(plugin);
            absolutePathPlugins.add(f.getAbsolutePath());
        });

        return absolutePathPlugins;
    }

    @Before
    public void setUp() throws Exception {
        super.setUp(1, MAX_REMOTE_CLUSTERS, DefaultClusterManager.TP_MULTI_SINK);
    }

    /**
     * The test verifies snapshot and log entry sync on a topology with 1 Source clusters and 3 Sink clusters.
     * The Source cluster replicates to all Sink clusters.  The set of streams to replicate as received from the
     * LogReplicationConfig is the same for all clusters.
     * Source Cluster 1 only has data in Table001.  This data will get replicated to all Sink clusters.
     *
     * The test verifies that the required number of updates were received on Table001 on all Sink clusters.
     * The test also verifies that the expected number of writes were made to the ReplicationStatus table on the Sink.
     * The number of updates depends on the number of available Source clusters(3 in this case).
     *
     * Later, 1 update and 1 delete(separate transactions) are performed on Table001 on the Source.  The test
     * verifies that they were applied correctly.
     */
    @Test
    public void testUpdatesOnReplicatedTables() throws Exception {
        super.setUp(1, MAX_REMOTE_CLUSTERS, DefaultClusterManager.TP_MULTI_SINK);
        verifySnapshotAndLogEntrySink(false);
    }

    @Test
    public void testUpdatesOnReplicatedTables_sinkConnectionStarter() throws Exception {
        super.setUp(1, MAX_REMOTE_CLUSTERS, DefaultClusterManager.TP_MULTI_SINK_REV_CONNECTION);
        verifySnapshotAndLogEntrySink(false);
    }

    @Test
    public void testRoleChange() throws Exception {
        verifySnapshotAndLogEntrySink(false);
        log.info("Preparing for role change");
        prepareTestTopologyForRoleChange(MAX_REMOTE_CLUSTERS, 1);
        log.info("Testing after role change");
        verifySnapshotAndLogEntrySink(true);
    }

    /*/**
     * Verify snapshot sync in a topology with 3 Sink clusters and 1 Source cluster.
     * Then perform a role switch by changing the topology to 3 Source clusters, 1 Sink cluster and verify that the new
     * Sink cluster successfully completes a snapshot sync.
     * @throws Exception
     *//*
    @Test
    public void testRoleChange() throws Exception {

        verifySnapshotSync();

        // Write data to a different table(Table004) on one of the current Sink clusters.  This table did not have any
        // replicated data and must be empty.
        Assert.assertEquals(0, sink1Table4.count());

        writeData(corfuStoreSink1, TABLE_4, sink1Table4, 0, NUM_RECORDS_IN_TABLE);

        // Verify no data exists for this table on the Source Cluster
        Assert.assertEquals(0, src1Table4.count());

        // Subscribe the to-be Sink, now-Source Cluster to replication writes on Table004.  There will be 1 TX with
        // the snapshot sync data from Sink1
        CountDownLatch snapshotWritesLatch = new CountDownLatch(1);
        ReplicatedStreamsListener sourceStreamsListener = new ReplicatedStreamsListener(snapshotWritesLatch, true);
        corfuStoreSource1.subscribeListener(sourceStreamsListener, NAMESPACE, STREAM_TAG, Arrays.asList(TABLE_4));

        // Perform a role switch.  This will change the topology (3 Sink, 1 Source) -> (3 Source, 1 Sink)
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

        snapshotWritesLatch.await();

        Assert.assertEquals(NUM_RECORDS_IN_TABLE+1, sourceStreamsListener.getTableToOpTypeMap().get(TABLE_4).size());
    }

    private void setupSourceAndSinkCorfu() throws Exception {
        sourceCorfu1 = runServer(sourceSiteCorfuPort1, true);
        sinkCorfu1 = runServer(sinkSiteCorfuPort1, true);
        sinkCorfu2 = runServer(sinkSiteCorfuPort2, true);
        sinkCorfu3 = runServer(sinkSiteCorfuPort3, true);

        // Setup the runtimes to each Corfu server
        CorfuRuntime.CorfuRuntimeParameters params = CorfuRuntime.CorfuRuntimeParameters
            .builder()
            .build();

        sourceRuntime1 = CorfuRuntime.fromParameters(params);
        sourceRuntime1.parseConfigurationString(sourceEndpoint1);
        sourceRuntime1.connect();

        sinkRuntime1 = CorfuRuntime.fromParameters(params);
        sinkRuntime1.parseConfigurationString(sinkEndpoint1);
        sinkRuntime1.connect();

        sinkRuntime2 = CorfuRuntime.fromParameters(params);
        sinkRuntime2.parseConfigurationString(sinkEndpoint2);
        sinkRuntime2.connect();

        sinkRuntime3 = CorfuRuntime.fromParameters(params);
        sinkRuntime3.parseConfigurationString(sinkEndpoint3);
        sinkRuntime3.connect();

        corfuStoreSource1 = new CorfuStore(sourceRuntime1);
        corfuStoreSink1 = new CorfuStore(sinkRuntime1);
        corfuStoreSink2 = new CorfuStore(sinkRuntime2);
        corfuStoreSink3 = new CorfuStore(sinkRuntime3);
    }

    private void openMaps() throws Exception {
        src1Table1 = corfuStoreSource1.openTable(NAMESPACE, TABLE_1, Sample.StringKey.class,
            SampleSchema.ValueFieldTagOne.class, null,
            TableOptions.fromProtoSchema(SampleSchema.ValueFieldTagOne.class));
        src1Table4 = corfuStoreSource1.openTable(NAMESPACE, TABLE_4, Sample.StringKey.class,
            SampleSchema.ValueFieldTagOne.class, null,
            TableOptions.fromProtoSchema(SampleSchema.ValueFieldTagOne.class));

        sink1Table1 = corfuStoreSink1.openTable(NAMESPACE, TABLE_1, Sample.StringKey.class,
            SampleSchema.ValueFieldTagOne.class, null,
            TableOptions.fromProtoSchema(SampleSchema.ValueFieldTagOne.class));
        sink1Table4 = corfuStoreSink1.openTable(NAMESPACE, TABLE_4, Sample.StringKey.class,
            SampleSchema.ValueFieldTagOne.class, null,
            TableOptions.fromProtoSchema(SampleSchema.ValueFieldTagOne.class));

        sink2Table1 = corfuStoreSink2.openTable(NAMESPACE, TABLE_1, Sample.StringKey.class,
            SampleSchema.ValueFieldTagOne.class, null,
            TableOptions.fromProtoSchema(SampleSchema.ValueFieldTagOne.class));
        sink2Table4 = corfuStoreSink2.openTable(NAMESPACE, TABLE_4, Sample.StringKey.class,
            SampleSchema.ValueFieldTagOne.class, null,
            TableOptions.fromProtoSchema(SampleSchema.ValueFieldTagOne.class));

        sink3Table1 = corfuStoreSink3.openTable(NAMESPACE, TABLE_1, Sample.StringKey.class,
            SampleSchema.ValueFieldTagOne.class, null,
            TableOptions.fromProtoSchema(SampleSchema.ValueFieldTagOne.class));
        sink3Table4 = corfuStoreSink3.openTable(NAMESPACE, TABLE_4, Sample.StringKey.class,
            SampleSchema.ValueFieldTagOne.class, null,
            TableOptions.fromProtoSchema(SampleSchema.ValueFieldTagOne.class));

    }

    private void startReplicationServers() throws Exception {
        sourceReplicationServer1 = runReplicationServer(sourceReplicationPort1, pluginConfigFilePath);
        sinkReplicationServer1 = runReplicationServer(sinkReplicationPort1, pluginConfigFilePath);
        sinkReplicationServer2 = runReplicationServer(sinkReplicationPort2, pluginConfigFilePath);
        sinkReplicationServer3 = runReplicationServer(sinkReplicationPort3, pluginConfigFilePath);
    }*/
}
