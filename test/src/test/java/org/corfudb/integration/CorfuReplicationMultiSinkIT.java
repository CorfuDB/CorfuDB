package org.corfudb.integration;

import com.google.protobuf.Message;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterManager;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata;
import org.corfudb.infrastructure.logreplication.proto.Sample;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.ExampleSchemas;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.test.SampleSchema;
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
import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * This IT uses a topology of 3 Sink clusters and 1 Source cluster, each with its own Corfu Server.  The Source
 * cluster replicates to all 3 Sink clusters.  The set of streams to replicate as received from the
 * LogReplicationConfig is the same for all clusters.
 */
@RunWith(Parameterized.class)
public class CorfuReplicationMultiSinkIT extends CorfuReplicationMultiSourceSinkIT {

    protected Table<Sample.StringKey, SampleSchema.ValueFieldTagOne, Message> sink1Table1;
    protected Table<Sample.StringKey, SampleSchema.ValueFieldTagOne, Message> sink1Table4;
    protected Table<Sample.StringKey, SampleSchema.ValueFieldTagOne, Message> sink2Table1;
    protected Table<Sample.StringKey, SampleSchema.ValueFieldTagOne, Message> sink2Table4;
    protected Table<Sample.StringKey, SampleSchema.ValueFieldTagOne, Message> sink3Table1;
    protected Table<Sample.StringKey, SampleSchema.ValueFieldTagOne, Message> sink3Table4;
    protected Table<Sample.StringKey, SampleSchema.ValueFieldTagOne, Message> src1Table1;
    protected Table<Sample.StringKey, SampleSchema.ValueFieldTagOne, Message> src1Table4;

    public CorfuReplicationMultiSinkIT(String pluginConfigFilePath) {
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
        setupSourceAndSinkCorfu();

        corfuStoreSink1.openTable(LogReplicationMetadataManager.NAMESPACE,
            LogReplicationMetadataManager.REPLICATION_STATUS_TABLE,
            LogReplicationMetadata.ReplicationStatusKey.class,
            LogReplicationMetadata.ReplicationStatusVal.class,
            null,
            TableOptions.fromProtoSchema(LogReplicationMetadata.ReplicationStatusVal.class));

        corfuStoreSink2.openTable(LogReplicationMetadataManager.NAMESPACE,
            LogReplicationMetadataManager.REPLICATION_STATUS_TABLE,
            LogReplicationMetadata.ReplicationStatusKey.class,
            LogReplicationMetadata.ReplicationStatusVal.class,
            null,
            TableOptions.fromProtoSchema(LogReplicationMetadata.ReplicationStatusVal.class));

        corfuStoreSink3.openTable(LogReplicationMetadataManager.NAMESPACE,
            LogReplicationMetadataManager.REPLICATION_STATUS_TABLE,
            LogReplicationMetadata.ReplicationStatusKey.class,
            LogReplicationMetadata.ReplicationStatusVal.class,
            null,
            TableOptions.fromProtoSchema(LogReplicationMetadata.ReplicationStatusVal.class));
    }

    /**
     * Verify snapshot sync in a topology with 3 Sink clusters and 1 Source cluster.  The streams to replicate is the
     * same for all Sink clusters.
     * @throws Exception
     */
    @Test
    public void testSnapshotSync() throws Exception {
        verifySnapshotSync();
    }

    private void verifySnapshotSync() throws Exception{
        // Open maps on the Source and Destination Sites
        openMaps();

        writeData(corfuStoreSource1, TABLE_1, src1Table1, 0, NUM_RECORDS_IN_TABLE);

        Assert.assertEquals(0, sink1Table1.count());
        Assert.assertEquals(0, sink2Table1.count());
        Assert.assertEquals(0, sink3Table1.count());


        int numAvailableSourceClusters = 1;
        // On startup, an initial default replication status is written for each
        // remote cluster(NUM_INITIAL_REPLICATION_STATUS_UPDATES).  Subsequently, the table will be updated on
        // snapshot sync from each available Source cluster(in this case, 1).
        int numExpectedSnapshotSyncUpdates = NUM_INITIAL_REPLICATION_STATUS_UPDATES +
            calculateSnapshotSyncUpdatesOnSinkStatusTable(numAvailableSourceClusters);

        // Subscribe to replication status table on all Sinks
        CountDownLatch statusUpdateLatch1 = new CountDownLatch(numExpectedSnapshotSyncUpdates);
        CountDownLatch statusUpdateLatch2 = new CountDownLatch(numExpectedSnapshotSyncUpdates);
        CountDownLatch statusUpdateLatch3 = new CountDownLatch(numExpectedSnapshotSyncUpdates);

        sinkListener1 = new ReplicationStatusListener(statusUpdateLatch1);
        corfuStoreSink1.subscribeListener(sinkListener1, LogReplicationMetadataManager.NAMESPACE,
            LogReplicationMetadataManager.LR_STATUS_STREAM_TAG);

        sinkListener2 = new ReplicationStatusListener(statusUpdateLatch2);
        corfuStoreSink2.subscribeListener(sinkListener2, LogReplicationMetadataManager.NAMESPACE,
            LogReplicationMetadataManager.LR_STATUS_STREAM_TAG);

        sinkListener3 = new ReplicationStatusListener(statusUpdateLatch3);
        corfuStoreSink3.subscribeListener(sinkListener3, LogReplicationMetadataManager.NAMESPACE,
            LogReplicationMetadataManager.LR_STATUS_STREAM_TAG);

        // Start Log Replication Servers
        startReplicationServers();
        statusUpdateLatch1.await();
        statusUpdateLatch2.await();
        statusUpdateLatch3.await();

        // Verify that each Sink cluster has the expected number of records in the table
        Assert.assertEquals(NUM_RECORDS_IN_TABLE, sink1Table1.count());
        Assert.assertEquals(NUM_RECORDS_IN_TABLE, sink2Table1.count());
        Assert.assertEquals(NUM_RECORDS_IN_TABLE, sink3Table1.count());
    }

    /**
     * Verify snapshot sync in a topology with 3 Sink clusters and 1 Source cluster.
     * Then perform a role switch by changing the topology to 3 Source clusters, 1 Sink cluster and verify that the new
     * Sink cluster successfully completes a snapshot sync.
     * @throws Exception
     */
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
    }
}
