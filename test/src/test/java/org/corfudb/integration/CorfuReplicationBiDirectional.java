package org.corfudb.integration;


import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterConfig;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterManager;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata;
import org.corfudb.infrastructure.logreplication.proto.Sample;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.runtime.ExampleSchemas;
import org.corfudb.runtime.LogReplication;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager.REPLICATION_STATUS_TABLE;

@Slf4j
@SuppressWarnings("checkstyle:magicnumber")
public class CorfuReplicationBiDirectional extends LogReplicationAbstractIT {

    private Table<ExampleSchemas.ClusterUuidMsg, ExampleSchemas.ClusterUuidMsg, ExampleSchemas.ClusterUuidMsg> configTable;

    // TO run this test, add another replication model to streamsToReplicateMap and have metadataMangaer update LRstatus using sessionKey

    /**
     * This test verifies the behaviour when cluster1 acts as both source and sink(orphaned sink, ie., sink has no remote source),
     * and cluster2 acts as a sink for cluster1
     *
     * 1. Init with corfu 9000 source and 9001 sink
     * 2. Topology: cluster1 as SOURCE for cluster2 and cluster1 SINK for no cluster. This is emulated by having
     * different replication models for SOURCE and SINK on cluster1
     * 3. Write 10 entries to source map
     * 4. Start log replication: Node 9010 - source, Node 9020 - sink
     * 5. Wait for Snapshot Sync, both maps have size 10
     * 6. Verify data is replicated
     * 7. Verify the replication status for SINKs on both cluster1/cluster2
     */
//    @Test
//    public void test_whenClusterSourceAndOrphanedSink() throws Exception {
//        //setup source and sink corfu
//        setupSourceAndSinkCorfu();
//        // Add a new kind of topology where a cluster is both source and destination, but the remote just acts as a sink
//        init();
//
//        //open Maps
//        log.debug("Open map on Source and Sink");
//        openMaps(2, false);
//
//        // Subscribe to replication status table on Sink (to be sure data change on status are captured)
//        Table<LogReplicationMetadata.ReplicationSessionKey, LogReplicationMetadata.ReplicationStatusVal, Message>
//                cluster2ReplicationStatus = corfuStoreSink.openTable(LogReplicationMetadataManager.NAMESPACE,
//                LogReplicationMetadataManager.REPLICATION_STATUS_TABLE,
//                LogReplicationMetadata.ReplicationSessionKey.class,
//                LogReplicationMetadata.ReplicationStatusVal.class,
//                null,
//                TableOptions.fromProtoSchema(LogReplicationMetadata.ReplicationStatusVal.class));
//
//        // (1) On startup, init the replication status for each session(2 sessions = 2 updates)
//        // (2) When starting snapshot sync apply : is_data_consistent = false
//        // (3) When completing snapshot sync apply : is_data_consistent = true
//        CountDownLatch cluster2StatusUpdateLatch = new CountDownLatch(4);
//        ReplicationStatusListener cluster2SinkListener =
//                new ReplicationStatusListener(cluster2StatusUpdateLatch, false);
//        corfuStoreSink.subscribeListener(cluster2SinkListener, LogReplicationMetadataManager.NAMESPACE,
//                LogReplicationMetadataManager.LR_STATUS_STREAM_TAG);
//
//
//        corfuStoreSource.openTable(LogReplicationMetadataManager.NAMESPACE,
//                LogReplicationMetadataManager.REPLICATION_STATUS_TABLE,
//                LogReplicationMetadata.ReplicationSessionKey.class,
//                LogReplicationMetadata.ReplicationStatusVal.class,
//                null,
//                TableOptions.fromProtoSchema(LogReplicationMetadata.ReplicationStatusVal.class));
//
//        CountDownLatch cluster1StatusUpdateLatch = new CountDownLatch(1);
//        ReplicationStatusListener cluster1SinkListener =
//                new ReplicationStatusListener(cluster1StatusUpdateLatch, false);
//        corfuStoreSource.subscribeListener(cluster1SinkListener, LogReplicationMetadataManager.NAMESPACE,
//                LogReplicationMetadataManager.LR_STATUS_STREAM_TAG);
//
//
//        //write to source maps
//        writeToSource(0, 10);
//
//        // Confirm data does exist on Source Cluster
//        for(Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> map : mapNameToMapSource.values()) {
//            assertThat(map.count()).isEqualTo(10);
//        }
//
//        //validate sink cluster is still empty
//        for(Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> map : mapNameToMapSink.values()) {
//            assertThat(map.count()).isEqualTo(0);
//        }
//
//        // startReplication
//        startLogReplicatorServers();
//
//        //validate that data is replicated
//        log.info(">> Wait ... Snapshot log replication in progress ...");
//        verifyDataOnSink(10);
//
//        // validate the status of replication table. Status -> COMPLETE
//        cluster2StatusUpdateLatch.await();
//        assertThat(cluster2SinkListener.getAccumulatedStatus().size()).isEqualTo(4);
//        //check is_data_consistent flag is set true
//        Assert.assertTrue(cluster2SinkListener.getAccumulatedStatus().get(3));
//
//
//        cluster1StatusUpdateLatch.await();
//        Assert.assertFalse(cluster1SinkListener.getAccumulatedStatus().get(0));
//
//        // Verify Sync Status for SINK on cluster1
//        LogReplicationMetadata.ReplicationSessionKey key =
//                LogReplicationMetadata.ReplicationSessionKey
//                        .newBuilder()
//                        .setRemoteClusterId(new DefaultClusterConfig().getSinkClusterIds().get(0))
//                        .setReplicationModel(LogReplicationMetadata.ReplicationModels.MOVE_DATA)
//                        .setClient("SampleClient")
//                        .build();
//        LogReplicationMetadata.ReplicationStatusVal replicationStatusVal;
//        try (TxnContext txn = corfuStoreSource.txn(LogReplicationMetadataManager.NAMESPACE)) {
//            replicationStatusVal = (LogReplicationMetadata.ReplicationStatusVal)txn.getRecord(LogReplicationMetadataManager.REPLICATION_STATUS_TABLE, key).getPayload();
//            txn.commit();
//        }
//        assertThat(replicationStatusVal.getSyncType())
//                .isEqualTo(LogReplicationMetadata.ReplicationStatusVal.SyncType.LOG_ENTRY);
//        assertThat(replicationStatusVal.getStatus())
//                .isEqualTo(LogReplicationMetadata.SyncStatus.NOT_STARTED);
//
//
//    }
//
//
//    private void init() throws Exception {
//        configTable = corfuStoreSource.openTable(
//                DefaultClusterManager.CONFIG_NAMESPACE, DefaultClusterManager.CONFIG_TABLE_NAME,
//                ExampleSchemas.ClusterUuidMsg.class, ExampleSchemas.ClusterUuidMsg.class, ExampleSchemas.ClusterUuidMsg.class,
//                TableOptions.fromProtoSchema(ExampleSchemas.ClusterUuidMsg.class)
//        );
//        try (TxnContext txn = corfuStoreSource.txn(DefaultClusterManager.CONFIG_NAMESPACE)) {
//            txn.putRecord(configTable, DefaultClusterManager.OP_BIDIR_ORPH_SINK,
//                    DefaultClusterManager.OP_BIDIR_ORPH_SINK, DefaultClusterManager.OP_BIDIR_ORPH_SINK);
//            txn.commit();
//        }
//    }

    /**
     * This test verifies the behaviour when cluster1 acts as both source and sink(orphaned sink, ie., sink has no remote source),
     * and cluster2 acts as a sink for cluster1
     *
     * 1. Init with corfu 9000 source and 9001 sink
     * 2. Topology: cluster1 as SOURCE cluster2 as SINK, but cluster2 is connectionInitiator
     * 3. Write 10 entries to source map
     * 4. Start log replication: Node 9010 - source, Node 9020 - sink
     * 5. Wait for Snapshot Sync, both maps have size 10
     * 6. Verify data is replicated
     * 7. Verify the replication status for SINKs on both cluster1/cluster2
     */
    @Test
    public void test_whenSinkIsConnectionInit() throws Exception {
        pluginConfigFilePath = grpcConfig;
        //setup source and sink corfu
        setupSourceAndSinkCorfu();
        // Add a new kind of topology where a cluster is both source and destination, but the remote just acts as a sink
        init();



        //open Maps
        System.out.println("Open map on Source and Sink");
        openMaps(2, false);

        // Subscribe to replication status table on Sink (to be sure data change on status are captured)
        Table<LogReplicationMetadata.ReplicationStatusKey, LogReplicationMetadata.ReplicationStatusVal, Message> sinkStatusTable =
                corfuStoreSink.openTable(LogReplicationMetadataManager.NAMESPACE,
                REPLICATION_STATUS_TABLE,
                LogReplicationMetadata.ReplicationStatusKey.class,
                LogReplicationMetadata.ReplicationStatusVal.class,
                null,
                TableOptions.fromProtoSchema(LogReplicationMetadata.ReplicationStatusVal.class));

        // (1) On startup, init the replication status for each session(2 sessions = 2 updates)
        // (2) When starting snapshot sync apply : is_data_consistent = false
        // (3) When completing snapshot sync apply : is_data_consistent = true
        CountDownLatch cluster2StatusUpdateLatch = new CountDownLatch(3);
        ReplicationStatusListener cluster2SinkListener =
                new ReplicationStatusListener(cluster2StatusUpdateLatch, false);
        corfuStoreSink.subscribeListener(cluster2SinkListener, LogReplicationMetadataManager.NAMESPACE,
                LogReplicationMetadataManager.LR_STATUS_STREAM_TAG);


        Table<LogReplicationMetadata.ReplicationStatusKey, LogReplicationMetadata.ReplicationStatusVal, Message> sourceStatusTable = corfuStoreSource.openTable(LogReplicationMetadataManager.NAMESPACE,
                REPLICATION_STATUS_TABLE,
                LogReplicationMetadata.ReplicationStatusKey.class,
                LogReplicationMetadata.ReplicationStatusVal.class,
                null,
                TableOptions.fromProtoSchema(LogReplicationMetadata.ReplicationStatusVal.class));

        CountDownLatch cluster1StatusUpdateLatch = new CountDownLatch(1);
        ReplicationStatusListener cluster1SinkListener =
                new ReplicationStatusListener(cluster1StatusUpdateLatch, false);
        corfuStoreSource.subscribeListener(cluster1SinkListener, LogReplicationMetadataManager.NAMESPACE,
                LogReplicationMetadataManager.LR_STATUS_STREAM_TAG);


        //write to source maps
        writeToSource(0, 10);

        // Confirm data does exist on Source Cluster
        for(Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> map : mapNameToMapSource.values()) {
            assertThat(map.count()).isEqualTo(10);
        }

        //validate sink cluster is still empty
        for(Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> map : mapNameToMapSink.values()) {
            assertThat(map.count()).isEqualTo(0);
        }

        // startReplication
        startLogReplicatorServers();

        //validate that data is replicated
        System.out.println(">> Wait ... Snapshot log replication in progress ...");
        verifyDataOnSink(10);
        System.out.println("Snapshot completed!. But the latch's value is " + cluster2StatusUpdateLatch.getCount());

        // validate the status of replication table. Status -> COMPLETE
        cluster2StatusUpdateLatch.await();

        verifyReplicationStatusFromSource();
//        assertThat(cluster2SinkListener.getAccumulatedStatus().size()).isEqualTo(3);
//        //check is_data_consistent flag is set true
//        Assert.assertTrue(cluster2SinkListener.getAccumulatedStatus().get(2));
//
//        System.out.println("assert for sink is done " + cluster1StatusUpdateLatch.getCount());
//        cluster1StatusUpdateLatch.await();
//        Assert.assertFalse(cluster1SinkListener.getAccumulatedStatus().get(0));
//
//        // Verify Sync Status during the first switchover
//        LogReplicationMetadata.ReplicationStatusKey key =
//                LogReplicationMetadata.ReplicationStatusKey
//                        .newBuilder()
//                        .setClusterId(new DefaultClusterConfig().getSinkClusterIds().get(0))
//                        .build();
//        LogReplicationMetadata.ReplicationStatusVal replicationStatusVal;
//
//        try (TxnContext txn = corfuStoreSource.txn(LogReplicationMetadataManager.NAMESPACE)) {
//            replicationStatusVal = (LogReplicationMetadata.ReplicationStatusVal) txn.getRecord(REPLICATION_STATUS_TABLE, key).getPayload();
//            txn.commit();
//        }

//        assertThat(replicationStatusVal.getSyncType())
//                .isEqualTo(LogReplicationMetadata.ReplicationStatusVal.SyncType.SNAPSHOT);
//        assertThat(replicationStatusVal.getStatus())
//                .isEqualTo(LogReplicationMetadata.SyncStatus.COMPLETED);

//        assertThat(replicationStatusVal.getRemainingEntriesToSend()).isEqualTo(0);

        log.info(">> Write deltas");
        System.out.println("Write deltas");
        writeToSource(numWrites, numWrites / 2);

        log.info(">> Wait ... Delta log replication in progress ...");
        System.out.println("verify deltas");
        verifyDataOnSink((numWrites + (numWrites / 2)));

        assertThat(cluster2SinkListener.getAccumulatedStatus().size()).isEqualTo(3);
        // Confirm last updates are set to true (corresponding to snapshot sync completed and log entry sync started)
        assertThat(cluster2SinkListener.getAccumulatedStatus().get(cluster2SinkListener.getAccumulatedStatus().size() - 1)).isTrue();
        assertThat(cluster2SinkListener.getAccumulatedStatus()).contains(false);
    }


    private void init() throws Exception {
        configTable = corfuStoreSource.openTable(
                DefaultClusterManager.CONFIG_NAMESPACE, DefaultClusterManager.CONFIG_TABLE_NAME,
                ExampleSchemas.ClusterUuidMsg.class, ExampleSchemas.ClusterUuidMsg.class, ExampleSchemas.ClusterUuidMsg.class,
                TableOptions.fromProtoSchema(ExampleSchemas.ClusterUuidMsg.class)
        );
        try (TxnContext txn = corfuStoreSource.txn(DefaultClusterManager.CONFIG_NAMESPACE)) {
            txn.putRecord(configTable, DefaultClusterManager.OP_SINK_CONNECT_INIT,
                    DefaultClusterManager.OP_SINK_CONNECT_INIT, DefaultClusterManager.OP_SINK_CONNECT_INIT);
            txn.commit();
        }
    }
}
