package org.corfudb.integration;

import com.google.common.reflect.TypeToken;
import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterConfig;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterManager;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationStatusKey;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationStatusVal;
import org.corfudb.infrastructure.logreplication.proto.Sample;
import org.corfudb.infrastructure.logreplication.proto.Sample.IntValue;
import org.corfudb.infrastructure.logreplication.proto.Sample.IntValueTag;
import org.corfudb.infrastructure.logreplication.proto.Sample.Metadata;
import org.corfudb.infrastructure.logreplication.proto.Sample.StringKey;
import org.corfudb.infrastructure.logreplication.replication.LogReplicationAckReader;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuOptions;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.ExampleSchemas.ClusterUuidMsg;
import org.corfudb.runtime.MultiCheckpointWriter;
import org.corfudb.runtime.collections.CorfuDynamicKey;
import org.corfudb.runtime.collections.CorfuDynamicRecord;
import org.corfudb.runtime.collections.CorfuRecord;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStreamEntries;
import org.corfudb.runtime.collections.PersistentCorfuTable;
import org.corfudb.runtime.collections.StreamListener;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.runtime.view.ObjectsView;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.util.Sleep;
import org.corfudb.util.serializer.DynamicProtobufSerializer;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.ProtobufSerializer;
import org.corfudb.utils.lock.LockDataTypes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.IntPredicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;


/**
 * This test suite exercises some topology config change scenarios.
 * Each test will start with two single node corfu servers, and two single node log replicators.
 */
@Slf4j
@SuppressWarnings("checkstyle:magicnumber")
public class CorfuReplicationClusterConfigIT extends AbstractIT {
    public static final String nettyPluginPath = "src/test/resources/transport/nettyConfig.properties";
    private static final String streamName = "Table001";
    private static final String LOCK_TABLE_NAME = "LOCK";

    private final static long shortInterval = 1L;
    private final static long mediumInterval = 10L;
    private final static int firstBatch = 10;
    private final static int secondBatch = 15;
    private final static int thirdBatch = 20;
    private final static int fourthBatch = 25;
    private final static int largeBatch = 50;

    private final static int sourceClusterCorfuPort = 9000;
    private final static int sinkClusterCorfuPort = 9001;
    private final static int backupClusterCorfuPort = 9007;
    private final static int sourceReplicationServerPort = 9010;
    private final static int sinkReplicationServerPort = 9020;
    private final static int backupReplicationServerPort = 9030;
    private static final String sourceCorfuEndpoint = DEFAULT_HOST + ":" + sourceClusterCorfuPort;
    private static final String sinkCorfuEndpoint = DEFAULT_HOST + ":" + sinkClusterCorfuPort;
    private static final String backupCorfuEndpoint = DEFAULT_HOST + ":" + backupClusterCorfuPort;

    private static final String REPLICATION_STATUS_TABLE = "LogReplicationStatus";

    private Process sourceCorfuServer = null;
    private Process sinkCorfuServer = null;
    private Process sourceReplicationServer = null;
    private Process sinkReplicationServer = null;

    private CorfuRuntime sourceRuntime;
    private CorfuRuntime sinkRuntime;
    private Table<StringKey, IntValue, Metadata> mapSource;
    private Table<StringKey, IntValue, Metadata> mapSink;

    private CorfuStore sourceCorfuStore;
    private CorfuStore sinkCorfuStore;
    private Table<ClusterUuidMsg, ClusterUuidMsg, ClusterUuidMsg> configTable;
    private Table<LockDataTypes.LockId, LockDataTypes.LockData, Message> sourceLockTable;

    public Map<String, Table<StringKey, IntValueTag, Metadata>> mapNameToMapSource;
    public Map<String, Table<StringKey, IntValueTag, Metadata>> mapNameToMapSink;

    public static final String TABLE_PREFIX = "Table00";

    public static final String NAMESPACE = "LR-Test";

    @Before
    public void setUp() throws Exception {
        sourceCorfuServer = runServer(sourceClusterCorfuPort, true);
        sinkCorfuServer = runServer(sinkClusterCorfuPort, true);

        CorfuRuntime.CorfuRuntimeParameters params = CorfuRuntime.CorfuRuntimeParameters
                .builder()
                .build();

        sourceRuntime = CorfuRuntime.fromParameters(params);
        sourceRuntime.parseConfigurationString(sourceCorfuEndpoint).connect();

        sinkRuntime = CorfuRuntime.fromParameters(params);
        sinkRuntime.parseConfigurationString(sinkCorfuEndpoint).connect();

        sourceCorfuStore = new CorfuStore(sourceRuntime);
        sinkCorfuStore = new CorfuStore(sinkRuntime);

        mapSource = sourceCorfuStore.openTable(
                NAMESPACE,
                streamName,
                StringKey.class,
                IntValue.class,
                Metadata.class,
                TableOptions.builder().schemaOptions(
                                CorfuOptions.SchemaOptions.newBuilder()
                                        .setIsFederated(true)
                                        .addStreamTag(ObjectsView.LOG_REPLICATOR_STREAM_INFO.getTagName())
                                        .build())
                        .build()
        );

        mapSink = sinkCorfuStore.openTable(
                NAMESPACE,
                streamName,
                StringKey.class,
                IntValue.class,
                Metadata.class,
                TableOptions.builder().schemaOptions(
                                CorfuOptions.SchemaOptions.newBuilder()
                                        .setIsFederated(true)
                                        .addStreamTag(ObjectsView.LOG_REPLICATOR_STREAM_INFO.getTagName())
                                        .build())
                        .build()
        );

        assertThat(mapSource.count()).isZero();
        assertThat(mapSink.count()).isZero();

        configTable = sourceCorfuStore.openTable(
                DefaultClusterManager.CONFIG_NAMESPACE, DefaultClusterManager.CONFIG_TABLE_NAME,
                ClusterUuidMsg.class, ClusterUuidMsg.class, ClusterUuidMsg.class,
                TableOptions.fromProtoSchema(ClusterUuidMsg.class)
        );

        sourceLockTable = sourceCorfuStore.openTable(
                CORFU_SYSTEM_NAMESPACE,
                LOCK_TABLE_NAME,
                LockDataTypes.LockId.class,
                LockDataTypes.LockData.class,
                null,
                TableOptions.fromProtoSchema(LockDataTypes.LockData.class));

        sourceCorfuStore.openTable(LogReplicationMetadataManager.NAMESPACE,
                REPLICATION_STATUS_TABLE,
                LogReplicationMetadata.ReplicationStatusKey.class,
                LogReplicationMetadata.ReplicationStatusVal.class,
                null,
                TableOptions.fromProtoSchema(LogReplicationMetadata.ReplicationStatusVal.class));

        sinkCorfuStore.openTable(LogReplicationMetadataManager.NAMESPACE,
                REPLICATION_STATUS_TABLE,
                LogReplicationMetadata.ReplicationStatusKey.class,
                LogReplicationMetadata.ReplicationStatusVal.class,
                null,
                TableOptions.fromProtoSchema(LogReplicationMetadata.ReplicationStatusVal.class));
    }

    @After
    public void tearDown() throws IOException, InterruptedException {
        if (sourceRuntime != null) {
            sourceRuntime.shutdown();
        }

        if (sinkRuntime != null) {
            sinkRuntime.shutdown();
        }

        shutdownCorfuServer(sourceCorfuServer);
        shutdownCorfuServer(sinkCorfuServer);
        shutdownCorfuServer(sourceReplicationServer);
        shutdownCorfuServer(sinkReplicationServer);
    }

    /**
     * This test verifies config change with a role switch.
     * <p>
     * 1. Init with corfu 9000 source and 9001 sink
     * 2. Write 10 entries to source map
     * 3. Start log replication: Node 9010 - source, Node 9020 - sink
     * 4. Wait for Snapshot Sync, both maps have size 10
     * 5. Write 5 more entries to source map, to verify Log Entry Sync
     * 6. Perform a role switch with corfu store
     * 7. Write 5 more entries to sink map, which becomes source right now.
     * 8. Verify data will be replicated in reverse direction.
     */
    @Test
    public void testNewConfigWithSwitchRole() throws Exception {
        // Write 10 entries to source map
        for (int i = 0; i < firstBatch; i++) {
            // Change to default source sink config
            try (TxnContext txn = sourceCorfuStore.txn(NAMESPACE)) {
                txn.putRecord(mapSource, StringKey.newBuilder().setKey(String.valueOf(i)).build(),
                        IntValue.newBuilder().setValue(i).build(), null);
                txn.commit();
            }
        }
        assertThat(mapSource.count()).isEqualTo(firstBatch);
        assertThat(mapSink.count()).isZero();

        log.info("Before log replication, append {} entries to source map. Current source corfu" +
                        "[{}] log tail is {}, sink corfu[{}] log tail is {}", firstBatch, sourceClusterCorfuPort,
                sourceRuntime.getAddressSpaceView().getLogTail(), sinkClusterCorfuPort,
                sinkRuntime.getAddressSpaceView().getLogTail());

        sourceReplicationServer = runReplicationServer(sourceReplicationServerPort, nettyPluginPath);
        sinkReplicationServer = runReplicationServer(sinkReplicationServerPort, nettyPluginPath);
        log.info("Replication servers started, and replication is in progress...");

        // Wait until data is fully replicated
        waitForReplication(size -> size == firstBatch, mapSink, firstBatch);
        log.info("After full sync, both maps have size {}. Current source corfu[{}] log tail " +
                        "is {}, sink corfu[{}] log tail is {}", firstBatch, sourceClusterCorfuPort,
                sourceRuntime.getAddressSpaceView().getLogTail(), sinkClusterCorfuPort,
                sinkRuntime.getAddressSpaceView().getLogTail());

        // Write 5 entries to source map
        for (int i = firstBatch; i < secondBatch; i++) {
            try (TxnContext txn = sourceCorfuStore.txn(NAMESPACE)) {
                txn.putRecord(mapSource, StringKey.newBuilder().setKey(String.valueOf(i)).build(),
                        IntValue.newBuilder().setValue(i).build(), null);
                txn.commit();
            }
        }
        assertThat(mapSource.count()).isEqualTo(secondBatch);

        // Wait until data is fully replicated again
        waitForReplication(size -> size == secondBatch, mapSink, secondBatch);
        log.info("After delta sync, both maps have size {}. Current source corfu[{}] log tail " +
                        "is {}, sink corfu[{}] log tail is {}", secondBatch, sourceClusterCorfuPort,
                sourceRuntime.getAddressSpaceView().getLogTail(), sinkClusterCorfuPort,
                sinkRuntime.getAddressSpaceView().getLogTail());

        // Verify data
        for (int i = 0; i < secondBatch; i++) {
            try (TxnContext tx = sinkCorfuStore.txn(NAMESPACE)) {
                assertThat(tx.getRecord(mapSink, StringKey.newBuilder().setKey(String.valueOf(i)).build())
                        .getPayload().getValue()).isEqualTo(i);
                tx.commit();
            }
        }
        log.info("Log replication succeeds without config change!");

        // Verify Sync Status before switchover
        LogReplicationMetadata.ReplicationStatusKey key =
                LogReplicationMetadata.ReplicationStatusKey
                        .newBuilder()
                        .setClusterId(new DefaultClusterConfig().getSinkClusterIds().get(0))
                        .build();

        ReplicationStatusVal replicationStatusVal;
        try (TxnContext txn = sourceCorfuStore.txn(LogReplicationMetadataManager.NAMESPACE)) {
            replicationStatusVal = (ReplicationStatusVal)txn.getRecord(REPLICATION_STATUS_TABLE, key).getPayload();
            txn.commit();
        }

        log.info("ReplicationStatusVal: RemainingEntriesToSend: {}, SyncType: {}, Status: {}",
                replicationStatusVal.getRemainingEntriesToSend(), replicationStatusVal.getSyncType(),
                replicationStatusVal.getStatus());

        log.info("SnapshotSyncInfo: Base: {}, Type: {}, Status: {}, CompletedTime: {}",
                replicationStatusVal.getSnapshotSyncInfo().getBaseSnapshot(), replicationStatusVal.getSnapshotSyncInfo().getType(),
                replicationStatusVal.getSnapshotSyncInfo().getStatus(), replicationStatusVal.getSnapshotSyncInfo().getCompletedTime());


        assertThat(replicationStatusVal.getSyncType())
                .isEqualTo(LogReplicationMetadata.ReplicationStatusVal.SyncType.LOG_ENTRY);
        assertThat(replicationStatusVal.getStatus())
                .isEqualTo(LogReplicationMetadata.SyncStatus.ONGOING);

        assertThat(replicationStatusVal.getSnapshotSyncInfo().getType())
                .isEqualTo(LogReplicationMetadata.SnapshotSyncInfo.SnapshotSyncType.DEFAULT);
        assertThat(replicationStatusVal.getSnapshotSyncInfo().getStatus())
                .isEqualTo(LogReplicationMetadata.SyncStatus.COMPLETED);

        // Perform a role switch
        try (TxnContext txn = sourceCorfuStore.txn(DefaultClusterManager.CONFIG_NAMESPACE)) {
            txn.putRecord(configTable, DefaultClusterManager.OP_SWITCH, DefaultClusterManager.OP_SWITCH, DefaultClusterManager.OP_SWITCH);
            txn.commit();
        }

        assertThat(configTable.count()).isOne();

        // Write 5 more entries to mapSink
        for (int i = secondBatch; i < thirdBatch; i++) {
            try (TxnContext txn = sinkCorfuStore.txn(NAMESPACE)) {
                txn.putRecord(mapSink, StringKey.newBuilder().setKey(String.valueOf(i)).build(),
                        IntValue.newBuilder().setValue(i).build(), null);
                txn.commit();
            }
        }
        assertThat(mapSink.count()).isEqualTo(thirdBatch);

        sleepUninterruptibly(5);

        // Verify Sync Status during the first switchover
        LogReplicationMetadata.ReplicationStatusKey sinkKey =
                LogReplicationMetadata.ReplicationStatusKey
                        .newBuilder()
                        .setClusterId(new DefaultClusterConfig().getSourceClusterIds().get(0))
                        .build();

        ReplicationStatusVal sinkStatusVal;
        try (TxnContext txn = sinkCorfuStore.txn(LogReplicationMetadataManager.NAMESPACE)) {
            sinkStatusVal = (ReplicationStatusVal)txn.getRecord(REPLICATION_STATUS_TABLE, sinkKey).getPayload();
            assertThat(txn.getRecord(REPLICATION_STATUS_TABLE, key).getPayload()).isNull();
            txn.commit();
        }

        log.info("ReplicationStatusVal: RemainingEntriesToSend: {}, SyncType: {}, Status: {}",
                sinkStatusVal.getRemainingEntriesToSend(), sinkStatusVal.getSyncType(),
                sinkStatusVal.getStatus());

        log.info("SnapshotSyncInfo: Base: {}, Type: {}, Status: {}, CompletedTime: {}",
                sinkStatusVal.getSnapshotSyncInfo().getBaseSnapshot(), sinkStatusVal.getSnapshotSyncInfo().getType(),
                sinkStatusVal.getSnapshotSyncInfo().getStatus(), sinkStatusVal.getSnapshotSyncInfo().getCompletedTime());

        assertThat(sinkStatusVal.getSyncType())
                .isEqualTo(LogReplicationMetadata.ReplicationStatusVal.SyncType.LOG_ENTRY);
        assertThat(sinkStatusVal.getStatus())
                .isEqualTo(LogReplicationMetadata.SyncStatus.ONGOING);

        assertThat(sinkStatusVal.getSnapshotSyncInfo().getType())
                .isEqualTo(LogReplicationMetadata.SnapshotSyncInfo.SnapshotSyncType.DEFAULT);
        assertThat(sinkStatusVal.getSnapshotSyncInfo().getStatus())
                .isEqualTo(LogReplicationMetadata.SyncStatus.COMPLETED);

        // Wait until data is fully replicated again
        waitForReplication(size -> size == thirdBatch, mapSource, thirdBatch);
        log.info("Data is fully replicated again after role switch, both maps have size {}. " +
                        "Current source corfu[{}] log tail is {}, sink corfu[{}] log tail is {}",
                thirdBatch, sourceClusterCorfuPort, sourceRuntime.getAddressSpaceView().getLogTail(),
                sinkClusterCorfuPort, sinkRuntime.getAddressSpaceView().getLogTail());

        // Double check after 10 seconds
        TimeUnit.SECONDS.sleep(mediumInterval);
        assertThat(mapSource.count()).isEqualTo(thirdBatch);
        assertThat(mapSink.count()).isEqualTo(thirdBatch);

        // Second Role Switch
        try (TxnContext txn = sourceCorfuStore.txn(DefaultClusterManager.CONFIG_NAMESPACE)) {
            txn.putRecord(configTable, DefaultClusterManager.OP_SWITCH, DefaultClusterManager.OP_SWITCH, DefaultClusterManager.OP_SWITCH);
            txn.commit();
        }
        assertThat(configTable.count()).isOne();

        // Write 5 more entries to mapSink
        for (int i = thirdBatch; i < fourthBatch; i++) {
            try (TxnContext txn = sourceCorfuStore.txn(NAMESPACE)) {
                txn.putRecord(mapSource, StringKey.newBuilder().setKey(String.valueOf(i)).build(),
                        IntValue.newBuilder().setValue(i).build(), null);
                txn.commit();
            }
        }
        assertThat(mapSource.count()).isEqualTo(fourthBatch);

        // Wait until data is fully replicated again
        waitForReplication(size -> size == fourthBatch, mapSink, fourthBatch);
        log.info("Data is fully replicated again after role switch, both maps have size {}. " +
                        "Current source corfu[{}] log tail is {}, sink corfu[{}] log tail is {}",
                fourthBatch, sourceClusterCorfuPort, sourceRuntime.getAddressSpaceView().getLogTail(),
                sinkClusterCorfuPort, sinkRuntime.getAddressSpaceView().getLogTail());

        // Double check after 10 seconds
        TimeUnit.SECONDS.sleep(mediumInterval);
        assertThat(mapSource.count()).isEqualTo(fourthBatch);
        assertThat(mapSink.count()).isEqualTo(fourthBatch);

        // Verify Sync Status
        try (TxnContext txn = sourceCorfuStore.txn(LogReplicationMetadataManager.NAMESPACE)) {
            replicationStatusVal = (ReplicationStatusVal)txn.getRecord(REPLICATION_STATUS_TABLE, key).getPayload();
            txn.commit();
        }

        log.info("ReplicationStatusVal: RemainingEntriesToSend: {}, SyncType: {}, Status: {}",
                replicationStatusVal.getRemainingEntriesToSend(), replicationStatusVal.getSyncType(),
                replicationStatusVal.getStatus());

        log.info("SnapshotSyncInfo: Base: {}, Type: {}, Status: {}, CompletedTime: {}",
                replicationStatusVal.getSnapshotSyncInfo().getBaseSnapshot(), replicationStatusVal.getSnapshotSyncInfo().getType(),
                replicationStatusVal.getSnapshotSyncInfo().getStatus(), replicationStatusVal.getSnapshotSyncInfo().getCompletedTime());

        assertThat(replicationStatusVal.getSyncType())
                .isEqualTo(LogReplicationMetadata.ReplicationStatusVal.SyncType.LOG_ENTRY);
        assertThat(replicationStatusVal.getStatus())
                .isEqualTo(LogReplicationMetadata.SyncStatus.ONGOING);

        assertThat(replicationStatusVal.getSnapshotSyncInfo().getType())
                .isEqualTo(LogReplicationMetadata.SnapshotSyncInfo.SnapshotSyncType.DEFAULT);
        assertThat(replicationStatusVal.getSnapshotSyncInfo().getStatus())
                .isEqualTo(LogReplicationMetadata.SyncStatus.COMPLETED);
    }

    /**
     * This test verifies that replicated streams opened and written to on the
     * cluster which becomes the Sink, do not contain any data written
     * prior to the snapshot sync from the Source.
     * 1. Open a subset of replicated streams on the Source and write data to
     * them
     * 2. Open a different set of replicated stream/s on the Sink and
     * write data to them
     * 3. Start LR on both clusters and assign roles
     * 4. Verify that after successful snapshot sync, no data from 2. is
     * present on the Sink
     * @throws Exception
     */
    @Test
    public void testDataConsistentForEmptyStreams() throws Exception {
        // Open 2/10 tables to be replicated on the source and write data
        // to it
        openMapsOnCluster(true, 2, 1);
        writeToMaps(true, 0, firstBatch);

        // Open another(different) table on sink.  This is also one of
        // the tables to replicate.  Write data to it.
        openMapsOnCluster(false, 1, 5);
        writeToMaps(false, 0, firstBatch);

        // Start LR on both source and sink clusters
        sourceReplicationServer = runReplicationServer(sourceReplicationServerPort, nettyPluginPath);
        sinkReplicationServer = runReplicationServer(sinkReplicationServerPort, nettyPluginPath);

        log.info("Replication servers started, and replication is in progress...");
        sleepUninterruptibly(20);

        // Verify that the replicated table opened on sink has no
        // data after snapshot sync
        verifyNoDataOnSinkOpenedTables();

        LogReplicationMetadata.ReplicationStatusKey key =
            LogReplicationMetadata.ReplicationStatusKey
                .newBuilder()
                .setClusterId(new DefaultClusterConfig().getSinkClusterIds().get(0))
                .build();
        ReplicationStatusVal replicationStatusVal;
        try (TxnContext txn = sourceCorfuStore.txn(LogReplicationMetadataManager.NAMESPACE)) {
            replicationStatusVal = (ReplicationStatusVal)txn.getRecord(REPLICATION_STATUS_TABLE, key).getPayload();
            txn.commit();
        }
        assertThat(replicationStatusVal.getSyncType())
            .isEqualTo(LogReplicationMetadata.ReplicationStatusVal.SyncType.LOG_ENTRY);
        assertThat(replicationStatusVal.getStatus())
            .isEqualTo(LogReplicationMetadata.SyncStatus.ONGOING);

        assertThat(replicationStatusVal.getSnapshotSyncInfo().getType())
            .isEqualTo(LogReplicationMetadata.SnapshotSyncInfo.SnapshotSyncType.DEFAULT);
        assertThat(replicationStatusVal.getSnapshotSyncInfo().getStatus())
            .isEqualTo(LogReplicationMetadata.SyncStatus.COMPLETED);
        log.info("Snapshot Sync was successful");
    }

    private void verifyNoDataOnSinkOpenedTables() {
        for(Map.Entry<String, Table<StringKey, IntValueTag, Metadata>> entry : mapNameToMapSink.entrySet()) {
            Table<StringKey, IntValueTag, Metadata> map = entry.getValue();
            assertThat(map.count()).isEqualTo(0);
        }
    }

    /**
     * This test verifies that unopened(no data), replicated, trimmed streams
     * do not cause TrimmedException on Source during snapshot sync.
     * 1. Open a subset of replicated streams on the Source and write data to
     * them.
     * 2. Start LR on both clusters
     * 3. Verify snapshot sync completes successfully (Snapshot sync will cause
     * the Sink to write a 'clear' for every replicated stream which has
     * data)
     * 4. Checkpoint and trim all streams on Source and Sink.
     * 5. Switchover
     * 6. In step 3., the expected behavior is for sink to 'not' add a
     * 'clear' for the unopened, empty replicated streams.  Hence,
     * verify that snapshot sync completes successfully without
     * TrimmedException for those streams.
     */
    @Test
    public void testSnapshotSyncOfUnopenedTrimmedStreams() throws Exception {
        // Open 2/10 tables to be replicated on the source
        openMapsOnCluster(true, 2, 1);

        // Write data to the 2 tables
        writeToMaps(true, 0, firstBatch);

        // Start LR on both source and sink clusters
        sourceReplicationServer = runReplicationServer(sourceReplicationServerPort, nettyPluginPath);
        sinkReplicationServer = runReplicationServer(sinkReplicationServerPort, nettyPluginPath);

        log.info("Replication servers started, and replication is in progress...");
        sleepUninterruptibly(20);

        // Verify snapshot sync completes as expected
        LogReplicationMetadata.ReplicationStatusKey key =
            LogReplicationMetadata.ReplicationStatusKey
                .newBuilder()
                .setClusterId(new DefaultClusterConfig().getSinkClusterIds().get(0))
                .build();
        ReplicationStatusVal replicationStatusVal;
        try (TxnContext txn = sourceCorfuStore.txn(LogReplicationMetadataManager.NAMESPACE)) {
            replicationStatusVal = (ReplicationStatusVal) txn.getRecord(REPLICATION_STATUS_TABLE, key).getPayload();
            txn.commit();
        }
        assertThat(replicationStatusVal.getSyncType())
            .isEqualTo(LogReplicationMetadata.ReplicationStatusVal.SyncType.LOG_ENTRY);
        assertThat(replicationStatusVal.getStatus())
            .isEqualTo(LogReplicationMetadata.SyncStatus.ONGOING);

        assertThat(replicationStatusVal.getSnapshotSyncInfo().getType())
            .isEqualTo(LogReplicationMetadata.SnapshotSyncInfo.SnapshotSyncType.DEFAULT);
        assertThat(replicationStatusVal.getSnapshotSyncInfo().getStatus())
            .isEqualTo(LogReplicationMetadata.SyncStatus.COMPLETED);
        log.info("Snapshot Sync successful");


        // Checkpoint and trim the maps on both source and sink
        checkpointAndTrim(true);
        checkpointAndTrim(false);

        // Perform Switchover and verify it succeeds
        try (TxnContext txn = sourceCorfuStore.txn(DefaultClusterManager.CONFIG_NAMESPACE)) {
            txn.putRecord(configTable, DefaultClusterManager.OP_SWITCH,
                DefaultClusterManager.OP_SWITCH, DefaultClusterManager.OP_SWITCH);
            txn.commit();
        }
        assertThat(configTable.count()).isOne();
        sleepUninterruptibly(10);

        // Verify snapshot sync completes as expected
        key = LogReplicationMetadata.ReplicationStatusKey
            .newBuilder()
            .setClusterId(new DefaultClusterConfig().getSourceClusterIds().get(0))
            .build();
        try (TxnContext txn =
                 sinkCorfuStore.txn(LogReplicationMetadataManager.NAMESPACE)) {
            replicationStatusVal = (ReplicationStatusVal) txn.getRecord(REPLICATION_STATUS_TABLE, key).getPayload();
            txn.commit();
        }

        assertThat(replicationStatusVal.getSnapshotSyncInfo().getType())
            .isEqualTo(LogReplicationMetadata.SnapshotSyncInfo.SnapshotSyncType.DEFAULT);
        assertThat(replicationStatusVal.getSnapshotSyncInfo().getStatus())
            .isEqualTo(LogReplicationMetadata.SyncStatus.COMPLETED);

        assertThat(replicationStatusVal.getSyncType())
            .isEqualTo(LogReplicationMetadata.ReplicationStatusVal.SyncType.LOG_ENTRY);
        assertThat(replicationStatusVal.getStatus())
            .isEqualTo(LogReplicationMetadata.SyncStatus.ONGOING);
        log.info("Snapshot Sync successful after CP/Trim and Switchover");
    }

    private void openMapsOnCluster(boolean isSource, int mapCount,
        int startIndex) throws Exception {
        mapNameToMapSource = new HashMap<>();
        mapNameToMapSink = new HashMap<>();

        for(int i=startIndex; i <= mapCount; i++) {
            String mapName = TABLE_PREFIX + i;

            if (isSource) {
                Table<StringKey, IntValueTag, Metadata> mapSource = sourceCorfuStore.openTable(
                    NAMESPACE, mapName, StringKey.class, IntValueTag.class, Metadata.class,
                    TableOptions.fromProtoSchema(IntValueTag.class));
                mapNameToMapSource.put(mapName, mapSource);
                assertThat(mapSource.count()).isEqualTo(0);
            } else {
                Table<StringKey, IntValueTag, Metadata> mapSink = sinkCorfuStore.openTable(
                    NAMESPACE, mapName, StringKey.class, IntValueTag.class, Metadata.class,
                    TableOptions.fromProtoSchema(IntValueTag.class));
                mapNameToMapSink.put(mapName, mapSink);
                assertThat(mapSink.count()).isEqualTo(0);
            }
        }
    }

    private void writeToMaps(boolean source, int startIndex, int totalEntries) {
        int maxIndex = totalEntries + startIndex;

        Map<String, Table<StringKey, IntValueTag, Metadata>> map;

        if (source) {
            map = mapNameToMapSource;
        } else {
            map = mapNameToMapSink;
        }
        for(Map.Entry<String, Table<StringKey, IntValueTag, Metadata>> entry : map.entrySet()) {

            log.debug(">>> Write to source cluster, map={}", entry.getKey());

            Table<StringKey, IntValueTag, Metadata> table = entry.getValue();
            for (int i = startIndex; i < maxIndex; i++) {
                StringKey stringKey = StringKey.newBuilder().setKey(String.valueOf(i)).build();
                IntValueTag intValueTag = IntValueTag.newBuilder().setValue(i).build();
                Metadata metadata = Metadata.newBuilder().setMetadata("Metadata_" + i).build();
                try (TxnContext txn = sourceCorfuStore.txn(NAMESPACE)) {
                    txn.putRecord(table, stringKey, intValueTag, metadata);
                    txn.commit();
                }
            }
            assertThat(table.count()).isEqualTo(totalEntries);
        }
    }

    private void checkpointAndTrim(boolean source) {
        CorfuRuntime cpRuntime;

        if (source) {
            cpRuntime = createRuntimeWithCache(sourceCorfuEndpoint);
        } else {
            cpRuntime = createRuntimeWithCache(sinkCorfuEndpoint);
        }
        checkpointAndTrimCorfuStore(cpRuntime);
    }

    public static void checkpointAndTrimCorfuStore(CorfuRuntime cpRuntime) {
        // Open Table Registry
        TableRegistry tableRegistry = cpRuntime.getTableRegistry();
        PersistentCorfuTable<CorfuStoreMetadata.TableName, CorfuRecord<CorfuStoreMetadata.TableDescriptors,
            CorfuStoreMetadata.TableMetadata>> tableRegistryCT = tableRegistry.getRegistryTable();

        // Save the regular serializer first..
        ISerializer protoBufSerializer = cpRuntime.getSerializers().getSerializer(ProtobufSerializer.PROTOBUF_SERIALIZER_CODE);

        // Must register dynamicProtoBufSerializer *AFTER* the getTableRegistry() call to ensure that
        // the serializer does not go back to the regular ProtoBufSerializer
        ISerializer dynamicProtoBufSerializer = new DynamicProtobufSerializer(cpRuntime);
        cpRuntime.getSerializers().registerSerializer(dynamicProtoBufSerializer);

        // First checkpoint the TableRegistry system table
        MultiCheckpointWriter<PersistentCorfuTable<?, ?>> mcw = new MultiCheckpointWriter<>();

        String author = "checkpointer";
        Token trimMark = null;

        for (CorfuStoreMetadata.TableName tableName : tableRegistry.listTables(null)) {
            String fullTableName = TableRegistry.getFullyQualifiedTableName(
                tableName.getNamespace(), tableName.getTableName()
            );

            PersistentCorfuTable<CorfuDynamicKey, CorfuDynamicRecord> corfuTable =
                    createCorfuTable(cpRuntime, fullTableName, dynamicProtoBufSerializer);

            log.info("Checkpointing - {}", fullTableName);
            mcw = new MultiCheckpointWriter<>();
            mcw.addMap(corfuTable);

            Token token = mcw.appendCheckpoints(cpRuntime, author);
            trimMark = trimMark == null ? token : Token.min(trimMark, token);
        }

        // Finally checkpoint the TableRegistry system table itself..
        mcw.addMap(tableRegistryCT);
        Token token = mcw.appendCheckpoints(cpRuntime, author);
        trimMark = trimMark != null ? Token.min(trimMark, token) : token;

        cpRuntime.getAddressSpaceView().prefixTrim(trimMark);
        cpRuntime.getAddressSpaceView().gc();

        // Lastly restore the regular protoBuf serializer and undo the dynamic protoBuf serializer
        // otherwise the test cannot continue beyond this point.
        cpRuntime.getSerializers().registerSerializer(protoBufSerializer);

        // Trim
        log.debug("**** Trim Log @address=" + trimMark);
        cpRuntime.getAddressSpaceView().prefixTrim(trimMark);
        cpRuntime.getAddressSpaceView().invalidateClientCache();
        cpRuntime.getAddressSpaceView().invalidateServerCaches();
        cpRuntime.getAddressSpaceView().gc();
    }

    /**
     * Test all combinations of source/sink LR start/stopped and the output of sync status
     */
    @Test
    public void testClusterSyncStatus() throws Exception {

        final int waitInMillis = 500;
        final int deltaSeconds = 5;

        // (1) Start with: source LR stopped & sink LR started
        // No status should be reported, as status is queried on source LR and it is stopped.
        sinkReplicationServer = runReplicationServer(sinkReplicationServerPort, nettyPluginPath);

        // Write 'N' entries to source map (to ensure nothing happens wrt. the status, as LR is not started on source)
        for (int i = 0; i < firstBatch; i++) {
            try (TxnContext txn = sourceCorfuStore.txn(NAMESPACE)) {
                txn.putRecord(mapSource, StringKey.newBuilder().setKey(String.valueOf(i)).build(),
                        IntValue.newBuilder().setValue(i).build(), null);
                txn.commit();
            }
        }
        assertThat(mapSource.count()).isEqualTo(firstBatch);

        // Verify Sync Status
        ReplicationStatusKey sinkClusterId = ReplicationStatusKey.newBuilder()
                        .setClusterId(new DefaultClusterConfig().getSinkClusterIds().get(0))
                        .build();
        ReplicationStatusVal sinkStatus;

        try (TxnContext txn = sourceCorfuStore.txn(LogReplicationMetadataManager.NAMESPACE)) {
            // Since LR has never been started, the table should not exist in the registry
            // Note that, in the case of a real client querying the status, this would simply time out
            // because LR is not available and status is only queried on the source site through LR. For the purpose of this
            // test, we query the database directly, so we should simply not find any record.
            sinkStatus = (ReplicationStatusVal)txn.getRecord(REPLICATION_STATUS_TABLE, sinkClusterId).getPayload();
            assertThat(sinkStatus).isNull();
        }

        // (2) Now stop sink LR and start source LR
        // The sync status should indicate replication has not started, as there is no way to stablish a connection
        // to the remote/sink site as it is stopped.
        shutdownCorfuServer(sinkReplicationServer);
        sourceReplicationServer = runReplicationServer(sourceReplicationServerPort, nettyPluginPath);

        // Verify Sync Status
        while (sinkStatus == null) {
            Sleep.sleepUninterruptibly(Duration.ofMillis(waitInMillis));

            try (TxnContext txn = sourceCorfuStore.txn(LogReplicationMetadataManager.NAMESPACE)) {
                sinkStatus = (ReplicationStatusVal) txn.getRecord(REPLICATION_STATUS_TABLE, sinkClusterId).getPayload();
                if (sinkStatus != null) {
                    assertThat(sinkStatus.getStatus()).isEqualTo(LogReplicationMetadata.SyncStatus.NOT_STARTED);
                }
                txn.commit();
            }
        }

        // Wait the polling period time and verify sync status again (to make sure it was not erroneously updated)
        Sleep.sleepUninterruptibly(Duration.ofSeconds(LogReplicationAckReader.ACKED_TS_READ_INTERVAL_SECONDS + deltaSeconds));

        try (TxnContext txn = sourceCorfuStore.txn(LogReplicationMetadataManager.NAMESPACE)) {
            sinkStatus = (ReplicationStatusVal)txn.getRecord(REPLICATION_STATUS_TABLE, sinkClusterId).getPayload();
            assertThat(sinkStatus.getStatus()).isEqualTo(LogReplicationMetadata.SyncStatus.NOT_STARTED);
            txn.commit();
        }

        // (3) Next, start sink LR, replication should start. wait until snapshot replication is completed and
        // confirm Log Entry is ONGOING.
        sinkReplicationServer = runReplicationServer(sinkReplicationServerPort, nettyPluginPath);
        waitForReplication(size -> size == firstBatch, mapSink, firstBatch);

        // Verify data on Sink
        for (int i = 0; i < firstBatch; i++) {
            try (TxnContext tx = sinkCorfuStore.txn(NAMESPACE)) {
                assertThat(tx.getRecord(mapSink, Sample.StringKey.newBuilder().setKey(String.valueOf(i)).build())
                        .getPayload().getValue()).isEqualTo(i);
                tx.commit();
            }
        }

        while (!sinkStatus.getSnapshotSyncInfo().getStatus().equals(LogReplicationMetadata.SyncStatus.COMPLETED)) {
            try (TxnContext txn = sourceCorfuStore.txn(LogReplicationMetadataManager.NAMESPACE)) {
                sinkStatus = (ReplicationStatusVal)txn.getRecord(REPLICATION_STATUS_TABLE, sinkClusterId).getPayload();
                txn.commit();
            }
        }

        log.info("Snapshot replication status : COMPLETED");
        // Confirm Log entry Sync status is ONGOING
        assertThat(sinkStatus.getStatus()).isEqualTo(LogReplicationMetadata.SyncStatus.ONGOING);

        // (4) Write noisy streams and check remaining entries
        // Write 'N' entries to source noisy map
        long txTail = sourceRuntime.getSequencerView().query(ObjectsView.getLogReplicatorStreamId());
        PersistentCorfuTable<String, Integer> noisyMap = sourceRuntime.getObjectsView()
                .build()
                .setStreamName(streamName+"noisy")
                .setStreamTags(ObjectsView.getLogReplicatorStreamId())
                .setTypeToken(new TypeToken<PersistentCorfuTable<String, Integer>>() {
                })
                .open();
        for (int i = 0; i < firstBatch; i++) {
            sourceRuntime.getObjectsView().TXBegin();
            noisyMap.insert(String.valueOf(i), i);
            sourceRuntime.getObjectsView().TXEnd();
        }
        assertThat(noisyMap.size()).isEqualTo(firstBatch);
        long newTxTail = sourceRuntime.getSequencerView().query(ObjectsView.getLogReplicatorStreamId());
        assertThat(newTxTail-txTail).isGreaterThanOrEqualTo(firstBatch);

        // Wait the polling period time and verify sync status again (to make sure it was not erroneously updated)
        Sleep.sleepUninterruptibly(Duration.ofSeconds(LogReplicationAckReader.ACKED_TS_READ_INTERVAL_SECONDS + deltaSeconds));

        try (TxnContext txn = sourceCorfuStore.txn(LogReplicationMetadataManager.NAMESPACE)) {
            sinkStatus = (ReplicationStatusVal)txn.getRecord(REPLICATION_STATUS_TABLE, sinkClusterId).getPayload();
            txn.commit();
        }

        // Confirm remaining entries is equal to 0
        assertThat(sinkStatus.getRemainingEntriesToSend()).isEqualTo(0L);

        // (5) Confirm that if sink LR is stopped, in the middle of replication, the status changes to STOPPED
        shutdownCorfuServer(sinkReplicationServer);

        while (!sinkStatus.getStatus().equals(LogReplicationMetadata.SyncStatus.STOPPED)) {
            try (TxnContext txn = sourceCorfuStore.txn(LogReplicationMetadataManager.NAMESPACE)) {
                sinkStatus = (ReplicationStatusVal) txn.getRecord(REPLICATION_STATUS_TABLE, sinkClusterId).getPayload();
                txn.commit();
            }
        }
        assertThat(sinkStatus.getStatus()).isEqualTo(LogReplicationMetadata.SyncStatus.STOPPED);
    }

    /**
     * This test verifies config change with a role switch during a snapshot sync transfer phase.
     * <p>
     * 1. Init with corfu 9000 source and 9001 sink
     * 2. Write 50 entries to source map
     * 3. Start log replication: Node 9010 - source, Node 9020 - sink
     * 4. Perform a role switch with corfu store
     * 5. Sink will drop messages and keep size 0
     * 6. Verify source map becomes size 0, since source size is 0
     */
    //@Test
    public void testNewConfigWithSwitchRoleDuringTransferPhase() throws Exception {
        // Write 50 entry to source map
        for (int i = 0; i < largeBatch; i++) {
            try (TxnContext txn = sourceCorfuStore.txn(NAMESPACE)) {
                txn.putRecord(mapSource, StringKey.newBuilder().setKey(String.valueOf(i)).build(),
                        IntValue.newBuilder().setValue(i).build(), null);
                txn.commit();
            }
        }
        assertThat(mapSource.count()).isEqualTo(largeBatch);
        assertThat(mapSink.count()).isZero();

        log.info("Before log replication, append {} entries to source map. Current source corfu" +
                        "[{}] log tail is {}, sink corfu[{}] log tail is {}", largeBatch, sourceClusterCorfuPort,
                sourceRuntime.getAddressSpaceView().getLogTail(), sinkClusterCorfuPort,
                sinkRuntime.getAddressSpaceView().getLogTail());

        sourceReplicationServer = runReplicationServer(sourceReplicationServerPort);
        sinkReplicationServer = runReplicationServer(sinkReplicationServerPort);
        log.info("Replication servers started, and replication is in progress...");
        TimeUnit.SECONDS.sleep(shortInterval);

        // Perform a role switch during transfer
        assertThat(mapSink.count()).isEqualTo(0);
        try (TxnContext txn = sourceCorfuStore.txn(DefaultClusterManager.CONFIG_NAMESPACE)) {
            txn.putRecord(configTable, DefaultClusterManager.OP_SWITCH, DefaultClusterManager.OP_SWITCH, DefaultClusterManager.OP_SWITCH);
            txn.commit();
        }
        assertThat(configTable.count()).isOne();
        assertThat(mapSink.count()).isEqualTo(0);

        // Wait until source map size becomes 0
        waitForReplication(size -> size == 0, mapSource, 0);
        log.info("After role switch during transfer phase, both maps have size {}. Current " +
            "source corfu[{}] log tail is {}, sink corfu[{}] log tail is {}",
            mapSource.count(), sourceClusterCorfuPort, sourceRuntime.getAddressSpaceView().getLogTail(),
            sinkClusterCorfuPort, sinkRuntime.getAddressSpaceView().getLogTail());

        // Double check after 10 seconds
        TimeUnit.SECONDS.sleep(mediumInterval);
        assertThat(mapSource.count()).isZero();
        assertThat(mapSink.count()).isZero();
    }

    /**
     * This test verifies config change with a role switch during a snapshot sync apply phase.
     * <p>
     * 1. Init with corfu 9000 source and 9001 sink
     * 2. Write 50 entries to source map
     * 3. Start log replication: Node 9010 - source, Node 9020 - sink
     * 4. Wait for Snapshot Sync goes to apply phase
     * 5. Perform a role switch with corfu store
     * 6. Sink will continue apply and have size 50
     * 7. Verify both maps have size 50
     */
    //@Test
    public void testNewConfigWithSwitchRoleDuringApplyPhase() throws Exception {
        // Write 50 entry to source map
        for (int i = 0; i < largeBatch; i++) {
            try (TxnContext txn = sourceCorfuStore.txn(NAMESPACE)) {
                txn.putRecord(mapSource, StringKey.newBuilder().setKey(String.valueOf(i)).build(),
                        IntValue.newBuilder().setValue(i).build(), null);
                txn.commit();
            }
        }
        assertThat(mapSource.count()).isEqualTo(largeBatch);
        assertThat(mapSink.count()).isZero();

        log.info("Before log replication, append {} entries to source map. Current source corfu" +
                        "[{}] log tail is {}, sink corfu[{}] log tail is {}", largeBatch, sourceClusterCorfuPort,
                sourceRuntime.getAddressSpaceView().getLogTail(), sinkClusterCorfuPort,
                sinkRuntime.getAddressSpaceView().getLogTail());

        sourceReplicationServer = runReplicationServer(sourceReplicationServerPort);
        sinkReplicationServer = runReplicationServer(sinkReplicationServerPort);
        log.info("Replication servers started, and replication is in progress...");

        // Wait until apply phase
        UUID sinkStream = CorfuRuntime.getStreamID(streamName);
        while (!sinkRuntime.getAddressSpaceView().getAllTails().getStreamTails().containsKey(sinkStream)) {
            TimeUnit.MILLISECONDS.sleep(100L);
        }

        log.info("======sink tail is : " + sinkRuntime.getAddressSpaceView().getAllTails().getStreamTails().get(sinkStream));

        // Perform a role switch
        try (TxnContext txn = sourceCorfuStore.txn(DefaultClusterManager.CONFIG_NAMESPACE)) {
            txn.putRecord(configTable, DefaultClusterManager.OP_SWITCH, DefaultClusterManager.OP_SWITCH, DefaultClusterManager.OP_SWITCH);
            txn.commit();
        }
        assertThat(configTable.count()).isOne();

        // Should finish apply
        waitForReplication(size -> size == largeBatch, mapSink, largeBatch);
        assertThat(mapSource.count()).isEqualTo(largeBatch);
        log.info("After role switch during apply phase, both maps have size {}. Current " +
                        "source corfu[{}] log tail is {}, sink corfu[{}] log tail is {}",
                mapSource.count(), sourceClusterCorfuPort, sourceRuntime.getAddressSpaceView().getLogTail(),
                sinkClusterCorfuPort, sinkRuntime.getAddressSpaceView().getLogTail());

        // Double check after 10 seconds
        TimeUnit.SECONDS.sleep(mediumInterval);
        assertThat(mapSource.count()).isEqualTo(largeBatch);
        assertThat(mapSink.count()).isEqualTo(largeBatch);
    }

    /**
     * This test verifies config change with two source clusters
     * <p>
     * 1. Init with corfu 9000 source and 9001 sink
     * 2. Write 10 entries to source map
     * 3. Start log replication: Node 9010 - source, Node 9020 - sink
     * 4. Wait for Snapshot Sync, both maps have size 10
     * 5. Write 5 more entries to source map, to verify Log Entry Sync
     * 6. Perform a two-source config update with corfu store
     * 7. Write 5 more entries to source map
     * 8. Verify data will not be replicated, since both are source
     */
    @Test
    public void testNewConfigWithTwoSource() throws Exception {
        // Write 10 entries to source map
        for (int i = 0; i < firstBatch; i++) {
            try (TxnContext txn = sourceCorfuStore.txn(NAMESPACE)) {
                txn.putRecord(mapSource, StringKey.newBuilder().setKey(String.valueOf(i)).build(),
                        IntValue.newBuilder().setValue(i).build(), null);
                txn.commit();
            }
        }
        assertThat(mapSource.count()).isEqualTo(firstBatch);
        assertThat(mapSink.count()).isZero();

        log.info("Before log replication, append {} entries to source map. Current source corfu" +
                        "[{}] log tail is {}, sink corfu[{}] log tail is {}", firstBatch, sourceClusterCorfuPort,
                sourceRuntime.getAddressSpaceView().getLogTail(), sinkClusterCorfuPort,
                sinkRuntime.getAddressSpaceView().getLogTail());

        sourceReplicationServer = runReplicationServer(sourceReplicationServerPort, nettyPluginPath);
        sinkReplicationServer = runReplicationServer(sinkReplicationServerPort, nettyPluginPath);
        log.info("Replication servers started, and replication is in progress...");

        // Wait until data is fully replicated
        waitForReplication(size -> size == firstBatch, mapSink, firstBatch);
        log.info("After full sync, both maps have size {}. Current source corfu[{}] log tail " +
                        "is {}, sink corfu[{}] log tail is {}", firstBatch, sourceClusterCorfuPort,
                sourceRuntime.getAddressSpaceView().getLogTail(), sinkClusterCorfuPort,
                sinkRuntime.getAddressSpaceView().getLogTail());

        // Write 5 entries to source map
        for (int i = firstBatch; i < secondBatch; i++) {
            try (TxnContext txn = sourceCorfuStore.txn(NAMESPACE)) {
                txn.putRecord(mapSource, StringKey.newBuilder().setKey(String.valueOf(i)).build(),
                        IntValue.newBuilder().setValue(i).build(), null);
                txn.commit();
            }
        }
        assertThat(mapSource.count()).isEqualTo(secondBatch);

        // Wait until data is fully replicated again
        waitForReplication(size -> size == secondBatch, mapSink, secondBatch);
        log.info("After delta sync, both maps have size {}. Current source corfu[{}] log tail " +
                        "is {}, sink corfu[{}] log tail is {}", secondBatch, sourceClusterCorfuPort,
                sourceRuntime.getAddressSpaceView().getLogTail(), sinkClusterCorfuPort,
                sinkRuntime.getAddressSpaceView().getLogTail());

        // Verify data
        for (int i = 0; i < secondBatch; i++) {
            try (TxnContext tx = sinkCorfuStore.txn(NAMESPACE)) {
                assertThat(tx.getRecord(mapSink, Sample.StringKey.newBuilder().setKey(String.valueOf(i)).build())
                        .getPayload().getValue()).isEqualTo(i);
                tx.commit();
            }
        }
        log.info("Log replication succeeds without config change!");

        // Perform a config update with two source
        try (TxnContext txn = sourceCorfuStore.txn(DefaultClusterManager.CONFIG_NAMESPACE)) {
            txn.putRecord(configTable, DefaultClusterManager.OP_TWO_SOURCE, DefaultClusterManager.OP_TWO_SOURCE, DefaultClusterManager.OP_TWO_SOURCE);
            txn.commit();
        }
        assertThat(configTable.count()).isOne();
        log.info("New topology config applied!");
        TimeUnit.SECONDS.sleep(mediumInterval);

        // Append to mapSource
        for (int i = secondBatch; i < thirdBatch; i++) {
            try (TxnContext txn = sourceCorfuStore.txn(NAMESPACE)) {
                txn.putRecord(mapSource, StringKey.newBuilder().setKey(String.valueOf(i)).build(),
                        IntValue.newBuilder().setValue(i).build(), null);
                txn.commit();
            }
        }
        assertThat(mapSource.count()).isEqualTo(thirdBatch);
        log.info("Source map has {} entries now!", thirdBatch);

        // Sink map should still have secondBatch size
        log.info("Sink map should still have {} size", secondBatch);
        waitForReplication(size -> size == secondBatch, mapSink, secondBatch);

        // Double check after 10 seconds
        TimeUnit.SECONDS.sleep(mediumInterval);
        assertThat(mapSource.count()).isEqualTo(thirdBatch);
        assertThat(mapSink.count()).isEqualTo(secondBatch);
    }

    /**
     * This test verifies config change with two sink clusters
     * <p>
     * 1. Init with corfu 9000 source and 9001 sink
     * 2. Write 10 entries to source map
     * 3. Start log replication: Node 9010 - source, Node 9020 - sink
     * 4. Wait for Snapshot Sync, both maps have size 10
     * 5. Write 5 more entries to source map, to verify Log Entry Sync
     * 6. Perform a two-sink config update with corfu store
     * 7. Write 5 more entries to source map
     * 8. Verify data will not be replicated, since both are sink
     */
    @Test
    public void testNewConfigWithAllSink() throws Exception {
        // Write 10 entries to source map
        for (int i = 0; i < firstBatch; i++) {
            try (TxnContext txn = sourceCorfuStore.txn(NAMESPACE)) {
                txn.putRecord(mapSource, StringKey.newBuilder().setKey(String.valueOf(i)).build(),
                        IntValue.newBuilder().setValue(i).build(), null);
                txn.commit();
            }
        }
        assertThat(mapSource.count()).isEqualTo(firstBatch);
        assertThat(mapSink.count()).isZero();

        log.info("Before log replication, append {} entries to source map. Current source corfu" +
                        "[{}] log tail is {}, sink corfu[{}] log tail is {}", firstBatch, sourceClusterCorfuPort,
                sourceRuntime.getAddressSpaceView().getLogTail(), sinkClusterCorfuPort,
                sinkRuntime.getAddressSpaceView().getLogTail());

        sourceReplicationServer = runReplicationServer(sourceReplicationServerPort, nettyPluginPath);
        sinkReplicationServer = runReplicationServer(sinkReplicationServerPort, nettyPluginPath);
        log.info("Replication servers started, and replication is in progress...");

        // Wait until data is fully replicated
        waitForReplication(size -> size == firstBatch, mapSink, firstBatch);
        log.info("After full sync, both maps have size {}. Current source corfu[{}] log tail " +
                        "is {}, sink corfu[{}] log tail is {}", firstBatch, sourceClusterCorfuPort,
                sourceRuntime.getAddressSpaceView().getLogTail(), sinkClusterCorfuPort,
                sinkRuntime.getAddressSpaceView().getLogTail());

        // Write 5 entries to source map
        for (int i = firstBatch; i < secondBatch; i++) {
            try (TxnContext txn = sourceCorfuStore.txn(NAMESPACE)) {
                txn.putRecord(mapSource, StringKey.newBuilder().setKey(String.valueOf(i)).build(),
                        IntValue.newBuilder().setValue(i).build(), null);
                txn.commit();
            }
        }
        assertThat(mapSource.count()).isEqualTo(secondBatch);

        // Wait until data is fully replicated again
        waitForReplication(size -> size == secondBatch, mapSink, secondBatch);
        log.info("After delta sync, both maps have size {}. Current source corfu[{}] log tail " +
                        "is {}, sink corfu[{}] log tail is {}", secondBatch, sourceClusterCorfuPort,
                sourceRuntime.getAddressSpaceView().getLogTail(), sinkClusterCorfuPort,
                sinkRuntime.getAddressSpaceView().getLogTail());

        // Verify data
        for (int i = 0; i < secondBatch; i++) {
            try (TxnContext tx = sinkCorfuStore.txn(NAMESPACE)) {
                assertThat(tx.getRecord(mapSink, Sample.StringKey.newBuilder().setKey(String.valueOf(i)).build())
                        .getPayload().getValue()).isEqualTo(i);
                tx.commit();
            }
        }
        log.info("Log replication succeeds without config change!");

        // Perform a config update with all sink
        try (TxnContext txn = sourceCorfuStore.txn(DefaultClusterManager.CONFIG_NAMESPACE)) {
            txn.putRecord(configTable, DefaultClusterManager.OP_ALL_SINK, DefaultClusterManager.OP_ALL_SINK, DefaultClusterManager.OP_ALL_SINK);
            txn.commit();
        }
        assertThat(configTable.count()).isOne();
        log.info("New topology config applied!");
        TimeUnit.SECONDS.sleep(mediumInterval);

        for (int i = secondBatch; i < thirdBatch; i++) {
            try (TxnContext txn = sourceCorfuStore.txn(NAMESPACE)) {
                txn.putRecord(mapSource, StringKey.newBuilder().setKey(String.valueOf(i)).build(),
                        IntValue.newBuilder().setValue(i).build(), null);
                txn.commit();
            }
        }
        assertThat(mapSource.count()).isEqualTo(thirdBatch);
        log.info("Source map has {} entries now!", thirdBatch);

        // Sink map should still have secondBatch size
        log.info("Sink map should still have {} size", secondBatch);
        waitForReplication(size -> size == secondBatch, mapSink, secondBatch);

        // Double check after 10 seconds
        TimeUnit.SECONDS.sleep(mediumInterval);
        assertThat(mapSource.count()).isEqualTo(thirdBatch);
        assertThat(mapSink.count()).isEqualTo(secondBatch);
    }

    /**
     * This test verifies config change with one source and one invalid
     * <p>
     * 1. Init with corfu 9000 source and 9001 sink
     * 2. Write 10 entries to source map
     * 3. Start log replication: Node 9010 - source, Node 9020 - sink
     * 4. Wait for Snapshot Sync, both maps have size 10
     * 5. Write 5 more entries to source map, to verify Log Entry Sync
     * 6. Perform a source-invalid config update with corfu store
     * 7. Write 5 more entries to source map
     * 8. Verify data will not be replicated, since sink is invalid
     * 9. Resume to sink and verify data is fully replicated again.
     */
    @Test
    public void testNewConfigWithInvalidClusters() throws Exception {
        // Write 10 entries to source map
        for (int i = 0; i < firstBatch; i++) {
            try (TxnContext txn = sourceCorfuStore.txn(NAMESPACE)) {
                txn.putRecord(mapSource, StringKey.newBuilder().setKey(String.valueOf(i)).build(),
                        IntValue.newBuilder().setValue(i).build(), null);
                txn.commit();
            }
        }
        assertThat(mapSource.count()).isEqualTo(firstBatch);
        assertThat(mapSink.count()).isZero();

        log.info("Before log replication, append {} entries to source map. Current source corfu" +
                        "[{}] log tail is {}, sink corfu[{}] log tail is {}", firstBatch, sourceClusterCorfuPort,
                sourceRuntime.getAddressSpaceView().getLogTail(), sinkClusterCorfuPort,
                sinkRuntime.getAddressSpaceView().getLogTail());

        sourceReplicationServer = runReplicationServer(sourceReplicationServerPort, nettyPluginPath);
        sinkReplicationServer = runReplicationServer(sinkReplicationServerPort, nettyPluginPath);
        log.info("Replication servers started, and replication is in progress...");

        // Wait until data is fully replicated
        waitForReplication(size -> size == firstBatch, mapSink, firstBatch);
        log.info("After full sync, both maps have size {}. Current source corfu[{}] log tail " +
                        "is {}, sink corfu[{}] log tail is {}", firstBatch, sourceClusterCorfuPort,
                sourceRuntime.getAddressSpaceView().getLogTail(), sinkClusterCorfuPort,
                sinkRuntime.getAddressSpaceView().getLogTail());

        // Write 5 entries to source map
        for (int i = firstBatch; i < secondBatch; i++) {
            try (TxnContext txn = sourceCorfuStore.txn(NAMESPACE)) {
                txn.putRecord(mapSource, StringKey.newBuilder().setKey(String.valueOf(i)).build(),
                        IntValue.newBuilder().setValue(i).build(), null);
                txn.commit();
            }
        }
        assertThat(mapSource.count()).isEqualTo(secondBatch);

        // Wait until data is fully replicated again
        waitForReplication(size -> size == secondBatch, mapSink, secondBatch);
        log.info("After delta sync, both maps have size {}. Current source corfu[{}] log tail " +
                        "is {}, sink corfu[{}] log tail is {}", secondBatch, sourceClusterCorfuPort,
                sourceRuntime.getAddressSpaceView().getLogTail(), sinkClusterCorfuPort,
                sinkRuntime.getAddressSpaceView().getLogTail());

        // Verify data
        for (int i = 0; i < secondBatch; i++) {
            try (TxnContext tx = sinkCorfuStore.txn(NAMESPACE)) {
                assertThat(tx.getRecord(mapSink, Sample.StringKey.newBuilder().setKey(String.valueOf(i)).build())
                        .getPayload().getValue()).isEqualTo(i);
                tx.commit();
            }
        }
        //verify the status table before topology change
        LogReplicationMetadata.ReplicationStatusKey key =
                LogReplicationMetadata.ReplicationStatusKey
                        .newBuilder()
                        .setClusterId(new DefaultClusterConfig().getSinkClusterIds().get(0))
                        .build();
        ReplicationStatusVal replicationStatus;
        try (TxnContext txn = sourceCorfuStore.txn(LogReplicationMetadataManager.NAMESPACE)) {
            replicationStatus = (ReplicationStatusVal)txn.getRecord(REPLICATION_STATUS_TABLE, key).getPayload();
            txn.commit();
        }
        assertThat(replicationStatus).isNotNull();
        log.info("Log replication succeeds without config change!");

        // Perform a config update with invalid state
        try (TxnContext txn = sourceCorfuStore.txn(DefaultClusterManager.CONFIG_NAMESPACE)) {
            txn.putRecord(configTable, DefaultClusterManager.OP_INVALID, DefaultClusterManager.OP_INVALID, DefaultClusterManager.OP_INVALID);
            txn.commit();
        }
        assertThat(configTable.count()).isOne();
        log.info("New topology config applied!");
        TimeUnit.SECONDS.sleep(mediumInterval);

        // Append to mapSource
        for (int i = secondBatch; i < thirdBatch; i++) {
            try (TxnContext txn = sourceCorfuStore.txn(NAMESPACE)) {
                txn.putRecord(mapSource, StringKey.newBuilder().setKey(String.valueOf(i)).build(),
                        IntValue.newBuilder().setValue(i).build(), null);
                txn.commit();
            }
        }
        assertThat(mapSource.count()).isEqualTo(thirdBatch);

        // Sink map should still have secondBatch size
        waitForReplication(size -> size == secondBatch, mapSink, secondBatch);

        // Double check after 10 seconds
        TimeUnit.SECONDS.sleep(mediumInterval);
        assertThat(mapSource.count()).isEqualTo(thirdBatch);
        assertThat(mapSink.count()).isEqualTo(secondBatch);
        // verify that source doesn't have the invalid cluster info in the LR status table
        try (TxnContext txn = sourceCorfuStore.txn(LogReplicationMetadataManager.NAMESPACE)) {
            replicationStatus = (ReplicationStatusVal)txn.getRecord(REPLICATION_STATUS_TABLE, key).getPayload();
            txn.commit();
        }
        assertThat(replicationStatus).isNull();
        log.info("After {} seconds sleep, double check passed", mediumInterval);

        // Change to default source sink config
        try (TxnContext txn = sourceCorfuStore.txn(DefaultClusterManager.CONFIG_NAMESPACE)) {
            txn.putRecord(configTable, DefaultClusterManager.OP_RESUME, DefaultClusterManager.OP_RESUME, DefaultClusterManager.OP_RESUME);
            txn.commit();
        }
        assertThat(configTable.count()).isEqualTo(2);
        log.info("New topology config applied!");
        TimeUnit.SECONDS.sleep(mediumInterval);

        // Sink map should have thirdBatch size, since topology config is resumed.
        waitForReplication(size -> size == thirdBatch, mapSink, thirdBatch);

        // Double check after 10 seconds
        TimeUnit.SECONDS.sleep(mediumInterval);
        assertThat(mapSource.count()).isEqualTo(thirdBatch);
        assertThat(mapSink.count()).isEqualTo(thirdBatch);
    }

    /**
     * This test verifies enforceSnapshotSync API
     * <p>
     * 1. Init with corfu 9000 source and 9001 sink
     * 2. Write 10 entries to source map
     * 3. Start log replication: Node 9010 - source, Node 9020 - sink
     * 4. Wait for Snapshot Sync, both maps have size 10
     * 5. Write 5 more entries to source map, to verify Log Entry Sync
     * 6. Write 5 more entries to source map and perform an enforced full snapshot sync
     * 7. Verify a full snapshot sync is triggered
     * 8. Verify a full snapshot sync is completed and data is correctly replicated.
     */
    @Test
    public void testEnforceSnapshotSync() throws Exception {
        // Write 10 entries to source map
        for (int i = 0; i < firstBatch; i++) {
            try (TxnContext txn = sourceCorfuStore.txn(NAMESPACE)) {
                txn.putRecord(mapSource, StringKey.newBuilder().setKey(String.valueOf(i)).build(),
                        IntValue.newBuilder().setValue(i).build(), null);
                txn.commit();
            }
        }
        assertThat(mapSource.count()).isEqualTo(firstBatch);
        assertThat(mapSink.count()).isZero();

        log.info("Before log replication, append {} entries to source map. Current source corfu" +
                        "[{}] log tail is {}, sink corfu[{}] log tail is {}", firstBatch, sourceClusterCorfuPort,
                sourceRuntime.getAddressSpaceView().getLogTail(), sinkClusterCorfuPort,
                sinkRuntime.getAddressSpaceView().getLogTail());

        sourceReplicationServer = runReplicationServer(sourceReplicationServerPort, nettyPluginPath);
        sinkReplicationServer = runReplicationServer(sinkReplicationServerPort, nettyPluginPath);
        log.info("Replication servers started, and replication is in progress...");

        // Wait until data is fully replicated
        waitForReplication(size -> size == firstBatch, mapSink, firstBatch);

        log.info("After full sync, both maps have size {}. Current source corfu[{}] log tail " +
                        "is {}, sink corfu[{}] log tail is {}",
                firstBatch, sourceClusterCorfuPort, sourceRuntime.getAddressSpaceView().getLogTail(),
                sinkClusterCorfuPort, sinkRuntime.getAddressSpaceView().getLogTail());

        // Verify Sync Status
        Sleep.sleepUninterruptibly(Duration.ofSeconds(3));
        LogReplicationMetadata.ReplicationStatusKey key =
                LogReplicationMetadata.ReplicationStatusKey
                        .newBuilder()
                        .setClusterId(new DefaultClusterConfig().getSinkClusterIds().get(0))
                        .build();

        LogReplicationMetadata.ReplicationStatusVal replicationStatusVal;
        try (TxnContext txn = sourceCorfuStore.txn(LogReplicationMetadataManager.NAMESPACE)) {
            replicationStatusVal = (ReplicationStatusVal)txn.getRecord(REPLICATION_STATUS_TABLE, key).getPayload();
            txn.commit();
        }

        log.info("ReplicationStatusVal: RemainingEntriesToSend: {}, SyncType: {}, Status: {}",
                replicationStatusVal.getRemainingEntriesToSend(), replicationStatusVal.getSyncType(),
                replicationStatusVal.getStatus());

        log.info("SnapshotSyncInfo: Base: {}, Type: {}, Status: {}, CompletedTime: {}",
                replicationStatusVal.getSnapshotSyncInfo().getBaseSnapshot(), replicationStatusVal.getSnapshotSyncInfo().getType(),
                replicationStatusVal.getSnapshotSyncInfo().getStatus(), replicationStatusVal.getSnapshotSyncInfo().getCompletedTime());

        assertThat(replicationStatusVal.getSyncType())
                .isEqualTo(LogReplicationMetadata.ReplicationStatusVal.SyncType.LOG_ENTRY);
        assertThat(replicationStatusVal.getStatus())
                .isEqualTo(LogReplicationMetadata.SyncStatus.ONGOING);

        assertThat(replicationStatusVal.getSnapshotSyncInfo().getType())
                .isEqualTo(LogReplicationMetadata.SnapshotSyncInfo.SnapshotSyncType.DEFAULT);
        assertThat(replicationStatusVal.getSnapshotSyncInfo().getStatus())
                .isEqualTo(LogReplicationMetadata.SyncStatus.COMPLETED);


        // Write 5 entries to source map
        for (int i = firstBatch; i < secondBatch; i++) {
            try (TxnContext txn = sourceCorfuStore.txn(NAMESPACE)) {
                txn.putRecord(mapSource, StringKey.newBuilder().setKey(String.valueOf(i)).build(),
                        IntValue.newBuilder().setValue(i).build(), null);
                txn.commit();
            }
        }
        assertThat(mapSource.count()).isEqualTo(secondBatch);

        // Wait until data is fully replicated again
        waitForReplication(size -> size == secondBatch, mapSink, secondBatch);
        log.info("After delta sync, both maps have size {}. Current source corfu[{}] log tail " +
                        "is {}, sink corfu[{}] log tail is {}", secondBatch, sourceClusterCorfuPort,
                sourceRuntime.getAddressSpaceView().getLogTail(), sinkClusterCorfuPort,
                sinkRuntime.getAddressSpaceView().getLogTail());

        // Verify data
        for (int i = 0; i < secondBatch; i++) {
            try (TxnContext tx = sinkCorfuStore.txn(NAMESPACE)) {
                assertThat(tx.getRecord(mapSink, Sample.StringKey.newBuilder().setKey(String.valueOf(i)).build())
                        .getPayload().getValue()).isEqualTo(i);
                tx.commit();
            }
        }

        // Append to mapSource
        for (int i = secondBatch; i < thirdBatch; i++) {
            try (TxnContext txn = sourceCorfuStore.txn(NAMESPACE)) {
                txn.putRecord(mapSource, StringKey.newBuilder().setKey(String.valueOf(i)).build(),
                        IntValue.newBuilder().setValue(i).build(), null);
                txn.commit();
            }
        }
        assertThat(mapSource.count()).isEqualTo(thirdBatch);

        // Perform an enforce full snapshot sync
        try (TxnContext txn = sourceCorfuStore.txn(DefaultClusterManager.CONFIG_NAMESPACE)) {
            txn.putRecord(configTable, DefaultClusterManager.OP_ENFORCE_SNAPSHOT_FULL_SYNC,
                    DefaultClusterManager.OP_ENFORCE_SNAPSHOT_FULL_SYNC, DefaultClusterManager.OP_ENFORCE_SNAPSHOT_FULL_SYNC);
            txn.commit();
        }
        TimeUnit.SECONDS.sleep(mediumInterval);

        // Sink map should have thirdBatch size, since topology config is resumed.
        waitForReplication(size -> size == thirdBatch, mapSink, thirdBatch);
        assertThat(mapSink.count()).isEqualTo(thirdBatch);

        // Verify that a forced snapshot sync is finished.
        try (TxnContext txn = sourceCorfuStore.txn(LogReplicationMetadataManager.NAMESPACE)) {
            replicationStatusVal = (ReplicationStatusVal)txn.getRecord(REPLICATION_STATUS_TABLE, key).getPayload();
            txn.commit();
        }

        log.info("ReplicationStatusVal: RemainingEntriesToSend: {}, SyncType: {}, Status: {}",
                replicationStatusVal.getRemainingEntriesToSend(), replicationStatusVal.getSyncType(),
                replicationStatusVal.getStatus());

        log.info("SnapshotSyncInfo: Base: {}, Type: {}, Status: {}, CompletedTime: {}",
                replicationStatusVal.getSnapshotSyncInfo().getBaseSnapshot(), replicationStatusVal.getSnapshotSyncInfo().getType(),
                replicationStatusVal.getSnapshotSyncInfo().getStatus(), replicationStatusVal.getSnapshotSyncInfo().getCompletedTime());

        assertThat(replicationStatusVal.getSyncType())
                .isEqualTo(LogReplicationMetadata.ReplicationStatusVal.SyncType.LOG_ENTRY);
        assertThat(replicationStatusVal.getStatus())
                .isEqualTo(LogReplicationMetadata.SyncStatus.ONGOING);
        assertThat(replicationStatusVal.getSnapshotSyncInfo().getType())
                .isEqualTo(LogReplicationMetadata.SnapshotSyncInfo.SnapshotSyncType.FORCED);
        assertThat(replicationStatusVal.getSnapshotSyncInfo().getStatus())
                .isEqualTo(LogReplicationMetadata.SyncStatus.COMPLETED);
    }


    /**
     * This test verifies source's lock release
     * <p>
     * 1. Init with corfu 9000 source and 9001 sink
     * 2. Write 10 entries to source map
     * 3. Start log replication: Node 9010 - source, Node 9020 - sink
     * 4. Wait for Snapshot Sync, both maps have size 10
     * 5. Write 5 more entries to source map, to verify Log Entry Sync
     * 6. Revoke source's lock and wait for 10 sec
     * 7. Write 5 more entries to source map
     * 8. Verify data will not be replicated, since source's lock is released
     */
    @Test
    public void testSourceLockRelease() throws Exception {
        // Write 10 entries to source map
        for (int i = 0; i < firstBatch; i++) {
            try (TxnContext txn = sourceCorfuStore.txn(NAMESPACE)) {
                txn.putRecord(mapSource, StringKey.newBuilder().setKey(String.valueOf(i)).build(),
                        IntValue.newBuilder().setValue(i).build(), null);
                txn.commit();
            }
        }
        assertThat(mapSource.count()).isEqualTo(firstBatch);
        assertThat(mapSink.count()).isZero();

        log.info("Before log replication, append {} entries to source map. Current source corfu" +
                        "[{}] log tail is {}, sink corfu[{}] log tail is {}", firstBatch, sourceClusterCorfuPort,
                sourceRuntime.getAddressSpaceView().getLogTail(), sinkClusterCorfuPort,
                sinkRuntime.getAddressSpaceView().getLogTail());

        // Start the source and sink replication servers with a lockLeaseDuration = 10 seconds.
        // The default lease duration is 60 seconds.  The duration between lease checks is set by the Discovery
        // Service to leaseDuration/10.  So reducing the lease duration will cause the detection of lease
        // expiry faster, i.e., 1 second instead of 6
        int lockLeaseDuration = 10;
        sourceReplicationServer = runReplicationServer(sourceReplicationServerPort, nettyPluginPath, lockLeaseDuration);
        sinkReplicationServer = runReplicationServer(sinkReplicationServerPort, nettyPluginPath, lockLeaseDuration);
        log.info("Replication servers started, and replication is in progress...");

        // Wait until data is fully replicated
        waitForReplication(size -> size == firstBatch, mapSink, firstBatch);
        log.info("After full sync, both maps have size {}. Current source corfu[{}] log tail " +
                        "is {}, sink corfu[{}] log tail is {}", firstBatch, sourceClusterCorfuPort,
                sourceRuntime.getAddressSpaceView().getLogTail(), sinkClusterCorfuPort,
                sinkRuntime.getAddressSpaceView().getLogTail());

        // Write 5 entries to source map
        for (int i = firstBatch; i < secondBatch; i++) {
            try (TxnContext txn = sourceCorfuStore.txn(NAMESPACE)) {
                txn.putRecord(mapSource, StringKey.newBuilder().setKey(String.valueOf(i)).build(),
                        IntValue.newBuilder().setValue(i).build(), null);
                txn.commit();
            }
        }
        assertThat(mapSource.count()).isEqualTo(secondBatch);

        // Wait until data is fully replicated again
        waitForReplication(size -> size == secondBatch, mapSink, secondBatch);
        log.info("After delta sync, both maps have size {}. Current source corfu[{}] log tail " +
                        "is {}, sink corfu[{}] log tail is {}", secondBatch, sourceClusterCorfuPort,
                sourceRuntime.getAddressSpaceView().getLogTail(), sinkClusterCorfuPort,
                sinkRuntime.getAddressSpaceView().getLogTail());

        // Verify data
        for (int i = 0; i < secondBatch; i++) {
            try (TxnContext tx = sinkCorfuStore.txn(NAMESPACE)) {
                assertThat(tx.getRecord(mapSink, Sample.StringKey.newBuilder().setKey(String.valueOf(i)).build())
                        .getPayload().getValue()).isEqualTo(i);
                tx.commit();
            }
        }
        log.info("Log replication succeeds without config change!");

        // Create a listener on the ReplicationStatus table on the Source cluster, which waits for Replication status
        // to change to STOPPED
        CountDownLatch countDownLatch = new CountDownLatch(1);
        ReplicationStopListener listener = new ReplicationStopListener(countDownLatch);
        sourceCorfuStore.subscribeListener(listener, LogReplicationMetadataManager.NAMESPACE,
            LogReplicationMetadataManager.LR_STATUS_STREAM_TAG);

        // Release Source's lock by deleting the lock table
        try (TxnContext txnContext = sourceCorfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            txnContext.clear(sourceLockTable);
            txnContext.commit();
        }

        Assert.assertEquals(0, sourceLockTable.count());
        log.info("Source's lock table cleared!");

        // Wait till the lock release is asynchronously processed and the replication status on Source changes to
        // STOPPED
        countDownLatch.await();

        // Write more data on the Source
        for (int i = secondBatch; i < thirdBatch; i++) {
            try (TxnContext txn = sourceCorfuStore.txn(NAMESPACE)) {
                txn.putRecord(mapSource, StringKey.newBuilder().setKey(String.valueOf(i)).build(),
                        IntValue.newBuilder().setValue(i).build(), null);
                txn.commit();
            }
        }
        assertThat(mapSource.count()).isEqualTo(thirdBatch);
        log.info("Source map has {} entries now!", thirdBatch);

        // Sink map should still have secondBatch size
        log.info("Sink map should still have {} size", secondBatch);
        assertThat(mapSink.count()).isEqualTo(secondBatch);
    }

    /**
     * This test verifies log entry sync works after a force snapshot sync
     * in the backup/restore workflow.
     * <p>
     * 1. Init with corfu 9000 source, 9001 sink, 9002 backup
     * 2. Write 10 entries to source map
     * 3. Start log replication: Node 9010 - source, Node 9020 - sink, Node 9030 - backup
     * 4. Wait for Snapshot Sync, both maps have size 10
     * 5. Write 50 entries to source map, to verify Log Entry Sync
     * 6. Right now both cluster have 50 entries,
     *    and we have a cluster that is a backup of source when source has 10 entries.
     *    Change the topology, the backup cluster 9030 will be the new source cluster.
     * 7. Trigger a force snapshot sync
     * 8. Verify the force snapshot sync is completed
     * 9. Write 5 entries to backup map, to verify Log Entry Sync
     */
    @Test
    public void testBackupRestoreWorkflow() throws Exception {
        Process backupCorfu = runServer(backupClusterCorfuPort, true);
        Process backupReplicationServer = runReplicationServer(backupReplicationServerPort, nettyPluginPath);

        CorfuRuntime.CorfuRuntimeParameters params = CorfuRuntime.CorfuRuntimeParameters
                .builder()
                .build();
        CorfuRuntime backupRuntime = CorfuRuntime.fromParameters(params);
        backupRuntime.parseConfigurationString(backupCorfuEndpoint).connect();
        CorfuStore backupCorfuStore = new CorfuStore(backupRuntime);

        Table<StringKey, IntValue, Metadata> mapBackup = backupCorfuStore.openTable(
                NAMESPACE,
                streamName,
                StringKey.class,
                IntValue.class,
                Metadata.class,
                TableOptions.builder().schemaOptions(
                                CorfuOptions.SchemaOptions.newBuilder()
                                        .setIsFederated(true)
                                        .addStreamTag(ObjectsView.LOG_REPLICATOR_STREAM_INFO.getTagName())
                                        .build())
                        .build()
        );

        assertThat(mapBackup.count()).isZero();

        // Write 10 entries to source map
        writeEntries(sourceCorfuStore, 0, firstBatch, mapSource);

        // Write 50 entries of dummy data to sink map, so we make it have longer corfu log tail.
        // We can also use it to confirm data is wiped during the snapshot sync.
        writeEntries(sinkCorfuStore, 0, largeBatch, mapSink);
        assertThat(mapSource.count()).isEqualTo(firstBatch);
        assertThat(mapSink.count()).isEqualTo(largeBatch);

        log.info("Before log replication, append {} entries to source map. Current source corfu" +
                        "[{}] log tail is {}, sink corfu[{}] log tail is {}", firstBatch, sourceClusterCorfuPort,
                sourceRuntime.getAddressSpaceView().getLogTail(), sinkClusterCorfuPort,
                sinkRuntime.getAddressSpaceView().getLogTail());

        sourceReplicationServer = runReplicationServer(sourceReplicationServerPort, nettyPluginPath);
        sinkReplicationServer = runReplicationServer(sinkReplicationServerPort, nettyPluginPath);
        log.info("Replication servers started, and replication is in progress...");

        // Wait until data is fully replicated
        waitForReplication(size -> size == firstBatch, mapSink, firstBatch);

        log.info("After full sync, both maps have size {}. Current source corfu[{}] log tail " +
                        "is {}, sink corfu[{}] log tail is {}",
                firstBatch, sourceClusterCorfuPort, sourceRuntime.getAddressSpaceView().getLogTail(),
                sinkClusterCorfuPort, sinkRuntime.getAddressSpaceView().getLogTail());

        // Write 50 entries to source map
        writeEntries(sourceCorfuStore, 0, largeBatch, mapSource);
        assertThat(mapSource.count()).isEqualTo(largeBatch);

        // Write 10 entries to backup map
        // It is a backup of the source cluster when it has 10 entries
        writeEntries(backupCorfuStore, 0, firstBatch, mapBackup);

        // Wait until data is fully replicated again
        waitForReplication(size -> size == largeBatch, mapSink, largeBatch);
        log.info("After delta sync, both maps have size {}. Current source corfu[{}] log tail " +
                        "is {}, sink corfu[{}] log tail is {}", secondBatch, sourceClusterCorfuPort,
                sourceRuntime.getAddressSpaceView().getLogTail(), sinkClusterCorfuPort,
                sinkRuntime.getAddressSpaceView().getLogTail());

        // Verify data
        for (int i = 0; i < largeBatch; i++) {
            try (TxnContext tx = sinkCorfuStore.txn(NAMESPACE)) {
                assertThat(tx.getRecord(mapSink, Sample.StringKey.newBuilder().setKey(String.valueOf(i)).build())
                        .getPayload().getValue()).isEqualTo(i);
                tx.commit();
            }
        }

        // Change the topology - brings up the backup cluster
        updateTopology(sourceCorfuStore, DefaultClusterManager.OP_BACKUP);
        log.info("Change the topology!!!");

        TimeUnit.SECONDS.sleep(shortInterval);


        // Perform an enforce full snapshot sync
        updateTopology(sourceCorfuStore, DefaultClusterManager.OP_ENFORCE_SNAPSHOT_FULL_SYNC);
        TimeUnit.SECONDS.sleep(mediumInterval);

        // Sink map size should be 10
        waitForReplication(size -> size == firstBatch, mapSink, firstBatch);

        // Write 5 entries to backup map
        writeEntries(backupCorfuStore, firstBatch, secondBatch, mapBackup);
        assertThat(mapBackup.count()).isEqualTo(secondBatch);

        // Wait until data is fully replicated again
        waitForReplication(size -> size == secondBatch, mapSink, secondBatch);
        log.info("After delta sync, both maps have size {}. Current backup corfu[{}] log tail " +
                        "is {}, sink corfu[{}] log tail is {}", firstBatch, backupClusterCorfuPort,
                backupRuntime.getAddressSpaceView().getLogTail(), sinkClusterCorfuPort,
                sinkRuntime.getAddressSpaceView().getLogTail());

        shutdownCorfuServer(backupCorfu);
        shutdownCorfuServer(backupReplicationServer);
    }

    private void waitForReplication(IntPredicate verifier, Table table, int expected) {
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_MODERATE; i++) {
            log.info("Waiting for replication, table size is {}, expected size is {}", table.count(), expected);
            if (verifier.test(table.count())) {
                break;
            }
            sleepUninterruptibly(shortInterval);
        }
        assertThat(verifier.test(table.count())).isTrue();
    }

    private void sleepUninterruptibly(long seconds) {
        try {
            TimeUnit.SECONDS.sleep(seconds);
        } catch (InterruptedException ie) {
            throw new UnrecoverableCorfuInterruptedError(ie);
        }
    }

    private void writeEntries(CorfuStore corfuStore, int startIdx, int endIdx, Table<StringKey, IntValue, Metadata> table) {
        for (int i = startIdx; i < endIdx; i++) {
            try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
                txn.putRecord(table, StringKey.newBuilder().setKey(String.valueOf(i)).build(),
                        IntValue.newBuilder().setValue(i).build(), null);
                txn.commit();
            }
        }
    }

    private void updateTopology(CorfuStore corfuStore, ClusterUuidMsg op) {
        try (TxnContext txn = corfuStore.txn(DefaultClusterManager.CONFIG_NAMESPACE)) {
            txn.putRecord(configTable, op, op, op);
            txn.commit();
        }
    }

    // StreamListener on the ReplicationStatus table which updates the latch when ReplicationStatus reaches STOPPED
    private class ReplicationStopListener implements StreamListener {

        private CountDownLatch countDownLatch;

        ReplicationStopListener(CountDownLatch countDownLatch) {
            this.countDownLatch = countDownLatch;
        }

        @Override
        public void onNext(CorfuStreamEntries results) {
            results.getEntries().forEach((schema, entries) -> entries.forEach(e -> {
                LogReplicationMetadata.ReplicationStatusVal statusVal =
                    (LogReplicationMetadata.ReplicationStatusVal)e.getPayload();
                if (statusVal.getStatus().equals(LogReplicationMetadata.SyncStatus.STOPPED)) {
                    countDownLatch.countDown();
                }
            }));
        }

        @Override
        public void onError(Throwable throwable) {
            String errorMsg = "Error in ReplicationStopListener: ";
            log.error(errorMsg, throwable);
            fail(errorMsg + throwable.toString());
        }
    }
}
