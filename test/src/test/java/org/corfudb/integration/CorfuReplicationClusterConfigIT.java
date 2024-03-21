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

    private static final long shortInterval = 1L;
    private static final long mediumInterval = 10L;
    private static final int firstBatch = 10;
    private static final int secondBatch = 15;
    private static final int thirdBatch = 20;
    private static final int fourthBatch = 25;
    private static final int largeBatch = 50;

    private static final int activeClusterCorfuPort = 9000;
    private static final int standbyClusterCorfuPort = 9001;
    private static final int backupClusterCorfuPort = 9002;
    private static final int activeReplicationServerPort = 9010;
    private static final int standbyReplicationServerPort = 9020;
    private static final int backupReplicationServerPort = 9030;
    private static final String activeCorfuEndpoint = DEFAULT_HOST + ":" + activeClusterCorfuPort;
    private static final String standbyCorfuEndpoint = DEFAULT_HOST + ":" + standbyClusterCorfuPort;
    private static final String backupCorfuEndpoint = DEFAULT_HOST + ":" + backupClusterCorfuPort;

    private static final String REPLICATION_STATUS_TABLE = "LogReplicationStatus";

    private Process activeCorfuServer = null;
    private Process standbyCorfuServer = null;
    private Process activeReplicationServer = null;
    private Process standbyReplicationServer = null;

    private CorfuRuntime activeRuntime;
    private CorfuRuntime standbyRuntime;
    private Table<StringKey, IntValue, Metadata> mapActive;
    private Table<StringKey, IntValue, Metadata> mapStandby;

    private CorfuStore activeCorfuStore;
    private CorfuStore standbyCorfuStore;
    private Table<ClusterUuidMsg, ClusterUuidMsg, ClusterUuidMsg> configTable;
    private Table<LockDataTypes.LockId, LockDataTypes.LockData, Message> activeLockTable;

    public Map<String, Table<StringKey, IntValueTag, Metadata>> mapNameToMapActive;
    public Map<String, Table<StringKey, IntValueTag, Metadata>> mapNameToMapStandby;

    public static final String TABLE_PREFIX = "Table00";

    public static final String NAMESPACE = "LR-Test";

    @Before
    public void setUp() throws Exception {
        activeCorfuServer = runServer(activeClusterCorfuPort, true);
        standbyCorfuServer = runServer(standbyClusterCorfuPort, true);

        CorfuRuntime.CorfuRuntimeParameters params = CorfuRuntime.CorfuRuntimeParameters
                .builder()
                .build();

        activeRuntime = CorfuRuntime.fromParameters(params);
        activeRuntime.parseConfigurationString(activeCorfuEndpoint).connect();

        standbyRuntime = CorfuRuntime.fromParameters(params);
        standbyRuntime.parseConfigurationString(standbyCorfuEndpoint).connect();

        activeCorfuStore = new CorfuStore(activeRuntime);
        standbyCorfuStore = new CorfuStore(standbyRuntime);

        mapActive = activeCorfuStore.openTable(
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

        mapStandby = standbyCorfuStore.openTable(
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

        assertThat(mapActive.count()).isZero();
        assertThat(mapStandby.count()).isZero();

        configTable = activeCorfuStore.openTable(
                DefaultClusterManager.CONFIG_NAMESPACE, DefaultClusterManager.CONFIG_TABLE_NAME,
                ClusterUuidMsg.class, ClusterUuidMsg.class, ClusterUuidMsg.class,
                TableOptions.fromProtoSchema(ClusterUuidMsg.class)
        );

        activeLockTable = activeCorfuStore.openTable(
                CORFU_SYSTEM_NAMESPACE,
                LOCK_TABLE_NAME,
                LockDataTypes.LockId.class,
                LockDataTypes.LockData.class,
                null,
                TableOptions.fromProtoSchema(LockDataTypes.LockData.class));

        activeCorfuStore.openTable(LogReplicationMetadataManager.NAMESPACE,
                REPLICATION_STATUS_TABLE,
                LogReplicationMetadata.ReplicationStatusKey.class,
                LogReplicationMetadata.ReplicationStatusVal.class,
                null,
                TableOptions.fromProtoSchema(LogReplicationMetadata.ReplicationStatusVal.class));

        standbyCorfuStore.openTable(LogReplicationMetadataManager.NAMESPACE,
                REPLICATION_STATUS_TABLE,
                LogReplicationMetadata.ReplicationStatusKey.class,
                LogReplicationMetadata.ReplicationStatusVal.class,
                null,
                TableOptions.fromProtoSchema(LogReplicationMetadata.ReplicationStatusVal.class));
    }

    @After
    public void tearDown() throws Exception {
        if (activeRuntime != null) {
            activeRuntime.shutdown();
        }

        if (standbyRuntime != null) {
            standbyRuntime.shutdown();
        }

        shutdownCorfuServer(activeCorfuServer);
        shutdownCorfuServer(standbyCorfuServer);
        shutdownCorfuServer(activeReplicationServer);
        shutdownCorfuServer(standbyReplicationServer);
    }

    /**
     * This test verifies config change with a role switch.
     * <p>
     * 1. Init with corfu 9000 active and 9001 standby
     * 2. Write 10 entries to active map
     * 3. Start log replication: Node 9010 - active, Node 9020 - standby
     * 4. Wait for Snapshot Sync, both maps have size 10
     * 5. Write 5 more entries to active map, to verify Log Entry Sync
     * 6. Perform a role switch with corfu store
     * 7. Write 5 more entries to standby map, which becomes source right now.
     * 8. Verify data will be replicated in reverse direction.
     */
    @Test
    public void testNewConfigWithSwitchRole() throws Exception {
        // Write 10 entries to active map
        for (int i = 0; i < firstBatch; i++) {
            // Change to default active standby config
            try (TxnContext txn = activeCorfuStore.txn(NAMESPACE)) {
                txn.putRecord(mapActive, StringKey.newBuilder().setKey(String.valueOf(i)).build(),
                        IntValue.newBuilder().setValue(i).build(), null);
                txn.commit();
            }
        }
        assertThat(mapActive.count()).isEqualTo(firstBatch);
        assertThat(mapStandby.count()).isZero();

        log.info("Before log replication, append {} entries to active map. Current active corfu" +
                        "[{}] log tail is {}, standby corfu[{}] log tail is {}", firstBatch, activeClusterCorfuPort,
                activeRuntime.getAddressSpaceView().getLogTail(), standbyClusterCorfuPort,
                standbyRuntime.getAddressSpaceView().getLogTail());

        activeReplicationServer = runReplicationServer(activeReplicationServerPort, nettyPluginPath);
        standbyReplicationServer = runReplicationServer(standbyReplicationServerPort, nettyPluginPath);
        log.info("Replication servers started, and replication is in progress...");

        // Wait until data is fully replicated
        waitForReplication(size -> size == firstBatch, mapStandby, firstBatch);
        log.info("After full sync, both maps have size {}. Current active corfu[{}] log tail " +
                        "is {}, standby corfu[{}] log tail is {}", firstBatch, activeClusterCorfuPort,
                activeRuntime.getAddressSpaceView().getLogTail(), standbyClusterCorfuPort,
                standbyRuntime.getAddressSpaceView().getLogTail());

        // Write 5 entries to active map
        for (int i = firstBatch; i < secondBatch; i++) {
            try (TxnContext txn = activeCorfuStore.txn(NAMESPACE)) {
                txn.putRecord(mapActive, StringKey.newBuilder().setKey(String.valueOf(i)).build(),
                        IntValue.newBuilder().setValue(i).build(), null);
                txn.commit();
            }
        }
        assertThat(mapActive.count()).isEqualTo(secondBatch);

        // Wait until data is fully replicated again
        waitForReplication(size -> size == secondBatch, mapStandby, secondBatch);
        log.info("After delta sync, both maps have size {}. Current active corfu[{}] log tail " +
                        "is {}, standby corfu[{}] log tail is {}", secondBatch, activeClusterCorfuPort,
                activeRuntime.getAddressSpaceView().getLogTail(), standbyClusterCorfuPort,
                standbyRuntime.getAddressSpaceView().getLogTail());

        // Verify data
        for (int i = 0; i < secondBatch; i++) {
            try (TxnContext tx = standbyCorfuStore.txn(NAMESPACE)) {
                assertThat(tx.getRecord(mapStandby, StringKey.newBuilder().setKey(String.valueOf(i)).build())
                        .getPayload().getValue()).isEqualTo(i);
                tx.commit();
            }
        }
        log.info("Log replication succeeds without config change!");

        // Verify Sync Status before switchover
        LogReplicationMetadata.ReplicationStatusKey key =
                LogReplicationMetadata.ReplicationStatusKey
                        .newBuilder()
                        .setClusterId(DefaultClusterConfig.getStandbyClusterId())
                        .build();

        ReplicationStatusVal replicationStatusVal;
        try (TxnContext txn = activeCorfuStore.txn(LogReplicationMetadataManager.NAMESPACE)) {
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
        try (TxnContext txn = activeCorfuStore.txn(DefaultClusterManager.CONFIG_NAMESPACE)) {
            txn.putRecord(configTable, DefaultClusterManager.OP_SWITCH, DefaultClusterManager.OP_SWITCH, DefaultClusterManager.OP_SWITCH);
            txn.commit();
        }

        assertThat(configTable.count()).isOne();

        // Write 5 more entries to mapStandby
        for (int i = secondBatch; i < thirdBatch; i++) {
            try (TxnContext txn = standbyCorfuStore.txn(NAMESPACE)) {
                txn.putRecord(mapStandby, StringKey.newBuilder().setKey(String.valueOf(i)).build(),
                        IntValue.newBuilder().setValue(i).build(), null);
                txn.commit();
            }
        }
        assertThat(mapStandby.count()).isEqualTo(thirdBatch);

        sleepUninterruptibly(5);

        // Verify Sync Status during the first switchover
        LogReplicationMetadata.ReplicationStatusKey StandbyKey =
                LogReplicationMetadata.ReplicationStatusKey
                        .newBuilder()
                        .setClusterId(DefaultClusterConfig.getActiveClusterId())
                        .build();

        ReplicationStatusVal standbyStatusVal;
        try (TxnContext txn = standbyCorfuStore.txn(LogReplicationMetadataManager.NAMESPACE)) {
            standbyStatusVal = (ReplicationStatusVal)txn.getRecord(REPLICATION_STATUS_TABLE, StandbyKey).getPayload();
            assertThat(txn.getRecord(REPLICATION_STATUS_TABLE, key).getPayload()).isNull();
            txn.commit();
        }

        log.info("ReplicationStatusVal: RemainingEntriesToSend: {}, SyncType: {}, Status: {}",
                standbyStatusVal.getRemainingEntriesToSend(), standbyStatusVal.getSyncType(),
                standbyStatusVal.getStatus());

        log.info("SnapshotSyncInfo: Base: {}, Type: {}, Status: {}, CompletedTime: {}",
                standbyStatusVal.getSnapshotSyncInfo().getBaseSnapshot(), standbyStatusVal.getSnapshotSyncInfo().getType(),
                standbyStatusVal.getSnapshotSyncInfo().getStatus(), standbyStatusVal.getSnapshotSyncInfo().getCompletedTime());

        assertThat(standbyStatusVal.getSyncType())
                .isEqualTo(LogReplicationMetadata.ReplicationStatusVal.SyncType.LOG_ENTRY);
        assertThat(standbyStatusVal.getStatus())
                .isEqualTo(LogReplicationMetadata.SyncStatus.ONGOING);

        assertThat(standbyStatusVal.getSnapshotSyncInfo().getType())
                .isEqualTo(LogReplicationMetadata.SnapshotSyncInfo.SnapshotSyncType.DEFAULT);
        assertThat(standbyStatusVal.getSnapshotSyncInfo().getStatus())
                .isEqualTo(LogReplicationMetadata.SyncStatus.COMPLETED);

        // Wait until data is fully replicated again
        waitForReplication(size -> size == thirdBatch, mapActive, thirdBatch);
        log.info("Data is fully replicated again after role switch, both maps have size {}. " +
                        "Current active corfu[{}] log tail is {}, standby corfu[{}] log tail is {}",
                thirdBatch, activeClusterCorfuPort, activeRuntime.getAddressSpaceView().getLogTail(),
                standbyClusterCorfuPort, standbyRuntime.getAddressSpaceView().getLogTail());

        // Double check after 10 seconds
        TimeUnit.SECONDS.sleep(mediumInterval);
        assertThat(mapActive.count()).isEqualTo(thirdBatch);
        assertThat(mapStandby.count()).isEqualTo(thirdBatch);

        // Second Role Switch
        try (TxnContext txn = activeCorfuStore.txn(DefaultClusterManager.CONFIG_NAMESPACE)) {
            txn.putRecord(configTable, DefaultClusterManager.OP_SWITCH, DefaultClusterManager.OP_SWITCH, DefaultClusterManager.OP_SWITCH);
            txn.commit();
        }
        assertThat(configTable.count()).isOne();

        // Write 5 more entries to mapStandby
        for (int i = thirdBatch; i < fourthBatch; i++) {
            try (TxnContext txn = activeCorfuStore.txn(NAMESPACE)) {
                txn.putRecord(mapActive, StringKey.newBuilder().setKey(String.valueOf(i)).build(),
                        IntValue.newBuilder().setValue(i).build(), null);
                txn.commit();
            }
        }
        assertThat(mapActive.count()).isEqualTo(fourthBatch);

        // Wait until data is fully replicated again
        waitForReplication(size -> size == fourthBatch, mapStandby, fourthBatch);
        log.info("Data is fully replicated again after role switch, both maps have size {}. " +
                        "Current active corfu[{}] log tail is {}, standby corfu[{}] log tail is {}",
                fourthBatch, activeClusterCorfuPort, activeRuntime.getAddressSpaceView().getLogTail(),
                standbyClusterCorfuPort, standbyRuntime.getAddressSpaceView().getLogTail());

        // Double check after 10 seconds
        TimeUnit.SECONDS.sleep(mediumInterval);
        assertThat(mapActive.count()).isEqualTo(fourthBatch);
        assertThat(mapStandby.count()).isEqualTo(fourthBatch);

        // Verify Sync Status
        try (TxnContext txn = activeCorfuStore.txn(LogReplicationMetadataManager.NAMESPACE)) {
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
     * cluster which becomes the Standby, do not contain any data written
     * prior to the snapshot sync from the Active.
     * 1. Open a subset of replicated streams on the Active and write data to
     * them
     * 2. Open a different set of replicated stream/s on the Standby and
     * write data to them
     * 3. Start LR on both clusters and assign roles
     * 4. Verify that after successful snapshot sync, no data from 2. is
     * present on the Standby
     * @throws Exception
     */
    @Test
    public void testDataConsistentForEmptyStreams() throws Exception {
        // Open 2/10 tables to be replicated on the active and write data
        // to it
        openMapsOnCluster(true, 2, 1);
        writeToMaps(true, 0, firstBatch);

        // Open another(different) table on standby.  This is also one of
        // the tables to replicate.  Write data to it.
        openMapsOnCluster(false, 1, 5);
        writeToMaps(false, 0, firstBatch);

        // Start LR on both active and standby clusters
        activeReplicationServer = runReplicationServer(activeReplicationServerPort, nettyPluginPath);
        standbyReplicationServer = runReplicationServer(standbyReplicationServerPort, nettyPluginPath);

        log.info("Replication servers started, and replication is in progress...");
        sleepUninterruptibly(20);

        // Verify that the replicated table opened on standby has no
        // data after snapshot sync
        verifyNoDataOnStandbyOpenedTables();

        LogReplicationMetadata.ReplicationStatusKey key =
            LogReplicationMetadata.ReplicationStatusKey
                .newBuilder()
                .setClusterId(DefaultClusterConfig.getStandbyClusterId())
                .build();
        ReplicationStatusVal replicationStatusVal;
        try (TxnContext txn = activeCorfuStore.txn(LogReplicationMetadataManager.NAMESPACE)) {
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

    private void verifyNoDataOnStandbyOpenedTables() {
        for(Map.Entry<String, Table<StringKey, IntValueTag, Metadata>> entry : mapNameToMapStandby.entrySet()) {
            Table<StringKey, IntValueTag, Metadata> map = entry.getValue();
            assertThat(map.count()).isEqualTo(0);
        }
    }

    /**
     * This test verifies that unopened(no data), replicated, trimmed streams
     * do not cause TrimmedException on Active during snapshot sync.
     * 1. Open a subset of replicated streams on the Active and write data to
     * them.
     * 2. Start LR on both clusters
     * 3. Verify snapshot sync completes successfully (Snapshot sync will cause
     * the Standby to write a 'clear' for every replicated stream which has
     * data)
     * 4. Checkpoint and trim all streams on Active and Standby.
     * 5. Switchover
     * 6. In step 3., the expected behavior is for standby to 'not' add a
     * 'clear' for the unopened, empty replicated streams.  Hence,
     * verify that snapshot sync completes successfully without
     * TrimmedException for those streams.
     */
    @Test
    public void testSnapshotSyncOfUnopenedTrimmedStreams() throws Exception {
        // Open 2/10 tables to be replicated on the active
        openMapsOnCluster(true, 2, 1);

        // Write data to the 2 tables
        writeToMaps(true, 0, firstBatch);

        // Start LR on both active and standby clusters
        activeReplicationServer = runReplicationServer(activeReplicationServerPort, nettyPluginPath);
        standbyReplicationServer = runReplicationServer(standbyReplicationServerPort, nettyPluginPath);

        log.info("Replication servers started, and replication is in progress...");
        sleepUninterruptibly(20);

        // Verify snapshot sync completes as expected
        LogReplicationMetadata.ReplicationStatusKey key =
            LogReplicationMetadata.ReplicationStatusKey
                .newBuilder()
                .setClusterId(DefaultClusterConfig.getStandbyClusterId())
                .build();
        ReplicationStatusVal replicationStatusVal;
        try (TxnContext txn = activeCorfuStore.txn(LogReplicationMetadataManager.NAMESPACE)) {
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


        // Checkpoint and trim the maps on both active and standby
        checkpointAndTrim(true);
        checkpointAndTrim(false);

        // Perform Switchover and verify it succeeds
        try (TxnContext txn = activeCorfuStore.txn(DefaultClusterManager.CONFIG_NAMESPACE)) {
            txn.putRecord(configTable, DefaultClusterManager.OP_SWITCH, DefaultClusterManager.OP_SWITCH, DefaultClusterManager.OP_SWITCH);
            txn.commit();
        }
        assertThat(configTable.count()).isOne();
        sleepUninterruptibly(10);

        // Verify snapshot sync completes as expected
        key = LogReplicationMetadata.ReplicationStatusKey
            .newBuilder()
            .setClusterId(DefaultClusterConfig.getActiveClusterId())
            .build();
        try (TxnContext txn =
                 standbyCorfuStore.txn(LogReplicationMetadataManager.NAMESPACE)) {
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

    private void openMapsOnCluster(boolean isActive, int mapCount,
        int startIndex) throws Exception {
        mapNameToMapActive = new HashMap<>();
        mapNameToMapStandby = new HashMap<>();

        for(int i=startIndex; i <= mapCount; i++) {
            String mapName = TABLE_PREFIX + i;

            if (isActive) {
                Table<StringKey, IntValueTag, Metadata> mapActive = activeCorfuStore.openTable(
                    NAMESPACE, mapName, StringKey.class, IntValueTag.class, Metadata.class,
                    TableOptions.fromProtoSchema(IntValueTag.class));
                mapNameToMapActive.put(mapName, mapActive);
                assertThat(mapActive.count()).isEqualTo(0);
            } else {
                Table<StringKey, IntValueTag, Metadata> mapStandby = standbyCorfuStore.openTable(
                    NAMESPACE, mapName, StringKey.class, IntValueTag.class, Metadata.class,
                    TableOptions.fromProtoSchema(IntValueTag.class));
                mapNameToMapStandby.put(mapName, mapStandby);
                assertThat(mapStandby.count()).isEqualTo(0);
            }
        }
    }

    private void writeToMaps(boolean active, int startIndex, int totalEntries) {
        int maxIndex = totalEntries + startIndex;

        Map<String, Table<StringKey, IntValueTag, Metadata>> map;

        if (active) {
            map = mapNameToMapActive;
        } else {
            map = mapNameToMapStandby;
        }
        for(Map.Entry<String, Table<StringKey, IntValueTag, Metadata>> entry : map.entrySet()) {

            log.debug(">>> Write to active cluster, map={}", entry.getKey());

            Table<StringKey, IntValueTag, Metadata> table = entry.getValue();
            for (int i = startIndex; i < maxIndex; i++) {
                StringKey stringKey = StringKey.newBuilder().setKey(String.valueOf(i)).build();
                IntValueTag intValueTag = IntValueTag.newBuilder().setValue(i).build();
                Metadata metadata = Metadata.newBuilder().setMetadata("Metadata_" + i).build();
                try (TxnContext txn = activeCorfuStore.txn(NAMESPACE)) {
                    txn.putRecord(table, stringKey, intValueTag, metadata);
                    txn.commit();
                }
            }
            assertThat(table.count()).isEqualTo(totalEntries);
        }
    }

    private void checkpointAndTrim(boolean active) {
        CorfuRuntime cpRuntime;

        if (active) {
            cpRuntime = createRuntimeWithCache(activeCorfuEndpoint);
        } else {
            cpRuntime = createRuntimeWithCache(standbyCorfuEndpoint);
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
     * Test all combinations of active/standby LR start/stopped and the output of sync status
     */
    @Test
    public void testClusterSyncStatus() throws Exception {

        final int waitInMillis = 500;
        final int deltaSeconds = 5;

        // (1) Start with: active LR stopped & standby LR started
        // No status should be reported, as status is queried on active LR and it is stopped.
        standbyReplicationServer = runReplicationServer(standbyReplicationServerPort, nettyPluginPath);

        // Write 'N' entries to active map (to ensure nothing happens wrt. the status, as LR is not started on active)
        for (int i = 0; i < firstBatch; i++) {
            try (TxnContext txn = activeCorfuStore.txn(NAMESPACE)) {
                txn.putRecord(mapActive, StringKey.newBuilder().setKey(String.valueOf(i)).build(),
                        IntValue.newBuilder().setValue(i).build(), null);
                txn.commit();
            }
        }
        assertThat(mapActive.count()).isEqualTo(firstBatch);

        // Verify Sync Status
        ReplicationStatusKey standbyClusterId = ReplicationStatusKey.newBuilder()
                        .setClusterId(DefaultClusterConfig.getStandbyClusterId())
                        .build();
        ReplicationStatusVal standbyStatus;

        try (TxnContext txn = activeCorfuStore.txn(LogReplicationMetadataManager.NAMESPACE)) {
            // Since LR has never been started, the table should not exist in the registry
            // Note that, in the case of a real client querying the status, this would simply time out
            // because LR is not available and status is only queried on the active site through LR. For the purpose of this
            // test, we query the database directly, so we should simply not find any record.
            standbyStatus = (ReplicationStatusVal)txn.getRecord(REPLICATION_STATUS_TABLE, standbyClusterId).getPayload();
            assertThat(standbyStatus).isNull();
        }

        // (2) Now stop standby LR and start active LR
        // The sync status should indicate replication has not started, as there is no way to stablish a connection
        // to the remote/standby site as it is stopped.
        shutdownCorfuServer(standbyReplicationServer);
        activeReplicationServer = runReplicationServer(activeReplicationServerPort, nettyPluginPath);

        // Verify Sync Status
        while (standbyStatus == null) {
            Sleep.sleepUninterruptibly(Duration.ofMillis(waitInMillis));

            try (TxnContext txn = activeCorfuStore.txn(LogReplicationMetadataManager.NAMESPACE)) {
                standbyStatus = (ReplicationStatusVal) txn.getRecord(REPLICATION_STATUS_TABLE, standbyClusterId).getPayload();
                if (standbyStatus != null) {
                    assertThat(standbyStatus.getStatus()).isEqualTo(LogReplicationMetadata.SyncStatus.NOT_STARTED);
                }
                txn.commit();
            }
        }

        // Wait the polling period time and verify sync status again (to make sure it was not erroneously updated)
        Sleep.sleepUninterruptibly(Duration.ofSeconds(LogReplicationAckReader.ACKED_TS_READ_INTERVAL_SECONDS + deltaSeconds));

        try (TxnContext txn = activeCorfuStore.txn(LogReplicationMetadataManager.NAMESPACE)) {
            standbyStatus = (ReplicationStatusVal)txn.getRecord(REPLICATION_STATUS_TABLE, standbyClusterId).getPayload();
            assertThat(standbyStatus.getStatus()).isEqualTo(LogReplicationMetadata.SyncStatus.NOT_STARTED);
            txn.commit();
        }

        // (3) Next, start standby LR, replication should start. wait until snapshot replication is completed and
        // confirm Log Entry is ONGOING.
        standbyReplicationServer = runReplicationServer(standbyReplicationServerPort, nettyPluginPath);
        waitForReplication(size -> size == firstBatch, mapStandby, firstBatch);

        // Verify data on Standby
        for (int i = 0; i < firstBatch; i++) {
            try (TxnContext tx = standbyCorfuStore.txn(NAMESPACE)) {
                assertThat(tx.getRecord(mapStandby, Sample.StringKey.newBuilder().setKey(String.valueOf(i)).build())
                        .getPayload().getValue()).isEqualTo(i);
                tx.commit();
            }
        }

        while (!standbyStatus.getSnapshotSyncInfo().getStatus().equals(LogReplicationMetadata.SyncStatus.COMPLETED)) {
            try (TxnContext txn = activeCorfuStore.txn(LogReplicationMetadataManager.NAMESPACE)) {
                standbyStatus = (ReplicationStatusVal)txn.getRecord(REPLICATION_STATUS_TABLE, standbyClusterId).getPayload();
                txn.commit();
            }
        }

        log.info("Snapshot replication status : COMPLETED");
        // Confirm Log entry Sync status is ONGOING
        assertThat(standbyStatus.getStatus()).isEqualTo(LogReplicationMetadata.SyncStatus.ONGOING);

        // (4) Write noisy streams and check remaining entries
        // Write 'N' entries to active noisy map
        long txTail = activeRuntime.getSequencerView().query(ObjectsView.getLogReplicatorStreamId());
        PersistentCorfuTable<String, Integer> noisyMap = activeRuntime.getObjectsView()
                .build()
                .setStreamName(streamName+"noisy")
                .setStreamTags(ObjectsView.getLogReplicatorStreamId())
                .setTypeToken(new TypeToken<PersistentCorfuTable<String, Integer>>() {
                })
                .open();
        for (int i = 0; i < firstBatch; i++) {
            activeRuntime.getObjectsView().TXBegin();
            noisyMap.insert(String.valueOf(i), i);
            activeRuntime.getObjectsView().TXEnd();
        }
        assertThat(noisyMap.size()).isEqualTo(firstBatch);
        long newTxTail = activeRuntime.getSequencerView().query(ObjectsView.getLogReplicatorStreamId());
        assertThat(newTxTail-txTail).isGreaterThanOrEqualTo(firstBatch);

        // Wait the polling period time and verify sync status again (to make sure it was not erroneously updated)
        Sleep.sleepUninterruptibly(Duration.ofSeconds(LogReplicationAckReader.ACKED_TS_READ_INTERVAL_SECONDS + deltaSeconds));

        try (TxnContext txn = activeCorfuStore.txn(LogReplicationMetadataManager.NAMESPACE)) {
            standbyStatus = (ReplicationStatusVal)txn.getRecord(REPLICATION_STATUS_TABLE, standbyClusterId).getPayload();
            txn.commit();
        }

        // Confirm remaining entries is equal to 0
        assertThat(standbyStatus.getRemainingEntriesToSend()).isEqualTo(0L);

        // (5) Confirm that if standby LR is stopped, in the middle of replication, the status changes to STOPPED
        shutdownCorfuServer(standbyReplicationServer);

        while (!standbyStatus.getStatus().equals(LogReplicationMetadata.SyncStatus.STOPPED)) {
            try (TxnContext txn = activeCorfuStore.txn(LogReplicationMetadataManager.NAMESPACE)) {
                standbyStatus = (ReplicationStatusVal) txn.getRecord(REPLICATION_STATUS_TABLE, standbyClusterId).getPayload();
                txn.commit();
            }
        }
        assertThat(standbyStatus.getStatus()).isEqualTo(LogReplicationMetadata.SyncStatus.STOPPED);
    }

    /**
     * This test verifies config change with a role switch during a snapshot sync transfer phase.
     * <p>
     * 1. Init with corfu 9000 active and 9001 standby
     * 2. Write 50 entries to active map
     * 3. Start log replication: Node 9010 - active, Node 9020 - standby
     * 4. Perform a role switch with corfu store
     * 5. Standby will drop messages and keep size 0
     * 6. Verify active map becomes size 0, since source size is 0
     */
    //@Test
    public void testNewConfigWithSwitchRoleDuringTransferPhase() throws Exception {
        // Write 50 entry to active map
        for (int i = 0; i < largeBatch; i++) {
            try (TxnContext txn = activeCorfuStore.txn(NAMESPACE)) {
                txn.putRecord(mapActive, StringKey.newBuilder().setKey(String.valueOf(i)).build(),
                        IntValue.newBuilder().setValue(i).build(), null);
                txn.commit();
            }
        }
        assertThat(mapActive.count()).isEqualTo(largeBatch);
        assertThat(mapStandby.count()).isZero();

        log.info("Before log replication, append {} entries to active map. Current active corfu" +
                        "[{}] log tail is {}, standby corfu[{}] log tail is {}", largeBatch, activeClusterCorfuPort,
                activeRuntime.getAddressSpaceView().getLogTail(), standbyClusterCorfuPort,
                standbyRuntime.getAddressSpaceView().getLogTail());

        activeReplicationServer = runReplicationServer(activeReplicationServerPort);
        standbyReplicationServer = runReplicationServer(standbyReplicationServerPort);
        log.info("Replication servers started, and replication is in progress...");
        TimeUnit.SECONDS.sleep(shortInterval);

        // Perform a role switch during transfer
        assertThat(mapStandby.count()).isEqualTo(0);
        try (TxnContext txn = activeCorfuStore.txn(DefaultClusterManager.CONFIG_NAMESPACE)) {
            txn.putRecord(configTable, DefaultClusterManager.OP_SWITCH, DefaultClusterManager.OP_SWITCH, DefaultClusterManager.OP_SWITCH);
            txn.commit();
        }
        assertThat(configTable.count()).isOne();
        assertThat(mapStandby.count()).isEqualTo(0);

        // Wait until active map size becomes 0
        waitForReplication(size -> size == 0, mapActive, 0);
        log.info("After role switch during transfer phase, both maps have size {}. Current " +
                        "active corfu[{}] log tail is {}, standby corfu[{}] log tail is {}",
                mapActive.count(), activeClusterCorfuPort, activeRuntime.getAddressSpaceView().getLogTail(),
                standbyClusterCorfuPort, standbyRuntime.getAddressSpaceView().getLogTail());

        // Double check after 10 seconds
        TimeUnit.SECONDS.sleep(mediumInterval);
        assertThat(mapActive.count()).isZero();
        assertThat(mapStandby.count()).isZero();
    }

    /**
     * This test verifies config change with a role switch during a snapshot sync apply phase.
     * <p>
     * 1. Init with corfu 9000 active and 9001 standby
     * 2. Write 50 entries to active map
     * 3. Start log replication: Node 9010 - active, Node 9020 - standby
     * 4. Wait for Snapshot Sync goes to apply phase
     * 5. Perform a role switch with corfu store
     * 6. Standby will continue apply and have size 50
     * 7. Verify both maps have size 50
     */
    //@Test
    public void testNewConfigWithSwitchRoleDuringApplyPhase() throws Exception {
        // Write 50 entry to active map
        for (int i = 0; i < largeBatch; i++) {
            try (TxnContext txn = activeCorfuStore.txn(NAMESPACE)) {
                txn.putRecord(mapActive, StringKey.newBuilder().setKey(String.valueOf(i)).build(),
                        IntValue.newBuilder().setValue(i).build(), null);
                txn.commit();
            }
        }
        assertThat(mapActive.count()).isEqualTo(largeBatch);
        assertThat(mapStandby.count()).isZero();

        log.info("Before log replication, append {} entries to active map. Current active corfu" +
                        "[{}] log tail is {}, standby corfu[{}] log tail is {}", largeBatch, activeClusterCorfuPort,
                activeRuntime.getAddressSpaceView().getLogTail(), standbyClusterCorfuPort,
                standbyRuntime.getAddressSpaceView().getLogTail());

        activeReplicationServer = runReplicationServer(activeReplicationServerPort);
        standbyReplicationServer = runReplicationServer(standbyReplicationServerPort);
        log.info("Replication servers started, and replication is in progress...");

        // Wait until apply phase
        UUID standbyStream = CorfuRuntime.getStreamID(streamName);
        while (!standbyRuntime.getAddressSpaceView().getAllTails().getStreamTails().containsKey(standbyStream)) {
            TimeUnit.MILLISECONDS.sleep(100L);
        }

        log.info("======standby tail is : " + standbyRuntime.getAddressSpaceView().getAllTails().getStreamTails().get(standbyStream));

        // Perform a role switch
        try (TxnContext txn = activeCorfuStore.txn(DefaultClusterManager.CONFIG_NAMESPACE)) {
            txn.putRecord(configTable, DefaultClusterManager.OP_SWITCH, DefaultClusterManager.OP_SWITCH, DefaultClusterManager.OP_SWITCH);
            txn.commit();
        }
        assertThat(configTable.count()).isOne();

        // Should finish apply
        waitForReplication(size -> size == largeBatch, mapStandby, largeBatch);
        assertThat(mapActive.count()).isEqualTo(largeBatch);
        log.info("After role switch during apply phase, both maps have size {}. Current " +
                        "active corfu[{}] log tail is {}, standby corfu[{}] log tail is {}",
                mapActive.count(), activeClusterCorfuPort, activeRuntime.getAddressSpaceView().getLogTail(),
                standbyClusterCorfuPort, standbyRuntime.getAddressSpaceView().getLogTail());

        // Double check after 10 seconds
        TimeUnit.SECONDS.sleep(mediumInterval);
        assertThat(mapActive.count()).isEqualTo(largeBatch);
        assertThat(mapStandby.count()).isEqualTo(largeBatch);
    }

    /**
     * This test verifies config change with two active clusters
     * <p>
     * 1. Init with corfu 9000 active and 9001 standby
     * 2. Write 10 entries to active map
     * 3. Start log replication: Node 9010 - active, Node 9020 - standby
     * 4. Wait for Snapshot Sync, both maps have size 10
     * 5. Write 5 more entries to active map, to verify Log Entry Sync
     * 6. Perform a two-active config update with corfu store
     * 7. Write 5 more entries to active map
     * 8. Verify data will not be replicated, since both are active
     */
    @Test
    public void testNewConfigWithTwoActive() throws Exception {
        // Write 10 entries to active map
        for (int i = 0; i < firstBatch; i++) {
            try (TxnContext txn = activeCorfuStore.txn(NAMESPACE)) {
                txn.putRecord(mapActive, StringKey.newBuilder().setKey(String.valueOf(i)).build(),
                        IntValue.newBuilder().setValue(i).build(), null);
                txn.commit();
            }
        }
        assertThat(mapActive.count()).isEqualTo(firstBatch);
        assertThat(mapStandby.count()).isZero();

        log.info("Before log replication, append {} entries to active map. Current active corfu" +
                        "[{}] log tail is {}, standby corfu[{}] log tail is {}", firstBatch, activeClusterCorfuPort,
                activeRuntime.getAddressSpaceView().getLogTail(), standbyClusterCorfuPort,
                standbyRuntime.getAddressSpaceView().getLogTail());

        activeReplicationServer = runReplicationServer(activeReplicationServerPort, nettyPluginPath);
        standbyReplicationServer = runReplicationServer(standbyReplicationServerPort, nettyPluginPath);
        log.info("Replication servers started, and replication is in progress...");

        // Wait until data is fully replicated
        waitForReplication(size -> size == firstBatch, mapStandby, firstBatch);
        log.info("After full sync, both maps have size {}. Current active corfu[{}] log tail " +
                        "is {}, standby corfu[{}] log tail is {}", firstBatch, activeClusterCorfuPort,
                activeRuntime.getAddressSpaceView().getLogTail(), standbyClusterCorfuPort,
                standbyRuntime.getAddressSpaceView().getLogTail());

        // Write 5 entries to active map
        for (int i = firstBatch; i < secondBatch; i++) {
            try (TxnContext txn = activeCorfuStore.txn(NAMESPACE)) {
                txn.putRecord(mapActive, StringKey.newBuilder().setKey(String.valueOf(i)).build(),
                        IntValue.newBuilder().setValue(i).build(), null);
                txn.commit();
            }
        }
        assertThat(mapActive.count()).isEqualTo(secondBatch);

        // Wait until data is fully replicated again
        waitForReplication(size -> size == secondBatch, mapStandby, secondBatch);
        log.info("After delta sync, both maps have size {}. Current active corfu[{}] log tail " +
                        "is {}, standby corfu[{}] log tail is {}", secondBatch, activeClusterCorfuPort,
                activeRuntime.getAddressSpaceView().getLogTail(), standbyClusterCorfuPort,
                standbyRuntime.getAddressSpaceView().getLogTail());

        // Verify data
        for (int i = 0; i < secondBatch; i++) {
            try (TxnContext tx = standbyCorfuStore.txn(NAMESPACE)) {
                assertThat(tx.getRecord(mapStandby, Sample.StringKey.newBuilder().setKey(String.valueOf(i)).build())
                        .getPayload().getValue()).isEqualTo(i);
                tx.commit();
            }
        }
        log.info("Log replication succeeds without config change!");

        // Perform a config update with two active
        try (TxnContext txn = activeCorfuStore.txn(DefaultClusterManager.CONFIG_NAMESPACE)) {
            txn.putRecord(configTable, DefaultClusterManager.OP_TWO_ACTIVE, DefaultClusterManager.OP_TWO_ACTIVE, DefaultClusterManager.OP_TWO_ACTIVE);
            txn.commit();
        }
        assertThat(configTable.count()).isOne();
        log.info("New topology config applied!");
        TimeUnit.SECONDS.sleep(mediumInterval);

        // Append to mapActive
        for (int i = secondBatch; i < thirdBatch; i++) {
            try (TxnContext txn = activeCorfuStore.txn(NAMESPACE)) {
                txn.putRecord(mapActive, StringKey.newBuilder().setKey(String.valueOf(i)).build(),
                        IntValue.newBuilder().setValue(i).build(), null);
                txn.commit();
            }
        }
        assertThat(mapActive.count()).isEqualTo(thirdBatch);
        log.info("Active map has {} entries now!", thirdBatch);

        // Standby map should still have secondBatch size
        log.info("Standby map should still have {} size", secondBatch);
        waitForReplication(size -> size == secondBatch, mapStandby, secondBatch);

        // Double check after 10 seconds
        TimeUnit.SECONDS.sleep(mediumInterval);
        assertThat(mapActive.count()).isEqualTo(thirdBatch);
        assertThat(mapStandby.count()).isEqualTo(secondBatch);
    }

    /**
     * This test verifies config change with two standby clusters
     * <p>
     * 1. Init with corfu 9000 active and 9001 standby
     * 2. Write 10 entries to active map
     * 3. Start log replication: Node 9010 - active, Node 9020 - standby
     * 4. Wait for Snapshot Sync, both maps have size 10
     * 5. Write 5 more entries to active map, to verify Log Entry Sync
     * 6. Perform a two-standby config update with corfu store
     * 7. Write 5 more entries to active map
     * 8. Verify data will not be replicated, since both are standby
     */
    @Test
    public void testNewConfigWithAllStandby() throws Exception {
        // Write 10 entries to active map
        for (int i = 0; i < firstBatch; i++) {
            try (TxnContext txn = activeCorfuStore.txn(NAMESPACE)) {
                txn.putRecord(mapActive, StringKey.newBuilder().setKey(String.valueOf(i)).build(),
                        IntValue.newBuilder().setValue(i).build(), null);
                txn.commit();
            }
        }
        assertThat(mapActive.count()).isEqualTo(firstBatch);
        assertThat(mapStandby.count()).isZero();

        log.info("Before log replication, append {} entries to active map. Current active corfu" +
                        "[{}] log tail is {}, standby corfu[{}] log tail is {}", firstBatch, activeClusterCorfuPort,
                activeRuntime.getAddressSpaceView().getLogTail(), standbyClusterCorfuPort,
                standbyRuntime.getAddressSpaceView().getLogTail());

        activeReplicationServer = runReplicationServer(activeReplicationServerPort, nettyPluginPath);
        standbyReplicationServer = runReplicationServer(standbyReplicationServerPort, nettyPluginPath);
        log.info("Replication servers started, and replication is in progress...");

        // Wait until data is fully replicated
        waitForReplication(size -> size == firstBatch, mapStandby, firstBatch);
        log.info("After full sync, both maps have size {}. Current active corfu[{}] log tail " +
                        "is {}, standby corfu[{}] log tail is {}", firstBatch, activeClusterCorfuPort,
                activeRuntime.getAddressSpaceView().getLogTail(), standbyClusterCorfuPort,
                standbyRuntime.getAddressSpaceView().getLogTail());

        // Write 5 entries to active map
        for (int i = firstBatch; i < secondBatch; i++) {
            try (TxnContext txn = activeCorfuStore.txn(NAMESPACE)) {
                txn.putRecord(mapActive, StringKey.newBuilder().setKey(String.valueOf(i)).build(),
                        IntValue.newBuilder().setValue(i).build(), null);
                txn.commit();
            }
        }
        assertThat(mapActive.count()).isEqualTo(secondBatch);

        // Wait until data is fully replicated again
        waitForReplication(size -> size == secondBatch, mapStandby, secondBatch);
        log.info("After delta sync, both maps have size {}. Current active corfu[{}] log tail " +
                        "is {}, standby corfu[{}] log tail is {}", secondBatch, activeClusterCorfuPort,
                activeRuntime.getAddressSpaceView().getLogTail(), standbyClusterCorfuPort,
                standbyRuntime.getAddressSpaceView().getLogTail());

        // Verify data
        for (int i = 0; i < secondBatch; i++) {
            try (TxnContext tx = standbyCorfuStore.txn(NAMESPACE)) {
                assertThat(tx.getRecord(mapStandby, Sample.StringKey.newBuilder().setKey(String.valueOf(i)).build())
                        .getPayload().getValue()).isEqualTo(i);
                tx.commit();
            }
        }
        log.info("Log replication succeeds without config change!");

        // Perform a config update with all standby
        try (TxnContext txn = activeCorfuStore.txn(DefaultClusterManager.CONFIG_NAMESPACE)) {
            txn.putRecord(configTable, DefaultClusterManager.OP_ALL_STANDBY, DefaultClusterManager.OP_ALL_STANDBY, DefaultClusterManager.OP_ALL_STANDBY);
            txn.commit();
        }
        assertThat(configTable.count()).isOne();
        log.info("New topology config applied!");
        TimeUnit.SECONDS.sleep(mediumInterval);

        for (int i = secondBatch; i < thirdBatch; i++) {
            try (TxnContext txn = activeCorfuStore.txn(NAMESPACE)) {
                txn.putRecord(mapActive, StringKey.newBuilder().setKey(String.valueOf(i)).build(),
                        IntValue.newBuilder().setValue(i).build(), null);
                txn.commit();
            }
        }
        assertThat(mapActive.count()).isEqualTo(thirdBatch);
        log.info("Active map has {} entries now!", thirdBatch);

        // Standby map should still have secondBatch size
        log.info("Standby map should still have {} size", secondBatch);
        waitForReplication(size -> size == secondBatch, mapStandby, secondBatch);

        // Double check after 10 seconds
        TimeUnit.SECONDS.sleep(mediumInterval);
        assertThat(mapActive.count()).isEqualTo(thirdBatch);
        assertThat(mapStandby.count()).isEqualTo(secondBatch);
    }

    /**
     * This test verifies config change with one active and one invalid
     * <p>
     * 1. Init with corfu 9000 active and 9001 standby
     * 2. Write 10 entries to active map
     * 3. Start log replication: Node 9010 - active, Node 9020 - standby
     * 4. Wait for Snapshot Sync, both maps have size 10
     * 5. Write 5 more entries to active map, to verify Log Entry Sync
     * 6. Perform a active-invalid config update with corfu store
     * 7. Write 5 more entries to active map
     * 8. Verify data will not be replicated, since standby is invalid
     * 9. Resume to standby and verify data is fully replicated again.
     */
    @Test
    public void testNewConfigWithInvalidClusters() throws Exception {
        // Write 10 entries to active map
        for (int i = 0; i < firstBatch; i++) {
            try (TxnContext txn = activeCorfuStore.txn(NAMESPACE)) {
                txn.putRecord(mapActive, StringKey.newBuilder().setKey(String.valueOf(i)).build(),
                        IntValue.newBuilder().setValue(i).build(), null);
                txn.commit();
            }
        }
        assertThat(mapActive.count()).isEqualTo(firstBatch);
        assertThat(mapStandby.count()).isZero();

        log.info("Before log replication, append {} entries to active map. Current active corfu" +
                        "[{}] log tail is {}, standby corfu[{}] log tail is {}", firstBatch, activeClusterCorfuPort,
                activeRuntime.getAddressSpaceView().getLogTail(), standbyClusterCorfuPort,
                standbyRuntime.getAddressSpaceView().getLogTail());

        activeReplicationServer = runReplicationServer(activeReplicationServerPort, nettyPluginPath);
        standbyReplicationServer = runReplicationServer(standbyReplicationServerPort, nettyPluginPath);
        log.info("Replication servers started, and replication is in progress...");

        // Wait until data is fully replicated
        waitForReplication(size -> size == firstBatch, mapStandby, firstBatch);
        log.info("After full sync, both maps have size {}. Current active corfu[{}] log tail " +
                        "is {}, standby corfu[{}] log tail is {}", firstBatch, activeClusterCorfuPort,
                activeRuntime.getAddressSpaceView().getLogTail(), standbyClusterCorfuPort,
                standbyRuntime.getAddressSpaceView().getLogTail());

        // Write 5 entries to active map
        for (int i = firstBatch; i < secondBatch; i++) {
            try (TxnContext txn = activeCorfuStore.txn(NAMESPACE)) {
                txn.putRecord(mapActive, StringKey.newBuilder().setKey(String.valueOf(i)).build(),
                        IntValue.newBuilder().setValue(i).build(), null);
                txn.commit();
            }
        }
        assertThat(mapActive.count()).isEqualTo(secondBatch);

        // Wait until data is fully replicated again
        waitForReplication(size -> size == secondBatch, mapStandby, secondBatch);
        log.info("After delta sync, both maps have size {}. Current active corfu[{}] log tail " +
                        "is {}, standby corfu[{}] log tail is {}", secondBatch, activeClusterCorfuPort,
                activeRuntime.getAddressSpaceView().getLogTail(), standbyClusterCorfuPort,
                standbyRuntime.getAddressSpaceView().getLogTail());

        // Verify data
        for (int i = 0; i < secondBatch; i++) {
            try (TxnContext tx = standbyCorfuStore.txn(NAMESPACE)) {
                assertThat(tx.getRecord(mapStandby, Sample.StringKey.newBuilder().setKey(String.valueOf(i)).build())
                        .getPayload().getValue()).isEqualTo(i);
                tx.commit();
            }
        }
        //verify the status table before topology change
        LogReplicationMetadata.ReplicationStatusKey key =
                LogReplicationMetadata.ReplicationStatusKey
                        .newBuilder()
                        .setClusterId(DefaultClusterConfig.getStandbyClusterId())
                        .build();
        ReplicationStatusVal replicationStatus;
        try (TxnContext txn = activeCorfuStore.txn(LogReplicationMetadataManager.NAMESPACE)) {
            replicationStatus = (ReplicationStatusVal)txn.getRecord(REPLICATION_STATUS_TABLE, key).getPayload();
            txn.commit();
        }
        assertThat(replicationStatus).isNotNull();
        log.info("Log replication succeeds without config change!");

        // Perform a config update with invalid state
        try (TxnContext txn = activeCorfuStore.txn(DefaultClusterManager.CONFIG_NAMESPACE)) {
            txn.putRecord(configTable, DefaultClusterManager.OP_INVALID, DefaultClusterManager.OP_INVALID, DefaultClusterManager.OP_INVALID);
            txn.commit();
        }
        assertThat(configTable.count()).isOne();
        log.info("New topology config applied!");
        TimeUnit.SECONDS.sleep(mediumInterval);

        // Append to mapActive
        for (int i = secondBatch; i < thirdBatch; i++) {
            try (TxnContext txn = activeCorfuStore.txn(NAMESPACE)) {
                txn.putRecord(mapActive, StringKey.newBuilder().setKey(String.valueOf(i)).build(),
                        IntValue.newBuilder().setValue(i).build(), null);
                txn.commit();
            }
        }
        assertThat(mapActive.count()).isEqualTo(thirdBatch);

        // Standby map should still have secondBatch size
        waitForReplication(size -> size == secondBatch, mapStandby, secondBatch);

        // Double check after 10 seconds
        TimeUnit.SECONDS.sleep(mediumInterval);
        assertThat(mapActive.count()).isEqualTo(thirdBatch);
        assertThat(mapStandby.count()).isEqualTo(secondBatch);
        // verify that active doesn't have the invalid cluster info in the LR status table
        try (TxnContext txn = activeCorfuStore.txn(LogReplicationMetadataManager.NAMESPACE)) {
            replicationStatus = (ReplicationStatusVal)txn.getRecord(REPLICATION_STATUS_TABLE, key).getPayload();
            txn.commit();
        }
        assertThat(replicationStatus).isNull();
        log.info("After {} seconds sleep, double check passed", mediumInterval);

        // Change to default active standby config
        try (TxnContext txn = activeCorfuStore.txn(DefaultClusterManager.CONFIG_NAMESPACE)) {
            txn.putRecord(configTable, DefaultClusterManager.OP_RESUME, DefaultClusterManager.OP_RESUME, DefaultClusterManager.OP_RESUME);
            txn.commit();
        }
        assertThat(configTable.count()).isEqualTo(2);
        log.info("New topology config applied!");
        TimeUnit.SECONDS.sleep(mediumInterval);

        // Standby map should have thirdBatch size, since topology config is resumed.
        waitForReplication(size -> size == thirdBatch, mapStandby, thirdBatch);

        // Double check after 10 seconds
        TimeUnit.SECONDS.sleep(mediumInterval);
        assertThat(mapActive.count()).isEqualTo(thirdBatch);
        assertThat(mapStandby.count()).isEqualTo(thirdBatch);
    }

    /**
     * This test verifies enforceSnapshotSync API
     * <p>
     * 1. Init with corfu 9000 active and 9001 standby
     * 2. Write 10 entries to active map
     * 3. Start log replication: Node 9010 - active, Node 9020 - standby
     * 4. Wait for Snapshot Sync, both maps have size 10
     * 5. Write 5 more entries to active map, to verify Log Entry Sync
     * 6. Write 5 more entries to active map and perform an enforced full snapshot sync
     * 7. Verify a full snapshot sync is triggered
     * 8. Verify a full snapshot sync is completed and data is correctly replicated.
     */
    @Test
    public void testEnforceSnapshotSync() throws Exception {
        // Write 10 entries to active map
        for (int i = 0; i < firstBatch; i++) {
            try (TxnContext txn = activeCorfuStore.txn(NAMESPACE)) {
                txn.putRecord(mapActive, StringKey.newBuilder().setKey(String.valueOf(i)).build(),
                        IntValue.newBuilder().setValue(i).build(), null);
                txn.commit();
            }
        }
        assertThat(mapActive.count()).isEqualTo(firstBatch);
        assertThat(mapStandby.count()).isZero();

        log.info("Before log replication, append {} entries to active map. Current active corfu" +
                        "[{}] log tail is {}, standby corfu[{}] log tail is {}", firstBatch, activeClusterCorfuPort,
                activeRuntime.getAddressSpaceView().getLogTail(), standbyClusterCorfuPort,
                standbyRuntime.getAddressSpaceView().getLogTail());

        activeReplicationServer = runReplicationServer(activeReplicationServerPort, nettyPluginPath);
        standbyReplicationServer = runReplicationServer(standbyReplicationServerPort, nettyPluginPath);
        log.info("Replication servers started, and replication is in progress...");

        // Wait until data is fully replicated
        waitForReplication(size -> size == firstBatch, mapStandby, firstBatch);

        log.info("After full sync, both maps have size {}. Current active corfu[{}] log tail " +
                        "is {}, standby corfu[{}] log tail is {}",
                firstBatch, activeClusterCorfuPort, activeRuntime.getAddressSpaceView().getLogTail(),
                standbyClusterCorfuPort, standbyRuntime.getAddressSpaceView().getLogTail());

        // Verify Sync Status
        Sleep.sleepUninterruptibly(Duration.ofSeconds(3));
        LogReplicationMetadata.ReplicationStatusKey key =
                LogReplicationMetadata.ReplicationStatusKey
                        .newBuilder()
                        .setClusterId(DefaultClusterConfig.getStandbyClusterId())
                        .build();

        LogReplicationMetadata.ReplicationStatusVal replicationStatusVal;
        try (TxnContext txn = activeCorfuStore.txn(LogReplicationMetadataManager.NAMESPACE)) {
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


        // Write 5 entries to active map
        for (int i = firstBatch; i < secondBatch; i++) {
            try (TxnContext txn = activeCorfuStore.txn(NAMESPACE)) {
                txn.putRecord(mapActive, StringKey.newBuilder().setKey(String.valueOf(i)).build(),
                        IntValue.newBuilder().setValue(i).build(), null);
                txn.commit();
            }
        }
        assertThat(mapActive.count()).isEqualTo(secondBatch);

        // Wait until data is fully replicated again
        waitForReplication(size -> size == secondBatch, mapStandby, secondBatch);
        log.info("After delta sync, both maps have size {}. Current active corfu[{}] log tail " +
                        "is {}, standby corfu[{}] log tail is {}", secondBatch, activeClusterCorfuPort,
                activeRuntime.getAddressSpaceView().getLogTail(), standbyClusterCorfuPort,
                standbyRuntime.getAddressSpaceView().getLogTail());

        // Verify data
        for (int i = 0; i < secondBatch; i++) {
            try (TxnContext tx = standbyCorfuStore.txn(NAMESPACE)) {
                assertThat(tx.getRecord(mapStandby, Sample.StringKey.newBuilder().setKey(String.valueOf(i)).build())
                        .getPayload().getValue()).isEqualTo(i);
                tx.commit();
            }
        }

        // Append to mapActive
        for (int i = secondBatch; i < thirdBatch; i++) {
            try (TxnContext txn = activeCorfuStore.txn(NAMESPACE)) {
                txn.putRecord(mapActive, StringKey.newBuilder().setKey(String.valueOf(i)).build(),
                        IntValue.newBuilder().setValue(i).build(), null);
                txn.commit();
            }
        }
        assertThat(mapActive.count()).isEqualTo(thirdBatch);

        // Perform an enforce full snapshot sync
        try (TxnContext txn = activeCorfuStore.txn(DefaultClusterManager.CONFIG_NAMESPACE)) {
            txn.putRecord(configTable, DefaultClusterManager.OP_ENFORCE_SNAPSHOT_FULL_SYNC,
                    DefaultClusterManager.OP_ENFORCE_SNAPSHOT_FULL_SYNC, DefaultClusterManager.OP_ENFORCE_SNAPSHOT_FULL_SYNC);
            txn.commit();
        }
        TimeUnit.SECONDS.sleep(mediumInterval);

        // Standby map should have thirdBatch size, since topology config is resumed.
        waitForReplication(size -> size == thirdBatch, mapStandby, thirdBatch);
        assertThat(mapStandby.count()).isEqualTo(thirdBatch);

        // Verify that a forced snapshot sync is finished.
        try (TxnContext txn = activeCorfuStore.txn(LogReplicationMetadataManager.NAMESPACE)) {
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
     * This test verifies active's lock release
     * <p>
     * 1. Init with corfu 9000 active and 9001 standby
     * 2. Write 10 entries to active map
     * 3. Start log replication: Node 9010 - active, Node 9020 - standby
     * 4. Wait for Snapshot Sync, both maps have size 10
     * 5. Write 5 more entries to active map, to verify Log Entry Sync
     * 6. Revoke active's lock and wait for 10 sec
     * 7. Write 5 more entries to active map
     * 8. Verify data will not be replicated, since active's lock is released
     */
    @Test
    public void testActiveLockRelease() throws Exception {
        // Write 10 entries to active map
        for (int i = 0; i < firstBatch; i++) {
            try (TxnContext txn = activeCorfuStore.txn(NAMESPACE)) {
                txn.putRecord(mapActive, StringKey.newBuilder().setKey(String.valueOf(i)).build(),
                        IntValue.newBuilder().setValue(i).build(), null);
                txn.commit();
            }
        }
        assertThat(mapActive.count()).isEqualTo(firstBatch);
        assertThat(mapStandby.count()).isZero();

        log.info("Before log replication, append {} entries to active map. Current active corfu" +
                        "[{}] log tail is {}, standby corfu[{}] log tail is {}", firstBatch, activeClusterCorfuPort,
                activeRuntime.getAddressSpaceView().getLogTail(), standbyClusterCorfuPort,
                standbyRuntime.getAddressSpaceView().getLogTail());

        // Start the active and standby replication servers with a lockLeaseDuration = 10 seconds.
        // The default lease duration is 60 seconds.  The duration between lease checks is set by the Discovery
        // Service to leaseDuration/10.  So reducing the lease duration will cause the detection of lease
        // expiry faster, i.e., 1 second instead of 6
        int lockLeaseDuration = 10;
        activeReplicationServer = runReplicationServer(activeReplicationServerPort, nettyPluginPath, lockLeaseDuration);
        standbyReplicationServer = runReplicationServer(standbyReplicationServerPort, nettyPluginPath, lockLeaseDuration);
        log.info("Replication servers started, and replication is in progress...");

        // Wait until data is fully replicated
        waitForReplication(size -> size == firstBatch, mapStandby, firstBatch);
        log.info("After full sync, both maps have size {}. Current active corfu[{}] log tail " +
                        "is {}, standby corfu[{}] log tail is {}", firstBatch, activeClusterCorfuPort,
                activeRuntime.getAddressSpaceView().getLogTail(), standbyClusterCorfuPort,
                standbyRuntime.getAddressSpaceView().getLogTail());

        // Write 5 entries to active map
        for (int i = firstBatch; i < secondBatch; i++) {
            try (TxnContext txn = activeCorfuStore.txn(NAMESPACE)) {
                txn.putRecord(mapActive, StringKey.newBuilder().setKey(String.valueOf(i)).build(),
                        IntValue.newBuilder().setValue(i).build(), null);
                txn.commit();
            }
        }
        assertThat(mapActive.count()).isEqualTo(secondBatch);

        // Wait until data is fully replicated again
        waitForReplication(size -> size == secondBatch, mapStandby, secondBatch);
        log.info("After delta sync, both maps have size {}. Current active corfu[{}] log tail " +
                        "is {}, standby corfu[{}] log tail is {}", secondBatch, activeClusterCorfuPort,
                activeRuntime.getAddressSpaceView().getLogTail(), standbyClusterCorfuPort,
                standbyRuntime.getAddressSpaceView().getLogTail());

        // Verify data
        for (int i = 0; i < secondBatch; i++) {
            try (TxnContext tx = standbyCorfuStore.txn(NAMESPACE)) {
                assertThat(tx.getRecord(mapStandby, Sample.StringKey.newBuilder().setKey(String.valueOf(i)).build())
                        .getPayload().getValue()).isEqualTo(i);
                tx.commit();
            }
        }
        log.info("Log replication succeeds without config change!");

        // Create a listener on the ReplicationStatus table on the Active cluster, which waits for Replication status
        // to change to STOPPED
        CountDownLatch countDownLatch = new CountDownLatch(1);
        ReplicationStopListener listener = new ReplicationStopListener(countDownLatch);
        activeCorfuStore.subscribeListener(listener, LogReplicationMetadataManager.NAMESPACE,
            LogReplicationMetadataManager.LR_STATUS_STREAM_TAG);

        // Release Active's lock by deleting the lock table
        try (TxnContext txnContext = activeCorfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            txnContext.clear(activeLockTable);
            txnContext.commit();
        }

        Assert.assertEquals(0, activeLockTable.count());
        log.info("Active's lock table cleared!");

        // Wait till the lock release is asynchronously processed and the replication status on Active changes to
        // STOPPED
        countDownLatch.await();

        // Write more data on the Active
        for (int i = secondBatch; i < thirdBatch; i++) {
            try (TxnContext txn = activeCorfuStore.txn(NAMESPACE)) {
                txn.putRecord(mapActive, StringKey.newBuilder().setKey(String.valueOf(i)).build(),
                        IntValue.newBuilder().setValue(i).build(), null);
                txn.commit();
            }
        }
        assertThat(mapActive.count()).isEqualTo(thirdBatch);
        log.info("Active map has {} entries now!", thirdBatch);

        // Standby map should still have secondBatch size
        log.info("Standby map should still have {} size", secondBatch);
        assertThat(mapStandby.count()).isEqualTo(secondBatch);
    }

    /**
     * This test verifies log entry works after a force snapshot sync
     * in the backup/restore workflow.
     * <p>
     * 1. Init with corfu 9000 active, 9001 standby, 9002 backup
     * 2. Write 10 entries to active map
     * 3. Start log replication: Node 9010 - active, Node 9020 - standby, Node 9030 - backup
     * 4. Wait for Snapshot Sync, both maps have size 10
     * 5. Write 50 entries to active map, to verify Log Entry Sync
     * 6. Right now both cluster have 50 entries,
     *    and we have a cluster that is a backup of active when active has 10 entries.
     *    Change the topology, the backup cluster 9030 will be the new active cluster.
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

        // Write 10 entries to active map
        writeEntries(activeCorfuStore, 0, firstBatch, mapActive);

        // Write 50 entries of dummy data to standby map, so we make it have longer corfu log tail.
        // We can also use it to confirm data is wiped during the snapshot sync.
        writeEntries(standbyCorfuStore, 0, largeBatch, mapStandby);
        assertThat(mapActive.count()).isEqualTo(firstBatch);
        assertThat(mapStandby.count()).isEqualTo(largeBatch);

        log.info("Before log replication, append {} entries to active map. Current active corfu" +
                        "[{}] log tail is {}, standby corfu[{}] log tail is {}", firstBatch, activeClusterCorfuPort,
                activeRuntime.getAddressSpaceView().getLogTail(), standbyClusterCorfuPort,
                standbyRuntime.getAddressSpaceView().getLogTail());

        activeReplicationServer = runReplicationServer(activeReplicationServerPort, nettyPluginPath);
        standbyReplicationServer = runReplicationServer(standbyReplicationServerPort, nettyPluginPath);
        log.info("Replication servers started, and replication is in progress...");

        // Wait until data is fully replicated
        waitForReplication(size -> size == firstBatch, mapStandby, firstBatch);

        log.info("After full sync, both maps have size {}. Current active corfu[{}] log tail " +
                        "is {}, standby corfu[{}] log tail is {}",
                firstBatch, activeClusterCorfuPort, activeRuntime.getAddressSpaceView().getLogTail(),
                standbyClusterCorfuPort, standbyRuntime.getAddressSpaceView().getLogTail());

        // Write 50 entries to active map
        writeEntries(activeCorfuStore, 0, largeBatch, mapActive);
        assertThat(mapActive.count()).isEqualTo(largeBatch);

        // Write 10 entries to backup map
        // It is a backup of the active cluster when it has 10 entries
        writeEntries(backupCorfuStore, 0, firstBatch, mapBackup);

        // Wait until data is fully replicated again
        waitForReplication(size -> size == largeBatch, mapStandby, largeBatch);
        log.info("After delta sync, both maps have size {}. Current active corfu[{}] log tail " +
                        "is {}, standby corfu[{}] log tail is {}", secondBatch, activeClusterCorfuPort,
                activeRuntime.getAddressSpaceView().getLogTail(), standbyClusterCorfuPort,
                standbyRuntime.getAddressSpaceView().getLogTail());

        // Verify data
        for (int i = 0; i < largeBatch; i++) {
            try (TxnContext tx = standbyCorfuStore.txn(NAMESPACE)) {
                assertThat(tx.getRecord(mapStandby, Sample.StringKey.newBuilder().setKey(String.valueOf(i)).build())
                        .getPayload().getValue()).isEqualTo(i);
                tx.commit();
            }
        }

        // Change the topology - brings up the backup cluster
        updateTopology(activeCorfuStore, DefaultClusterManager.OP_BACKUP);
        log.info("Change the topology!!!");

        TimeUnit.SECONDS.sleep(shortInterval);


        // Perform an enforce full snapshot sync
        updateTopology(activeCorfuStore, DefaultClusterManager.OP_ENFORCE_SNAPSHOT_FULL_SYNC);
        TimeUnit.SECONDS.sleep(mediumInterval);

        // Standby map size should be 10
        waitForReplication(size -> size == firstBatch, mapStandby, firstBatch);

        // Write 5 entries to backup map
        writeEntries(backupCorfuStore, firstBatch, secondBatch, mapBackup);
        assertThat(mapBackup.count()).isEqualTo(secondBatch);

        // Wait until data is fully replicated again
        waitForReplication(size -> size == secondBatch, mapStandby, secondBatch);
        log.info("After delta sync, both maps have size {}. Current backup corfu[{}] log tail " +
                        "is {}, standby corfu[{}] log tail is {}", firstBatch, backupClusterCorfuPort,
                backupRuntime.getAddressSpaceView().getLogTail(), standbyClusterCorfuPort,
                standbyRuntime.getAddressSpaceView().getLogTail());

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
