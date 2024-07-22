package org.corfudb.integration;

import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterConfig;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterManager;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationEvent;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationEventKey;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationStatusVal;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationStatusKey;
import org.corfudb.infrastructure.logreplication.proto.Sample;
import org.corfudb.infrastructure.logreplication.proto.Sample.IntValue;
import org.corfudb.infrastructure.logreplication.proto.Sample.Metadata;
import org.corfudb.infrastructure.logreplication.proto.Sample.StringKey;
import org.corfudb.infrastructure.logreplication.replication.LogReplicationAckReader;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.CorfuOptions;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.ExampleSchemas.ClusterUuidMsg;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStreamEntries;
import org.corfudb.runtime.collections.StreamListener;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.ObjectsView;
import org.corfudb.runtime.view.stream.IStreamView;
import org.corfudb.test.SampleSchema.SampleTableAMsg;
import org.corfudb.test.SampleSchema.ValueFieldTagOne;
import org.corfudb.test.SampleSchema.ValueFieldTagOneAndTwo;
import org.corfudb.util.Sleep;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager.REPLICATION_EVENT_TABLE_NAME;
import static org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager.REPLICATION_STATUS_TABLE;

/**
 * This suite of tests validates the behavior of Log Replication
 * when nodes are shutdown and brought back up.
 *
 * @author amartinezman
 */
@Slf4j
public class CorfuReplicationReconfigurationIT extends LogReplicationAbstractIT {

    private static final int SLEEP_DURATION = 5;
    private static final int WAIT_DELTA = 50;

    private static final int MAP_COUNT = 10;

    private AtomicBoolean replicationEnded = new AtomicBoolean(false);

    private Map<String, Table<Sample.StringKey, ValueFieldTagOne, Sample.Metadata>> mapNameToMapActiveTypeA
            = new HashMap<>();
    private Map<String, Table<Sample.StringKey, ValueFieldTagOne, Sample.Metadata>> mapNameToMapStandbyTypeA =
            new HashMap<>();
    private Map<String, Table<Sample.StringKey, ValueFieldTagOneAndTwo, Sample.Metadata>> mapNameToMapActiveTypeB =
            new HashMap<>();
    private Map<String, Table<Sample.StringKey, ValueFieldTagOneAndTwo, Sample.Metadata>> mapNameToMapStandbyTypeB =
            new HashMap<>();

    private volatile AtomicBoolean stopWrites = new AtomicBoolean(false);

    private Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> noisyMap;

    /**
     * Sets the plugin path before starting any test
     *
     * @throws Exception
     */
    @Before
    public void setupPluginPath() {
        if(runProcess) {
            File f = new File(nettyConfig);
            this.pluginConfigFilePath = f.getAbsolutePath();
        } else {
            this.pluginConfigFilePath = nettyConfig;
        }
    }

    /**
     * Test the case where Standby leader node is restarted during log entry sync.
     *
     * The expectation is that replication should resume.
     */
    @Test
    public void testStandbyClusterReset() throws Exception {
        // (1) Snapshot and Log Entry Sync
        log.debug(">>> (1) Start Snapshot and Log Entry Sync");
        testEndToEndSnapshotAndLogEntrySyncUFO(false, false);

        ExecutorService writerService = Executors.newSingleThreadExecutor();

        // (2) Stop Standby Log Replicator Server
        log.debug(">>> (2) Stop Standby Node");
        stopStandbyLogReplicator();

        // (3) Start daemon thread writing data to active
        log.debug(">>> (3) Start daemon writer service");
        // Since step (1) wrote numWrites for snapshotSync and numWrites/2 in logEntrySync, continue from this starting point
        writerService.submit(() -> writeToActive((numWrites + numWrites/2), numWrites));

        // (4) Sleep Interval so writes keep going through, while standby is down
        log.debug(">>> (4) Wait for some time");
        Sleep.sleepUninterruptibly(Duration.ofSeconds(SLEEP_DURATION));

        // (5) Restart Standby Log Replicator
        log.debug(">>> (5) Restart Standby Node");
        startStandbyLogReplicator();

        // (6) Verify Data on Standby after Restart
        log.debug(">>> (6) Verify Data on Standby");
        verifyStandbyData((numWrites*2 + numWrites/2));
    }

    /**
     * Test the case where Active leader node is restarted during log entry sync.
     *
     * The expectation is that replication should resume.
     */
    @Test
    public void testActiveClusterReset() throws Exception {

        final int delta = 5;

        // (1) Snapshot and Log Entry Sync
        log.debug(">>> (1) Start Snapshot and Log Entry Sync");
        testEndToEndSnapshotAndLogEntrySyncUFO(false, false);

        ExecutorService writerService = Executors.newSingleThreadExecutor();

        // (2) Start daemon thread writing data to active
        log.debug(">>> (2) Start daemon writer service");
        // Since step (1) wrote numWrites for snapshotSync and numWrites/2 in logEntrySync, continue from this starting point
        writerService.submit(() -> writeToActive((numWrites + numWrites/2), numWrites));

        // (3) Stop Active Log Replicator Server
        log.debug(">>> (3) Stop Active Node");
        stopActiveLogReplicator();

        // (4) Sleep Interval so writes keep going through, while active is down
        log.debug(">>> (4) Wait for some time");
        Sleep.sleepUninterruptibly(Duration.ofSeconds(SLEEP_DURATION));

        // (5) Restart Active Log Replicator
        log.debug(">>> (5) Restart Active Node");
        startActiveLogReplicator();

        // (6) Verify Data on Standby after Restart
        log.debug(">>> (6) Verify Data on Standby");
        verifyStandbyData((numWrites*2 + numWrites/2));

        // (7) Verify replication status after all data has been replicated (no further data)
        corfuStoreActive.openTable(LogReplicationMetadataManager.NAMESPACE,
                REPLICATION_STATUS_TABLE,
                LogReplicationMetadata.ReplicationStatusKey.class,
                LogReplicationMetadata.ReplicationStatusVal.class,
                null,
                TableOptions.fromProtoSchema(LogReplicationMetadata.ReplicationStatusVal.class));

        // Wait the polling period time before verifying sync status (to make sure it was updated)
        Sleep.sleepUninterruptibly(Duration.ofSeconds(LogReplicationAckReader.ACKED_TS_READ_INTERVAL_SECONDS + 1));

        long remainingEntriesToSend = verifyReplicationStatus(ReplicationStatusVal.SyncType.LOG_ENTRY,
                LogReplicationMetadata.SyncStatus.ONGOING, LogReplicationMetadata.SnapshotSyncInfo.SnapshotSyncType.DEFAULT,
                LogReplicationMetadata.SyncStatus.COMPLETED);
       assertThat(remainingEntriesToSend).isEqualTo(0L);

        // (8) Keep writing data into the TX stream (but with data not intended for replication) while
        // checking the status, confirm, remainingEntriesToSend is '0'
        writerService.submit(() -> {
            try {
                openNonReplicatedTable();
                while (!stopWrites.get()) {
                    writeNonReplicatedTable(numWrites);
                }
            } catch (Exception e) {
                fail("Failed Test!");
            }
        });

        // Wait the polling period time and verify sync status again (to make sure it was not erroneously updated)
        Sleep.sleepUninterruptibly(Duration.ofSeconds(LogReplicationAckReader.ACKED_TS_READ_INTERVAL_SECONDS + 1));

        // While the TX log is growing, the remaining entries to send can be changing during this time, as it is computed
        // wrt. the tail of the log and this varies depending on how fast we are catching the tail of the log.
        verifyReplicationStatus(ReplicationStatusVal.SyncType.LOG_ENTRY,
                LogReplicationMetadata.SyncStatus.ONGOING, LogReplicationMetadata.SnapshotSyncInfo.SnapshotSyncType.DEFAULT,
                LogReplicationMetadata.SyncStatus.COMPLETED);

        stopWrites.set(true);

        // (9) Stop active LR again, so server restarts from Log Entry Sync, with no actual deltas (as there is no new data)
        // and verify remainingEntriesToSend is still '0'
        stopActiveLogReplicator();
        startActiveLogReplicator();

        // Wait the polling period time and verify sync status again (to make sure it was not erroneously updated)
        Sleep.sleepUninterruptibly(Duration.ofSeconds(LogReplicationAckReader.ACKED_TS_READ_INTERVAL_SECONDS + delta));

        remainingEntriesToSend = verifyReplicationStatus(ReplicationStatusVal.SyncType.LOG_ENTRY,
                LogReplicationMetadata.SyncStatus.ONGOING, LogReplicationMetadata.SnapshotSyncInfo.SnapshotSyncType.DEFAULT,
                LogReplicationMetadata.SyncStatus.COMPLETED);
        assertThat(remainingEntriesToSend).isEqualTo(0L);
    }

    /**
     * This test verifies that concurrent Snapshot Syncs are handled gracefully on the Standby.  If messages from
     * a new Snapshot Sync are received on the Standby when it is in 'apply' phase of a previous sync, these new
     * messages are rejected.  This avoids the race between transfer and apply phases on Standby.
     * The Active will continue to re-send messages from the new sync.  They will be accepted and ACK'd by Standby
     * after the ongoing apply finishes.
     *
     * Test workflow:
     * 1. Create a topology with Active and Standby.  On Standby, introduce a 20 sec latency during apply phase to
     *    simulate the above race.  Wait for initial snapshot sync to complete.
     * 2. Trigger 2 forced snapshot sync events with a delay of 3 seconds between requests.  This is to ensure that
     *    the 1st request completes transfer and Standby is in 'apply' phase of the 1st sync.
     * 3. When the 2nd forced sync is triggered, Active will be in WaitSnapshotApplyState due to the 'apply delay'
     *    on Standby.  When the 2nd forced sync starts, the 1st one gets cancelled on Active.  Standby continues
     *    to apply data from the 1st sync, rejecting incoming messages from the 2nd one.  Active continues to
     *    resend these messages from the 2nd sync request.
     *    Eventually, the apply phase of the 1st sync completes and Standby accepts messages from the 2nd one.
     4.   When this 2nd sync completes on the Standby, all existing forced sync events are deleted from the Event
     *    table.  Wait for the table to be empty, which indicates that this 2nd sync completed successfully.
     * @throws Exception
     */
    @Test
    public void testConcurrentSnapshotSyncOnStandby() throws Exception {
        String testStreamName = "Table001";
        int batchSize = 5;
        int numForcedSyncEvents = 2;

        // Time delay of 20 seconds introduced on the Standby when in Apply phase
        int waitSnapshotApplyMs = 20000;

        // Time delay of 3 seconds between requesting 2 consecutive forced syncs.  This is to ensure that the first
        // sync request reaches the WaitForSnapshotApplyState (implies that the Standby will be in the Apply phase)
        int waitTimeBetweenSyncRequestsMs = 3000;

        // Perform initial setup and wait for initial snapshot sync to complete
        try {
            setupActiveAndStandbyCorfu();

            Table<LogReplicationMetadata.ReplicationEventKey, LogReplicationMetadata.ReplicationEvent, Message> eventTable =
                corfuStoreActive.openTable(LogReplicationMetadataManager.NAMESPACE,
                    REPLICATION_EVENT_TABLE_NAME,
                    LogReplicationMetadata.ReplicationEventKey.class,
                    LogReplicationMetadata.ReplicationEvent.class,
                    null,
                    TableOptions.fromProtoSchema(LogReplicationMetadata.ReplicationEvent.class));

            Table<StringKey, IntValue, Metadata> mapActive = corfuStoreActive.openTable(
                NAMESPACE,
                testStreamName,
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

            Table<StringKey, IntValue, Metadata> mapStandby = corfuStoreStandby.openTable(
                NAMESPACE,
                testStreamName,
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

            // Write batchSize num of entries to active map
            for (int i = 0; i < batchSize; i++) {
                try (TxnContext txn = corfuStoreActive.txn(NAMESPACE)) {
                    txn.putRecord(mapActive, StringKey.newBuilder().setKey(String.valueOf(i)).build(),
                        IntValue.newBuilder().setValue(i).build(), null);
                    txn.commit();
                }
            }
            assertThat(mapActive.count()).isEqualTo(batchSize);
            assertThat(mapStandby.count()).isZero();

            // Start LR on both sides.  Introduce a latency of 20 seconds in the apply phase on Standby
            startActiveLogReplicator();
            startStandbyLogReplicator(waitSnapshotApplyMs);
            log.info("Replication servers started, and replication is in progress...");

            // Wait for replication to finish and check sink has all the writes
            while (mapStandby.count() != mapActive.count()) {
                log.debug("Waiting for entries to be replicated");
            }
            assertThat(mapStandby.count()).isEqualTo(mapActive.count());

            // Check that the event table is empty to begin with
            assertThat(eventTable.count()).isZero();

            // Add 2 full sync events with delay of 'waitTimeBetweenSyncRequestsMs'
            Table<ClusterUuidMsg, ClusterUuidMsg, ClusterUuidMsg> configTable = corfuStoreActive.openTable(
                DefaultClusterManager.CONFIG_NAMESPACE, DefaultClusterManager.CONFIG_TABLE_NAME,
                ClusterUuidMsg.class, ClusterUuidMsg.class, ClusterUuidMsg.class,
                TableOptions.fromProtoSchema(ClusterUuidMsg.class)
            );

            for (int i = 0; i < numForcedSyncEvents; i++) {
                try (TxnContext txn = corfuStoreActive.txn(DefaultClusterManager.CONFIG_NAMESPACE)) {
                    txn.putRecord(configTable, DefaultClusterManager.OP_ENFORCE_SNAPSHOT_FULL_SYNC,
                        DefaultClusterManager.OP_ENFORCE_SNAPSHOT_FULL_SYNC, DefaultClusterManager.OP_ENFORCE_SNAPSHOT_FULL_SYNC);
                    txn.commit();
                }
                TimeUnit.MILLISECONDS.sleep(waitTimeBetweenSyncRequestsMs);
            }

            // Wait for the forced sync events to be added to the event table
            while(eventTable.count() < numForcedSyncEvents) {
                log.info("Num Forced Sync Events Added: {}.  Expected {}", eventTable.count(), numForcedSyncEvents);
            }

            log.info("Forced Sync Events written to the table and will execute concurrently");

            // When the 2nd forced sync is triggered, Active will be in WaitSnapshotApplyState due to the 'apply delay'
            // on Standby.  When the 2nd forced sync starts, the 1st one gets cancelled on Active.  Standby continues
            // to apply data from the 1st sync, rejecting incoming messages from the 2nd one.  Active continues to
            // resend these messages.
            // Eventually, the apply phase of the 1st sync completes and Standby accepts messages from the 2nd one.
            // When this 2nd sync completes on the Standby, all existing forced sync events are deleted from the Event
            // table.
            // Wait for the table to be empty, which indicates that this 2nd sync completed successfully.
            while(eventTable.count() > 0) {
                log.debug("Waiting for the Event Table to be cleared.  Current Size = {}", eventTable.count());
            }

            // Verify that data is consistent on the Standby
            corfuStoreStandby.openTable(LogReplicationMetadataManager.NAMESPACE,
                REPLICATION_STATUS_TABLE,
                ReplicationStatusKey.class,
                ReplicationStatusVal.class,
                null,
                TableOptions.fromProtoSchema(LogReplicationMetadata.ReplicationStatusVal.class));
            ReplicationStatusKey key = ReplicationStatusKey.newBuilder().setClusterId(
                DefaultClusterConfig.getStandbyClusterId()).build();
            try (TxnContext txnContext = corfuStoreStandby.txn(LogReplicationMetadataManager.NAMESPACE)) {
                ReplicationStatusVal val =
                    (ReplicationStatusVal) txnContext.getRecord(REPLICATION_STATUS_TABLE, key).getPayload();
                Assert.assertTrue(val.getDataConsistent());
            }

        } finally {
            executorService.shutdownNow();

            if (activeCorfu != null) {
                activeCorfu.destroy();
            }

            if (standbyCorfu != null) {
                standbyCorfu.destroy();
            }

            if (activeReplicationServer != null) {
                activeReplicationServer.destroy();
            }

            if (standbyReplicationServer != null) {
                standbyReplicationServer.destroy();
            }
        }
    }

    /**
     * Verify event table is cleared after processing latest force sync event to avoid keeping older events.
     *
     */
    @Test
    public void testEventTableCleared() throws Exception {
        String testStreamName = "Table001";
        int batchSize = 5;
        int numStaleEvents = 3;

        try {
            setupActiveAndStandbyCorfu();

            Table<LogReplicationMetadata.ReplicationEventKey, LogReplicationMetadata.ReplicationEvent, Message> eventTable =
                    corfuStoreActive.openTable(LogReplicationMetadataManager.NAMESPACE,
                            REPLICATION_EVENT_TABLE_NAME,
                            LogReplicationMetadata.ReplicationEventKey.class,
                            LogReplicationMetadata.ReplicationEvent.class,
                            null,
                            TableOptions.fromProtoSchema(LogReplicationMetadata.ReplicationEvent.class));

            Table<StringKey, IntValue, Metadata>  mapActive = corfuStoreActive.openTable(
                    NAMESPACE,
                    testStreamName,
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

            Table<StringKey, IntValue, Metadata>  mapStandby = corfuStoreStandby.openTable(
                    NAMESPACE,
                    testStreamName,
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

            // Write batchSize num of entries to active map
            for (int i = 0; i < batchSize; i++) {
                try (TxnContext txn = corfuStoreActive.txn(NAMESPACE)) {
                    txn.putRecord(mapActive, StringKey.newBuilder().setKey(String.valueOf(i)).build(),
                            IntValue.newBuilder().setValue(i).build(), null);
                    txn.commit();
                }
            }
            assertThat(mapActive.count()).isEqualTo(batchSize);
            assertThat(mapStandby.count()).isZero();

            startActiveLogReplicator();
            startStandbyLogReplicator();
            log.info("Replication servers started, and replication is in progress...");

            // Wait for replication to finish and check sink has all the writes
            boolean entriesReplicated = false;
            for(int i = 0; i < PARAMETERS.NUM_ITERATIONS_MODERATE; i++) {
                if (mapStandby.count() == mapActive.count()) {
                    entriesReplicated = true;
                    break;
                }
                Thread.sleep(50);
            }
            assertThat(entriesReplicated).isTrue();

            // Check that the event table is empty to begin
            assertThat(eventTable.count()).isZero();

            corfuStoreStandby.openTable(LogReplicationMetadataManager.NAMESPACE,
                    REPLICATION_STATUS_TABLE,
                    LogReplicationMetadata.ReplicationStatusKey.class,
                    LogReplicationMetadata.ReplicationStatusVal.class,
                    null,
                    TableOptions.fromProtoSchema(LogReplicationMetadata.ReplicationStatusVal.class));

            // Subscribe listener to know when data is consistent on the sink
            CountDownLatch awaitSyncCompletion = new CountDownLatch(1);
            DataConsistentListener standbyListener = new DataConsistentListener(awaitSyncCompletion);
            corfuStoreStandby.subscribeListener(standbyListener, LogReplicationMetadataManager.NAMESPACE,
                    LogReplicationMetadataManager.LR_STATUS_STREAM_TAG);

            // Add some full sync events with an empty cluster ID, these will fail to run
            for (int i = 0; i < numStaleEvents; i++) {
                try (TxnContext txn = corfuStoreActive.txn(LogReplicationMetadataManager.NAMESPACE)) {
                    ReplicationEventKey key = ReplicationEventKey.newBuilder().setKey(
                            System.currentTimeMillis() + " " + DefaultClusterConfig.getStandbyClusterId()
                    ).build();
                    ReplicationEvent event = ReplicationEvent.newBuilder()
                            .setEventId(UUID.randomUUID().toString())
                            .build();

                    txn.putRecord(eventTable, key, event, null);
                    txn.commit();
                }
            }

            Table<ClusterUuidMsg, ClusterUuidMsg, ClusterUuidMsg> configTable = corfuStoreActive.openTable(
                    DefaultClusterManager.CONFIG_NAMESPACE, DefaultClusterManager.CONFIG_TABLE_NAME,
                    ClusterUuidMsg.class, ClusterUuidMsg.class, ClusterUuidMsg.class,
                    TableOptions.fromProtoSchema(ClusterUuidMsg.class)
            );

            // Enqueue one last force sync event through the config table, this will be the one to go through
            try (TxnContext txn = corfuStoreActive.txn(DefaultClusterManager.CONFIG_NAMESPACE)) {
                txn.putRecord(configTable, DefaultClusterManager.OP_ENFORCE_SNAPSHOT_FULL_SYNC,
                        DefaultClusterManager.OP_ENFORCE_SNAPSHOT_FULL_SYNC, DefaultClusterManager.OP_ENFORCE_SNAPSHOT_FULL_SYNC);
                txn.commit();
            }

            // Wait for last force sync event queued
            boolean forceSyncQueued = false;
            for(int i = 0; i < PARAMETERS.NUM_ITERATIONS_MODERATE; i++) {
                if (eventTable.count() == numStaleEvents + 1) {
                    forceSyncQueued = true;
                    break;
                }
                Thread.sleep(50);
            }
            assertThat(forceSyncQueued).isTrue();

            // Wait for the last force sync event to complete which should clear all events when done
            awaitSyncCompletion.await();

            // Check that the event table is cleared, could be small delay for clear after data is consistent
            boolean tableCleared = false;
            for(int i = 0; i < PARAMETERS.NUM_ITERATIONS_LOW; i++) {
                if (eventTable.count() == 0) {
                    tableCleared = true;
                    break;
                }
                Thread.sleep(50);
            }
            assertThat(tableCleared).isTrue();
        } finally {
            executorService.shutdownNow();

            if (activeCorfu != null) {
                activeCorfu.destroy();
            }

            if (standbyCorfu != null) {
                standbyCorfu.destroy();
            }

            if (activeReplicationServer != null) {
                activeReplicationServer.destroy();
            }

            if (standbyReplicationServer != null) {
                standbyReplicationServer.destroy();
            }
        }
    }

    private long verifyReplicationStatus(ReplicationStatusVal.SyncType targetSyncType,
                                         LogReplicationMetadata.SyncStatus targetSyncStatus,
                                         LogReplicationMetadata.SnapshotSyncInfo.SnapshotSyncType targetSnapshotSyncType,
                                         LogReplicationMetadata.SyncStatus targetSnapshotSyncStatus) {

        LogReplicationMetadata.ReplicationStatusKey key =
                LogReplicationMetadata.ReplicationStatusKey
                        .newBuilder()
                        .setClusterId(DefaultClusterConfig.getStandbyClusterId())
                        .build();

        ReplicationStatusVal replicationStatusVal;
        try (TxnContext txn = corfuStoreActive.txn(LogReplicationMetadataManager.NAMESPACE)) {
            replicationStatusVal = (ReplicationStatusVal)txn.getRecord(REPLICATION_STATUS_TABLE, key).getPayload();
            txn.commit();
        }

        log.info("ReplicationStatusVal: RemainingEntriesToSend: {}, SyncType: {}, Status: {}",
                replicationStatusVal.getRemainingEntriesToSend(), replicationStatusVal.getSyncType(),
                replicationStatusVal.getStatus());

        log.info("ReplicationStatusVal: Base: {}, Type: {}, Status: {}, CompletedTime: {}",
                replicationStatusVal.getSnapshotSyncInfo().getBaseSnapshot(), replicationStatusVal.getSnapshotSyncInfo().getType(),
                replicationStatusVal.getSnapshotSyncInfo().getStatus(), replicationStatusVal.getSnapshotSyncInfo().getCompletedTime());

        assertThat(replicationStatusVal.getSyncType()).isEqualTo(targetSyncType);
        assertThat(replicationStatusVal.getStatus()).isEqualTo(targetSyncStatus);
        assertThat(replicationStatusVal.getSnapshotSyncInfo().getType()).isEqualTo(targetSnapshotSyncType);
        assertThat(replicationStatusVal.getSnapshotSyncInfo().getStatus()).isEqualTo(targetSnapshotSyncStatus);
        assertThat(replicationStatusVal.getSnapshotSyncInfo().getBaseSnapshot()).isGreaterThan(Address.NON_ADDRESS);

        return replicationStatusVal.getRemainingEntriesToSend();
    }

    private void openNonReplicatedTable() throws Exception {
        noisyMap = corfuStoreActive.openTable(
                NAMESPACE, "noisyMap", Sample.StringKey.class, Sample.IntValueTag.class, Sample.Metadata.class,
                TableOptions.fromProtoSchema(Sample.IntValueTag.class));
    }

    private void writeNonReplicatedTable(int numWrites) {
        for (int i = 0; i < numWrites; i++) {
            Sample.StringKey stringKey = Sample.StringKey.newBuilder().setKey(String.valueOf(i)).build();
            Sample.IntValueTag intValueTag = Sample.IntValueTag.newBuilder().setValue(i).build();
            Sample.Metadata metadata = Sample.Metadata.newBuilder().setMetadata("Metadata_" + i).build();
            try (TxnContext txn = corfuStoreActive.txn(NAMESPACE)) {
                txn.putRecord(noisyMap, stringKey, intValueTag, metadata);
                txn.commit();
            }
        }
    }

    @Test
    public void testSnapshotSyncApplyInterrupted() throws Exception {
        final int standbyIndex = 2;
        final int numWritesSmaller = 1000;

        try {
            log.debug("Setup active and standby Corfu's");
            setupActiveAndStandbyCorfu();

            log.debug("Open map on active and standby");
            openMaps(MAP_COUNT, false);

            // Subscribe to standby map 'Table002' (standbyIndex) to stop Standby LR as soon as updates are received,
            // forcing snapshot sync apply to be interrupted and resumed after LR standby is restarted
            subscribe(TABLE_PREFIX + standbyIndex);

            log.debug("Write data to active CorfuDB before LR is started ...");
            // Add Data for Snapshot Sync
            writeToActive(0, numWritesSmaller);

            // Confirm data does exist on Active Cluster
            for (Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> map : mapNameToMapActive.values()) {
                assertThat(map.count()).isEqualTo(numWritesSmaller);
            }

            // Confirm data does not exist on Standby Cluster
            for (Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> map : mapNameToMapStandby.values()) {
                assertThat(map.count()).isEqualTo(0);
            }

            startLogReplicatorServers();

            log.debug("Wait ... Snapshot log replication in progress ...");
            verifyStandbyData(numWritesSmaller);

            // Add Delta's for Log Entry Sync
            writeToActive(numWritesSmaller, numWritesSmaller / 2);

            log.debug("Wait ... Delta log replication in progress ...");
            verifyStandbyData((numWritesSmaller + (numWritesSmaller / 2)));
        } finally {
            executorService.shutdownNow();

            if (activeCorfu != null) {
                activeCorfu.destroy();
            }

            if (standbyCorfu != null) {
                standbyCorfu.destroy();
            }

            if (activeReplicationServer != null) {
                activeReplicationServer.destroy();
            }

            if (standbyReplicationServer != null) {
                standbyReplicationServer.destroy();
            }
        }
    }

    /**
     * Validate that no data is written into the Standby's Transaction Log during replication
     * of 5K objects.
     *
     * @throws Exception
     */
    @Test
    public void testStandbyTransactionLogging() throws Exception {
        final long timeout = 30;

        replicationEnded.set(false);

        // (1) Subscribe Client to Standby Transaction Log
        log.debug(">>> (1) Subscribe to Transaction Stream on Standby");
        Future<Boolean> consumerState = subscribeTransactionStream();

        // (2) Snapshot and Log Entry Sync
        log.debug(">>> (2) Start Snapshot and Log Entry Sync");
        testEndToEndSnapshotAndLogEntrySync();

        replicationEnded.set(true);

        Boolean txStreamNotEmpty = consumerState.get(timeout, TimeUnit.SECONDS);
        assertThat(txStreamNotEmpty).isTrue();
    }

    private Future<Boolean> subscribeTransactionStream() {

        ExecutorService consumer = Executors.newSingleThreadExecutor();
        List<CorfuRuntime> consumerRts = new ArrayList<>();

        // A thread that starts and consumes transaction updates via the Transaction Stream.
        return consumer.submit(() -> {

            CorfuRuntime consumerRt = CorfuRuntime.fromParameters(CorfuRuntime.CorfuRuntimeParameters
                    .builder()
                    .build())
                    .parseConfigurationString(standbyEndpoint)
                    .connect();

            consumerRts.add(consumerRt);

            IStreamView txStream = consumerRt.getStreamsView().get(ObjectsView.getLogReplicatorStreamId());

            int counter = 0;

            // Stop polling only when all updates (from all writers) have
            // been consumed.
            while (!replicationEnded.get()) {
                List<ILogData> entries = txStream.remaining();

                if (!entries.isEmpty()) {
                    log.error("Transaction Log Entry Found. Entries={}", entries);
                    counter++;
                }
            }

            System.out.println("Total Transaction Stream updates, count=" + counter);
            log.info("Total Tx Stream updates = {}", counter);

            // We should have Txn Stream Updates
            return counter != 0;
        });
    }

    private void subscribe(String mapName) {
        // Subscribe to mapName and upon changes stop standby LR
        CorfuStore corfuStore = new CorfuStore(standbyRuntime);

        try {
            corfuStore.openTable(
                    NAMESPACE, mapName,
                    Sample.StringKey.class, Sample.IntValueTag.class, Sample.Metadata.class,
                    TableOptions.fromProtoSchema(Sample.IntValueTag.class)
            );
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        StandbyMapListener configStreamListener = new StandbyMapListener(this);
        corfuStore.subscribeListener(configStreamListener, NAMESPACE, "test");
    }

    /**
     * Stream Listener on topology config table, which is for test only.
     * It enables ITs run as processes and communicate with the cluster manager
     * to update topology config.
     **/
    public static class StandbyMapListener implements StreamListener {

        private final LogReplicationAbstractIT abstractIT;

        private boolean interruptedSnapshotSyncApply = false;

        public StandbyMapListener(LogReplicationAbstractIT abstractIT) {
            this.abstractIT = abstractIT;
        }

        @Override
        public synchronized void onNext(CorfuStreamEntries results) {
            log.info("StandbyMapListener:: onNext {} with entry size {}", results, results.getEntries().size());

            if (!interruptedSnapshotSyncApply) {

                interruptedSnapshotSyncApply = true;

                // Stop Log Replication Server so Snapshot Sync Apply is interrupted in the middle and restart
                log.debug("StandbyMapListener:: Stop Standby LR while in snapshot sync apply phase...");
                this.abstractIT.stopStandbyLogReplicator();

                log.debug("StandbyMapListener:: Restart Standby LR...");
                this.abstractIT.startStandbyLogReplicator();
            }
        }

        @Override
        public void onError(Throwable throwable) {
            // Ignore
        }
    }

    /**
     * Test standby streaming, which depends on external configuration of the tags and tables of interest.
     *
     * This test relies on a custom ConfigAdapter (DefaultLogReplicationConfigAdapter) which has hard coded
     * the tables to stream on standby (two tables: table_1 and table_2 for TAG_ONE). We attach a listener to this
     * tag and verify updates are received accordingly. Note that, we also write to other tables with the same tags
     * to confirm these are not erroneously streamed as well (as they're not part of the configuration).
     *
     * @throws Exception
     */
    @Test
    public void testStandbyStreaming() throws Exception {
        try {
            final int totalEntries = 20;

            setupActiveAndStandbyCorfu();
            openMaps();

            Set<UUID> tablesToListen = getTablesToListen();

            // Start Listener on the 'stream_tag' of interest, on standby site + tables to listen (which accounts
            // for the notification for 'clear' table)
            CountDownLatch streamingStandbySnapshotCompletion = new CountDownLatch(totalEntries*2 + tablesToListen.size());

            // Countdown latch for the number of expected transactions to be received on the listener.  All updates
            // in a table are applied in a single transaction so the expected number = numTablesToListen
            CountDownLatch snapshotSyncNumTxLatch = new CountDownLatch(tablesToListen.size());
            StreamingStandbyListener listener = new StreamingStandbyListener(streamingStandbySnapshotCompletion,
                snapshotSyncNumTxLatch, tablesToListen);
            corfuStoreStandby.subscribeListener(listener, NAMESPACE, TAG_ONE);

            // Add Data for Snapshot Sync (before LR is started)
            writeToActiveDifferentTypes(0, totalEntries);

            // Confirm data does exist on Active Cluster
            verifyActiveData(totalEntries);

            // Confirm data does not exist on Standby Cluster
            verifyStandbyData(0);

            // Open a local table on Standby Cluster
            openTable(corfuStoreStandby, "local");

            // Open an extra table on Active Cluster
            openTable(corfuStoreActive, "extra");

            // Confirm local table is opened on Standby
            verifyTableOpened(corfuStoreStandby, "local");

            // Confirm extra table is opened on Active
            verifyTableOpened(corfuStoreActive, "extra");

            // Start LR
            startLogReplicatorServers();

            // Wait until snapshot sync has completed
            // Open replication status table and monitor completion field
            corfuStoreActive.openTable(LogReplicationMetadataManager.NAMESPACE,
                    REPLICATION_STATUS_TABLE,
                    LogReplicationMetadata.ReplicationStatusKey.class,
                    LogReplicationMetadata.ReplicationStatusVal.class,
                    null,
                    TableOptions.fromProtoSchema(LogReplicationMetadata.ReplicationStatusVal.class));
            blockUntilSnapshotSyncCompleted();

            // Verify Snapshot has successfully replicated
            verifyStandbyData(totalEntries);

            log.info("** Wait for data change notifications (snapshot)");
            streamingStandbySnapshotCompletion.await();
            snapshotSyncNumTxLatch.await();
            assertThat(listener.messages.size()).isEqualTo(totalEntries*2 + tablesToListen.size());

            // Verify both extra and local table are opened on Standby
            verifyTableOpened(corfuStoreStandby, "local");
            verifyTableOpened(corfuStoreStandby, "extra");

            // Attach new listener for deltas (the same listener could be used) but simplifying the use of the latch
            CountDownLatch streamingStandbyDeltaCompletion = new CountDownLatch(totalEntries*2);

            // The number of expected transactions to be received on the listener during delta sync.  The total
            // number of transactions = numTablesToListen * entries written in each table.  In this test,
            // 'totalEntries' are written to each table.
            CountDownLatch logEntrySyncNumTxLatch = new CountDownLatch(totalEntries*tablesToListen.size());

            StreamingStandbyListener listenerDeltas = new StreamingStandbyListener(streamingStandbyDeltaCompletion,
                logEntrySyncNumTxLatch, tablesToListen);
            corfuStoreStandby.subscribeListener(listenerDeltas, NAMESPACE, TAG_ONE);

            // Add Delta's for Log Entry Sync
            writeToActiveDifferentTypes(totalEntries, totalEntries);
            openTable(corfuStoreActive, "extra_delta");

            // Verify Delta's are replicated to standby
            verifyStandbyData((totalEntries*2));

            // Verify tableRegistry's delta is replicated to Standby
            verifyTableOpened(corfuStoreStandby, "extra_delta");

            // Confirm data has been received by standby streaming listeners (deltas generated)
            // Block until all updates are received
            log.info("** Wait for data change notifications (delta)");
            streamingStandbyDeltaCompletion.await();
            logEntrySyncNumTxLatch.await();
            assertThat(listenerDeltas.messages.size()).isEqualTo(totalEntries*2);

            // Add a delta to a 'mergeOnly' stream and confirm it is replicated. RegistryTable is a 'mergeOnly' stream
            openTable(corfuStoreActive, "extra_delta");
            verifyTableOpened(corfuStoreStandby, "extra_delta");

        } finally {
            executorService.shutdownNow();

            if (activeCorfu != null) {
                activeCorfu.destroy();
            }

            if (standbyCorfu != null) {
                standbyCorfu.destroy();
            }

            if (activeReplicationServer != null) {
                activeReplicationServer.destroy();
            }

            if (standbyReplicationServer != null) {
                standbyReplicationServer.destroy();
            }
        }

    }

    private void blockUntilSnapshotSyncCompleted() {
        LogReplicationMetadata.ReplicationStatusKey key =
                LogReplicationMetadata.ReplicationStatusKey
                        .newBuilder()
                        .setClusterId(DefaultClusterConfig.getStandbyClusterId())
                        .build();

        ReplicationStatusVal replicationStatusVal;
        boolean snapshotSyncCompleted = false;

        while (snapshotSyncCompleted) {
            try (TxnContext txn = corfuStoreActive.txn(LogReplicationMetadataManager.NAMESPACE)) {
                replicationStatusVal = (ReplicationStatusVal) txn.getRecord(REPLICATION_STATUS_TABLE, key).getPayload();
                txn.commit();
            }

            log.info("ReplicationStatusVal: RemainingEntriesToSend: {}, SyncType: {}, Status: {}",
                    replicationStatusVal.getRemainingEntriesToSend(), replicationStatusVal.getSyncType(),
                    replicationStatusVal.getStatus());

            log.info("ReplicationStatusVal: Base: {}, Type: {}, Status: {}, CompletedTime: {}",
                    replicationStatusVal.getSnapshotSyncInfo().getBaseSnapshot(), replicationStatusVal.getSnapshotSyncInfo().getType(),
                    replicationStatusVal.getSnapshotSyncInfo().getStatus(), replicationStatusVal.getSnapshotSyncInfo().getCompletedTime());

            snapshotSyncCompleted = replicationStatusVal.getSnapshotSyncInfo().getStatus() == LogReplicationMetadata.SyncStatus.COMPLETED;
        }
    }

    private void verifyActiveData(int totalEntries) {
        for (Table<Sample.StringKey, ValueFieldTagOne, Sample.Metadata> map : mapNameToMapActiveTypeA.values()) {
            assertThat(map.count()).isEqualTo(totalEntries);
        }

        for (Table<Sample.StringKey, ValueFieldTagOneAndTwo, Sample.Metadata> map : mapNameToMapActiveTypeB.values()) {
            assertThat(map.count()).isEqualTo(totalEntries);
        }
    }

    public void verifyStandbyData(int expectedConsecutiveWrites) {
        for (Map.Entry<String, Table<Sample.StringKey, ValueFieldTagOne, Sample.Metadata>> entry : mapNameToMapStandbyTypeA.entrySet()) {

            log.debug("Verify Data on Standby's Table {}", entry.getKey());

            // Wait until data is fully replicated
            while (entry.getValue().count() != expectedConsecutiveWrites) {
                // Block until expected number of entries is reached
            }

            log.debug("Number updates on Standby Map {} :: {} ", entry.getKey(), expectedConsecutiveWrites);

            // Verify data is present in Standby Site
            assertThat(entry.getValue().count()).isEqualTo(expectedConsecutiveWrites);

            for (int i = 0; i < (expectedConsecutiveWrites); i++) {
                try (TxnContext tx = corfuStoreStandby.txn(entry.getValue().getNamespace())) {
                    assertThat(tx.getRecord(entry.getValue(),
                            Sample.StringKey.newBuilder().setKey(String.valueOf(i)).build()).getPayload()).isNotNull();
                    tx.commit();
                }
            }
        }

        for (Map.Entry<String, Table<Sample.StringKey, ValueFieldTagOneAndTwo, Sample.Metadata>> entry : mapNameToMapStandbyTypeB.entrySet()) {

            log.debug("Verify Data on Standby's Table {}", entry.getKey());

            // Wait until data is fully replicated
            while (entry.getValue().count() != expectedConsecutiveWrites) {
                // Block until expected number of entries is reached
            }

            log.debug("Number updates on Standby Map {} :: {} ", entry.getKey(), expectedConsecutiveWrites);

            // Verify data is present in Standby Site
            assertThat(entry.getValue().count()).isEqualTo(expectedConsecutiveWrites);

            for (int i = 0; i < (expectedConsecutiveWrites); i++) {
                try (TxnContext tx = corfuStoreStandby.txn(entry.getValue().getNamespace())) {
                    assertThat(tx.getRecord(entry.getValue(),
                            Sample.StringKey.newBuilder().setKey(String.valueOf(i)).build()).getPayload()).isNotNull();
                    tx.commit();
                }
            }
        }
    }

    /**
     * Helper method for opening tables with is_federated flag to be true, which will be used to verify the registry
     * table entries are correctly replicated.
     */
    private void openTable(CorfuStore corfuStore, String tableName) throws Exception {
        corfuStore.openTable(
                NAMESPACE, tableName,
                Sample.StringKey.class, Sample.IntValueTag.class, Sample.Metadata.class,
                TableOptions.fromProtoSchema(SampleTableAMsg.class)
        );
    }

    private void verifyTableOpened(CorfuStore corfuStore, String tableName) {
        CorfuStoreMetadata.TableName key = CorfuStoreMetadata.TableName.newBuilder()
                .setNamespace(NAMESPACE)
                .setTableName(tableName)
                .build();

        while (!corfuStore.getRuntime().getTableRegistry().listTables().contains(key)) {
            // Wait for delta replication
            log.trace("Wait for delta replication...");
            Sleep.sleepUninterruptibly(Duration.ofMillis(WAIT_DELTA));
        }

        assertThat(corfuStore.getRuntime().getTableRegistry().listTables())
                .contains(key);
    }

    public void openMaps() throws Exception {
        for (int i = 1; i <= MAP_COUNT; i++) {
            String mapName = TABLE_PREFIX + i;

            if (i % 2 == 0) {
                Table<Sample.StringKey, ValueFieldTagOne, Sample.Metadata> mapActive = corfuStoreActive.openTable(
                        NAMESPACE, mapName, Sample.StringKey.class, ValueFieldTagOne.class, Sample.Metadata.class,
                        TableOptions.fromProtoSchema(ValueFieldTagOne.class));
                mapNameToMapActiveTypeA.put(mapName, mapActive);

                Table<Sample.StringKey, ValueFieldTagOne, Sample.Metadata> mapStandby = corfuStoreStandby.openTable(
                        NAMESPACE, mapName, Sample.StringKey.class, ValueFieldTagOne.class, Sample.Metadata.class,
                        TableOptions.fromProtoSchema(ValueFieldTagOne.class));
                mapNameToMapStandbyTypeA.put(mapName, mapStandby);

            } else {
                Table<Sample.StringKey, ValueFieldTagOneAndTwo , Sample.Metadata> mapActive = corfuStoreActive.openTable(
                        NAMESPACE, mapName, Sample.StringKey.class, ValueFieldTagOneAndTwo.class, Sample.Metadata.class,
                        TableOptions.fromProtoSchema(ValueFieldTagOneAndTwo.class));
                mapNameToMapActiveTypeB.put(mapName, mapActive);

                Table<Sample.StringKey, ValueFieldTagOneAndTwo, Sample.Metadata> mapStandby = corfuStoreStandby.openTable(
                        NAMESPACE, mapName, Sample.StringKey.class, ValueFieldTagOneAndTwo.class, Sample.Metadata.class,
                        TableOptions.fromProtoSchema(ValueFieldTagOneAndTwo.class));
                mapNameToMapStandbyTypeB.put(mapName, mapStandby);
            }
        }

        mapNameToMapActiveTypeA.values().forEach(map -> assertThat(map.count()).isZero());
        mapNameToMapStandbyTypeA.values().forEach(map -> assertThat(map.count()).isZero());
        mapNameToMapActiveTypeB.values().forEach(map -> assertThat(map.count()).isZero());
        mapNameToMapStandbyTypeB.values().forEach(map -> assertThat(map.count()).isZero());
    }

    public void writeToActiveDifferentTypes(int startIndex, int totalEntries) {
        int maxIndex = totalEntries + startIndex;
        for(Map.Entry<String, Table<Sample.StringKey, ValueFieldTagOne, Sample.Metadata>> entry : mapNameToMapActiveTypeA.entrySet()) {

            Table<Sample.StringKey, ValueFieldTagOne, Sample.Metadata> map = entry.getValue();

            for (int i = startIndex; i < maxIndex; i++) {
                Sample.StringKey stringKey = Sample.StringKey.newBuilder().setKey(String.valueOf(i)).build();
                ValueFieldTagOne value = ValueFieldTagOne.newBuilder().setPayload(Integer.toString(i)).build();
                Sample.Metadata metadata = Sample.Metadata.newBuilder().setMetadata("Metadata_" + i).build();
                try (TxnContext txn = corfuStoreActive.txn(NAMESPACE)) {
                    txn.putRecord(map, stringKey, value, metadata);
                    txn.commit();
                }
            }
        }

        for(Map.Entry<String, Table<Sample.StringKey, ValueFieldTagOneAndTwo, Sample.Metadata>> entry : mapNameToMapActiveTypeB.entrySet()) {

            Table<Sample.StringKey, ValueFieldTagOneAndTwo, Sample.Metadata> map = entry.getValue();

            for (int i = startIndex; i < maxIndex; i++) {
                Sample.StringKey stringKey = Sample.StringKey.newBuilder().setKey(String.valueOf(i)).build();
                ValueFieldTagOneAndTwo value = ValueFieldTagOneAndTwo.newBuilder().setPayload(Integer.toString(i)).build();
                Sample.Metadata metadata = Sample.Metadata.newBuilder().setMetadata("Metadata_" + i).build();
                try (TxnContext txn = corfuStoreActive.txn(NAMESPACE)) {
                    txn.putRecord(map, stringKey, value, metadata);
                    txn.commit();
                }
            }
        }
    }

    /**
     * Stream listener that listens on the replication status and counts down it's latch on
     * the dataConsistent field being set to true.
     */
    class DataConsistentListener implements StreamListener {

        private CountDownLatch countDownLatch;

        public DataConsistentListener(CountDownLatch countdownLatch) {
            this.countDownLatch = countdownLatch;
        }

        @Override
        public void onNext(CorfuStreamEntries results) {
            results.getEntries().forEach((schema, entries) -> entries.forEach(e -> {
                LogReplicationMetadata.ReplicationStatusVal statusVal = (LogReplicationMetadata.ReplicationStatusVal)e.getPayload();
                if (statusVal.getDataConsistent()) {
                    countDownLatch.countDown();
                }
            }));
        }

        @Override
        public void onError(Throwable throwable) {
            Assert.fail("onError for DataConsistentListener : " + throwable.toString());
        }
    }

    private Set<UUID> getTablesToListen() {
        String SEPARATOR = "$";
        int indexOne = 1;
        int indexTwo = 2;
        Set<UUID> tablesToListen = new HashSet<>();
        tablesToListen.add(CorfuRuntime.getStreamID(NAMESPACE + SEPARATOR + TABLE_PREFIX + indexOne));
        tablesToListen.add(CorfuRuntime.getStreamID(NAMESPACE + SEPARATOR + TABLE_PREFIX + indexTwo));
        return tablesToListen;
    }
}
