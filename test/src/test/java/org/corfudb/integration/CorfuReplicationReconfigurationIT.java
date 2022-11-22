package org.corfudb.integration;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterConfig;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata;
import org.corfudb.infrastructure.logreplication.proto.Sample;
import org.corfudb.infrastructure.logreplication.replication.LogReplicationAckReader;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationStatusVal;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
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
                LogReplicationMetadataManager.REPLICATION_STATUS_TABLE,
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
            replicationStatusVal = (ReplicationStatusVal)txn.getRecord(LogReplicationMetadataManager.REPLICATION_STATUS_TABLE, key).getPayload();
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
                    LogReplicationMetadataManager.REPLICATION_STATUS_TABLE,
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
                replicationStatusVal = (ReplicationStatusVal) txn.getRecord(LogReplicationMetadataManager.REPLICATION_STATUS_TABLE, key).getPayload();
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
