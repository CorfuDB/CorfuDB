package org.corfudb.integration;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterConfig;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationStatusVal;
import org.corfudb.infrastructure.logreplication.proto.Sample;
import org.corfudb.infrastructure.logreplication.replication.LogReplicationAckReader;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
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

    private Map<String, Table<Sample.StringKey, ValueFieldTagOne, Sample.Metadata>> mapNameToMapSourceTypeA
            = new HashMap<>();
    private Map<String, Table<Sample.StringKey, ValueFieldTagOne, Sample.Metadata>> mapNameToMapSinkTypeA =
            new HashMap<>();
    private Map<String, Table<Sample.StringKey, ValueFieldTagOneAndTwo, Sample.Metadata>> mapNameToMapSourceTypeB =
            new HashMap<>();
    private Map<String, Table<Sample.StringKey, ValueFieldTagOneAndTwo, Sample.Metadata>> mapNameToMapSinkTypeB =
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
     * Test the case where Sink leader node is restarted during log entry sync.
     *
     * The expectation is that replication should resume.
     */
    @Test
    public void testSinkClusterReset() throws Exception {
        // (1) Snapshot and Log Entry Sync
        log.debug(">>> (1) Start Snapshot and Log Entry Sync");
        testEndToEndSnapshotAndLogEntrySyncUFO(false, false);

        ExecutorService writerService = Executors.newSingleThreadExecutor();

        // (2) Stop Sink Log Replicator Server
        log.debug(">>> (2) Stop Sink Node");
        stopSinkLogReplicator();

        // (3) Start daemon thread writing data to source
        log.debug(">>> (3) Start daemon writer service");
        // Since step (1) wrote numWrites for snapshotSync and numWrites/2 in logEntrySync, continue from this starting point
        writerService.submit(() -> writeToSource((numWrites + numWrites/2), numWrites));

        // (4) Sleep Interval so writes keep going through, while sink is down
        log.debug(">>> (4) Wait for some time");
        Sleep.sleepUninterruptibly(Duration.ofSeconds(SLEEP_DURATION));

        // (5) Restart Sink Log Replicator
        log.debug(">>> (5) Restart Sink Node");
        startSinkLogReplicator();

        // (6) Verify Data on Sink after Restart
        log.debug(">>> (6) Verify Data on Sink");
        verifySinkData((numWrites*2 + numWrites/2));
    }

    /**
     * Test the case where Source leader node is restarted during log entry sync.
     *
     * The expectation is that replication should resume.
     */
    @Test
    public void testSourceClusterReset() throws Exception {

        final int delta = 5;

        // (1) Snapshot and Log Entry Sync
        log.debug(">>> (1) Start Snapshot and Log Entry Sync");
        testEndToEndSnapshotAndLogEntrySyncUFO(false, false);

        ExecutorService writerService = Executors.newSingleThreadExecutor();

        // (2) Start daemon thread writing data to source
        log.debug(">>> (2) Start daemon writer service");
        // Since step (1) wrote numWrites for snapshotSync and numWrites/2 in logEntrySync, continue from this starting point
        writerService.submit(() -> writeToSource((numWrites + numWrites/2), numWrites));

        // (3) Stop Source Log Replicator Server
        log.debug(">>> (3) Stop Source Node");
        stopSourceLogReplicator();

        // (4) Sleep Interval so writes keep going through, while source is down
        log.debug(">>> (4) Wait for some time");
        Sleep.sleepUninterruptibly(Duration.ofSeconds(SLEEP_DURATION));

        // (5) Restart Source Log Replicator
        log.debug(">>> (5) Restart Source Node");
        startSourceLogReplicator();

        // (6) Verify Data on Sink after Restart
        log.debug(">>> (6) Verify Data on Sink");
        verifySinkData((numWrites*2 + numWrites/2));

        // (7) Verify replication status after all data has been replicated (no further data)
        corfuStoreSource.openTable(LogReplicationMetadataManager.NAMESPACE,
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

        // (9) Stop source LR again, so server restarts from Log Entry Sync, with no actual deltas (as there is no new data)
        // and verify remainingEntriesToSend is still '0'
        stopSourceLogReplicator();
        startSourceLogReplicator();

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
                        .setClusterId(new DefaultClusterConfig().getSinkClusterIds().get(0))
                        .build();

        ReplicationStatusVal replicationStatusVal;
        try (TxnContext txn = corfuStoreSource.txn(LogReplicationMetadataManager.NAMESPACE)) {
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
        noisyMap = corfuStoreSource.openTable(
                NAMESPACE, "noisyMap", Sample.StringKey.class, Sample.IntValueTag.class, Sample.Metadata.class,
                TableOptions.fromProtoSchema(Sample.IntValueTag.class));
    }

    private void writeNonReplicatedTable(int numWrites) {
        for (int i = 0; i < numWrites; i++) {
            Sample.StringKey stringKey = Sample.StringKey.newBuilder().setKey(String.valueOf(i)).build();
            Sample.IntValueTag intValueTag = Sample.IntValueTag.newBuilder().setValue(i).build();
            Sample.Metadata metadata = Sample.Metadata.newBuilder().setMetadata("Metadata_" + i).build();
            try (TxnContext txn = corfuStoreSource.txn(NAMESPACE)) {
                txn.putRecord(noisyMap, stringKey, intValueTag, metadata);
                txn.commit();
            }
        }
    }

    @Test
    public void testSnapshotSyncApplyInterrupted() throws Exception {
        final int sinkIndex = 2;
        final int numWritesSmaller = 1000;

        try {
            log.debug("Setup source and sink Corfu's");
            setupSourceAndSinkCorfu();

            log.debug("Open map on source and sink");
            openMaps(MAP_COUNT, false);

            // Subscribe to sink map 'Table002' (sinkIndex) to stop Sink LR as soon as updates are received,
            // forcing snapshot sync apply to be interrupted and resumed after LR sink is restarted
            subscribe(TABLE_PREFIX + sinkIndex);

            log.debug("Write data to source CorfuDB before LR is started ...");
            // Add Data for Snapshot Sync
            writeToSource(0, numWritesSmaller);

            // Confirm data does exist on Source Cluster
            for (Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> map : mapNameToMapSource.values()) {
                assertThat(map.count()).isEqualTo(numWritesSmaller);
            }

            // Confirm data does not exist on Sink Cluster
            for (Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> map : mapNameToMapSink.values()) {
                assertThat(map.count()).isEqualTo(0);
            }

            startLogReplicatorServers();

            log.debug("Wait ... Snapshot log replication in progress ...");
            verifySinkData(numWritesSmaller);

            // Add Delta's for Log Entry Sync
            writeToSource(numWritesSmaller, numWritesSmaller / 2);

            log.debug("Wait ... Delta log replication in progress ...");
            verifySinkData((numWritesSmaller + (numWritesSmaller / 2)));
        } finally {
            executorService.shutdownNow();

            if (sourceCorfu != null) {
                sourceCorfu.destroy();
            }

            if (sinkCorfu != null) {
                sinkCorfu.destroy();
            }

            if (sourceReplicationServer != null) {
                sourceReplicationServer.destroy();
            }

            if (sinkReplicationServer != null) {
                sinkReplicationServer.destroy();
            }
        }
    }

    /**
     * Validate that no data is written into the Sink's Transaction Log during replication
     * of 5K objects.
     *
     * @throws Exception
     */
    @Test
    public void testSinkTransactionLogging() throws Exception {
        final long timeout = 30;

        replicationEnded.set(false);

        // (1) Subscribe Client to Sink Transaction Log
        log.debug(">>> (1) Subscribe to Transaction Stream on Sink");
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
                    .parseConfigurationString(sinkEndpoint)
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
        // Subscribe to mapName and upon changes stop sink LR
        CorfuStore corfuStore = new CorfuStore(sinkRuntime);

        try {
            corfuStore.openTable(
                    NAMESPACE, mapName,
                    Sample.StringKey.class, Sample.IntValueTag.class, Sample.Metadata.class,
                    TableOptions.fromProtoSchema(Sample.IntValueTag.class)
            );
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        SinkMapListener configStreamListener = new SinkMapListener(this);
        corfuStore.subscribeListener(configStreamListener, NAMESPACE, "test");
    }

    /**
     * Stream Listener on topology config table, which is for test only.
     * It enables ITs run as processes and communicate with the cluster manager
     * to update topology config.
     **/
    public static class SinkMapListener implements StreamListener {

        private final LogReplicationAbstractIT abstractIT;

        private boolean interruptedSnapshotSyncApply = false;

        public SinkMapListener(LogReplicationAbstractIT abstractIT) {
            this.abstractIT = abstractIT;
        }

        @Override
        public synchronized void onNext(CorfuStreamEntries results) {
            log.info("SinkMapListener:: onNext {} with entry size {}", results, results.getEntries().size());

            if (!interruptedSnapshotSyncApply) {

                interruptedSnapshotSyncApply = true;

                // Stop Log Replication Server so Snapshot Sync Apply is interrupted in the middle and restart
                log.debug("SinkMapListener:: Stop Sink LR while in snapshot sync apply phase...");
                this.abstractIT.stopSinkLogReplicator();

                log.debug("SinkMapListener:: Restart Sink LR...");
                this.abstractIT.startSinkLogReplicator();
            }
        }

        @Override
        public void onError(Throwable throwable) {
            // Ignore
        }
    }

    /**
     * Test sink streaming, which depends on external configuration of the tags and tables of interest.
     *
     * This test relies on a custom ConfigAdapter (DefaultLogReplicationConfigAdapter) which has hard coded
     * the tables to stream on sink (two tables: table_1 and table_2 for TAG_ONE). We attach a listener to this
     * tag and verify updates are received accordingly. Note that, we also write to other tables with the same tags
     * to confirm these are not erroneously streamed as well (as they're not part of the configuration).
     *
     * @throws Exception
     */
    @Test
    public void testSinkStreaming() throws Exception {
        try {
            final int totalEntries = 20;

            setupSourceAndSinkCorfu();
            openMaps();

            Set<UUID> tablesToListen = getTablesToListen();

            // Start Listener on the 'stream_tag' of interest, on sink site + tables to listen (which accounts
            // for the notification for 'clear' table)
            CountDownLatch streamingSinkSnapshotCompletion = new CountDownLatch(
                    totalEntries*tablesToListen.size() + tablesToListen.size());

            // Countdown latch for the number of expected transactions to be received on the listener.  All updates
            // in a table are applied in a single transaction so the expected number = numTablesToListen
            CountDownLatch snapshotSyncNumTxLatch = new CountDownLatch(tablesToListen.size());
            StreamingSinkListener listener = new StreamingSinkListener(streamingSinkSnapshotCompletion,
                snapshotSyncNumTxLatch, tablesToListen);
            corfuStoreSink.subscribeListener(listener, NAMESPACE, TAG_ONE);

            // Add Data for Snapshot Sync (before LR is started)
            writeToSourceDifferentTypes(0, totalEntries);

            // Confirm data does exist on Source Cluster
            verifySourceData(totalEntries);

            // Confirm data does not exist on Sink Cluster
            verifySinkData(0);

            // Open a local table on Sink Cluster
            openTable(corfuStoreSink, "local");

            // Open an extra table on Source Cluster
            openTable(corfuStoreSource, "extra");

            // Confirm local table is opened on Sink
            verifyTableOpened(corfuStoreSink, "local");

            // Confirm extra table is opened on Source
            verifyTableOpened(corfuStoreSource, "extra");

            // Start LR
            startLogReplicatorServers();

            // Wait until snapshot sync has completed
            // Open replication status table and monitor completion field
            corfuStoreSource.openTable(LogReplicationMetadataManager.NAMESPACE,
                    LogReplicationMetadataManager.REPLICATION_STATUS_TABLE,
                    LogReplicationMetadata.ReplicationStatusKey.class,
                    LogReplicationMetadata.ReplicationStatusVal.class,
                    null,
                    TableOptions.fromProtoSchema(LogReplicationMetadata.ReplicationStatusVal.class));
            blockUntilSnapshotSyncCompleted();

            // Verify Snapshot has successfully replicated
            verifySinkData(totalEntries);

            log.info("** Wait for data change notifications (snapshot)");
            streamingSinkSnapshotCompletion.await();
            snapshotSyncNumTxLatch.await();
            assertThat(listener.messages.size()).isEqualTo(
                    totalEntries*tablesToListen.size() + tablesToListen.size());

            // Verify both extra and local table are opened on Sink
            verifyTableOpened(corfuStoreSink, "local");
            verifyTableOpened(corfuStoreSink, "extra");

            // Attach new listener for deltas (the same listener could be used) but simplifying the use of the latch
            CountDownLatch streamingSinkDeltaCompletion = new CountDownLatch(
                    totalEntries*tablesToListen.size());

            // The number of expected transactions to be received on the listener during delta sync.  The total
            // number of transactions = numTablesToListen * entries written in each table.  In this test,
            // 'totalEntries' are written to each table.
            CountDownLatch logEntrySyncNumTxLatch = new CountDownLatch(totalEntries*tablesToListen.size());

            StreamingSinkListener listenerDeltas = new StreamingSinkListener(streamingSinkDeltaCompletion,
                logEntrySyncNumTxLatch, tablesToListen);
            corfuStoreSink.subscribeListener(listenerDeltas, NAMESPACE, TAG_ONE);

            // Add Delta's for Log Entry Sync
            writeToSourceDifferentTypes(totalEntries, totalEntries);

            // Verify Delta's are replicated to sink
            verifySinkData(totalEntries*2);

            // Confirm data has been received by sink streaming listeners (deltas generated)
            // Block until all updates are received
            log.info("** Wait for data change notifications (delta)");
            streamingSinkDeltaCompletion.await();
            logEntrySyncNumTxLatch.await();
            assertThat(listenerDeltas.messages.size()).isEqualTo(
                    totalEntries*tablesToListen.size());

            // Add a delta to a 'mergeOnly' stream and confirm it is replicated. RegistryTable is a 'mergeOnly' stream
            openTable(corfuStoreSource, "extra_delta");
            verifyTableOpened(corfuStoreSink, "extra_delta");

        } finally {
            executorService.shutdownNow();

            if (sourceCorfu != null) {
                sourceCorfu.destroy();
            }

            if (sinkCorfu != null) {
                sinkCorfu.destroy();
            }

            if (sourceReplicationServer != null) {
                sourceReplicationServer.destroy();
            }

            if (sinkReplicationServer != null) {
                sinkReplicationServer.destroy();
            }
        }

    }

    private void blockUntilSnapshotSyncCompleted() {
        LogReplicationMetadata.ReplicationStatusKey key =
                LogReplicationMetadata.ReplicationStatusKey
                        .newBuilder()
                        .setClusterId(new DefaultClusterConfig().getSinkClusterIds().get(0))
                        .build();

        ReplicationStatusVal replicationStatusVal;
        boolean snapshotSyncCompleted = false;

        while (snapshotSyncCompleted) {
            try (TxnContext txn = corfuStoreSource.txn(LogReplicationMetadataManager.NAMESPACE)) {
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

    private void verifySourceData(int totalEntries) {
        for (Table<Sample.StringKey, ValueFieldTagOne, Sample.Metadata> map : mapNameToMapSourceTypeA.values()) {
            assertThat(map.count()).isEqualTo(totalEntries);
        }

        for (Table<Sample.StringKey, ValueFieldTagOneAndTwo, Sample.Metadata> map : mapNameToMapSourceTypeB.values()) {
            assertThat(map.count()).isEqualTo(totalEntries);
        }
    }

    public void verifySinkData(int expectedConsecutiveWrites) {
        for (Map.Entry<String, Table<Sample.StringKey, ValueFieldTagOne, Sample.Metadata>> entry : mapNameToMapSinkTypeA.entrySet()) {

            log.debug("Verify Data on Sink's Table {}", entry.getKey());

            // Wait until data is fully replicated
            while (entry.getValue().count() != expectedConsecutiveWrites) {
                // Block until expected number of entries is reached
            }

            log.debug("Number updates on Sink Map {} :: {} ", entry.getKey(), expectedConsecutiveWrites);

            // Verify data is present in Sink Site
            assertThat(entry.getValue().count()).isEqualTo(expectedConsecutiveWrites);

            for (int i = 0; i < (expectedConsecutiveWrites); i++) {
                try (TxnContext tx = corfuStoreSink.txn(entry.getValue().getNamespace())) {
                    assertThat(tx.getRecord(entry.getValue(),
                            Sample.StringKey.newBuilder().setKey(String.valueOf(i)).build()).getPayload()).isNotNull();
                    tx.commit();
                }
            }
        }

        for (Map.Entry<String, Table<Sample.StringKey, ValueFieldTagOneAndTwo, Sample.Metadata>> entry : mapNameToMapSinkTypeB.entrySet()) {

            log.debug("Verify Data on Sink's Table {}", entry.getKey());

            // Wait until data is fully replicated
            while (entry.getValue().count() != expectedConsecutiveWrites) {
                // Block until expected number of entries is reached
            }

            log.debug("Number updates on Sink Map {} :: {} ", entry.getKey(), expectedConsecutiveWrites);

            // Verify data is present in Sink Site
            assertThat(entry.getValue().count()).isEqualTo(expectedConsecutiveWrites);

            for (int i = 0; i < (expectedConsecutiveWrites); i++) {
                try (TxnContext tx = corfuStoreSink.txn(entry.getValue().getNamespace())) {
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
                Table<Sample.StringKey, ValueFieldTagOne, Sample.Metadata> mapSource = corfuStoreSource.openTable(
                        NAMESPACE, mapName, Sample.StringKey.class, ValueFieldTagOne.class, Sample.Metadata.class,
                        TableOptions.fromProtoSchema(ValueFieldTagOne.class));
                mapNameToMapSourceTypeA.put(mapName, mapSource);

                Table<Sample.StringKey, ValueFieldTagOne, Sample.Metadata> mapSink = corfuStoreSink.openTable(
                        NAMESPACE, mapName, Sample.StringKey.class, ValueFieldTagOne.class, Sample.Metadata.class,
                        TableOptions.fromProtoSchema(ValueFieldTagOne.class));
                mapNameToMapSinkTypeA.put(mapName, mapSink);

            } else {
                Table<Sample.StringKey, ValueFieldTagOneAndTwo , Sample.Metadata> mapSource = corfuStoreSource.openTable(
                        NAMESPACE, mapName, Sample.StringKey.class, ValueFieldTagOneAndTwo.class, Sample.Metadata.class,
                        TableOptions.fromProtoSchema(ValueFieldTagOneAndTwo.class));
                mapNameToMapSourceTypeB.put(mapName, mapSource);

                Table<Sample.StringKey, ValueFieldTagOneAndTwo, Sample.Metadata> mapSink = corfuStoreSink.openTable(
                        NAMESPACE, mapName, Sample.StringKey.class, ValueFieldTagOneAndTwo.class, Sample.Metadata.class,
                        TableOptions.fromProtoSchema(ValueFieldTagOneAndTwo.class));
                mapNameToMapSinkTypeB.put(mapName, mapSink);
            }
        }

        mapNameToMapSourceTypeA.values().forEach(map -> assertThat(map.count()).isZero());
        mapNameToMapSinkTypeA.values().forEach(map -> assertThat(map.count()).isZero());
        mapNameToMapSourceTypeB.values().forEach(map -> assertThat(map.count()).isZero());
        mapNameToMapSinkTypeB.values().forEach(map -> assertThat(map.count()).isZero());
    }

    public void writeToSourceDifferentTypes(int startIndex, int totalEntries) {
        int maxIndex = totalEntries + startIndex;
        for(Map.Entry<String, Table<Sample.StringKey, ValueFieldTagOne, Sample.Metadata>> entry : mapNameToMapSourceTypeA.entrySet()) {

            Table<Sample.StringKey, ValueFieldTagOne, Sample.Metadata> map = entry.getValue();

            for (int i = startIndex; i < maxIndex; i++) {
                Sample.StringKey stringKey = Sample.StringKey.newBuilder().setKey(String.valueOf(i)).build();
                ValueFieldTagOne value = ValueFieldTagOne.newBuilder().setPayload(Integer.toString(i)).build();
                Sample.Metadata metadata = Sample.Metadata.newBuilder().setMetadata("Metadata_" + i).build();
                try (TxnContext txn = corfuStoreSource.txn(NAMESPACE)) {
                    txn.putRecord(map, stringKey, value, metadata);
                    txn.commit();
                }
            }
        }

        for(Map.Entry<String, Table<Sample.StringKey, ValueFieldTagOneAndTwo, Sample.Metadata>> entry : mapNameToMapSourceTypeB.entrySet()) {

            Table<Sample.StringKey, ValueFieldTagOneAndTwo, Sample.Metadata> map = entry.getValue();

            for (int i = startIndex; i < maxIndex; i++) {
                Sample.StringKey stringKey = Sample.StringKey.newBuilder().setKey(String.valueOf(i)).build();
                ValueFieldTagOneAndTwo value = ValueFieldTagOneAndTwo.newBuilder().setPayload(Integer.toString(i)).build();
                Sample.Metadata metadata = Sample.Metadata.newBuilder().setMetadata("Metadata_" + i).build();
                try (TxnContext txn = corfuStoreSource.txn(NAMESPACE)) {
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
