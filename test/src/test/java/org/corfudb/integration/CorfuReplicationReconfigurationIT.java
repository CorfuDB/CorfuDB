package org.corfudb.integration;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultLogReplicationConfigAdapter;
import org.corfudb.infrastructure.logreplication.proto.Sample;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStreamEntries;
import org.corfudb.runtime.collections.CorfuStreamEntry;
import org.corfudb.runtime.collections.StreamListener;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.view.ObjectsView;
import org.corfudb.runtime.view.stream.IStreamView;
import org.corfudb.test.SampleSchema.ValueFieldTagOne;
import org.corfudb.test.SampleSchema.ValueFieldTagOneAndTwo;
import org.corfudb.util.Sleep;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
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

/**
 * This suite of tests validates the behavior of Log Replication
 * when nodes are shutdown and brought back up.
 *
 * @author amartinezman
 */
@Slf4j
public class CorfuReplicationReconfigurationIT extends LogReplicationAbstractIT {

    private static final int SLEEP_DURATION = 5;

    private AtomicBoolean replicationEnded = new AtomicBoolean(false);

    private Map<String, Table<Sample.StringKey, ValueFieldTagOne, Sample.Metadata>> mapNameToMapActiveTypeA
            = new HashMap<>();
    private Map<String, Table<Sample.StringKey, ValueFieldTagOne, Sample.Metadata>> mapNameToMapStandbyTypeA =
            new HashMap<>();
    private Map<String, Table<Sample.StringKey, ValueFieldTagOneAndTwo, Sample.Metadata>> mapNameToMapActiveTypeB =
            new HashMap<>();
    private Map<String, Table<Sample.StringKey, ValueFieldTagOneAndTwo, Sample.Metadata>> mapNameToMapStandbyTypeB =
            new HashMap<>();

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
        testEndToEndSnapshotAndLogEntrySyncUFO();

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
        verifyDataOnStandby((numWrites*2 + numWrites/2));
    }

    /**
     * Test the case where Active leader node is restarted during log entry sync.
     *
     * The expectation is that replication should resume.
     */
    @Test
    public void testActiveClusterReset() throws Exception {
        // (1) Snapshot and Log Entry Sync
        log.debug(">>> (1) Start Snapshot and Log Entry Sync");
        testEndToEndSnapshotAndLogEntrySyncUFO();

        ExecutorService writerService = Executors.newSingleThreadExecutor();

        // (2) Start daemon thread writing data to active
        log.debug(">>> (2) Start daemon writer service");
        // Since step (1) wrote numWrites for snapshotSync and numWrites/2 in logEntrySync, continue from this starting point
        writerService.submit(() -> writeToActive((numWrites + numWrites/2), numWrites));

        // (3) Stop Active Log Replicator Server
        log.debug(">>> (3) Stop Active Node");
        stopActiveLogReplicator();

        // (4) Sleep Interval so writes keep going through, while standby is down
        log.debug(">>> (4) Wait for some time");
        Sleep.sleepUninterruptibly(Duration.ofSeconds(SLEEP_DURATION));

        // (5) Restart Active Log Replicator
        log.debug(">>> (5) Restart Active Node");
        startActiveLogReplicator();

        // (6) Verify Data on Standby after Restart
        log.debug(">>> (6) Verify Data on Standby");
        verifyDataOnStandby((numWrites*2 + numWrites/2));
    }

    @Test
    public void testSnapshotSyncApplyInterrupted() throws Exception {
        final int standbyIndex = 2;
        final int numWritesSmaller = 1000;

        try {
            log.debug("Setup active and standby Corfu's");
            setupActiveAndStandbyCorfu();

            log.debug("Open map on active and standby");
            openMaps(DefaultLogReplicationConfigAdapter.MAP_COUNT);

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
            verifyDataOnStandby(numWritesSmaller);

            // Add Delta's for Log Entry Sync
            writeToActive(numWritesSmaller, numWritesSmaller / 2);

            log.debug("Wait ... Delta log replication in progress ...");
            verifyDataOnStandby((numWritesSmaller + (numWritesSmaller / 2)));
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
                    TableOptions.builder().build()
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

            // Start Listener on the 'stream_tag' of interest, on standby site
            CountDownLatch streamingStandbySnapshotCompletion = new CountDownLatch(totalEntries*2);
            StreamingStandbyListener listener = new StreamingStandbyListener(streamingStandbySnapshotCompletion,
                    new DefaultLogReplicationConfigAdapter().getStreamingConfigOnSink().keySet());
            corfuStoreStandby.subscribeListener(listener, NAMESPACE, DefaultLogReplicationConfigAdapter.TAG_ONE);

            // Add Data for Snapshot Sync (before LR is started)
            writeToActiveDifferentTypes(0, totalEntries);

            // Confirm data does exist on Active Cluster
            verifyDataOnActive(totalEntries);

            // Confirm data does not exist on Standby Cluster
            verifyDataOnStandby(0);

            // Start LR
            startLogReplicatorServers();

            // Verify Snapshot has successfully replicated
            verifyDataOnStandby(totalEntries);

            log.info("** Wait for data change notifications (snapshot)");
            streamingStandbySnapshotCompletion.await();
            assertThat(listener.messages.size()).isEqualTo(totalEntries*2);

            // Attach new listener for deltas (the same listener could be used) but simplifying the use of the latch
            CountDownLatch streamingStandbyDeltaCompletion = new CountDownLatch(totalEntries*2);
            StreamingStandbyListener listenerDeltas = new StreamingStandbyListener(streamingStandbyDeltaCompletion,
                    new DefaultLogReplicationConfigAdapter().getStreamingConfigOnSink().keySet());
            corfuStoreStandby.subscribeListener(listenerDeltas, NAMESPACE, DefaultLogReplicationConfigAdapter.TAG_ONE);

            // Add Delta's for Log Entry Sync
            writeToActiveDifferentTypes(totalEntries, totalEntries);

            // Verify Delta's are replicated to standby
            verifyDataOnStandby((totalEntries*2));

            // Confirm data has been received by standby streaming listeners (deltas generated)
            // Block until all updates are received
            log.info("** Wait for data change notifications (delta)");
            streamingStandbyDeltaCompletion.await();
            assertThat(listenerDeltas.messages.size()).isEqualTo(totalEntries*2);
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

    private void verifyDataOnActive(int totalEntries) {
        for (Table<Sample.StringKey, ValueFieldTagOne, Sample.Metadata> map : mapNameToMapActiveTypeA.values()) {
            assertThat(map.count()).isEqualTo(totalEntries);
        }

        for (Table<Sample.StringKey, ValueFieldTagOneAndTwo, Sample.Metadata> map : mapNameToMapActiveTypeB.values()) {
            assertThat(map.count()).isEqualTo(totalEntries);
        }
    }

    public void verifyDataOnStandby(int expectedConsecutiveWrites) {
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

    public void openMaps() throws Exception {
        for (int i = 1; i <= DefaultLogReplicationConfigAdapter.MAP_COUNT; i++) {
            String mapName = DefaultLogReplicationConfigAdapter.TABLE_PREFIX + i;

            if (i % 2 == 0) {
                Table<Sample.StringKey, ValueFieldTagOne, Sample.Metadata> mapActive = corfuStoreActive.openTable(
                        NAMESPACE, mapName, Sample.StringKey.class, ValueFieldTagOne.class, Sample.Metadata.class,
                        TableOptions.builder().build());
                mapNameToMapActiveTypeA.put(mapName, mapActive);

                Table<Sample.StringKey, ValueFieldTagOne, Sample.Metadata> mapStandby = corfuStoreStandby.openTable(
                        NAMESPACE, mapName, Sample.StringKey.class, ValueFieldTagOne.class, Sample.Metadata.class,
                        TableOptions.builder().build());
                mapNameToMapStandbyTypeA.put(mapName, mapStandby);

            } else {
                Table<Sample.StringKey, ValueFieldTagOneAndTwo , Sample.Metadata> mapActive = corfuStoreActive.openTable(
                        NAMESPACE, mapName, Sample.StringKey.class, ValueFieldTagOneAndTwo.class, Sample.Metadata.class,
                        TableOptions.builder().build());
                mapNameToMapActiveTypeB.put(mapName, mapActive);

                Table<Sample.StringKey, ValueFieldTagOneAndTwo, Sample.Metadata> mapStandby = corfuStoreStandby.openTable(
                        NAMESPACE, mapName, Sample.StringKey.class, ValueFieldTagOneAndTwo.class, Sample.Metadata.class,
                        TableOptions.builder().build());
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
     * Stream Listener used for testing streaming on standby site. This listener decreases a latch
     * until all expected updates are received/
     */
    public static class StreamingStandbyListener implements StreamListener {

        private final CountDownLatch updatesLatch;
        public List<CorfuStreamEntry> messages = new ArrayList<>();
        private final Set<UUID> tablesToListenTo;

        public StreamingStandbyListener(CountDownLatch updatesLatch, Set<UUID> tablesToListenTo) {
            this.updatesLatch = updatesLatch;
            this.tablesToListenTo = tablesToListenTo;
        }

        @Override
        public synchronized void onNext(CorfuStreamEntries results) {
            log.info("StreamingStandbyListener:: onNext {} with entry size {}", results, results.getEntries().size());

            System.out.println("StreamingStandbyListener:: onNext " + results);

            results.getEntries().forEach((schema, entries) -> {
                if (tablesToListenTo.contains(CorfuRuntime.getStreamID(NAMESPACE + "$" + schema.getTableName()))) {
                    messages.addAll(entries);
                    entries.forEach(e -> updatesLatch.countDown());
                }
            });
        }

        @Override
        public void onError(Throwable throwable) {
            log.error("ERROR :: unsubscribed listener");
        }
    }
}
