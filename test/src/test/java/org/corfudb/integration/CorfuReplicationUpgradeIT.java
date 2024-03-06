package org.corfudb.integration;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultAdapterForUpgrade;
import org.corfudb.infrastructure.logreplication.proto.Sample;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.corfudb.runtime.LogReplication.ReplicationStatus;
import org.corfudb.runtime.collections.CorfuStreamEntries;
import org.corfudb.runtime.collections.CorfuStreamEntry;
import org.corfudb.runtime.collections.StreamListener;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.runtime.LogReplicationUtils.REPLICATION_STATUS_TABLE_NAME;
import static org.corfudb.runtime.LogReplicationUtils.LR_STATUS_STREAM_TAG;
import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;
import static org.junit.Assert.fail;

@Slf4j
@SuppressWarnings("checkstyle:magicnumber")
public class CorfuReplicationUpgradeIT extends LogReplicationAbstractIT {

    private static final int FIVE = 5;

    private static final int NUM_WRITES = 500;

    private static final String UPGRADE_PLUGIN_PATH =
            "./test/src/test/resources/transport/pluginConfigForUpgrade.properties";

    private static final String NON_UPGRADE_PLUGIN_PATH =
            "./test/src/test/resources/transport/pluginConfig.properties";

    private SnapshotApplyCompletionListener sinkStatusListener;

    @Before
    public void setUp() {
        pluginConfigFilePath = NON_UPGRADE_PLUGIN_PATH;
    }

    /**
     * This test verifies that LR startup is paused until rolling upgrade completes, i.e., all nodes in a cluster are
     * on the same version.
     * 1. Start the Source and Sink clusters and wait for initial snapshot sync to complete
     * 2. Simulate rolling upgrade on the Sink Cluster as follows:
     *  a) stop the Sink cluster
     *  b) start the Sink cluster in a state where node and cluster versions are different, i.e., rolling upgrade in
     *  progress
     *  c) Write some data on the Source
     *  d) Verify no data written in c) is observed on the Sink as it is paused
     *  e) End rolling upgrade using the upgrade test plugin
     *  f) Verify that the Sink cluster now has the data from c)
     * 3. Simulate rolling upgrade on the Source cluster and verify that no snapshot sync gets triggered by it as it
     * is paused.
     * 4. End rolling upgrade on Source cluster using the upgrade test plugin
     * 5. Verify that a forced snapshot sync now gets triggered.
     * @throws Exception
     */
    @Test
    public void testLRNotStartedUntilRollingUpgradeCompletes() throws Exception {
        verifyInitialSnapshotSyncAfterStartup(FIVE, NUM_WRITES);

        // Simulate a rolling upgrade on the plugin
        stopSinkLogReplicator();
        DefaultAdapterForUpgrade defaultAdapterForUpgrade = new DefaultAdapterForUpgrade(sinkRuntime);
        defaultAdapterForUpgrade.startRollingUpgrade();

        // Wait till the Sink Cluster is in a paused state after starting.  This is indicated by a 'CLEAR' entry
        // written to the Replication Status table.
        CountDownLatch clearEntryLatch = new CountDownLatch(1);
        ClearOpListenerOnUpgrade clearOpListener = new ClearOpListenerOnUpgrade(clearEntryLatch);
        corfuStoreSink.subscribeListener(clearOpListener, CORFU_SYSTEM_NAMESPACE, LR_STATUS_STREAM_TAG);
        startSinkLogReplicator();
        clearEntryLatch.await();
        corfuStoreSink.unsubscribeListener(clearOpListener);

        // Write some more entries on the Source
        writeToSource(NUM_WRITES, NUM_WRITES/2);

        // Verify that these entries are not found on the Sink as it is paused
        verifyNoMoreDataOnSink(NUM_WRITES);

        // End the rolling upgrade and verify that the new entries are now present
        defaultAdapterForUpgrade.endRollingUpgrade();
        verifyDataOnSink(NUM_WRITES + NUM_WRITES/2);

        defaultAdapterForUpgrade.reset();

        // Simulate a rolling upgrade on Source
        stopSourceLogReplicator();
        defaultAdapterForUpgrade = new DefaultAdapterForUpgrade(sourceRuntime);
        defaultAdapterForUpgrade.startRollingUpgrade();

        CountDownLatch statusUpdateLatch = new CountDownLatch(1);
        sinkStatusListener = new SnapshotApplyCompletionListener(statusUpdateLatch);
        corfuStoreSink.subscribeListener(sinkStatusListener, CORFU_SYSTEM_NAMESPACE, LR_STATUS_STREAM_TAG);
        startSourceLogReplicator();

        verifyNoSnapshotSync(statusUpdateLatch);

        // End the rolling upgrade
        defaultAdapterForUpgrade.endRollingUpgrade();

        // Verify that a forced snapshot sync gets triggered
        statusUpdateLatch.await();
        verifyDataOnSink(NUM_WRITES + NUM_WRITES/2);
    }

    @Test
    public void testLogEntrySyncAfterSinkUpgraded() throws Exception {

        verifyInitialSnapshotSyncAfterStartup(FIVE, NUM_WRITES);

        // Upgrade the sink site
        log.info(">> Upgrading the sink site ...");
        performRollingUpgrade(false);

        // Verify that subsequent log entry sync is successful
        log.info("Write more data on the source");
        writeToSource(NUM_WRITES, NUM_WRITES / 2);

        log.info("Verify that data is replicated on the sink after it is upgraded");
        verifyDataOnSink(NUM_WRITES + (NUM_WRITES / 2));
    }

    @Test
    public void testSnapshotSyncAfterSinkUpgraded() throws Exception {
        verifyInitialSnapshotSyncAfterStartup(FIVE, NUM_WRITES);

        // Upgrade the sink site
        log.info(">> Upgrading the sink site ...");
        performRollingUpgrade(false);

        // Trigger a snapshot sync by stopping the source LR and running a CP+trim.  Verify that snapshot sync took
        // place
        verifySnapshotSyncAfterCPTrim();

        // Verify the number of entries on the sink
        verifyDataOnSink(NUM_WRITES);
    }

    @Test
    public void testSnapshotSyncAfterBothUpgraded() throws Exception {
        verifyInitialSnapshotSyncAfterStartup(FIVE, NUM_WRITES);

        // Upgrade the sink site first
        log.info(">> Upgrading the sink site ...");
        performRollingUpgrade(false);

        // Upgrading the source site will force a snapshot sync
        verifySnapshotSyncAfterSourceUpgrade();

        verifyDataOnSink(NUM_WRITES);
    }

    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void testLogEntrySyncAfterSinkUpgradedStreamsAddedAndRemoved() throws Exception {

        Set<String> streamsToReplicateSource = new HashSet<>();
        for (int i = 1; i <= 2; i++) {
            streamsToReplicateSource.add(TABLE_PREFIX + i);
        }

        verifyInitialSnapshotSyncAfterStartup(streamsToReplicateSource.size(), NUM_WRITES);

        Set<String> streamsToReplicateSink = new HashSet<>();
        for (int i = 2; i <= 3; i++) {
            streamsToReplicateSink.add(TABLE_PREFIX + i);
        }
        performRollingUpgrade(false);

        List<String> sourceOnlyStreams = streamsToReplicateSource.stream()
                .filter(s -> !streamsToReplicateSink.contains(s))
                .collect(Collectors.toList());

        List<String> sinkOnlyStreams = streamsToReplicateSink.stream()
                .filter(s -> !streamsToReplicateSource.contains(s))
                .collect(Collectors.toList());

        List<String> commonStreams = streamsToReplicateSource.stream()
                .filter(streamsToReplicateSink::contains)
                .collect(Collectors.toList());

        // Open maps corresponding to the new streams found after upgrade
        openMapsAfterUpgrade(sourceOnlyStreams, sinkOnlyStreams);

        // Write to sourceOnlyStreams
        writeDataOnSource(sourceOnlyStreams, NUM_WRITES, NUM_WRITES / 2);

        // Write to common streams
        writeDataOnSource(commonStreams, NUM_WRITES, NUM_WRITES / 2);

        // Write to sinkOnlyStreams
        writeDataOnSource(sinkOnlyStreams, 0, NUM_WRITES);

        verifyDataOnSink(commonStreams, NUM_WRITES + NUM_WRITES / 2);
        log.info("Verified on Common Streams");

        verifyDataOnSink(sourceOnlyStreams, NUM_WRITES);
        log.info("Verified on Source-only streams");

        verifyDataOnSink(sinkOnlyStreams, 0);
        log.info("Verified on Sink-only streams");
    }

    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void testSnapshotSyncAfterSinkUpgradedStreamsAddedAndRemoved() throws Exception {

        Set<String> streamsToReplicateSource = new HashSet<>();
        for (int i = 1; i <= 2; i++) {
            streamsToReplicateSource.add(TABLE_PREFIX + i);
        }
        verifyInitialSnapshotSyncAfterStartup(streamsToReplicateSource.size(), NUM_WRITES);

        // Upgrade the sink site
        log.info(">> Upgrading the sink site ...");
        performRollingUpgrade(false);

        Set<String> streamsToReplicateSink = new HashSet<>();
        for (int i = 2; i <= 3; i++) {
            streamsToReplicateSink.add(TABLE_PREFIX + i);
        }

        stopSourceLogReplicator();

        List<String> sourceOnlyStreams = streamsToReplicateSource.stream()
                .filter(s -> !streamsToReplicateSink.contains(s))
                .collect(Collectors.toList());

        List<String> sinkOnlyStreams = streamsToReplicateSink.stream()
                .filter(s -> !streamsToReplicateSource.contains(s))
                .collect(Collectors.toList());

        List<String> commonStreams = streamsToReplicateSource.stream()
                .filter(streamsToReplicateSink::contains)
                .collect(Collectors.toList());

        // Open maps corresponding to the new streams in the upgraded config
        openMapsAfterUpgrade(sourceOnlyStreams, sinkOnlyStreams);

        // Write data on sourceOnly streams
        writeDataOnSource(sourceOnlyStreams, NUM_WRITES, NUM_WRITES / 2);

        // Write data on sinkOnly streams
        writeDataOnSource(sinkOnlyStreams, 0, NUM_WRITES);

        // Write data on common streams
        writeDataOnSource(commonStreams, NUM_WRITES, NUM_WRITES / 2);

        CountDownLatch statusUpdateLatch = new CountDownLatch(1);
        sinkStatusListener = new SnapshotApplyCompletionListener(statusUpdateLatch);
        corfuStoreSink.subscribeListener(sinkStatusListener, CORFU_SYSTEM_NAMESPACE, LR_STATUS_STREAM_TAG);

        // Trigger a snapshot sync by running a CP+trim
        checkpointAndTrim(true);
        startSourceLogReplicator();
        initSingleSourceSinkCluster();

        // Verify that snapshot sync between the different versions was successful
        statusUpdateLatch.await();

        verifyDataOnSink(sourceOnlyStreams, NUM_WRITES);

        // No new data for sink-only streams
        verifyDataOnSink(sinkOnlyStreams, 0);

        // New data present for common streams
        verifyDataOnSink(commonStreams, NUM_WRITES + NUM_WRITES / 2);
    }

    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void testSnapshotSyncAfterBothUpgradedStreamsAddedAndRemoved() throws Exception {
        Set<String> streamsToReplicateSource = new HashSet<>();
        for (int i = 1; i <= 2; i++) {
            streamsToReplicateSource.add(TABLE_PREFIX + i);
        }

        verifyInitialSnapshotSyncAfterStartup(streamsToReplicateSource.size(), NUM_WRITES);

        Set<String> streamsToReplicateSink = new HashSet<>();
        for (int i = 2; i <= 3; i++) {
            streamsToReplicateSink.add(TABLE_PREFIX + i);
        }

        performRollingUpgrade(false);

        List<String> sourceOnlyStreams = streamsToReplicateSource.stream()
                .filter(s -> !streamsToReplicateSink.contains(s))
                .collect(Collectors.toList());

        List<String> sinkOnlyStreams = streamsToReplicateSink.stream()
                .filter(s -> !streamsToReplicateSource.contains(s))
                .collect(Collectors.toList());

        List<String> commonStreams = streamsToReplicateSource.stream()
                .filter(streamsToReplicateSink::contains)
                .collect(Collectors.toList());

        // Open maps corresponding to the new streams in the upgraded config
        openMapsAfterUpgrade(sourceOnlyStreams, sinkOnlyStreams);

        // Write to sourceOnlyStreams
        writeDataOnSource(sourceOnlyStreams, NUM_WRITES, NUM_WRITES / 2);

        // Write to common streams
        writeDataOnSource(commonStreams, NUM_WRITES, NUM_WRITES / 2);

        // Write to sinkOnlyStreams
        writeDataOnSource(sinkOnlyStreams, 0, NUM_WRITES);

        verifyDataOnSink(commonStreams, NUM_WRITES + NUM_WRITES / 2);
        verifyDataOnSink(sourceOnlyStreams, NUM_WRITES);
        verifyDataOnSink(sinkOnlyStreams, 0);

        CountDownLatch statusUpdateLatch = new CountDownLatch(1);
        sinkStatusListener = new SnapshotApplyCompletionListener(statusUpdateLatch);
        corfuStoreSink.subscribeListener(sinkStatusListener, CORFU_SYSTEM_NAMESPACE, LR_STATUS_STREAM_TAG);

        // Now upgrade the source site
        openMapsAfterUpgradeSource(sourceOnlyStreams, sinkOnlyStreams);
        performRollingUpgrade(true);

        // Verify that snapshot sync was triggered by checking the completion status on the ReplicationStatus table
        // on Source
        statusUpdateLatch.await();

        // Verify that the streams only on sink prior to upgrade have data
        // and no data is lost for the common streams
        verifyDataOnSink(commonStreams, NUM_WRITES + NUM_WRITES / 2);
        verifyDataOnSink(sinkOnlyStreams, NUM_WRITES);
        verifyDataOnSink(sourceOnlyStreams, NUM_WRITES);
    }

    private void verifyInitialSnapshotSyncAfterStartup(int numTables, int numWrites) throws Exception {
        log.info(">> Setup source and sink Corfu's");
        setupSourceAndSinkCorfu();
        initSingleSourceSinkCluster();

        log.info(">> Open map(s) on source and sink");
        openMaps(numTables, false);

        log.info(">> Write data to source CorfuDB before LR is started ...");
        // Add Data for Snapshot Sync
        writeToSource(0, numWrites);

        // Confirm data does exist on Source Cluster
        for(Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> map : mapNameToMapSource.values()) {
            assertThat(map.count()).isEqualTo(numWrites);
        }

        // Confirm data does not exist on Sink Cluster
        for(Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> map : mapNameToMapSink.values()) {
            assertThat(map.count()).isEqualTo(0);
        }

        // Subscribe to replication status table on Source to capture the status of snapshot sync completion
        corfuStoreSink.openTable(CORFU_SYSTEM_NAMESPACE,
            REPLICATION_STATUS_TABLE_NAME,
            LogReplicationSession.class,
            ReplicationStatus.class,
            null,
            TableOptions.fromProtoSchema(ReplicationStatus.class));

        CountDownLatch statusUpdateLatch = new CountDownLatch(1);
        sinkStatusListener = new SnapshotApplyCompletionListener(statusUpdateLatch);
        corfuStoreSink.subscribeListener(sinkStatusListener, CORFU_SYSTEM_NAMESPACE, LR_STATUS_STREAM_TAG);

        startSourceLogReplicator();
        startSinkLogReplicator();

        log.info(">> Wait ... Snapshot log replication in progress ...");
        statusUpdateLatch.await();
        verifyDataOnSink(numWrites);

        corfuStoreSink.unsubscribeListener(sinkStatusListener);
    }

    private void verifySnapshotSyncAfterCPTrim() throws Exception {
        CountDownLatch statusUpdateLatch = new CountDownLatch(1);
        sinkStatusListener = new SnapshotApplyCompletionListener(statusUpdateLatch);
        corfuStoreSink.subscribeListener(sinkStatusListener, CORFU_SYSTEM_NAMESPACE, LR_STATUS_STREAM_TAG);

        stopSourceLogReplicator();
        checkpointAndTrim(true);
        initSingleSourceSinkCluster();
        startSourceLogReplicator();

        // Verify that snapshot sync between the different versions was successful
        log.info("Waiting for updates after CP+Trim on the Source");
        statusUpdateLatch.await();

        corfuStoreSink.unsubscribeListener(sinkStatusListener);
    }

    private void verifySnapshotSyncAfterSourceUpgrade() throws Exception {

        CountDownLatch statusUpdateLatch = new CountDownLatch(1);
        sinkStatusListener = new SnapshotApplyCompletionListener(statusUpdateLatch);
        corfuStoreSink.subscribeListener(sinkStatusListener, CORFU_SYSTEM_NAMESPACE, LR_STATUS_STREAM_TAG);

        // Upgrade the source site
        log.info(">> Upgrading the source site ...");
        performRollingUpgrade(true);

        // Verify that snapshot sync completed successfully by checking the replication status on the Source
        statusUpdateLatch.await();

        corfuStoreSink.unsubscribeListener(sinkStatusListener);
    }

    private void openMapsAfterUpgrade(List<String> sourceOnlyStreams, List<String> sinkOnlyStreams) throws Exception {
        for (String tableName : sourceOnlyStreams) {
            Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> mapSink = corfuStoreSink.openTable(
                    NAMESPACE, tableName, Sample.StringKey.class,
                    Sample.IntValueTag.class, Sample.Metadata.class,
                    TableOptions.fromProtoSchema(Sample.IntValue.class,
                            TableOptions.builder().persistentDataPath(null).build()));

            mapNameToMapSink.put(tableName, mapSink);
        }
        for (String tableName : sinkOnlyStreams) {
            Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> mapSource = corfuStoreSource.openTable(
                    NAMESPACE, tableName, Sample.StringKey.class,
                    Sample.IntValueTag.class, Sample.Metadata.class,
                    TableOptions.fromProtoSchema(Sample.IntValue.class,
                            TableOptions.builder().persistentDataPath(null).build()));

            Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> mapSink = corfuStoreSink.openTable(
                    NAMESPACE, tableName, Sample.StringKey.class,
                    Sample.IntValueTag.class, Sample.Metadata.class,
                    TableOptions.fromProtoSchema(Sample.IntValueTag.class,
                            TableOptions.builder().persistentDataPath(null).build()));

            mapNameToMapSource.put(tableName, mapSource);
            mapNameToMapSink.put(tableName, mapSink);
        }
    }

    private void openMapsAfterUpgradeSource(List<String> sourceOnlyStreams, List<String> sinkOnlyStreams) throws Exception {
        for (String tableName : sourceOnlyStreams) {
            Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> mapSource = corfuStoreSource.openTable(
                    NAMESPACE, tableName, Sample.StringKey.class,
                    Sample.IntValueTag.class, Sample.Metadata.class,
                    TableOptions.fromProtoSchema(Sample.IntValue.class,
                            TableOptions.builder().persistentDataPath(null).build()));

            mapNameToMapSource.put(tableName, mapSource);
        }
        for (String tableName : sinkOnlyStreams) {
            Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> mapSource = corfuStoreSource.openTable(
                    NAMESPACE, tableName, Sample.StringKey.class,
                    Sample.IntValueTag.class, Sample.Metadata.class,
                    TableOptions.fromProtoSchema(Sample.IntValueTag.class,
                            TableOptions.builder().persistentDataPath(null).build()));

            mapNameToMapSource.put(tableName, mapSource);
        }
    }

    private void writeDataOnSource(List<String> tableNames, int start,
                                   int NUM_WRITES) {
        int totalWrites = start + NUM_WRITES;
        for (String tableName : tableNames) {
            Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> table =
                    mapNameToMapSource.get(tableName);

            for (int i = start; i < totalWrites; i++) {
                Sample.StringKey stringKey = Sample.StringKey.newBuilder().setKey(String.valueOf(i)).build();
                Sample.IntValueTag IntValueTag = Sample.IntValueTag.newBuilder().setValue(i).build();
                Sample.Metadata metadata = Sample.Metadata.newBuilder().setMetadata("Metadata_" + i).build();
                try (TxnContext txn = corfuStoreSource.txn(NAMESPACE)) {
                    txn.putRecord(table, stringKey, IntValueTag, metadata);
                    txn.commit();
                }
            }
        }
    }

    private void verifyDataOnSink(List<String> tableNames, int expectedNumWrites) {
        for (String tableName : tableNames) {
            Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> table =
                    mapNameToMapSink.get(tableName);
            while (table.count() != expectedNumWrites) {
                // block until expected entries get replicated to Sink
                log.trace("Current table size: {}, expected entries: {}", table.count(), expectedNumWrites);
            }
            Assert.assertEquals(expectedNumWrites, table.count());

            for (int i = 0; i < expectedNumWrites; i++) {
                try (TxnContext tx = corfuStoreSink.txn(table.getNamespace())) {
                    assertThat(tx.getRecord(table,
                            Sample.StringKey.newBuilder().setKey(String.valueOf(i)).build()).getPayload()).isNotNull();
                    tx.commit();
                }
            }
        }
    }

    private void verifyNoMoreDataOnSink(int numExpectedEntries) throws Exception {
        for (int i = 0; i < FIVE; i++) {
            for (Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> map : mapNameToMapSink.values()) {
                assertThat(map.count()).isEqualTo(numExpectedEntries);
            }
            TimeUnit.MILLISECONDS.sleep(1);
        }
    }

    private void verifyNoSnapshotSync(CountDownLatch countDownLatch) throws Exception {
        long expected = countDownLatch.getCount();
        for (int i = 0; i < FIVE; i++) {
            Assert.assertEquals(expected, countDownLatch.getCount());
        }
        TimeUnit.MILLISECONDS.sleep(2);
    }

    private void performRollingUpgrade(boolean source) throws Exception {
        DefaultAdapterForUpgrade defaultAdapterForUpgrade;

        if (source) {
            stopSourceLogReplicator();
            defaultAdapterForUpgrade = new DefaultAdapterForUpgrade(sourceRuntime);
        } else {
            stopSinkLogReplicator();
            defaultAdapterForUpgrade = new DefaultAdapterForUpgrade(sinkRuntime);
        }

        defaultAdapterForUpgrade.startRollingUpgrade();

        // Change the plugin path to use the upgrade plugin so that the restart makes the node believe it is running
        // a newer version
        pluginConfigFilePath = UPGRADE_PLUGIN_PATH;

        if (source) {
            startSourceLogReplicator();
        } else {
            startSinkLogReplicator();
        }

        defaultAdapterForUpgrade.endRollingUpgrade();

        // Change the global plugin config file path to the default plugin so that subsequent restarts do not use the
        // upgrade plugin
        pluginConfigFilePath = NON_UPGRADE_PLUGIN_PATH;

        // Reset the static versions back to the initial values so that subsequent invocations do not use the stale
        // values
        defaultAdapterForUpgrade.reset();
    }

    @After
    public void tearDown() throws Exception {
        corfuStoreSink.unsubscribeListener(sinkStatusListener);
        executorService.shutdownNow();
        super.cleanUp();
    }

    private class ClearOpListenerOnUpgrade implements StreamListener {

        private CountDownLatch countDownLatch;

        ClearOpListenerOnUpgrade(CountDownLatch countdownLatch) {
            this.countDownLatch = countdownLatch;
        }

        @Override
        public void onNext(CorfuStreamEntries results) {
            results.getEntries().forEach((schema, entries) -> entries.forEach(e -> {
               if (e.getOperation() == CorfuStreamEntry.OperationType.CLEAR) {
                   countDownLatch.countDown();
               }
            }));
        }

        @Override
        public void onError(Throwable throwable) {
            fail("onError for ClearOpListenerOnUpgrade : " + throwable.toString());
        }
    }

    private class SnapshotApplyCompletionListener implements StreamListener {
        private CountDownLatch countDownLatch;

        SnapshotApplyCompletionListener(CountDownLatch countDownLatch) {
            this.countDownLatch = countDownLatch;
        }

        @Override
        public void onNext(CorfuStreamEntries results) {
            results.getEntries().forEach((schema, entries) -> entries.forEach(e -> {
                if (e.getOperation() == CorfuStreamEntry.OperationType.CLEAR) {
                    // Ignore the operation
                    return;
                }
                ReplicationStatus status = (ReplicationStatus)e.getPayload();
                if (status.getSinkStatus().getDataConsistent()) {
                    countDownLatch.countDown();
                }
            }));
        }

        @Override
        public void onError(Throwable throwable) {
            fail("onError for SnapshotApplyCompletionListener : " + throwable.toString());
        }
    }
}
