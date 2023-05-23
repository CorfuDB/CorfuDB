package org.corfudb.integration;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultAdapterForUpgrade;
import org.corfudb.infrastructure.logreplication.proto.Sample;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.corfudb.runtime.LogReplication.ReplicationStatus;
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

    // The number of updates on the ReplicationStatus table on the Sink during initial startup is 1(replication
    // status corresponding to each session is initialized in a single transaction)
    private static final int NUM_INIT_UPDATES_ON_SINK_STATUS_TABLE = 1;

    // The number of subsequent updates on the ReplicationStatus Table
    // (1) When starting snapshot sync apply : is_data_consistent = false
    // (2) When completing snapshot sync apply : is_data_consistent = true
    private static final int NUM_SNAPSHOT_SYNC_UPDATES_ON_SINK_STATUS_TABLE = 2;

    // The number of transactions on the ReplicationStatus table on the Sink after a Snapshot Sync following a
    // restart or init
    private static final int TOTAL_SINK_STATUS_TX_INIT_SNAPSHOT_SYNC = NUM_INIT_UPDATES_ON_SINK_STATUS_TABLE +
        NUM_SNAPSHOT_SYNC_UPDATES_ON_SINK_STATUS_TABLE;

    // The total number of updates made to the ReplicationStatus table on the Sink after a Snapshot Sync following a
    // restart or init.  1 update for each remote source cluster on init + NUM_SNAPSHOT_SYNC_UPDATES_ON_SINK_STATUS_TABLE
    private static final int TOTAL_SINK_STATUS_ENTRIES_INIT_SNAPSHOT_SYNC =
        1 + NUM_SNAPSHOT_SYNC_UPDATES_ON_SINK_STATUS_TABLE;

    private static final String UPGRADE_PLUGIN_PATH =
            "./test/src/test/resources/transport/pluginConfigForUpgrade.properties";

    private static final String NON_UPGRADE_PLUGIN_PATH =
            "./test/src/test/resources/transport/pluginConfig.properties";

    private SnapshotSyncPluginListener snapshotSyncPluginListener;

    private ReplicationStatusListener sinkListener;

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
     *  b) write some data on the Source cluster
     *  c) start the Sink cluster in a state where node and cluster versions are different, i.e., rolling upgrade in
     *  progress
     *  d) Verify no data written in b) is observed on the Sink as it is paused
     *  e) End rolling upgrade using the upgrade test plugin
     *  f) Verify that the Sink cluster now has the data from b)
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
        writeToSource(NUM_WRITES, NUM_WRITES/2);
        DefaultAdapterForUpgrade defaultAdapterForUpgrade = new DefaultAdapterForUpgrade(sinkRuntime);
        defaultAdapterForUpgrade.startRollingUpgrade();
        startSinkLogReplicator();

        // Verify that the new writes were not received as Sink LR has not started
        verifyNoMoreDataOnSink(NUM_WRITES);

        // End the rolling upgrade
        defaultAdapterForUpgrade.endRollingUpgrade();
        verifyDataOnSink(NUM_WRITES + NUM_WRITES/2);

        defaultAdapterForUpgrade.reset();

        // Simulate a rolling upgrade on Source
        stopSourceLogReplicator();
        defaultAdapterForUpgrade = new DefaultAdapterForUpgrade(sourceRuntime);
        defaultAdapterForUpgrade.startRollingUpgrade();

        // Two updates are expected onStart of snapshot sync and onEnd.
        CountDownLatch latchSnapshotSyncPlugin = new CountDownLatch(2);
        snapshotSyncPluginListener = new SnapshotSyncPluginListener(latchSnapshotSyncPlugin);
        subscribeToSnapshotSyncPluginTable(snapshotSyncPluginListener);

        CountDownLatch statusUpdateLatch = new CountDownLatch(NUM_SNAPSHOT_SYNC_UPDATES_ON_SINK_STATUS_TABLE);
        sinkListener = new ReplicationStatusListener(statusUpdateLatch, false);
        corfuStoreSink.subscribeListener(sinkListener, CORFU_SYSTEM_NAMESPACE, LR_STATUS_STREAM_TAG);
        startSourceLogReplicator();

        verifyNoSnapshotSync(statusUpdateLatch);

        // End the rolling upgrade
        defaultAdapterForUpgrade.endRollingUpgrade();

        // Verify that a forced snapshot sync gets triggered
        latchSnapshotSyncPlugin.await();
        statusUpdateLatch.await();
        Assert.assertEquals(NUM_SNAPSHOT_SYNC_UPDATES_ON_SINK_STATUS_TABLE, sinkListener.getAccumulatedStatus().size());
        Assert.assertTrue(sinkListener.getAccumulatedStatus().get(1));
        Assert.assertFalse(sinkListener.getAccumulatedStatus().get(0));
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

        // Open maps corresponding to the new streams in the upgraded config
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
        Set<String> streamsToReplicateSink = new HashSet<>();
        for (int i = 2; i <= 3; i++) {
            streamsToReplicateSink.add(TABLE_PREFIX + i);
        }
        performRollingUpgrade(false);


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

        CountDownLatch latchSnapshotSyncPlugin = new CountDownLatch(2);
        snapshotSyncPluginListener = new SnapshotSyncPluginListener(latchSnapshotSyncPlugin);
        subscribeToSnapshotSyncPluginTable(snapshotSyncPluginListener);

        CountDownLatch statusUpdateLatch = new CountDownLatch(NUM_SNAPSHOT_SYNC_UPDATES_ON_SINK_STATUS_TABLE);
        sinkListener = new ReplicationStatusListener(statusUpdateLatch, false);
        corfuStoreSink.subscribeListener(sinkListener, CORFU_SYSTEM_NAMESPACE, LR_STATUS_STREAM_TAG);

        // Trigger a snapshot sync by running a CP+trim
        checkpointAndTrim(true);
        startSourceLogReplicator();
        initSingleSourceSinkCluster();

        // Verify that snapshot sync between the different versions was successful
        latchSnapshotSyncPlugin.await();
        statusUpdateLatch.await();
        Assert.assertEquals(NUM_SNAPSHOT_SYNC_UPDATES_ON_SINK_STATUS_TABLE, sinkListener.getAccumulatedStatus().size());
        Assert.assertTrue(sinkListener.getAccumulatedStatus().get(1));
        Assert.assertFalse(sinkListener.getAccumulatedStatus().get(0));

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

        corfuStoreSink.unsubscribeListener(sinkListener);
        corfuStoreSink.unsubscribeListener(snapshotSyncPluginListener);

        CountDownLatch latchSnapshotSyncPlugin = new CountDownLatch(2);
        snapshotSyncPluginListener = new SnapshotSyncPluginListener(latchSnapshotSyncPlugin);
        subscribeToSnapshotSyncPluginTable(snapshotSyncPluginListener);

        CountDownLatch statusUpdateLatch = new CountDownLatch(NUM_SNAPSHOT_SYNC_UPDATES_ON_SINK_STATUS_TABLE);
        sinkListener = new ReplicationStatusListener(statusUpdateLatch, false);
        corfuStoreSink.subscribeListener(sinkListener, CORFU_SYSTEM_NAMESPACE, LR_STATUS_STREAM_TAG);

    private void verifySnapshotSyncAfterCPTrim() throws Exception {
        CountDownLatch statusUpdateLatch = new CountDownLatch(1);
        sinkStatusListener = new SnapshotApplyCompletionListener(statusUpdateLatch);
        corfuStoreSink.subscribeListener(sinkStatusListener, CORFU_SYSTEM_NAMESPACE, LR_STATUS_STREAM_TAG);

        // Now upgrade the source site
        openMapsAfterUpgradeSource(sourceOnlyStreams, sinkOnlyStreams);
        performRollingUpgrade(true);

        // Verify that snapshot sync was triggered by checking the number of
        // updates to the ReplicationStatus table on the sink.
        latchSnapshotSyncPlugin.await();
        validateSnapshotSyncPlugin(snapshotSyncPluginListener);
        statusUpdateLatch.await();

        Assert.assertEquals(NUM_SNAPSHOT_SYNC_UPDATES_ON_SINK_STATUS_TABLE, sinkListener.getAccumulatedStatus().size());
        Assert.assertTrue(sinkListener.getAccumulatedStatus().get(1));
        Assert.assertFalse(sinkListener.getAccumulatedStatus().get(0));

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

        // Two updates are expected onStart of snapshot sync and onEnd.
        CountDownLatch latchSnapshotSyncPlugin = new CountDownLatch(2);
        snapshotSyncPluginListener = new SnapshotSyncPluginListener(latchSnapshotSyncPlugin);
        subscribeToSnapshotSyncPluginTable(snapshotSyncPluginListener);

        // Subscribe to replication status table on Sink (to be sure data change on status are captured)
        corfuStoreSink.openTable(CORFU_SYSTEM_NAMESPACE,
            REPLICATION_STATUS_TABLE_NAME,
            LogReplicationSession.class,
            ReplicationStatus.class,
            null,
            TableOptions.fromProtoSchema(ReplicationStatus.class));

        CountDownLatch statusUpdateLatch = new CountDownLatch(TOTAL_SINK_STATUS_TX_INIT_SNAPSHOT_SYNC);
        sinkListener = new ReplicationStatusListener(statusUpdateLatch, false);
        corfuStoreSink.subscribeListener(sinkListener, CORFU_SYSTEM_NAMESPACE, LR_STATUS_STREAM_TAG);

        startSourceLogReplicator();
        startSinkLogReplicator();

        log.info(">> Wait ... Snapshot log replication in progress ...");
        verifyDataOnSink(numWrites);

        // Verify that snapshot sync was triggered by checking the number of
        // updates to the ReplicationStatus table on the sink.
        latchSnapshotSyncPlugin.await();
        log.info("Verified Snapshot Sync plugin updates");
        validateSnapshotSyncPlugin(snapshotSyncPluginListener);
        statusUpdateLatch.await();
        log.info("Verified Status updates");

        Assert.assertEquals(TOTAL_SINK_STATUS_ENTRIES_INIT_SNAPSHOT_SYNC, sinkListener.getAccumulatedStatus().size());
        Assert.assertTrue(sinkListener.getAccumulatedStatus().get(2));
        Assert.assertFalse(sinkListener.getAccumulatedStatus().get(1));

        corfuStoreSink.unsubscribeListener(snapshotSyncPluginListener);
        corfuStoreSink.unsubscribeListener(sinkListener);
    }

    private void verifySnapshotSyncAfterCPTrim() throws Exception {
        CountDownLatch statusUpdateLatch = new CountDownLatch(NUM_SNAPSHOT_SYNC_UPDATES_ON_SINK_STATUS_TABLE);
        sinkListener = new ReplicationStatusListener(statusUpdateLatch, false);
        corfuStoreSink.subscribeListener(sinkListener, CORFU_SYSTEM_NAMESPACE, LR_STATUS_STREAM_TAG);

        CountDownLatch latchSnapshotSyncPlugin = new CountDownLatch(2);
        snapshotSyncPluginListener = new SnapshotSyncPluginListener(latchSnapshotSyncPlugin);
        subscribeToSnapshotSyncPluginTable(snapshotSyncPluginListener);

        stopSourceLogReplicator();
        checkpointAndTrim(true);
        initSingleSourceSinkCluster();
        startSourceLogReplicator();

        // Verify that snapshot sync between the different versions was successful
        log.info("Waiting for updates after CP+Trim on the Source");
        latchSnapshotSyncPlugin.await();
        statusUpdateLatch.await();
        Assert.assertEquals(NUM_SNAPSHOT_SYNC_UPDATES_ON_SINK_STATUS_TABLE, sinkListener.getAccumulatedStatus().size());
        Assert.assertTrue(sinkListener.getAccumulatedStatus().get(1));
        Assert.assertFalse(sinkListener.getAccumulatedStatus().get(0));

        corfuStoreSink.unsubscribeListener(snapshotSyncPluginListener);
        corfuStoreSink.unsubscribeListener(sinkListener);
    }

    private void verifySnapshotSyncAfterSourceUpgrade() throws Exception {
        CountDownLatch latchSnapshotSyncPlugin = new CountDownLatch(2);
        snapshotSyncPluginListener = new SnapshotSyncPluginListener(latchSnapshotSyncPlugin);
        subscribeToSnapshotSyncPluginTable(snapshotSyncPluginListener);

        CountDownLatch statusUpdateLatch = new CountDownLatch(NUM_SNAPSHOT_SYNC_UPDATES_ON_SINK_STATUS_TABLE);
        sinkListener = new ReplicationStatusListener(statusUpdateLatch, false);
        corfuStoreSink.subscribeListener(sinkListener, CORFU_SYSTEM_NAMESPACE, LR_STATUS_STREAM_TAG);

        // Upgrade the source site
        log.info(">> Upgrading the source site ...");
        performRollingUpgrade(true);

        // Verify that snapshot sync was triggered by checking the number of
        // updates to the ReplicationStatus table on the sink.
        latchSnapshotSyncPlugin.await();
        validateSnapshotSyncPlugin(snapshotSyncPluginListener);
        statusUpdateLatch.await();

        Assert.assertEquals(NUM_SNAPSHOT_SYNC_UPDATES_ON_SINK_STATUS_TABLE, sinkListener.getAccumulatedStatus().size());
        Assert.assertTrue(sinkListener.getAccumulatedStatus().get(1));
        Assert.assertFalse(sinkListener.getAccumulatedStatus().get(0));

        corfuStoreSink.unsubscribeListener(snapshotSyncPluginListener);
        corfuStoreSink.unsubscribeListener(sinkListener);
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
    public void tearDown() {
        corfuStoreSink.unsubscribeListener(snapshotSyncPluginListener);
        corfuStoreSink.unsubscribeListener(sinkListener);
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
