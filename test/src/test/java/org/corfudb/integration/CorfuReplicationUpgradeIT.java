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

    // The number of updates on the ReplicationStatus table on the Sink during initial startup is 3(one for each
    // Source cluster)
    private static final int NUM_INIT_UPDATES_ON_SINK_STATUS_TABLE = 3;

    // The number of subsequent updates on the ReplicationStatus Table
    // (1) When starting snapshot sync apply : is_data_consistent = false
    // (2) When completing snapshot sync apply : is_data_consistent = true
    private static final int NUM_SNAPSHOT_SYNC_UPDATES_ON_SINK_STATUS_TABLE = 2;

    // The number of updates on the ReplicationStatus table on the Sink after a Snapshot Sync following a restart
    private static final int TOTAL_SINK_UPDATES_INIT_SNAPSHOT_SYNC = NUM_INIT_UPDATES_ON_SINK_STATUS_TABLE +
        NUM_SNAPSHOT_SYNC_UPDATES_ON_SINK_STATUS_TABLE;

    private static final String STREAMS_TEST_TABLE =
            "StreamsToReplicateTestTable";

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
    public void testLogEntrySyncAfterStandbyUpgraded() throws Exception {
        log.info(">> Setup active and standby Corfu's");
        setupActiveAndStandbyCorfu();

        log.info(">> Open map(s) on active and standby");
        openMaps(FIVE, false);

        log.info(">> Write data to active CorfuDB before LR is started ...");
        // Add Data for Snapshot Sync
        writeToActive(0, NUM_WRITES);

        // Confirm data does exist on Active Cluster
        for(Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> map : mapNameToMapActive.values()) {
            assertThat(map.count()).isEqualTo(NUM_WRITES);
        }

        // Confirm data does not exist on Standby Cluster
        for(Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> map : mapNameToMapStandby.values()) {
            assertThat(map.count()).isEqualTo(0);
        }

        // Two updates are expected onStart of snapshot sync and onEnd.
        CountDownLatch latchSnapshotSyncPlugin = new CountDownLatch(2);
        SnapshotSyncPluginListener snapshotSyncPluginListener = new SnapshotSyncPluginListener(latchSnapshotSyncPlugin);
        subscribeToSnapshotSyncPluginTable(snapshotSyncPluginListener);

        // Subscribe to replication status table on Standby (to be sure data change on status are captured)
        corfuStoreStandby.openTable(LogReplicationMetadataManager.NAMESPACE,
                LogReplicationMetadataManager.REPLICATION_STATUS_TABLE,
                LogReplicationMetadata.ReplicationStatusKey.class,
                LogReplicationMetadata.ReplicationStatusVal.class,
                null,
                TableOptions.fromProtoSchema(LogReplicationMetadata.ReplicationStatusVal.class));

        CountDownLatch statusUpdateLatch = new CountDownLatch(TOTAL_SINK_UPDATES_INIT_SNAPSHOT_SYNC);
        ReplicationStatusListener standbyListener = new ReplicationStatusListener(statusUpdateLatch, false);
        corfuStoreStandby.subscribeListener(standbyListener, LogReplicationMetadataManager.NAMESPACE,
                LogReplicationMetadataManager.LR_STATUS_STREAM_TAG);

        setupVersionTable(corfuStoreActive, false);
        setupVersionTable(corfuStoreStandby, false);

        pluginConfigFilePath = TEST_PLUGIN_CONFIG_PATH_ACTIVE;
        startActiveLogReplicator();

        pluginConfigFilePath = TEST_PLUGIN_CONFIG_PATH_STANDBY;
        startStandbyLogReplicator();

        log.info(">> Wait ... Snapshot log replication in progress ...");
        verifyDataOnStandby(NUM_WRITES);

        // Verify that snapshot sync was triggered by checking the number of
        // updates to the ReplicationStatus table on the standby.
        latchSnapshotSyncPlugin.await();
        validateSnapshotSyncPlugin(snapshotSyncPluginListener);
        statusUpdateLatch.await();

        Assert.assertEquals(TOTAL_SINK_UPDATES_INIT_SNAPSHOT_SYNC, standbyListener.getAccumulatedStatus().size());
        Assert.assertTrue(standbyListener.getAccumulatedStatus().get(4));
        Assert.assertFalse(standbyListener.getAccumulatedStatus().get(3));

        // Upgrade the standby site
        log.info(">> Upgrading the standby site ...");
        pluginConfigFilePath = TEST_PLUGIN_CONFIG_PATH_STANDBY;
        upgradeSite(false, corfuStoreStandby);
        verifyVersion(corfuStoreStandby, UPGRADE_VERSION_STRING, true);
        verifyVersion(corfuStoreActive, VERSION_STRING, false);

        // Verify that subsequent log entry sync is successful
        log.info("Write more data on the active");
        writeToActive(NUM_WRITES, NUM_WRITES / 2);

        log.info("Verify that data is replicated on the standby after it is upgraded");
        verifyDataOnStandby(NUM_WRITES + (NUM_WRITES / 2));

        corfuStoreStandby.unsubscribeListener(snapshotSyncPluginListener);
        corfuStoreStandby.unsubscribeListener(standbyListener);
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

    @Test
    public void testLogEntrySyncAfterSinkUpgraded() throws Exception {

        verifyInitialSnapshotSyncAfterStartup(FIVE, NUM_WRITES);

        // Upgrade the sink site
        log.info(">> Upgrading the sink site ...");
        performRollingUpgrade(false);

        // Subscribe to replication status table on Standby (to be sure data change on status are captured)
        corfuStoreStandby.openTable(LogReplicationMetadataManager.NAMESPACE,
                LogReplicationMetadataManager.REPLICATION_STATUS_TABLE,
                LogReplicationMetadata.ReplicationStatusKey.class,
                LogReplicationMetadata.ReplicationStatusVal.class,
                null,
                TableOptions.fromProtoSchema(LogReplicationMetadata.ReplicationStatusVal.class));

        CountDownLatch statusUpdateLatch = new CountDownLatch(TOTAL_SINK_UPDATES_INIT_SNAPSHOT_SYNC);
        ReplicationStatusListener standbyListener = new ReplicationStatusListener(statusUpdateLatch, false);
        corfuStoreStandby.subscribeListener(standbyListener, LogReplicationMetadataManager.NAMESPACE,
                LogReplicationMetadataManager.LR_STATUS_STREAM_TAG);

        setupVersionTable(corfuStoreActive, false);
        setupVersionTable(corfuStoreStandby, false);

        pluginConfigFilePath = TEST_PLUGIN_CONFIG_PATH_ACTIVE;
        startActiveLogReplicator();

        pluginConfigFilePath = TEST_PLUGIN_CONFIG_PATH_STANDBY;
        startStandbyLogReplicator();

        log.info(">> Wait ... Snapshot log replication in progress ...");
        verifyDataOnStandby(NUM_WRITES);

        // Verify that snapshot sync was triggered by checking the number of
        // updates to the ReplicationStatus table on the standby.
        latchSnapshotSyncPlugin.await();
        validateSnapshotSyncPlugin(snapshotSyncPluginListener);
        statusUpdateLatch.await();

        Assert.assertEquals(TOTAL_SINK_UPDATES_INIT_SNAPSHOT_SYNC, standbyListener.getAccumulatedStatus().size());
        Assert.assertTrue(standbyListener.getAccumulatedStatus().get(4));
        Assert.assertFalse(standbyListener.getAccumulatedStatus().get(3));
        corfuStoreStandby.unsubscribeListener(standbyListener);
        corfuStoreStandby.unsubscribeListener(snapshotSyncPluginListener);

        // Upgrade the standby site

        statusUpdateLatch = new CountDownLatch(TOTAL_SINK_UPDATES_INIT_SNAPSHOT_SYNC);
        standbyListener = new ReplicationStatusListener(statusUpdateLatch, false);
        corfuStoreStandby.subscribeListener(standbyListener, LogReplicationMetadataManager.NAMESPACE,
            LogReplicationMetadataManager.LR_STATUS_STREAM_TAG);

        log.info(">> Upgrading the standby site ...");
        pluginConfigFilePath = TEST_PLUGIN_CONFIG_PATH_STANDBY;
        upgradeSite(false, corfuStoreStandby);
        verifyVersion(corfuStoreStandby, UPGRADE_VERSION_STRING, true);
        verifyVersion(corfuStoreActive, VERSION_STRING, false);

        latchSnapshotSyncPlugin = new CountDownLatch(2);
        snapshotSyncPluginListener = new SnapshotSyncPluginListener(latchSnapshotSyncPlugin);
        subscribeToSnapshotSyncPluginTable(snapshotSyncPluginListener);

        // Trigger a snapshot sync by stopping the active LR and running a CP+trim
        stopActiveLogReplicator();
        checkpointAndTrim(true);
        pluginConfigFilePath = TEST_PLUGIN_CONFIG_PATH_ACTIVE;
        startActiveLogReplicator();

        // Verify that snapshot sync between the different versions was successful
        latchSnapshotSyncPlugin.await();
        statusUpdateLatch.await();
        Assert.assertEquals(TOTAL_SINK_UPDATES_INIT_SNAPSHOT_SYNC, standbyListener.getAccumulatedStatus().size());
        Assert.assertTrue(standbyListener.getAccumulatedStatus().get(4));
        Assert.assertFalse(standbyListener.getAccumulatedStatus().get(3));
        verifyDataOnStandby(NUM_WRITES);

        corfuStoreStandby.unsubscribeListener(snapshotSyncPluginListener);
        corfuStoreStandby.unsubscribeListener(standbyListener);
        executorService.shutdownNow();

        log.info("Verify that data is replicated on the sink after it is upgraded");
        verifyDataOnSink(NUM_WRITES + (NUM_WRITES / 2));
    }

    @Test
    public void testSnapshotSyncAfterStandbyAndActiveUpgraded() throws Exception {
        log.info(">> Setup active and standby Corfu");
        setupActiveAndStandbyCorfu();

        // Two updates are expected onStart of snapshot sync and onEnd.
        CountDownLatch latchSnapshotSyncPlugin = new CountDownLatch(2);
        SnapshotSyncPluginListener snapshotSyncPluginListener = new SnapshotSyncPluginListener(latchSnapshotSyncPlugin);
        subscribeToSnapshotSyncPluginTable(snapshotSyncPluginListener);

        // Subscribe to replication status table on Standby (to be sure data change on status are captured)
        corfuStoreStandby.openTable(LogReplicationMetadataManager.NAMESPACE,
                LogReplicationMetadataManager.REPLICATION_STATUS_TABLE,
                LogReplicationMetadata.ReplicationStatusKey.class,
                LogReplicationMetadata.ReplicationStatusVal.class,
                null,
                TableOptions.fromProtoSchema(LogReplicationMetadata.ReplicationStatusVal.class));

        CountDownLatch statusUpdateLatch = new CountDownLatch(TOTAL_SINK_UPDATES_INIT_SNAPSHOT_SYNC);
        ReplicationStatusListener standbyListener = new ReplicationStatusListener(statusUpdateLatch, false);
        corfuStoreStandby.subscribeListener(standbyListener, LogReplicationMetadataManager.NAMESPACE,
                LogReplicationMetadataManager.LR_STATUS_STREAM_TAG);

        log.info(">> Open map(s) on active and standby");
        openMaps(FIVE, false);

        log.info(">> Write data to active CorfuDB before LR is started ...");
        // Add Data for Snapshot Sync
        writeToActive(0, NUM_WRITES);

        // Confirm data does exist on Active Cluster
        for (Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> map : mapNameToMapActive.values()) {
            assertThat(map.count()).isEqualTo(NUM_WRITES);
        }

        // Confirm data does not exist on Standby Cluster
        for (Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> map : mapNameToMapStandby.values()) {
            assertThat(map.count()).isEqualTo(0);
        }

        setupVersionTable(corfuStoreActive, false);
        setupVersionTable(corfuStoreStandby, false);

        pluginConfigFilePath = TEST_PLUGIN_CONFIG_PATH_ACTIVE;
        startActiveLogReplicator();

        // Upgrade the sink site
        log.info(">> Upgrading the sink site ...");
        performRollingUpgrade(false);

        // Trigger a snapshot sync by stopping the source LR and running a CP+trim.  Verify that snapshot sync took
        // place
        verifySnapshotSyncAfterCPTrim();

        Assert.assertEquals(TOTAL_SINK_UPDATES_INIT_SNAPSHOT_SYNC, standbyListener.getAccumulatedStatus().size());
        Assert.assertTrue(standbyListener.getAccumulatedStatus().get(4));
        Assert.assertFalse(standbyListener.getAccumulatedStatus().get(3));
        corfuStoreStandby.unsubscribeListener(standbyListener);
        corfuStoreStandby.unsubscribeListener(snapshotSyncPluginListener);

        // Upgrade the standby site first
        log.info(">> Upgrading the standby site ...");
        pluginConfigFilePath = TEST_PLUGIN_CONFIG_PATH_STANDBY;
        upgradeSite(false, corfuStoreStandby);
        verifyVersion(corfuStoreStandby, UPGRADE_VERSION_STRING, true);
        verifyVersion(corfuStoreActive, VERSION_STRING, false);
        log.info(">> Plugin config verified after standby upgrade");

        // Upgrading the active site will force a snapshot sync
        latchSnapshotSyncPlugin = new CountDownLatch(2);
        snapshotSyncPluginListener = new SnapshotSyncPluginListener(latchSnapshotSyncPlugin);
        subscribeToSnapshotSyncPluginTable(snapshotSyncPluginListener);

        statusUpdateLatch = new CountDownLatch(TOTAL_SINK_UPDATES_INIT_SNAPSHOT_SYNC);
        standbyListener = new ReplicationStatusListener(statusUpdateLatch, false);
        corfuStoreStandby.subscribeListener(standbyListener, LogReplicationMetadataManager.NAMESPACE,
                LogReplicationMetadataManager.LR_STATUS_STREAM_TAG);

        // Upgrade the active site
        log.info(">> Upgrading the active site ...");
        pluginConfigFilePath = TEST_PLUGIN_CONFIG_PATH_ACTIVE;
        upgradeSite(true, corfuStoreActive);
        verifyVersion(corfuStoreActive, UPGRADE_VERSION_STRING, true);
        verifyVersion(corfuStoreStandby, UPGRADE_VERSION_STRING, true);

        // Verify that snapshot sync was triggered by checking the number of
        // updates to the ReplicationStatus table on the standby.
        latchSnapshotSyncPlugin.await();
        validateSnapshotSyncPlugin(snapshotSyncPluginListener);
        statusUpdateLatch.await();

        Assert.assertEquals(TOTAL_SINK_UPDATES_INIT_SNAPSHOT_SYNC, standbyListener.getAccumulatedStatus().size());
        Assert.assertTrue(standbyListener.getAccumulatedStatus().get(4));
        Assert.assertFalse(standbyListener.getAccumulatedStatus().get(3));

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

        setupVersionTable(corfuStoreActive, false);
        setupVersionTable(corfuStoreStandby, false);

        // Two updates are expected onStart of snapshot sync and onEnd.
        CountDownLatch latchSnapshotSyncPlugin = new CountDownLatch(2);
        SnapshotSyncPluginListener snapshotSyncPluginListener = new SnapshotSyncPluginListener(latchSnapshotSyncPlugin);
        subscribeToSnapshotSyncPluginTable(snapshotSyncPluginListener);

        // Subscribe to replication status table on Standby (to be sure data change on status are captured)
        corfuStoreStandby.openTable(LogReplicationMetadataManager.NAMESPACE,
                LogReplicationMetadataManager.REPLICATION_STATUS_TABLE,
                LogReplicationMetadata.ReplicationStatusKey.class,
                LogReplicationMetadata.ReplicationStatusVal.class,
                null,
                TableOptions.fromProtoSchema(LogReplicationMetadata.ReplicationStatusVal.class));

        CountDownLatch statusUpdateLatch = new CountDownLatch(TOTAL_SINK_UPDATES_INIT_SNAPSHOT_SYNC);
        ReplicationStatusListener standbyListener = new ReplicationStatusListener(statusUpdateLatch, false);
        corfuStoreStandby.subscribeListener(standbyListener, LogReplicationMetadataManager.NAMESPACE,
                LogReplicationMetadataManager.LR_STATUS_STREAM_TAG);

        log.info(">> Open map(s) on active and standby");
        openMaps(2, false);

        log.info(">> Write data to active CorfuDB before LR is started ...");
        // Add Data for Snapshot Sync
        writeToActive(0, NUM_WRITES);

        // Confirm data does exist on Active Cluster
        for(Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> map : mapNameToMapActive.values()) {
            assertThat(map.count()).isEqualTo(NUM_WRITES);
        }

        // Confirm data does not exist on Standby Cluster
        for(Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> map : mapNameToMapStandby.values()) {
            assertThat(map.count()).isEqualTo(0);
        }

        pluginConfigFilePath = TEST_PLUGIN_CONFIG_PATH_ACTIVE;
        startActiveLogReplicator();

        pluginConfigFilePath = TEST_PLUGIN_CONFIG_PATH_STANDBY;
        startStandbyLogReplicator();

        log.info(">> Wait ... Snapshot log replication in progress ...");
        verifyDataOnStandby(NUM_WRITES);

        // Verify that snapshot sync was triggered by checking the number of
        // updates to the ReplicationStatus table on the standby.
        latchSnapshotSyncPlugin.await();
        validateSnapshotSyncPlugin(snapshotSyncPluginListener);
        statusUpdateLatch.await();

        Assert.assertEquals(TOTAL_SINK_UPDATES_INIT_SNAPSHOT_SYNC, standbyListener.getAccumulatedStatus().size());
        Assert.assertTrue(standbyListener.getAccumulatedStatus().get(4));
        Assert.assertFalse(standbyListener.getAccumulatedStatus().get(3));

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
            streamsToReplicateActive.add(TABLE_PREFIX + i);
        }
        setupVersionTable(corfuStoreActive, false);
        setupVersionTable(corfuStoreStandby, false);

        // Two updates are expected onStart of snapshot sync and onEnd.
        CountDownLatch latchSnapshotSyncPlugin = new CountDownLatch(2);
        SnapshotSyncPluginListener snapshotSyncPluginListener = new SnapshotSyncPluginListener(latchSnapshotSyncPlugin);
        subscribeToSnapshotSyncPluginTable(snapshotSyncPluginListener);

        // Subscribe to replication status table on Standby (to be sure data change on status are captured)
        corfuStoreStandby.openTable(LogReplicationMetadataManager.NAMESPACE,
                LogReplicationMetadataManager.REPLICATION_STATUS_TABLE,
                LogReplicationMetadata.ReplicationStatusKey.class,
                LogReplicationMetadata.ReplicationStatusVal.class,
                null,
                TableOptions.fromProtoSchema(LogReplicationMetadata.ReplicationStatusVal.class));

        CountDownLatch statusUpdateLatch = new CountDownLatch(TOTAL_SINK_UPDATES_INIT_SNAPSHOT_SYNC);
        ReplicationStatusListener standbyListener = new ReplicationStatusListener(statusUpdateLatch, false);
        corfuStoreStandby.subscribeListener(standbyListener, LogReplicationMetadataManager.NAMESPACE,
                LogReplicationMetadataManager.LR_STATUS_STREAM_TAG);

        log.info(">> Open map(s) on active and standby");
        openMaps(2, false);

        log.info(">> Write data to active CorfuDB before LR is started ...");
        // Add Data for Snapshot Sync
        writeToActive(0, NUM_WRITES);

        // Confirm data does exist on Active Cluster
        for(Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> map : mapNameToMapActive.values()) {
            assertThat(map.count()).isEqualTo(NUM_WRITES);
        }

        // Confirm data does not exist on Standby Cluster
        for(Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> map : mapNameToMapStandby.values()) {
            assertThat(map.count()).isEqualTo(0);
        }
        verifyInitialSnapshotSyncAfterStartup(streamsToReplicateSource.size(), NUM_WRITES);

        // Upgrade the sink site
        log.info(">> Upgrading the sink site ...");
        performRollingUpgrade(false);

        log.info(">> Wait ... Snapshot log replication in progress ...");
        verifyDataOnStandby(NUM_WRITES);

        // Verify that snapshot sync was triggered by checking the number of
        // updates to the ReplicationStatus table on the standby.
        latchSnapshotSyncPlugin.await();
        validateSnapshotSyncPlugin(snapshotSyncPluginListener);
        statusUpdateLatch.await();

        Assert.assertEquals(TOTAL_SINK_UPDATES_INIT_SNAPSHOT_SYNC, standbyListener.getAccumulatedStatus().size());
        Assert.assertTrue(standbyListener.getAccumulatedStatus().get(4));
        Assert.assertFalse(standbyListener.getAccumulatedStatus().get(3));
        corfuStoreStandby.unsubscribeListener(standbyListener);
        corfuStoreStandby.unsubscribeListener(snapshotSyncPluginListener);

        // Upgrade the standby site

        statusUpdateLatch = new CountDownLatch(TOTAL_SINK_UPDATES_INIT_SNAPSHOT_SYNC);
        standbyListener = new ReplicationStatusListener(statusUpdateLatch, false);
        corfuStoreStandby.subscribeListener(standbyListener, LogReplicationMetadataManager.NAMESPACE,
            LogReplicationMetadataManager.LR_STATUS_STREAM_TAG);

        log.info(">> Upgrading the standby site ...");
        Set<String> streamsToReplicateStandby = new HashSet<>();
        for (int i = 2; i <= 3; i++) {
            streamsToReplicateSink.add(TABLE_PREFIX + i);
        }

        stopActiveLogReplicator();

        List<String> activeOnlyStreams = streamsToReplicateActive.stream()
                .filter(s -> !streamsToReplicateStandby.contains(s))
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
        latchSnapshotSyncPlugin.await();
        statusUpdateLatch.await();
        Assert.assertEquals(TOTAL_SINK_UPDATES_INIT_SNAPSHOT_SYNC, standbyListener.getAccumulatedStatus().size());
        Assert.assertTrue(standbyListener.getAccumulatedStatus().get(4));
        Assert.assertFalse(standbyListener.getAccumulatedStatus().get(3));

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

        CountDownLatch statusUpdateLatch = new CountDownLatch(TOTAL_SINK_UPDATES_INIT_SNAPSHOT_SYNC);
        ReplicationStatusListener standbyListener = new ReplicationStatusListener(statusUpdateLatch, false);
        corfuStoreStandby.subscribeListener(standbyListener, LogReplicationMetadataManager.NAMESPACE,
                LogReplicationMetadataManager.LR_STATUS_STREAM_TAG);

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

        Assert.assertEquals(TOTAL_SINK_UPDATES_INIT_SNAPSHOT_SYNC, standbyListener.getAccumulatedStatus().size());
        Assert.assertTrue(standbyListener.getAccumulatedStatus().get(4));
        Assert.assertFalse(standbyListener.getAccumulatedStatus().get(3));

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

        statusUpdateLatch = new CountDownLatch(NUM_SNAPSHOT_SYNC_UPDATES_ON_SINK_STATUS_TABLE);
        standbyListener = new ReplicationStatusListener(statusUpdateLatch, false);
        corfuStoreStandby.subscribeListener(standbyListener, LogReplicationMetadataManager.NAMESPACE,
                LogReplicationMetadataManager.LR_STATUS_STREAM_TAG);

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

        Assert.assertEquals(NUM_SNAPSHOT_SYNC_UPDATES_ON_SINK_STATUS_TABLE, standbyListener.getAccumulatedStatus().size());
        Assert.assertTrue(standbyListener.getAccumulatedStatus().get(1));
        Assert.assertFalse(standbyListener.getAccumulatedStatus().get(0));

        // Verify that the streams only on standby prior to upgrade have data
        // and no data is lost for the common streams
        verifyDataOnStandby(commonStreams, NUM_WRITES + NUM_WRITES / 2);
        verifyDataOnStandby(standbyOnlyStreams, NUM_WRITES);
        verifyDataOnStandby(activeOnlyStreams, NUM_WRITES);

        corfuStoreStandby.unsubscribeListener(snapshotSyncPluginListener);
        corfuStoreStandby.unsubscribeListener(standbyListener);
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
            defaultAdapterForUpgrade = new DefaultAdapterForUpgrade();
            defaultAdapterForUpgrade.openVersionTable(sourceRuntime);
        } else {
            stopSinkLogReplicator();
            defaultAdapterForUpgrade = new DefaultAdapterForUpgrade();
            defaultAdapterForUpgrade.openVersionTable(sinkRuntime);
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
