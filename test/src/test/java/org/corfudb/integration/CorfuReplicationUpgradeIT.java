package org.corfudb.integration;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata;
import org.corfudb.infrastructure.logreplication.proto.Sample;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.utils.CommonTypes;
import org.corfudb.utils.LogReplicationStreams;
import org.corfudb.utils.LogReplicationStreams.Namespace;
import org.corfudb.utils.LogReplicationStreams.TableInfo;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.infrastructure.logreplication.utils.LogReplicationConfigManager.LOG_REPLICATION_PLUGIN_VERSION_TABLE;
import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

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

    private static final String VERSION_TEST_TABLE = "VersionTestTable";

    private static final String TEST_PLUGIN_CONFIG_PATH_ACTIVE =
            "./test/src/test/resources/transport/nettyConfigUpgradeActive.properties";

    private static final String TEST_PLUGIN_CONFIG_PATH_STANDBY =
            "./test/src/test/resources/transport/nettyConfigUpgradeStandby.properties";

    private static final String SEPARATOR = "$";
    private static final String VERSION_STRING = "test_version";
    private static final String VERSION_KEY = "VERSION";
    private static final String UPGRADE_VERSION_STRING = "new_version";

    private void openVersionTables() throws Exception {
        corfuStoreActive.openTable(CORFU_SYSTEM_NAMESPACE,
                LOG_REPLICATION_PLUGIN_VERSION_TABLE, LogReplicationStreams.VersionString.class,
                LogReplicationStreams.Version.class, CommonTypes.Uuid.class, TableOptions.builder().build());

        corfuStoreStandby.openTable(CORFU_SYSTEM_NAMESPACE,
                LOG_REPLICATION_PLUGIN_VERSION_TABLE, LogReplicationStreams.VersionString.class,
                LogReplicationStreams.Version.class, CommonTypes.Uuid.class, TableOptions.builder().build());
    }

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

        Set<String> streamsToReplicate = new HashSet<>();
        for (int i = 1; i <= FIVE; i++) {
            streamsToReplicate.add(TABLE_PREFIX + i);
        }
        setupStreamsToReplicateTable(streamsToReplicate, true, false);
        setupStreamsToReplicateTable(streamsToReplicate, false, false);
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
    public void testSnapshotSyncAfterStandbyUpgraded() throws Exception {
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

        Set<String> streamsToReplicate = new HashSet<>();
        for (int i = 1; i <= FIVE; i++) {
            streamsToReplicate.add(TABLE_PREFIX + i);
        }
        setupStreamsToReplicateTable(streamsToReplicate, true, false);
        setupStreamsToReplicateTable(streamsToReplicate, false, false);
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

        Set<String> streamsToReplicate = new HashSet<>();
        for (int i = 1; i <= FIVE; i++) {
            streamsToReplicate.add(TABLE_PREFIX + i);
        }
        setupStreamsToReplicateTable(streamsToReplicate, true, false);
        setupStreamsToReplicateTable(streamsToReplicate, false, false);
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

        verifyDataOnStandby(NUM_WRITES);

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
    @SuppressWarnings("checkstyle:magicnumber")
    public void testLogEntrySyncAfterStandbyUpgradedStreamsAddedAndRemoved() throws Exception {
        log.info(">> Setup active and standby Corfu");
        setupActiveAndStandbyCorfu();

        Set<String> streamsToReplicateActive = new HashSet<>();
        for (int i = 1; i <= 2; i++) {
            streamsToReplicateActive.add(TABLE_PREFIX + i);
        }

        setupStreamsToReplicateTable(streamsToReplicateActive, true, false);
        setupStreamsToReplicateTable(streamsToReplicateActive, false, false);
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

        Set<String> streamsToReplicateStandby = new HashSet<>();
        for (int i = 2; i <= 3; i++) {
            streamsToReplicateStandby.add(TABLE_PREFIX + i);
        }
        upgradeSiteWithNewConfig(false, streamsToReplicateStandby);
        verifyVersion(corfuStoreStandby, UPGRADE_VERSION_STRING, true);
        verifyVersion(corfuStoreActive, VERSION_STRING, false);

        List<String> activeOnlyStreams = streamsToReplicateActive.stream()
                .filter(s -> !streamsToReplicateStandby.contains(s))
                .collect(Collectors.toList());

        List<String> standbyOnlyStreams = streamsToReplicateStandby.stream()
                .filter(s -> !streamsToReplicateActive.contains(s))
                .collect(Collectors.toList());

        List<String> commonStreams = streamsToReplicateActive.stream()
                .filter(streamsToReplicateStandby::contains)
                .collect(Collectors.toList());

        // Open maps corresponding to the new streams in the upgraded config
        openMapsAfterUpgrade(standbyOnlyStreams);

        // Write to activeOnlyStreams
        writeDataOnActive(activeOnlyStreams, NUM_WRITES, NUM_WRITES / 2);

        // Write to common streams
        writeDataOnActive(commonStreams, NUM_WRITES, NUM_WRITES / 2);

        // Write to standbyOnlyStreams
        writeDataOnActive(standbyOnlyStreams, 0, NUM_WRITES);

        verifyDataOnStandby(commonStreams, NUM_WRITES + NUM_WRITES / 2);
        verifyDataOnStandby(activeOnlyStreams, NUM_WRITES);
        verifyDataOnStandby(standbyOnlyStreams, 0);

        corfuStoreStandby.unsubscribeListener(snapshotSyncPluginListener);
        corfuStoreStandby.unsubscribeListener(standbyListener);
    }

    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void testSnapshotSyncAfterStandbyUpgradedStreamsAddedAndRemoved() throws Exception {
        log.info(">> Setup active and standby Corfu's");
        setupActiveAndStandbyCorfu();

        Set<String> streamsToReplicateActive = new HashSet<>();
        for (int i = 1; i <= 2; i++) {
            streamsToReplicateActive.add(TABLE_PREFIX + i);
        }
        setupStreamsToReplicateTable(streamsToReplicateActive, true, false);
        setupStreamsToReplicateTable(streamsToReplicateActive, false, false);
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
            streamsToReplicateStandby.add(TABLE_PREFIX + i);
        }
        pluginConfigFilePath = TEST_PLUGIN_CONFIG_PATH_STANDBY;
        upgradeSiteWithNewConfig(false, streamsToReplicateStandby);
        verifyVersion(corfuStoreStandby, UPGRADE_VERSION_STRING, true);
        verifyVersion(corfuStoreActive, VERSION_STRING, false);

        latchSnapshotSyncPlugin = new CountDownLatch(2);
        snapshotSyncPluginListener = new SnapshotSyncPluginListener(latchSnapshotSyncPlugin);
        subscribeToSnapshotSyncPluginTable(snapshotSyncPluginListener);

        stopActiveLogReplicator();

        List<String> activeOnlyStreams = streamsToReplicateActive.stream()
                .filter(s -> !streamsToReplicateStandby.contains(s))
                .collect(Collectors.toList());

        List<String> standbyOnlyStreams = streamsToReplicateStandby.stream()
                .filter(s -> !streamsToReplicateActive.contains(s))
                .collect(Collectors.toList());

        List<String> commonStreams = streamsToReplicateActive.stream()
                .filter(streamsToReplicateStandby::contains)
                .collect(Collectors.toList());

        // Open maps corresponding to the new streams in the upgraded config
        openMapsAfterUpgrade(standbyOnlyStreams);

        // Write data on activeOnly streams
        writeDataOnActive(activeOnlyStreams, NUM_WRITES, NUM_WRITES / 2);

        // Write data on standbyOnly streams
        writeDataOnActive(standbyOnlyStreams, 0, NUM_WRITES);

        // Write data on common streams
        writeDataOnActive(commonStreams, NUM_WRITES, NUM_WRITES / 2);

        // Trigger a snapshot sync by running a CP+trim
        checkpointAndTrim(true);
        pluginConfigFilePath = TEST_PLUGIN_CONFIG_PATH_ACTIVE;
        startActiveLogReplicator();

        // Verify that snapshot sync between the different versions was successful
        latchSnapshotSyncPlugin.await();
        statusUpdateLatch.await();
        Assert.assertEquals(TOTAL_SINK_UPDATES_INIT_SNAPSHOT_SYNC, standbyListener.getAccumulatedStatus().size());
        Assert.assertTrue(standbyListener.getAccumulatedStatus().get(4));
        Assert.assertFalse(standbyListener.getAccumulatedStatus().get(3));

        // No new data for active-only streams
        verifyDataOnStandby(activeOnlyStreams, NUM_WRITES);

        // No new data for standby-only streams
        verifyDataOnStandby(standbyOnlyStreams, 0);

        // New data present for common streams
        verifyDataOnStandby(commonStreams, NUM_WRITES + NUM_WRITES / 2);

        corfuStoreStandby.unsubscribeListener(snapshotSyncPluginListener);
        corfuStoreStandby.unsubscribeListener(standbyListener);
    }

    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void testSnapshotSyncAfterBothUpgradedStreamsAddedAndRemoved() throws Exception {
        log.info(">> Setup active and standby Corfu's");
        setupActiveAndStandbyCorfu();

        Set<String> streamsToReplicateActive = new HashSet<>();
        for (int i = 1; i <= 2; i++) {
            streamsToReplicateActive.add(TABLE_PREFIX + i);
        }

        setupStreamsToReplicateTable(streamsToReplicateActive, true, false);
        setupStreamsToReplicateTable(streamsToReplicateActive, false, false);
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
        for (Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> map : mapNameToMapActive.values()) {
            assertThat(map.count()).isEqualTo(NUM_WRITES);
        }

        // Confirm data does not exist on Standby Cluster
        for (Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> map : mapNameToMapStandby.values()) {
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

        Set<String> streamsToReplicateStandby = new HashSet<>();
        for (int i = 2; i <= 3; i++) {
            streamsToReplicateStandby.add(TABLE_PREFIX + i);
        }
        pluginConfigFilePath = TEST_PLUGIN_CONFIG_PATH_STANDBY;
        upgradeSiteWithNewConfig(false, streamsToReplicateStandby);
        verifyVersion(corfuStoreStandby, UPGRADE_VERSION_STRING, true);
        verifyVersion(corfuStoreActive, VERSION_STRING, false);

        List<String> activeOnlyStreams = streamsToReplicateActive.stream()
                .filter(s -> !streamsToReplicateStandby.contains(s))
                .collect(Collectors.toList());

        List<String> standbyOnlyStreams = streamsToReplicateStandby.stream()
                .filter(s -> !streamsToReplicateActive.contains(s))
                .collect(Collectors.toList());

        List<String> commonStreams = streamsToReplicateActive.stream()
                .filter(streamsToReplicateStandby::contains)
                .collect(Collectors.toList());

        // Open maps corresponding to the new streams in the upgraded config
        openMapsAfterUpgrade(standbyOnlyStreams);

        // Write to activeOnlyStreams
        writeDataOnActive(activeOnlyStreams, NUM_WRITES, NUM_WRITES / 2);

        // Write to common streams
        writeDataOnActive(commonStreams, NUM_WRITES, NUM_WRITES / 2);

        // Write to standbyOnlyStreams
        writeDataOnActive(standbyOnlyStreams, 0, NUM_WRITES);

        verifyDataOnStandby(commonStreams, NUM_WRITES + NUM_WRITES / 2);
        verifyDataOnStandby(activeOnlyStreams, NUM_WRITES);
        verifyDataOnStandby(standbyOnlyStreams, 0);

        corfuStoreStandby.unsubscribeListener(standbyListener);
        corfuStoreStandby.unsubscribeListener(snapshotSyncPluginListener);

        latchSnapshotSyncPlugin = new CountDownLatch(2);
        snapshotSyncPluginListener = new SnapshotSyncPluginListener(latchSnapshotSyncPlugin);
        subscribeToSnapshotSyncPluginTable(snapshotSyncPluginListener);

        statusUpdateLatch = new CountDownLatch(NUM_SNAPSHOT_SYNC_UPDATES_ON_SINK_STATUS_TABLE);
        standbyListener = new ReplicationStatusListener(statusUpdateLatch, false);
        corfuStoreStandby.subscribeListener(standbyListener, LogReplicationMetadataManager.NAMESPACE,
                LogReplicationMetadataManager.LR_STATUS_STREAM_TAG);


        // Now upgrade the active site
        pluginConfigFilePath = TEST_PLUGIN_CONFIG_PATH_ACTIVE;
        upgradeSiteWithNewConfig(true, streamsToReplicateStandby);
        verifyVersion(corfuStoreStandby, UPGRADE_VERSION_STRING, true);
        verifyVersion(corfuStoreActive, UPGRADE_VERSION_STRING, true);

        // Verify that snapshot sync was triggered by checking the number of
        // updates to the ReplicationStatus table on the standby.
        latchSnapshotSyncPlugin.await();
        validateSnapshotSyncPlugin(snapshotSyncPluginListener);
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

    private void setupStreamsToReplicateTable(Set<String> streamsToReplicate,
                                              boolean active, boolean clear) throws Exception {

        CorfuStore corfuStore;
        Table<TableInfo, Namespace, CommonTypes.Uuid> streamsNameTable;
        if (active) {
            corfuStore = corfuStoreActive;
        } else {
            corfuStore = corfuStoreStandby;
        }

        if (clear) {
            corfuStore.deleteTable(NAMESPACE, STREAMS_TEST_TABLE);
        }

        streamsNameTable = corfuStore.openTable(NAMESPACE, STREAMS_TEST_TABLE,
                TableInfo.class, Namespace.class, CommonTypes.Uuid.class,
                TableOptions.builder().build());

        try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
            for (String stream : streamsToReplicate) {
                TableInfo tableInfo = TableInfo.newBuilder().setName(NAMESPACE + SEPARATOR + stream).build();
                Namespace namespace = Namespace.newBuilder().setName(NAMESPACE).build();
                txn.putRecord(streamsNameTable, tableInfo, namespace, null);
            }
            txn.commit();
        }
    }

    private void upgradeSiteWithNewConfig(boolean active,
                                          Set<String> streamsToReplicate) throws Exception {
        setupStreamsToReplicateTable(streamsToReplicate, active, true);
        upgradeSite(active, active ? corfuStoreActive : corfuStoreStandby);
    }

    private void upgradeSite(boolean active, CorfuStore corfuStore) throws Exception {
        if (active) {
            stopActiveLogReplicator();
        } else {
            stopStandbyLogReplicator();
        }

        // Write a new version to the plugin version table so that an upgrade
        // is detected
        setupVersionTable(corfuStore, true);

        if (active) {
            startActiveLogReplicator();
        } else {
            startStandbyLogReplicator();
        }
    }

    private void setupVersionTable(CorfuStore corfuStore, boolean upgrade)
            throws Exception {

        String versionString = VERSION_STRING;
        if (upgrade) {
            versionString = UPGRADE_VERSION_STRING;
        }
        Table<LogReplicationStreams.VersionString,
                LogReplicationStreams.Version, CommonTypes.Uuid>
                pluginVersionTable = corfuStore.openTable(NAMESPACE,
                VERSION_TEST_TABLE, LogReplicationStreams.VersionString.class,
                LogReplicationStreams.Version.class, CommonTypes.Uuid.class,
                TableOptions.builder().build());

        LogReplicationStreams.VersionString versionStringKey =
                LogReplicationStreams.VersionString.newBuilder()
                        .setName(VERSION_KEY).build();

        LogReplicationStreams.Version version = LogReplicationStreams.Version.newBuilder()
                .setVersion(versionString).build();
        try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
            log.info("Putting version {}", version);
            txn.putRecord(pluginVersionTable, versionStringKey, version, null);
            txn.commit();
        }
    }

    private void openMapsAfterUpgrade(List<String> tableNames) throws Exception {
        for (String tableName : tableNames) {
            Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> mapActive = corfuStoreActive.openTable(
                    NAMESPACE, tableName, Sample.StringKey.class,
                    Sample.IntValueTag.class, Sample.Metadata.class,
                    TableOptions.fromProtoSchema(Sample.IntValueTag.class,
                            TableOptions.builder().persistentDataPath(null).build()));

            Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> mapStandby = corfuStoreStandby.openTable(
                    NAMESPACE, tableName, Sample.StringKey.class,
                    Sample.IntValueTag.class, Sample.Metadata.class,
                    TableOptions.fromProtoSchema(Sample.IntValueTag.class,
                            TableOptions.builder().persistentDataPath(null).build()));

            mapNameToMapActive.put(tableName, mapActive);
            mapNameToMapStandby.put(tableName, mapStandby);
        }
    }

    private void writeDataOnActive(List<String> tableNames, int start,
                                   int NUM_WRITES) {
        int totalWrites = start + NUM_WRITES;
        for (String tableName : tableNames) {
            Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> table =
                    mapNameToMapActive.get(tableName);

            for (int i = start; i < totalWrites; i++) {
                Sample.StringKey stringKey = Sample.StringKey.newBuilder().setKey(String.valueOf(i)).build();
                Sample.IntValueTag IntValueTag = Sample.IntValueTag.newBuilder().setValue(i).build();
                Sample.Metadata metadata = Sample.Metadata.newBuilder().setMetadata("Metadata_" + i).build();
                try (TxnContext txn = corfuStoreActive.txn(NAMESPACE)) {
                    txn.putRecord(table, stringKey, IntValueTag, metadata);
                    txn.commit();
                }
            }
        }
    }

    @SuppressWarnings("checkstyle:magicnumber")
    private void verifyDataOnStandby(List<String> tableNames, int expectedNumWrites) {
        for (String tableName : tableNames) {
            Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> table =
                    mapNameToMapStandby.get(tableName);
            while (table.count() != expectedNumWrites) {
                // block until expected entries get replicated to Standby
                log.trace("Current table size: {}, expected entries: {}", table.count(), expectedNumWrites);
            }
            Assert.assertEquals(expectedNumWrites, table.count());

            for (int i = 0; i < expectedNumWrites; i++) {
                try (TxnContext tx = corfuStoreStandby.txn(table.getNamespace())) {
                    assertThat(tx.getRecord(table,
                            Sample.StringKey.newBuilder().setKey(String.valueOf(i)).build()).getPayload()).isNotNull();
                    tx.commit();
                }
            }
        }
    }

    private void verifyVersion(CorfuStore corfuStore, String expectedVersion, boolean isUpgraded) throws Exception {
        openVersionTables();
        LogReplicationStreams.VersionString versionStringKey =
                LogReplicationStreams.VersionString.newBuilder()
                        .setName(VERSION_KEY).build();

        String actualVersion = "";
        boolean actualUpgradedFlag = false;

        while (!Objects.equals(expectedVersion, actualVersion)) {
            try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                if (txn.getRecord(LOG_REPLICATION_PLUGIN_VERSION_TABLE,
                        versionStringKey) != null && txn.getRecord(LOG_REPLICATION_PLUGIN_VERSION_TABLE,
                        versionStringKey).getPayload() != null) {
                    LogReplicationStreams.Version version = (LogReplicationStreams.Version)
                            txn.getRecord(LOG_REPLICATION_PLUGIN_VERSION_TABLE,
                                    versionStringKey).getPayload();
                    actualVersion = version.getVersion();
                    actualUpgradedFlag = version.getIsUpgraded();
                }
                txn.commit();
            }
        }
        Assert.assertEquals(expectedVersion, actualVersion);
        Assert.assertEquals(isUpgraded, actualUpgradedFlag);
        log.info("Verified version");
    }
}
