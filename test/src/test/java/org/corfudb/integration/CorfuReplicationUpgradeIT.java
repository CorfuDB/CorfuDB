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
import org.junit.Assert;
import org.junit.Test;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.infrastructure.logreplication.utils.LogReplicationStreamNameTableManager.LOG_REPLICATION_STREAMS_NAME_TABLE;
import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;
import static org.corfudb.infrastructure.logreplication.utils
    .LogReplicationStreamNameTableManager.LOG_REPLICATION_PLUGIN_VERSION_TABLE;

@Slf4j
public class CorfuReplicationUpgradeIT extends LogReplicationAbstractIT {

    private static final int FIVE = 5;
    private static final int THREE = 3;
    private static final String STREAMS_TEST_TABLE =
        "StreamsToReplicateTestTable";

    @Test
    public void testLogEntrySyncAfterStandbyUpgraded() throws Exception {
        log.info(">> Setup active and standby Corfu's");
        setupActiveAndStandbyCorfu();

        log.info(">> Open map(s) on active and standby");
        openMaps(FIVE, false);

        log.info(">> Write data to active CorfuDB before LR is started ...");
        // Add Data for Snapshot Sync
        writeToActive(0, numWrites);

        // Confirm data does exist on Active Cluster
        for(Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> map : mapNameToMapActive.values()) {
            assertThat(map.count()).isEqualTo(numWrites);
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

        CountDownLatch statusUpdateLatch = new CountDownLatch(2);
        ReplicationStatusListener standbyListener = new ReplicationStatusListener(statusUpdateLatch);
        corfuStoreStandby.subscribeListener(standbyListener, LogReplicationMetadataManager.NAMESPACE,
            LogReplicationMetadataManager.LR_STATUS_STREAM_TAG);

        startLogReplicatorServers();

        log.info(">> Wait ... Snapshot log replication in progress ...");
        verifyDataOnStandby(numWrites);

        // Verify that snapshot sync was triggered by checking the number of
        // updates to the ReplicationStatus table on the standby.
        latchSnapshotSyncPlugin.await();
        validateSnapshotSyncPlugin(snapshotSyncPluginListener);
        statusUpdateLatch.await();

        Assert.assertEquals(2, standbyListener.getAccumulatedStatus().size());
        Assert.assertTrue(standbyListener.getAccumulatedStatus().get(1));
        Assert.assertFalse(standbyListener.getAccumulatedStatus().get(0));

        // Upgrade the standby site
        log.info(">> Upgrading the standby site ...");
        upgradeSite(false, corfuStoreStandby);

        // Verify that subsequent log entry sync is successful
        log.info("Write more data on the active");
        writeToActive(numWrites, numWrites / 2);

        log.info("Verify that data is replicated on the standby after it is upgraded");
        verifyDataOnStandby((numWrites + (numWrites / 2)));

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
        writeToActive(0, numWrites);

        // Confirm data does exist on Active Cluster
        for(Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> map : mapNameToMapActive.values()) {
            assertThat(map.count()).isEqualTo(numWrites);
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

        CountDownLatch statusUpdateLatch = new CountDownLatch(2);
        ReplicationStatusListener standbyListener = new ReplicationStatusListener(statusUpdateLatch);
        corfuStoreStandby.subscribeListener(standbyListener, LogReplicationMetadataManager.NAMESPACE,
            LogReplicationMetadataManager.LR_STATUS_STREAM_TAG);

        startLogReplicatorServers();

        log.info(">> Wait ... Snapshot log replication in progress ...");
        verifyDataOnStandby(numWrites);

        // Verify that snapshot sync was triggered by checking the number of
        // updates to the ReplicationStatus table on the standby.
        latchSnapshotSyncPlugin.await();
        validateSnapshotSyncPlugin(snapshotSyncPluginListener);
        statusUpdateLatch.await();

        Assert.assertEquals(2, standbyListener.getAccumulatedStatus().size());
        Assert.assertTrue(standbyListener.getAccumulatedStatus().get(1));
        Assert.assertFalse(standbyListener.getAccumulatedStatus().get(0));
        corfuStoreStandby.unsubscribeListener(standbyListener);
        corfuStoreStandby.unsubscribeListener(snapshotSyncPluginListener);

        // Upgrade the standby site
        log.info(">> Upgrading the standby site ...");
        upgradeSite(false, corfuStoreStandby);

        latchSnapshotSyncPlugin = new CountDownLatch(2);
        snapshotSyncPluginListener = new SnapshotSyncPluginListener(latchSnapshotSyncPlugin);
        subscribeToSnapshotSyncPluginTable(snapshotSyncPluginListener);

        statusUpdateLatch = new CountDownLatch(2);
        standbyListener = new ReplicationStatusListener(statusUpdateLatch);
        corfuStoreStandby.subscribeListener(standbyListener, LogReplicationMetadataManager.NAMESPACE,
            LogReplicationMetadataManager.LR_STATUS_STREAM_TAG);

        // Trigger a snapshot sync by stopping the active LR and running a CP+trim
        stopActiveLogReplicator();
        checkpointAndTrim(true);
        startActiveLogReplicator();

        // Verify that snapshot sync between the different versions was
        // successful
        latchSnapshotSyncPlugin.await();
        statusUpdateLatch.await();
        Assert.assertEquals(2, standbyListener.getAccumulatedStatus().size());
        Assert.assertTrue(standbyListener.getAccumulatedStatus().get(1));
        Assert.assertFalse(standbyListener.getAccumulatedStatus().get(0));
        verifyDataOnStandby(numWrites);

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
        log.info(">> Setup active and standby Corfu's");
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

        CountDownLatch statusUpdateLatch = new CountDownLatch(2);
        ReplicationStatusListener standbyListener = new ReplicationStatusListener(statusUpdateLatch);
        corfuStoreStandby.subscribeListener(standbyListener, LogReplicationMetadataManager.NAMESPACE,
            LogReplicationMetadataManager.LR_STATUS_STREAM_TAG);

        log.info(">> Open map(s) on active and standby");
        openMaps(FIVE, false);

        log.info(">> Write data to active CorfuDB before LR is started ...");
        // Add Data for Snapshot Sync
        writeToActive(0, numWrites);

        // Confirm data does exist on Active Cluster
        for(Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> map : mapNameToMapActive.values()) {
            assertThat(map.count()).isEqualTo(numWrites);
        }

        // Confirm data does not exist on Standby Cluster
        for(Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> map : mapNameToMapStandby.values()) {
            assertThat(map.count()).isEqualTo(0);
        }

        startLogReplicatorServers();

        log.info(">> Wait ... Snapshot log replication in progress ...");
        verifyDataOnStandby(numWrites);

        // Verify that snapshot sync was triggered by checking the number of
        // updates to the ReplicationStatus table on the standby.
        latchSnapshotSyncPlugin.await();
        validateSnapshotSyncPlugin(snapshotSyncPluginListener);
        statusUpdateLatch.await();

        Assert.assertEquals(2, standbyListener.getAccumulatedStatus().size());
        Assert.assertTrue(standbyListener.getAccumulatedStatus().get(1));
        Assert.assertFalse(standbyListener.getAccumulatedStatus().get(0));
        corfuStoreStandby.unsubscribeListener(standbyListener);
        corfuStoreStandby.unsubscribeListener(snapshotSyncPluginListener);

        // Upgrade the standby site first
        log.info(">> Upgrading the standby site ...");
        upgradeSite(false, corfuStoreStandby);

        // Upgrading the active site will force a snapshot sync
        latchSnapshotSyncPlugin = new CountDownLatch(2);
        snapshotSyncPluginListener = new SnapshotSyncPluginListener(latchSnapshotSyncPlugin);
        subscribeToSnapshotSyncPluginTable(snapshotSyncPluginListener);

        statusUpdateLatch = new CountDownLatch(2);
        standbyListener = new ReplicationStatusListener(statusUpdateLatch);
        corfuStoreStandby.subscribeListener(standbyListener, LogReplicationMetadataManager.NAMESPACE,
            LogReplicationMetadataManager.LR_STATUS_STREAM_TAG);

        // Upgrade the active site
        log.info(">> Upgrading the active site ...");
        upgradeSite(true, corfuStoreActive);

        // Verify that snapshot sync was triggered by checking the number of
        // updates to the ReplicationStatus table on the standby.
        latchSnapshotSyncPlugin.await();
        validateSnapshotSyncPlugin(snapshotSyncPluginListener);
        statusUpdateLatch.await();

        Assert.assertEquals(2, standbyListener.getAccumulatedStatus().size());
        Assert.assertTrue(standbyListener.getAccumulatedStatus().get(1));
        Assert.assertFalse(standbyListener.getAccumulatedStatus().get(0));

        verifyDataOnStandby(numWrites);

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
    public void testLogEntrySyncAfterStandbyUpgradedStreamsAddedAndRemoved() throws Exception {
        log.info(">> Setup active and standby Corfu's");
        setupActiveAndStandbyCorfu();

        List<String> streamsToReplicateActive = new ArrayList<>();
        for(int i=1; i<=2; i++) {
            streamsToReplicateActive.add(NAMESPACE + "$" + TABLE_PREFIX + i);
        }

        setupStreamsToReplicateTable(streamsToReplicateActive, true);
        setupStreamsToReplicateTable(streamsToReplicateActive, false);
        pluginConfigFilePath = "src/test/resources/transport/nettyConfigUpgrade.properties";

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

        CountDownLatch statusUpdateLatch = new CountDownLatch(2);
        ReplicationStatusListener standbyListener = new ReplicationStatusListener(statusUpdateLatch);
        corfuStoreStandby.subscribeListener(standbyListener, LogReplicationMetadataManager.NAMESPACE,
            LogReplicationMetadataManager.LR_STATUS_STREAM_TAG);

        log.info(">> Open map(s) on active and standby");
        openMaps(2, false);

        log.info(">> Write data to active CorfuDB before LR is started ...");
        // Add Data for Snapshot Sync
        writeToActive(0, numWrites);

        // Confirm data does exist on Active Cluster
        for(Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> map : mapNameToMapActive.values()) {
            assertThat(map.count()).isEqualTo(numWrites);
        }

        // Confirm data does not exist on Standby Cluster
        for(Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> map : mapNameToMapStandby.values()) {
            assertThat(map.count()).isEqualTo(0);
        }

        startLogReplicatorServers();

        log.info(">> Wait ... Snapshot log replication in progress ...");
        verifyDataOnStandby(numWrites);

        // Verify that snapshot sync was triggered by checking the number of
        // updates to the ReplicationStatus table on the standby.
        latchSnapshotSyncPlugin.await();
        validateSnapshotSyncPlugin(snapshotSyncPluginListener);
        statusUpdateLatch.await();

        Assert.assertEquals(2, standbyListener.getAccumulatedStatus().size());
        Assert.assertTrue(standbyListener.getAccumulatedStatus().get(1));
        Assert.assertFalse(standbyListener.getAccumulatedStatus().get(0));

        List<String> streamsToReplicateStandby = new ArrayList<>();
        for (int i=2; i<=THREE; i++) {
            streamsToReplicateStandby.add(NAMESPACE + "$" + TABLE_PREFIX + i);
        }
        upgradeSiteWithNewConfig(false, streamsToReplicateStandby);

        List<String> activeOnlyStreams = streamsToReplicateActive.stream()
            .filter(s -> !streamsToReplicateStandby.contains(s))
            .collect(Collectors.toList());

        List<String> standbyOnlyStreams = streamsToReplicateStandby.stream()
            .filter(s -> !streamsToReplicateActive.contains(s))
            .collect(Collectors.toList());

        List<String> commonStreams = streamsToReplicateActive.stream()
            .filter(s -> streamsToReplicateStandby.contains(s))
            .collect(Collectors.toList());

        // Open maps corresponding to the new streams in the upgraded config
        openMapsAfterUpgrade(standbyOnlyStreams);

        // Write to activeOnlyStreams
        writeDataOnActive(activeOnlyStreams, numWrites, numWrites/2);

        // Write to common streams
        writeDataOnActive(commonStreams, numWrites, numWrites/2);

        // Write to standbyOnlyStreams
        writeDataOnActive(standbyOnlyStreams, 0, numWrites);

        verifyDataOnStandby(commonStreams, (numWrites + numWrites/2));
        verifyDataOnStandby(activeOnlyStreams, numWrites);
        verifyDataOnStandby(standbyOnlyStreams, 0);
    }

    @Test
    public void testSnapshotSyncAfterStandbyUpgradedStreamsAddedAndRemoved() throws Exception {
        log.info(">> Setup active and standby Corfu's");
        setupActiveAndStandbyCorfu();

        List<String> streamsToReplicateActive = new ArrayList<>();
        for(int i=1; i<=2; i++) {
            streamsToReplicateActive.add(NAMESPACE + "$" + TABLE_PREFIX + i);
        }
        setupStreamsToReplicateTable(streamsToReplicateActive, true);
        setupStreamsToReplicateTable(streamsToReplicateActive, false);
        pluginConfigFilePath = "src/test/resources/transport/nettyConfigUpgrade.properties";

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

        CountDownLatch statusUpdateLatch = new CountDownLatch(2);
        ReplicationStatusListener standbyListener = new ReplicationStatusListener(statusUpdateLatch);
        corfuStoreStandby.subscribeListener(standbyListener, LogReplicationMetadataManager.NAMESPACE,
            LogReplicationMetadataManager.LR_STATUS_STREAM_TAG);

        log.info(">> Open map(s) on active and standby");
        openMaps(2, false);

        log.info(">> Write data to active CorfuDB before LR is started ...");
        // Add Data for Snapshot Sync
        writeToActive(0, numWrites);

        // Confirm data does exist on Active Cluster
        for(Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> map : mapNameToMapActive.values()) {
            assertThat(map.count()).isEqualTo(numWrites);
        }

        // Confirm data does not exist on Standby Cluster
        for(Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> map : mapNameToMapStandby.values()) {
            assertThat(map.count()).isEqualTo(0);
        }

        startLogReplicatorServers();

        log.info(">> Wait ... Snapshot log replication in progress ...");
        verifyDataOnStandby(numWrites);

        // Verify that snapshot sync was triggered by checking the number of
        // updates to the ReplicationStatus table on the standby.
        latchSnapshotSyncPlugin.await();
        validateSnapshotSyncPlugin(snapshotSyncPluginListener);
        statusUpdateLatch.await();

        Assert.assertEquals(2, standbyListener.getAccumulatedStatus().size());
        Assert.assertTrue(standbyListener.getAccumulatedStatus().get(1));
        Assert.assertFalse(standbyListener.getAccumulatedStatus().get(0));
        corfuStoreStandby.unsubscribeListener(standbyListener);
        corfuStoreStandby.unsubscribeListener(snapshotSyncPluginListener);

        // Upgrade the standby site
        log.info(">> Upgrading the standby site ...");
        List<String> streamsToReplicateStandby = new ArrayList<>();
        for (int i=2; i<=THREE; i++) {
            streamsToReplicateStandby.add(NAMESPACE + "$" + TABLE_PREFIX + i);
        }
        upgradeSiteWithNewConfig(false, streamsToReplicateStandby);

        latchSnapshotSyncPlugin = new CountDownLatch(2);
        snapshotSyncPluginListener = new SnapshotSyncPluginListener(latchSnapshotSyncPlugin);
        subscribeToSnapshotSyncPluginTable(snapshotSyncPluginListener);

        statusUpdateLatch = new CountDownLatch(2);
        standbyListener = new ReplicationStatusListener(statusUpdateLatch);
        corfuStoreStandby.subscribeListener(standbyListener, LogReplicationMetadataManager.NAMESPACE,
            LogReplicationMetadataManager.LR_STATUS_STREAM_TAG);

        stopActiveLogReplicator();

        List<String> activeOnlyStreams = streamsToReplicateActive.stream()
            .filter(s -> !streamsToReplicateStandby.contains(s))
            .collect(Collectors.toList());

        List<String> standbyOnlyStreams = streamsToReplicateStandby.stream()
            .filter(s -> !streamsToReplicateActive.contains(s))
            .collect(Collectors.toList());

        List<String> commonStreams = streamsToReplicateActive.stream()
            .filter(s -> streamsToReplicateStandby.contains(s))
            .collect(Collectors.toList());

        // Open maps corresponding to the new streams in the upgraded config
        openMapsAfterUpgrade(standbyOnlyStreams);

        // Write data on activeOnly streams
        writeDataOnActive(activeOnlyStreams, numWrites, numWrites/2);

        // Write data on standbyOnly streams
        writeDataOnActive(standbyOnlyStreams, 0, numWrites);

        // Write data on common streams
        writeDataOnActive(commonStreams, numWrites, numWrites/2);

        // Trigger a snapshot sync by running a CP+trim
        checkpointAndTrim(true);
        startActiveLogReplicator();

        // Verify that snapshot sync between the different versions was
        // successful
        latchSnapshotSyncPlugin.await();
        statusUpdateLatch.await();
        Assert.assertEquals(2, standbyListener.getAccumulatedStatus().size());
        Assert.assertTrue(standbyListener.getAccumulatedStatus().get(1));
        Assert.assertFalse(standbyListener.getAccumulatedStatus().get(0));

        // No new data for active-only streams
        verifyDataOnStandby(activeOnlyStreams, numWrites);

        // No new data for standby-only streams
        verifyDataOnStandby(standbyOnlyStreams, 0);

        // New data present for common streams
        verifyDataOnStandby(commonStreams, (numWrites + numWrites/2));
    }

    @Test
    public void testSnapshotSyncAfterBothUpgradedStreamsAddedAndRemoved() throws Exception {
        log.info(">> Setup active and standby Corfu's");
        setupActiveAndStandbyCorfu();

        List<String> streamsToReplicateActive = new ArrayList<>();
        for(int i=1; i<=2; i++) {
            streamsToReplicateActive.add(NAMESPACE + "$" + TABLE_PREFIX + i);
        }

        setupStreamsToReplicateTable(streamsToReplicateActive, true);
        setupStreamsToReplicateTable(streamsToReplicateActive, false);
        pluginConfigFilePath = "src/test/resources/transport/nettyConfigUpgrade.properties";

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

        CountDownLatch statusUpdateLatch = new CountDownLatch(2);
        ReplicationStatusListener standbyListener = new ReplicationStatusListener(statusUpdateLatch);
        corfuStoreStandby.subscribeListener(standbyListener, LogReplicationMetadataManager.NAMESPACE,
            LogReplicationMetadataManager.LR_STATUS_STREAM_TAG);

        log.info(">> Open map(s) on active and standby");
        openMaps(2, false);

        log.info(">> Write data to active CorfuDB before LR is started ...");
        // Add Data for Snapshot Sync
        writeToActive(0, numWrites);

        // Confirm data does exist on Active Cluster
        for(Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> map : mapNameToMapActive.values()) {
            assertThat(map.count()).isEqualTo(numWrites);
        }

        // Confirm data does not exist on Standby Cluster
        for(Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> map : mapNameToMapStandby.values()) {
            assertThat(map.count()).isEqualTo(0);
        }

        startLogReplicatorServers();

        log.info(">> Wait ... Snapshot log replication in progress ...");
        verifyDataOnStandby(numWrites);

        // Verify that snapshot sync was triggered by checking the number of
        // updates to the ReplicationStatus table on the standby.
        latchSnapshotSyncPlugin.await();
        validateSnapshotSyncPlugin(snapshotSyncPluginListener);
        statusUpdateLatch.await();

        Assert.assertEquals(2, standbyListener.getAccumulatedStatus().size());
        Assert.assertTrue(standbyListener.getAccumulatedStatus().get(1));
        Assert.assertFalse(standbyListener.getAccumulatedStatus().get(0));

        List<String> streamsToReplicateStandby = new ArrayList<>();
        for (int i=2; i<=THREE; i++) {
            streamsToReplicateStandby.add(NAMESPACE + "$" + TABLE_PREFIX + i);
        }
        upgradeSiteWithNewConfig(false, streamsToReplicateStandby);

        List<String> activeOnlyStreams = streamsToReplicateActive.stream()
            .filter(s -> !streamsToReplicateStandby.contains(s))
            .collect(Collectors.toList());

        List<String> standbyOnlyStreams = streamsToReplicateStandby.stream()
            .filter(s -> !streamsToReplicateActive.contains(s))
            .collect(Collectors.toList());

        List<String> commonStreams = streamsToReplicateActive.stream()
            .filter(s -> streamsToReplicateStandby.contains(s))
            .collect(Collectors.toList());

        // Open maps corresponding to the new streams in the upgraded config
        openMapsAfterUpgrade(standbyOnlyStreams);

        // Write to activeOnlyStreams
        writeDataOnActive(activeOnlyStreams, numWrites, numWrites/2);

        // Write to common streams
        writeDataOnActive(commonStreams, numWrites, numWrites/2);

        // Write to standbyOnlyStreams
        writeDataOnActive(standbyOnlyStreams, 0, numWrites);

        verifyDataOnStandby(commonStreams, (numWrites + numWrites/2));
        verifyDataOnStandby(activeOnlyStreams, numWrites);
        verifyDataOnStandby(standbyOnlyStreams, 0);

        corfuStoreActive.unsubscribeListener(standbyListener);
        corfuStoreActive.unsubscribeListener(snapshotSyncPluginListener);

        upgradeSiteWithNewConfig(true, streamsToReplicateStandby);
        latchSnapshotSyncPlugin = new CountDownLatch(2);
        snapshotSyncPluginListener = new SnapshotSyncPluginListener(latchSnapshotSyncPlugin);
        subscribeToSnapshotSyncPluginTable(snapshotSyncPluginListener);

        statusUpdateLatch = new CountDownLatch(2);
        standbyListener = new ReplicationStatusListener(statusUpdateLatch);
        corfuStoreStandby.subscribeListener(standbyListener, LogReplicationMetadataManager.NAMESPACE,
            LogReplicationMetadataManager.LR_STATUS_STREAM_TAG);

        // Now upgrade the active site
        upgradeSiteWithNewConfig(true, streamsToReplicateStandby);

        // Verify that the streams only on standby prior to upgrade have data
        // and no data is lost for the common streams
        verifyDataOnStandby(commonStreams, (numWrites + numWrites/2));
        verifyDataOnStandby(standbyOnlyStreams, numWrites);
    }

    private void setupStreamsToReplicateTable(List<String> streamsToReplicate,
        boolean active) throws Exception {

        CorfuStore corfuStore;
        Table<LogReplicationStreams.TableInfo, LogReplicationStreams.Namespace,
            CommonTypes.Uuid> streamsNameTable;
        if (active) {
            corfuStore = corfuStoreActive;
        } else {
            corfuStore = corfuStoreStandby;
        }

        streamsNameTable = corfuStore.openTable(NAMESPACE, STREAMS_TEST_TABLE,
            LogReplicationStreams.TableInfo.class,
            LogReplicationStreams.Namespace.class, CommonTypes.Uuid.class,
            TableOptions.builder().build());

        try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
            for (String stream : streamsToReplicate) {
                LogReplicationStreams.TableInfo tableInfo =
                    LogReplicationStreams.TableInfo.newBuilder().setName(stream).build();
                LogReplicationStreams.Namespace namespace =
                    LogReplicationStreams.Namespace.newBuilder().setName(NAMESPACE).build();
                txn.putRecord(streamsNameTable, tableInfo, namespace, null);
                txn.commit();
            }
        }
    }

    private void upgradeSiteWithNewConfig(boolean active,
        List<String>streamsToReplicate) throws Exception {
        setupStreamsToReplicateTable(streamsToReplicate, active);
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
        Table<LogReplicationStreams.VersionString,
            LogReplicationStreams.Version, CommonTypes.Uuid>
            pluginVersionTable = corfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
            LOG_REPLICATION_PLUGIN_VERSION_TABLE, LogReplicationStreams.VersionString.class,
            LogReplicationStreams.Version.class, CommonTypes.Uuid.class,
            TableOptions.builder().build());

        LogReplicationStreams.VersionString versionString = LogReplicationStreams.VersionString.newBuilder()
            .setName("VERSION").build();

        LogReplicationStreams.Version version = LogReplicationStreams.Version.newBuilder()
            .setVersion("new_version").build();
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            txn.putRecord(pluginVersionTable, versionString, version, null);
            txn.commit();
        }

        if (active) {
            startActiveLogReplicator();
        } else {
            startStandbyLogReplicator();
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
                                   int totalWrites) {
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

    private void verifyDataOnStandby(List<String> tableNames,
        int expectedNumWrites) {
        for (String tableName : tableNames) {
            Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> table =
                mapNameToMapStandby.get(tableName);
            while(table.count() != expectedNumWrites) {
                // block
            }
            Assert.assertEquals(expectedNumWrites, table.count());

            for (int i = 0; i < (expectedNumWrites); i++) {
                try (TxnContext tx = corfuStoreStandby.txn(table.getNamespace())) {
                    assertThat(tx.getRecord(table,
                        Sample.StringKey.newBuilder().setKey(String.valueOf(i)).build()).getPayload()).isNotNull();
                    tx.commit();
                }
            }
        }
    }
}
