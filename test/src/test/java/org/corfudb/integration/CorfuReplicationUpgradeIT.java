package org.corfudb.integration;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.LRRollingUpgradeHandler;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultAdapterForUpgrade;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultAdapterForUpgradeSink;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultAdapterForUpgradeSource;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationEvent;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationEventInfoKey;
import org.corfudb.infrastructure.logreplication.proto.Sample;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.corfudb.runtime.LogReplication.ReplicationStatus;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.runtime.LogReplicationUtils.REPLICATION_STATUS_TABLE_NAME;
import static org.corfudb.runtime.LogReplicationUtils.LR_STATUS_STREAM_TAG;
import static org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager.REPLICATION_EVENT_TABLE_NAME;
import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

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

    private static final String TEST_PLUGIN_CONFIG_PATH_SOURCE =
            "./test/src/test/resources/transport/grpcConfigUpgradeSource.properties";

    private static final String TEST_PLUGIN_CONFIG_PATH_SINK =
            "./test/src/test/resources/transport/grpcConfigUpgradeSink.properties";

    @Test
    public void testLogEntrySyncAfterSinkUpgraded() throws Exception {
        log.info(">> Setup source and sink Corfu's");
        setupSourceAndSinkCorfu();
        initSingleSourceSinkCluster();

        log.info(">> Open map(s) on source and sink");
        openMaps(FIVE, false);

        log.info(">> Write data to source CorfuDB before LR is started ...");
        // Add Data for Snapshot Sync
        writeToSource(0, NUM_WRITES);

        // Confirm data does exist on Source Cluster
        for(Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> map : mapNameToMapSource.values()) {
            assertThat(map.count()).isEqualTo(NUM_WRITES);
        }

        // Confirm data does not exist on Sink Cluster
        for(Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> map : mapNameToMapSink.values()) {
            assertThat(map.count()).isEqualTo(0);
        }

        // Two updates are expected onStart of snapshot sync and onEnd.
        CountDownLatch latchSnapshotSyncPlugin = new CountDownLatch(2);
        SnapshotSyncPluginListener snapshotSyncPluginListener = new SnapshotSyncPluginListener(latchSnapshotSyncPlugin);
        subscribeToSnapshotSyncPluginTable(snapshotSyncPluginListener);

        // Subscribe to replication status table on Sink (to be sure data change on status are captured)
        corfuStoreSink.openTable(CORFU_SYSTEM_NAMESPACE,
                REPLICATION_STATUS_TABLE_NAME,
                LogReplicationSession.class,
                ReplicationStatus.class,
                null,
                TableOptions.fromProtoSchema(ReplicationStatus.class));

        CountDownLatch statusUpdateLatch = new CountDownLatch(TOTAL_SINK_STATUS_TX_INIT_SNAPSHOT_SYNC);
        ReplicationStatusListener sinkListener = new ReplicationStatusListener(statusUpdateLatch, false);
        corfuStoreSink.subscribeListener(sinkListener, CORFU_SYSTEM_NAMESPACE, LR_STATUS_STREAM_TAG);

        pluginConfigFilePath = TEST_PLUGIN_CONFIG_PATH_SOURCE;
        startSourceLogReplicator();

        pluginConfigFilePath = TEST_PLUGIN_CONFIG_PATH_SINK;
        startSinkLogReplicator();

        log.info(">> Wait ... Snapshot log replication in progress ...");
        verifyDataOnSink(NUM_WRITES);

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

        // Upgrade the sink site
        log.info(">> Upgrading the sink site ...");
        pluginConfigFilePath = TEST_PLUGIN_CONFIG_PATH_SINK;
        verifyRollingUpgrade(false);

        // Verify that subsequent log entry sync is successful
        log.info("Write more data on the source");
        writeToSource(NUM_WRITES, NUM_WRITES / 2);

        log.info("Verify that data is replicated on the sink after it is upgraded");
        verifyDataOnSink(NUM_WRITES + (NUM_WRITES / 2));

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

    @Test
    public void testSnapshotSyncAfterSinkUpgraded() throws Exception {
        log.info(">> Setup source and sink Corfu's");
        setupSourceAndSinkCorfu();
        initSingleSourceSinkCluster();

        log.info(">> Open map(s) on source and sink");
        openMaps(FIVE, false);

        log.info(">> Write data to source CorfuDB before LR is started ...");
        // Add Data for Snapshot Sync
        writeToSource(0, NUM_WRITES);

        // Confirm data does exist on Source Cluster
        for(Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> map : mapNameToMapSource.values()) {
            assertThat(map.count()).isEqualTo(NUM_WRITES);
        }

        // Confirm data does not exist on Sink Cluster
        for(Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> map : mapNameToMapSink.values()) {
            assertThat(map.count()).isEqualTo(0);
        }

        // Two updates are expected onStart of snapshot sync and onEnd.
        CountDownLatch latchSnapshotSyncPlugin = new CountDownLatch(2);
        SnapshotSyncPluginListener snapshotSyncPluginListener = new SnapshotSyncPluginListener(latchSnapshotSyncPlugin);
        subscribeToSnapshotSyncPluginTable(snapshotSyncPluginListener);

        // Subscribe to replication status table on Sink (to be sure data change on status are captured)
        corfuStoreSink.openTable(CORFU_SYSTEM_NAMESPACE,
                REPLICATION_STATUS_TABLE_NAME,
                LogReplicationSession.class,
                ReplicationStatus.class,
                null,
                TableOptions.fromProtoSchema(ReplicationStatus.class));

        CountDownLatch statusUpdateLatch = new CountDownLatch(TOTAL_SINK_STATUS_TX_INIT_SNAPSHOT_SYNC);
        ReplicationStatusListener sinkListener = new ReplicationStatusListener(statusUpdateLatch, false);
        corfuStoreSink.subscribeListener(sinkListener, CORFU_SYSTEM_NAMESPACE,
                LR_STATUS_STREAM_TAG);

        pluginConfigFilePath = TEST_PLUGIN_CONFIG_PATH_SOURCE;
        startSourceLogReplicator();

        pluginConfigFilePath = TEST_PLUGIN_CONFIG_PATH_SINK;
        startSinkLogReplicator();

        log.info(">> Wait ... Snapshot log replication in progress ...");
        verifyDataOnSink(NUM_WRITES);

        // Verify that snapshot sync was triggered by checking the number of
        // updates to the ReplicationStatus table on the sink.
        latchSnapshotSyncPlugin.await();
        validateSnapshotSyncPlugin(snapshotSyncPluginListener);
        statusUpdateLatch.await();

        Assert.assertEquals(TOTAL_SINK_STATUS_ENTRIES_INIT_SNAPSHOT_SYNC, sinkListener.getAccumulatedStatus().size());
        Assert.assertTrue(sinkListener.getAccumulatedStatus().get(2));
        Assert.assertFalse(sinkListener.getAccumulatedStatus().get(1));
        corfuStoreSink.unsubscribeListener(sinkListener);
        corfuStoreSink.unsubscribeListener(snapshotSyncPluginListener);

        // Upgrade the sink site

        statusUpdateLatch = new CountDownLatch(NUM_SNAPSHOT_SYNC_UPDATES_ON_SINK_STATUS_TABLE);
        sinkListener = new ReplicationStatusListener(statusUpdateLatch, false);
        corfuStoreSink.subscribeListener(sinkListener, CORFU_SYSTEM_NAMESPACE,
            LR_STATUS_STREAM_TAG);

        log.info(">> Upgrading the sink site ...");
        pluginConfigFilePath = TEST_PLUGIN_CONFIG_PATH_SINK;
        verifyRollingUpgrade(false);

        latchSnapshotSyncPlugin = new CountDownLatch(2);
        snapshotSyncPluginListener = new SnapshotSyncPluginListener(latchSnapshotSyncPlugin);
        subscribeToSnapshotSyncPluginTable(snapshotSyncPluginListener);

        // Trigger a snapshot sync by stopping the source LR and running a CP+trim
        stopSourceLogReplicator();
        checkpointAndTrim(true);
        initSingleSourceSinkCluster();
        pluginConfigFilePath = TEST_PLUGIN_CONFIG_PATH_SOURCE;
        startSourceLogReplicator();

        // Verify that snapshot sync between the different versions was successful
        latchSnapshotSyncPlugin.await();
        statusUpdateLatch.await();
        Assert.assertEquals(NUM_SNAPSHOT_SYNC_UPDATES_ON_SINK_STATUS_TABLE, sinkListener.getAccumulatedStatus().size());
        Assert.assertTrue(sinkListener.getAccumulatedStatus().get(1));
        Assert.assertFalse(sinkListener.getAccumulatedStatus().get(0));
        verifyDataOnSink(NUM_WRITES);

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

    @Test
    public void testSnapshotSyncAfterSinkAndSourceUpgraded() throws Exception {
        log.info(">> Setup source and sink Corfu");
        setupSourceAndSinkCorfu();
        initSingleSourceSinkCluster();

        // Two updates are expected onStart of snapshot sync and onEnd.
        CountDownLatch latchSnapshotSyncPlugin = new CountDownLatch(2);
        SnapshotSyncPluginListener snapshotSyncPluginListener = new SnapshotSyncPluginListener(latchSnapshotSyncPlugin);
        subscribeToSnapshotSyncPluginTable(snapshotSyncPluginListener);

        // Subscribe to replication status table on Sink (to be sure data change on status are captured)
        corfuStoreSink.openTable(CORFU_SYSTEM_NAMESPACE,
                REPLICATION_STATUS_TABLE_NAME,
                LogReplicationSession.class,
                ReplicationStatus.class,
                null,
                TableOptions.fromProtoSchema(ReplicationStatus.class));

        CountDownLatch statusUpdateLatch = new CountDownLatch(TOTAL_SINK_STATUS_TX_INIT_SNAPSHOT_SYNC);
        ReplicationStatusListener sinkListener = new ReplicationStatusListener(statusUpdateLatch, false);
        corfuStoreSink.subscribeListener(sinkListener, CORFU_SYSTEM_NAMESPACE,
                LR_STATUS_STREAM_TAG);

        log.info(">> Open map(s) on source and sink");
        openMaps(FIVE, false);

        log.info(">> Write data to source CorfuDB before LR is started ...");
        // Add Data for Snapshot Sync
        writeToSource(0, NUM_WRITES);

        // Confirm data does exist on Source Cluster
        for (Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> map : mapNameToMapSource.values()) {
            assertThat(map.count()).isEqualTo(NUM_WRITES);
        }

        // Confirm data does not exist on Sink Cluster
        for (Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> map : mapNameToMapSink.values()) {
            assertThat(map.count()).isEqualTo(0);
        }

        pluginConfigFilePath = TEST_PLUGIN_CONFIG_PATH_SOURCE;
        startSourceLogReplicator();

        pluginConfigFilePath = TEST_PLUGIN_CONFIG_PATH_SINK;
        startSinkLogReplicator();

        log.info(">> Wait ... Snapshot log replication in progress ...");
        verifyDataOnSink(NUM_WRITES);

        // Verify that snapshot sync was triggered by checking the number of
        // updates to the ReplicationStatus table on the sink.
        latchSnapshotSyncPlugin.await();
        validateSnapshotSyncPlugin(snapshotSyncPluginListener);
        statusUpdateLatch.await();

        Assert.assertEquals(TOTAL_SINK_STATUS_ENTRIES_INIT_SNAPSHOT_SYNC, sinkListener.getAccumulatedStatus().size());
        Assert.assertTrue(sinkListener.getAccumulatedStatus().get(2));
        Assert.assertFalse(sinkListener.getAccumulatedStatus().get(1));
        corfuStoreSink.unsubscribeListener(sinkListener);
        corfuStoreSink.unsubscribeListener(snapshotSyncPluginListener);

        // Upgrade the sink site first
        log.info(">> Upgrading the sink site ...");
        pluginConfigFilePath = TEST_PLUGIN_CONFIG_PATH_SINK;
        verifyRollingUpgrade(false);
        log.info(">> Plugin config verified after sink upgrade");

        // Upgrading the source site will force a snapshot sync
        latchSnapshotSyncPlugin = new CountDownLatch(2);
        snapshotSyncPluginListener = new SnapshotSyncPluginListener(latchSnapshotSyncPlugin);
        subscribeToSnapshotSyncPluginTable(snapshotSyncPluginListener);

        statusUpdateLatch = new CountDownLatch(NUM_SNAPSHOT_SYNC_UPDATES_ON_SINK_STATUS_TABLE);
        sinkListener = new ReplicationStatusListener(statusUpdateLatch, false);
        corfuStoreSink.subscribeListener(sinkListener, CORFU_SYSTEM_NAMESPACE, LR_STATUS_STREAM_TAG);

        // Upgrade the source site
        log.info(">> Upgrading the source site ...");
        pluginConfigFilePath = TEST_PLUGIN_CONFIG_PATH_SOURCE;
        verifyRollingUpgrade(true);

        // Verify that snapshot sync was triggered by checking the number of
        // updates to the ReplicationStatus table on the sink.
        latchSnapshotSyncPlugin.await();
        validateSnapshotSyncPlugin(snapshotSyncPluginListener);
        statusUpdateLatch.await();

        Assert.assertEquals(NUM_SNAPSHOT_SYNC_UPDATES_ON_SINK_STATUS_TABLE, sinkListener.getAccumulatedStatus().size());
        Assert.assertTrue(sinkListener.getAccumulatedStatus().get(1));
        Assert.assertFalse(sinkListener.getAccumulatedStatus().get(0));

        verifyDataOnSink(NUM_WRITES);

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

    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void testLogEntrySyncAfterSinkUpgradedStreamsAddedAndRemoved() throws Exception {
        log.info(">> Setup source and sink Corfu");
        setupSourceAndSinkCorfu();
        initSingleSourceSinkCluster();

        Set<String> streamsToReplicateSource = new HashSet<>();
        for (int i = 1; i <= 2; i++) {
            streamsToReplicateSource.add(TABLE_PREFIX + i);
        }

        // Two updates are expected onStart of snapshot sync and onEnd.
        CountDownLatch latchSnapshotSyncPlugin = new CountDownLatch(2);
        SnapshotSyncPluginListener snapshotSyncPluginListener = new SnapshotSyncPluginListener(latchSnapshotSyncPlugin);
        subscribeToSnapshotSyncPluginTable(snapshotSyncPluginListener);

        // Subscribe to replication status table on Sink (to be sure data change on status are captured)
        corfuStoreSink.openTable(CORFU_SYSTEM_NAMESPACE,
                REPLICATION_STATUS_TABLE_NAME,
                LogReplicationSession.class,
                ReplicationStatus.class,
                null,
                TableOptions.fromProtoSchema(ReplicationStatus.class));

        CountDownLatch statusUpdateLatch = new CountDownLatch(TOTAL_SINK_STATUS_TX_INIT_SNAPSHOT_SYNC);
        ReplicationStatusListener sinkListener = new ReplicationStatusListener(statusUpdateLatch, false);
        corfuStoreSink.subscribeListener(sinkListener, CORFU_SYSTEM_NAMESPACE,
                LR_STATUS_STREAM_TAG);

        log.info(">> Open map(s) on source and sink");
        openMaps(2, false);

        log.info(">> Write data to source CorfuDB before LR is started ...");
        // Add Data for Snapshot Sync
        writeToSource(0, NUM_WRITES);

        // Confirm data does exist on Source Cluster
        for(Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> map : mapNameToMapSource.values()) {
            assertThat(map.count()).isEqualTo(NUM_WRITES);
        }

        // Confirm data does not exist on Sink Cluster
        for(Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> map : mapNameToMapSink.values()) {
            assertThat(map.count()).isEqualTo(0);
        }

        pluginConfigFilePath = TEST_PLUGIN_CONFIG_PATH_SOURCE;
        startSourceLogReplicator();

        pluginConfigFilePath = TEST_PLUGIN_CONFIG_PATH_SINK;
        startSinkLogReplicator();

        log.info(">> Wait ... Snapshot log replication in progress ...");
        verifyDataOnSink(NUM_WRITES);

        // Verify that snapshot sync was triggered by checking the number of
        // updates to the ReplicationStatus table on the sink.
        latchSnapshotSyncPlugin.await();
        validateSnapshotSyncPlugin(snapshotSyncPluginListener);
        statusUpdateLatch.await();

        Assert.assertEquals(TOTAL_SINK_STATUS_ENTRIES_INIT_SNAPSHOT_SYNC, sinkListener.getAccumulatedStatus().size());
        Assert.assertTrue(sinkListener.getAccumulatedStatus().get(2));
        Assert.assertFalse(sinkListener.getAccumulatedStatus().get(1));

        Set<String> streamsToReplicateSink = new HashSet<>();
        for (int i = 2; i <= 3; i++) {
            streamsToReplicateSink.add(TABLE_PREFIX + i);
        }
        verifyRollingUpgrade(false);

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

        corfuStoreSink.unsubscribeListener(snapshotSyncPluginListener);
        corfuStoreSink.unsubscribeListener(sinkListener);
    }

    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void testSnapshotSyncAfterSinkUpgradedStreamsAddedAndRemoved() throws Exception {
        log.info(">> Setup source and sink Corfu's");
        setupSourceAndSinkCorfu();
        initSingleSourceSinkCluster();

        Set<String> streamsToReplicateSource = new HashSet<>();
        for (int i = 1; i <= 2; i++) {
            streamsToReplicateSource.add(TABLE_PREFIX + i);
        }

        // Two updates are expected onStart of snapshot sync and onEnd.
        CountDownLatch latchSnapshotSyncPlugin = new CountDownLatch(2);
        SnapshotSyncPluginListener snapshotSyncPluginListener = new SnapshotSyncPluginListener(latchSnapshotSyncPlugin);
        subscribeToSnapshotSyncPluginTable(snapshotSyncPluginListener);

        // Subscribe to replication status table on Sink (to be sure data change on status are captured)
        corfuStoreSink.openTable(CORFU_SYSTEM_NAMESPACE,
                REPLICATION_STATUS_TABLE_NAME,
                LogReplicationSession.class,
                ReplicationStatus.class,
                null,
                TableOptions.fromProtoSchema(ReplicationStatus.class));

        CountDownLatch statusUpdateLatch = new CountDownLatch(TOTAL_SINK_STATUS_TX_INIT_SNAPSHOT_SYNC);
        ReplicationStatusListener sinkListener = new ReplicationStatusListener(statusUpdateLatch, false);
        corfuStoreSink.subscribeListener(sinkListener, CORFU_SYSTEM_NAMESPACE, LR_STATUS_STREAM_TAG);

        log.info(">> Open map(s) on source and sink");
        openMaps(2, false);

        log.info(">> Write data to source CorfuDB before LR is started ...");
        // Add Data for Snapshot Sync
        writeToSource(0, NUM_WRITES);

        // Confirm data does exist on Source Cluster
        for(Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> map : mapNameToMapSource.values()) {
            assertThat(map.count()).isEqualTo(NUM_WRITES);
        }

        // Confirm data does not exist on Sink Cluster
        for(Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> map : mapNameToMapSink.values()) {
            assertThat(map.count()).isEqualTo(0);
        }

        pluginConfigFilePath = TEST_PLUGIN_CONFIG_PATH_SOURCE;
        startSourceLogReplicator();

        pluginConfigFilePath = TEST_PLUGIN_CONFIG_PATH_SINK;
        startSinkLogReplicator();

        log.info(">> Wait ... Snapshot log replication in progress ...");
        verifyDataOnSink(NUM_WRITES);

        // Verify that snapshot sync was triggered by checking the number of
        // updates to the ReplicationStatus table on the sink.
        latchSnapshotSyncPlugin.await();
        validateSnapshotSyncPlugin(snapshotSyncPluginListener);
        statusUpdateLatch.await();

        Assert.assertEquals(TOTAL_SINK_STATUS_ENTRIES_INIT_SNAPSHOT_SYNC, sinkListener.getAccumulatedStatus().size());
        Assert.assertTrue(sinkListener.getAccumulatedStatus().get(2));
        Assert.assertFalse(sinkListener.getAccumulatedStatus().get(1));
        corfuStoreSink.unsubscribeListener(sinkListener);
        corfuStoreSink.unsubscribeListener(snapshotSyncPluginListener);

        // Upgrade the sink site

        statusUpdateLatch = new CountDownLatch(NUM_SNAPSHOT_SYNC_UPDATES_ON_SINK_STATUS_TABLE);
        sinkListener = new ReplicationStatusListener(statusUpdateLatch, false);
        corfuStoreSink.subscribeListener(sinkListener, CORFU_SYSTEM_NAMESPACE, LR_STATUS_STREAM_TAG);

        log.info(">> Upgrading the sink site ...");
        Set<String> streamsToReplicateSink = new HashSet<>();
        for (int i = 2; i <= 3; i++) {
            streamsToReplicateSink.add(TABLE_PREFIX + i);
        }
        pluginConfigFilePath = TEST_PLUGIN_CONFIG_PATH_SINK;
        verifyRollingUpgrade(false);

        latchSnapshotSyncPlugin = new CountDownLatch(2);
        snapshotSyncPluginListener = new SnapshotSyncPluginListener(latchSnapshotSyncPlugin);
        subscribeToSnapshotSyncPluginTable(snapshotSyncPluginListener);

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

        // Trigger a snapshot sync by running a CP+trim
        checkpointAndTrim(true);
        pluginConfigFilePath = TEST_PLUGIN_CONFIG_PATH_SOURCE;
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

        corfuStoreSink.unsubscribeListener(snapshotSyncPluginListener);
        corfuStoreSink.unsubscribeListener(sinkListener);
    }

    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void testSnapshotSyncAfterBothUpgradedStreamsAddedAndRemoved() throws Exception {
        log.info(">> Setup source and sink Corfu's");
        setupSourceAndSinkCorfu();
        initSingleSourceSinkCluster();

        Set<String> streamsToReplicateSource = new HashSet<>();
        for (int i = 1; i <= 2; i++) {
            streamsToReplicateSource.add(TABLE_PREFIX + i);
        }

        // Two updates are expected onStart of snapshot sync and onEnd.
        CountDownLatch latchSnapshotSyncPlugin = new CountDownLatch(2);
        SnapshotSyncPluginListener snapshotSyncPluginListener = new SnapshotSyncPluginListener(latchSnapshotSyncPlugin);
        subscribeToSnapshotSyncPluginTable(snapshotSyncPluginListener);

        // Subscribe to replication status table on Sink (to be sure data change on status are captured)
        corfuStoreSink.openTable(CORFU_SYSTEM_NAMESPACE,
                REPLICATION_STATUS_TABLE_NAME,
                LogReplicationSession.class,
                ReplicationStatus.class,
                null,
                TableOptions.fromProtoSchema(ReplicationStatus.class));

        CountDownLatch statusUpdateLatch = new CountDownLatch(TOTAL_SINK_STATUS_TX_INIT_SNAPSHOT_SYNC);
        ReplicationStatusListener sinkListener = new ReplicationStatusListener(statusUpdateLatch, false);
        corfuStoreSink.subscribeListener(sinkListener, CORFU_SYSTEM_NAMESPACE, LR_STATUS_STREAM_TAG);

        log.info(">> Open map(s) on source and sink");
        openMaps(2, false);

        log.info(">> Write data to source CorfuDB before LR is started ...");
        // Add Data for Snapshot Sync
        writeToSource(0, NUM_WRITES);

        // Confirm data does exist on Source Cluster
        for (Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> map : mapNameToMapSource.values()) {
            assertThat(map.count()).isEqualTo(NUM_WRITES);
        }

        // Confirm data does not exist on Sink Cluster
        for (Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> map : mapNameToMapSink.values()) {
            assertThat(map.count()).isEqualTo(0);
        }

        pluginConfigFilePath = TEST_PLUGIN_CONFIG_PATH_SOURCE;
        startSourceLogReplicator();

        pluginConfigFilePath = TEST_PLUGIN_CONFIG_PATH_SINK;
        startSinkLogReplicator();

        log.info(">> Wait ... Snapshot log replication in progress ...");
        verifyDataOnSink(NUM_WRITES);

        // Verify that snapshot sync was triggered by checking the number of
        // updates to the ReplicationStatus table on the sink.
        latchSnapshotSyncPlugin.await();
        validateSnapshotSyncPlugin(snapshotSyncPluginListener);
        statusUpdateLatch.await();

        Assert.assertEquals(TOTAL_SINK_STATUS_ENTRIES_INIT_SNAPSHOT_SYNC, sinkListener.getAccumulatedStatus().size());
        Assert.assertTrue(sinkListener.getAccumulatedStatus().get(2));
        Assert.assertFalse(sinkListener.getAccumulatedStatus().get(1));

        Set<String> streamsToReplicateSink = new HashSet<>();
        for (int i = 2; i <= 3; i++) {
            streamsToReplicateSink.add(TABLE_PREFIX + i);
        }
        pluginConfigFilePath = TEST_PLUGIN_CONFIG_PATH_SINK;
        verifyRollingUpgrade(false);

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

        latchSnapshotSyncPlugin = new CountDownLatch(2);
        snapshotSyncPluginListener = new SnapshotSyncPluginListener(latchSnapshotSyncPlugin);
        subscribeToSnapshotSyncPluginTable(snapshotSyncPluginListener);

        statusUpdateLatch = new CountDownLatch(NUM_SNAPSHOT_SYNC_UPDATES_ON_SINK_STATUS_TABLE);
        sinkListener = new ReplicationStatusListener(statusUpdateLatch, false);
        corfuStoreSink.subscribeListener(sinkListener, CORFU_SYSTEM_NAMESPACE, LR_STATUS_STREAM_TAG);


        // Now upgrade the source site
        pluginConfigFilePath = TEST_PLUGIN_CONFIG_PATH_SINK;
        openMapsAfterUpgradeSource(sourceOnlyStreams, sinkOnlyStreams);
        verifyRollingUpgrade(true);

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

    @SuppressWarnings("checkstyle:magicnumber")
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

    /**
     * Code coverage test for the simple LRRollingUpgradeHandler to test if we are able to successfully
     * 1. Simulate startRollingUpgrade()
     * 2. verify that rolling upgrade is detected
     * 3. simulate endRollingUpgrade()
     * 4. verify that data migration and rolling upgrade end is detected
     *
     * @throws Exception
     */
    @Test
    public void testLocalClusterRollingUpgrade() throws Exception {
        log.info(">> Setup replication for testing during rolling upgrade of active cluster");
        setupSourceAndSinkCorfu();

        verifyRollingUpgrade(true);

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

    private void verifyRollingUpgrade(boolean source) throws Exception {
        CorfuStore corfuStore;
        DefaultAdapterForUpgrade defaultAdapterForUpgrade;

        if (source) {
            corfuStore = corfuStoreSource;
            defaultAdapterForUpgrade = new DefaultAdapterForUpgradeSource(sourceRuntime);
        } else {
            corfuStore = corfuStoreSink;
            defaultAdapterForUpgrade = new DefaultAdapterForUpgradeSink(sinkRuntime);
        }

        corfuStore.openTable(CORFU_SYSTEM_NAMESPACE, REPLICATION_EVENT_TABLE_NAME,
                ReplicationEventInfoKey.class,
                ReplicationEvent.class,
                null,
                TableOptions.fromProtoSchema(ReplicationEvent.class));

        LRRollingUpgradeHandler rollingUpgradeHandler = new LRRollingUpgradeHandler(defaultAdapterForUpgrade);
        defaultAdapterForUpgrade.startRollingUpgrade(corfuStore);

        try (TxnContext txnContext = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            Assert.assertTrue(rollingUpgradeHandler.isLRUpgradeInProgress(txnContext));
            txnContext.commit();
        }

        defaultAdapterForUpgrade.endRollingUpgrade();

        try (TxnContext txnContext = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            Assert.assertFalse(rollingUpgradeHandler.isLRUpgradeInProgress(txnContext));
            txnContext.commit();
        }
    }
}
