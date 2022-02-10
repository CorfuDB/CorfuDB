package org.corfudb.integration;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterConfig;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultLogReplicationConfigAdapter;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationStatusKey;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationStatusVal;
import org.corfudb.infrastructure.logreplication.proto.Sample.IntValue;
import org.corfudb.infrastructure.logreplication.proto.Sample.IntValueTag;
import org.corfudb.infrastructure.logreplication.proto.Sample.Metadata;
import org.corfudb.infrastructure.logreplication.proto.Sample.StringKey;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata.TableName;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.test.SampleSchema.OptionTagOne;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager.REPLICATION_STATUS_TABLE;

/**
 * This suite of tests validates the behavior of Log Replication
 * when the set of streams to replicated is dynamically built by
 * querying registry table instead of provided by static file.
 */
@Slf4j
public class LogReplicationDynamicStreamIT extends LogReplicationAbstractIT {

    /**
     * Sets the plugin path before starting any test
     */
    @Before
    public void setupPluginPath() throws Exception {
        if(runProcess) {
            File f = new File(nettyConfig);
            this.pluginConfigFilePath = f.getAbsolutePath();
        } else {
            this.pluginConfigFilePath = nettyConfig;
        }

        // Initiate active and standby runtime and CorfuStore
        setupActiveAndStandbyCorfu();

        // Open replication status table to for verification purpose
        corfuStoreActive.openTable(LogReplicationMetadataManager.NAMESPACE,
                REPLICATION_STATUS_TABLE,
                ReplicationStatusKey.class,
                ReplicationStatusVal.class,
                null,
                TableOptions.fromProtoSchema(ReplicationStatusVal.class));

        corfuStoreStandby.openTable(LogReplicationMetadataManager.NAMESPACE,
                REPLICATION_STATUS_TABLE,
                ReplicationStatusKey.class,
                ReplicationStatusVal.class,
                null,
                TableOptions.fromProtoSchema(ReplicationStatusVal.class));
    }

    /*
     * Helper methods section begin
     */

    public void openMapAOnActive() throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        // Write to StreamA on Active Site
        mapA = corfuStoreActive.openTable(
                NAMESPACE,
                streamA,
                StringKey.class,
                IntValue.class,
                Metadata.class,
                TableOptions.fromProtoSchema(IntValueTag.class)
        );
        assertThat(mapA.count()).isEqualTo(0);
    }

    public void openMapAOnStandby() throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        // Write to StreamA on Active Site
        mapAStandby = corfuStoreStandby.openTable(
                NAMESPACE,
                streamA,
                StringKey.class,
                IntValue.class,
                Metadata.class,
                TableOptions.fromProtoSchema(IntValueTag.class)
        );
    }

    public void writeToMap(Table<StringKey, IntValue, Metadata> map, boolean isActive,
                                   int startIndex, int totalEntries) {
        int maxIndex = totalEntries + startIndex;
        CorfuStore corfuStore = isActive ? corfuStoreActive : corfuStoreStandby;
        for (int i = startIndex; i < maxIndex; i++) {
            try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
                txn.putRecord(map, StringKey.newBuilder().setKey(String.valueOf(i)).build(),
                        IntValue.newBuilder().setValue(i).build(), null);
                txn.commit();
            }
        }
    }

    public void verifyDataOnStandby(Table<StringKey, IntValue, Metadata> mapStandby,
                                    int expectNumEntries) {
        // Wait until data is fully replicated
        while (mapStandby.count() != expectNumEntries) {
            log.trace("Current map size on Standby:: {}", mapStandby.count());
            // Block until expected number of entries is reached
        }

        // Verify data is present in Standby Site
        assertThat(mapStandby.count()).isEqualTo(expectNumEntries);

        for (int i = 0; i < expectNumEntries; i++) {
            assertThat(mapStandby.containsKey(StringKey
                    .newBuilder().setKey(String.valueOf(i)).build())).isTrue();
            assertThat(mapStandby.get(StringKey
                    .newBuilder().setKey(String.valueOf(i)).build()).getPayload()).isEqualTo(
                    IntValue.newBuilder().setValue(i).build()
            );
        }
    }

    public void verifyLogEntrySyncStatus() {
        ReplicationStatusKey key = ReplicationStatusKey
                .newBuilder()
                .setClusterId(DefaultClusterConfig.getStandbyClusterId())
                .build();

        ReplicationStatusVal replicationStatusVal;
        try (TxnContext txn = corfuStoreActive.txn(LogReplicationMetadataManager.NAMESPACE)) {
            replicationStatusVal = (ReplicationStatusVal)txn.getRecord(REPLICATION_STATUS_TABLE, key).getPayload();
            txn.commit();
        }

        assertThat(replicationStatusVal.getSyncType())
                .isEqualTo(LogReplicationMetadata.ReplicationStatusVal.SyncType.LOG_ENTRY);
        assertThat(replicationStatusVal.getStatus())
                .isEqualTo(LogReplicationMetadata.SyncStatus.ONGOING);

        assertThat(replicationStatusVal.getSnapshotSyncInfo().getType())
                .isEqualTo(LogReplicationMetadata.SnapshotSyncInfo.SnapshotSyncType.DEFAULT);
        assertThat(replicationStatusVal.getSnapshotSyncInfo().getStatus())
                .isEqualTo(LogReplicationMetadata.SyncStatus.COMPLETED);
    }

    /*
     * Helper methods section end
     */

    /**
     * Note that we already have basic end-to-end test for snapshot sync
     * and log entry sync. {@link CorfuReplicationE2EIT} This test here
     * is mainly for validating their behavior when tables / streams are
     * only opened at ACTIVE side.
     *
     * (1) Set up ACTIVE and STANDBY CorfuRuntime and CorfuStore
     * (2) Open mapA only at ACTIVE, write some entries to it
     * (3) Start log replication server for snapshot sync
     * (4) Open mapA at STANDBY and verify data replicated successfully
     * (5) Write more entries to mapA at ACTIVE and verify log entry sync at STANDBY
     */
    @Test
    public void testSnapshotAndLogEntrySync() throws Exception {
        // Open mapA on ACTIVE
        openMapAOnActive();

        // writeToActive for initial snapshot sync
        writeToMap(mapA, true, 0, numWrites);

        // Confirm data does exist on Active Cluster
        assertThat(mapA.count()).isEqualTo(numWrites);

        startLogReplicatorServers();

        // Open mapA on STANDBY after log replication started
        openMapAOnStandby();

        // Verify succeed of snapshot sync
        verifyDataOnStandby(mapAStandby, numWrites);

        // Add Delta's for Log Entry Sync
        writeToMap(mapA, true, numWrites, numWrites / 2);

        // Verify log replication status is log entry sync
        verifyLogEntrySyncStatus();

        // Verify succeed of log entry sync
        verifyDataOnStandby(mapAStandby, numWrites + (numWrites / 2));
    }

    /**
     * As in dynamic streams implementation, we cannot get the list of streams to
     * replicate in advance. This test will verify it works correctly when new streams
     * are opened during log entry sync.
     *
     * (1) Perform basic snapshot and log entry sync, leave the clusters in log entry sync state
     * (2) Open a new stream mapB at ACTIVE, write some entries to it
     * (3) Verify at the STANDBY that mapB is replicated successfully during log entry sync
     */
    @Test
    public void testNewStreamsInLogEntrySync() throws Exception {
        // perform basic snapshot and log entry sync, the cluster should be in log entry sync state now
        testSnapshotAndLogEntrySync();

        // Verify log replication status is log entry sync
        verifyLogEntrySyncStatus();

        String streamB = "Table002";
        // open mapB at ACTIVE
        Table<StringKey, IntValue, Metadata> mapB = corfuStoreActive.openTable(
                NAMESPACE,
                streamB,
                StringKey.class,
                IntValue.class,
                Metadata.class,
                TableOptions.fromProtoSchema(IntValueTag.class)
        );

        writeToMap(mapB, true, 0, numWrites);

        // open mapB at STANDBY
        Table<StringKey, IntValue, Metadata> mapBStandby = corfuStoreStandby.openTable(
                NAMESPACE,
                streamB,
                StringKey.class,
                IntValue.class,
                Metadata.class,
                TableOptions.fromProtoSchema(IntValueTag.class)
        );

        verifyDataOnStandby(mapBStandby, numWrites);
    }

    /**
     * In log entry sync when new streams are opened, we cannot guarantee new entries in registry
     * table could come before the entries in new streams. The new implementation of LogEntryWriter
     * will apply entries of those streams later.
     *
     * (1) Perform basic snapshot sync and log entry sync, verify the cluster is in log entry sync status
     * (2) Stop ACTIVE log replicator, such that the new entries in registry table will not be replicated immediately
     * (3) Open two maps (mapB and mapC) on ACTIVE and write entries to them
     * (4) Remove their registration from registry table and restart ACTIVE log replicator
     * (5) Sleep the thread for a while and reopened those two table on ACTIVE, verify they are successfully replicated
     */
    @SuppressWarnings("checkstyle:magicnumber")
    @Test
    public void testLogEntrySyncPendingEntries() throws Exception {
        testSnapshotAndLogEntrySync();

        stopActiveLogReplicator();

        verifyLogEntrySyncStatus();

        String streamB = "Table002";
        String streamC = "Table003";
        // open mapB and mapC at ACTIVE
        Table<StringKey, IntValue, Metadata> mapB = corfuStoreActive.openTable(
                NAMESPACE,
                streamB,
                StringKey.class,
                IntValue.class,
                Metadata.class,
                TableOptions.fromProtoSchema(IntValueTag.class)
        );

        Table<StringKey, IntValue, Metadata> mapC = corfuStoreActive.openTable(
                NAMESPACE,
                streamC,
                StringKey.class,
                IntValue.class,
                Metadata.class,
                TableOptions.fromProtoSchema(IntValueTag.class)
        );

        writeToMap(mapB, true, 0, numWrites);
        writeToMap(mapC, true, 0, numWrites);

        // Remove the entry of mapB and mapC from registry table, such that we can make sure the
        // entries of registry table will not be replicated before the entries of mapB and mapC
        activeRuntime.getTableRegistry().getRegistryTable().remove(TableName.newBuilder()
                .setNamespace(NAMESPACE).setTableName(streamB).build());
        activeRuntime.getTableRegistry().getRegistryTable().remove(TableName.newBuilder()
                .setNamespace(NAMESPACE).setTableName(streamC).build());

        startActiveLogReplicator();

        verifyLogEntrySyncStatus();

        Thread.sleep(3000);
        // open mapB at ACTIVE to register it to registry table again
        corfuStoreActive.openTable(
                NAMESPACE,
                streamB,
                StringKey.class,
                IntValue.class,
                Metadata.class,
                TableOptions.fromProtoSchema(IntValueTag.class)
        );

        corfuStoreActive.openTable(
                NAMESPACE,
                streamC,
                StringKey.class,
                IntValue.class,
                Metadata.class,
                TableOptions.fromProtoSchema(IntValueTag.class)
        );


        // open mapB and mapC at STANDBY
        Table<StringKey, IntValue, Metadata> mapBStandby = corfuStoreStandby.openTable(
                NAMESPACE,
                streamB,
                StringKey.class,
                IntValue.class,
                Metadata.class,
                TableOptions.fromProtoSchema(IntValueTag.class)
        );

        Table<StringKey, IntValue, Metadata> mapCStandby = corfuStoreStandby.openTable(
                NAMESPACE,
                streamC,
                StringKey.class,
                IntValue.class,
                Metadata.class,
                TableOptions.fromProtoSchema(IntValueTag.class)
        );

        verifyDataOnStandby(mapBStandby, numWrites);
        verifyDataOnStandby(mapCStandby, numWrites);
    }

    /**
     * Before dynamic stream, we clear the whole list of streams to replicate in snapshot
     * sync. For now, we can only clear local writes on-demand. This test will verify local
     * writes are cleared (STANDBY consistent with ACTIVE) in snapshot sync.
     *
     * (1) Set up ACTIVE and STANDBY corfu, open mapA in both of them
     * (2) Write some entries to ACTIVE mapA, and write some different entries to STANDBY mapA
     * (3) Start log replication, verify the STANDBY mapA is consistent with ACTIVE mapA
     */
    @Test
    public void testLocalStreamClearingSnapshotSync() throws Exception {
        // Open mapA on ACTIVE
        openMapAOnActive();
        // Open mapA on STANDBY
        openMapAOnStandby();

        // Write to mapA on both sides before log replication starts
        writeToMap(mapA, true, 0, numWrites);
        writeToMap(mapAStandby, false, 2 * numWrites, numWrites / 2);

        startLogReplicatorServers();

        // Verify succeed of snapshot sync
        verifyDataOnStandby(mapAStandby, numWrites);

        // Verify old entries are cleared at STANDBY
        for (int i = 2 * numWrites; i < 2 * numWrites + numWrites / 2; i++) {
            assertThat(mapAStandby.containsKey(StringKey
                    .newBuilder().setKey(String.valueOf(i)).build())).isFalse();
        }
    }

    /**
     * Similar to {@link LogReplicationDynamicStreamIT#testLocalStreamClearingSnapshotSync()}
     * This test will verify local writes are cleared in log entry sync.
     *
     * (1) Perform a basic snapshot sync and log entry sync on a single map
     * (2) Verify the log entry sync status and open a new mapB on STANDBY and write some entries to it
     * (3) Open mapB on ACTIVE and write some entries to it
     * (4) Verify mapB on STANDBY is consistent with mapB on ACTIVE
     */
    @Test
    public void testLocalStreamClearingLogEntrySync() throws Exception {
        // perform basic snapshot and log entry sync, the cluster should be in log entry sync state now
        testSnapshotAndLogEntrySync();

        // Verify log replication status is log entry sync
        verifyLogEntrySyncStatus();

        // Open mapB on STANDBY and write entries
        String streamB = "Table002";

        Table<StringKey, IntValue, Metadata> mapBStandby = corfuStoreStandby.openTable(
                NAMESPACE,
                streamB,
                StringKey.class,
                IntValue.class,
                Metadata.class,
                TableOptions.fromProtoSchema(IntValueTag.class)
        );
        writeToMap(mapBStandby, false, 2 * numWrites, numWrites / 2);

        // Open mapB on ACTIVE and write entries
        Table<StringKey, IntValue, Metadata> mapB = corfuStoreActive.openTable(
                NAMESPACE,
                streamB,
                StringKey.class,
                IntValue.class,
                Metadata.class,
                TableOptions.fromProtoSchema(IntValueTag.class)
        );
        writeToMap(mapB, true, 0, numWrites);

        // Verify succeed of snapshot sync
        verifyDataOnStandby(mapBStandby, numWrites);

        // Verify old entries are cleared at STANDBY
        for (int i = 2 * numWrites; i < 2 * numWrites + (numWrites) / 2; i++) {
            assertThat(mapAStandby.containsKey(StringKey
                    .newBuilder().setKey(String.valueOf(i)).build())).isFalse();
        }
    }

    /**
     * We need to clear local writes on STANDBY for streams with is_federated set to be true,
     * even if those streams are not initially replicated or opened on ACTIVE.
     *
     * (1) Open mapA on ACTIVE and mapB on STANDBY, write different entries to them
     * (2) Start log replication servers
     * (3) Verify mapA is successfully replicated and mapB is cleared on STANDBY
     */
    @Test
    public void testStandbyLocalWritesClearing() throws Exception {
        // Open mapA on ACTIVE and write entries
        openMapAOnActive();
        writeToMap(mapA, true, 0, numWrites);

        // Open mapB on STANDBY and write entries
        String streamB = "Table002";
        Table<StringKey, IntValue, Metadata> mapBStandby = corfuStoreStandby.openTable(
                NAMESPACE,
                streamB,
                StringKey.class,
                IntValue.class,
                Metadata.class,
                TableOptions.fromProtoSchema(IntValueTag.class)
        );
        writeToMap(mapBStandby, false, 0, numWrites);

        // Start log replication
        startLogReplicatorServers();

        // Verify mapA is successfully replicated
        openMapAOnStandby();
        verifyDataOnStandby(mapAStandby, numWrites);

        // Verify mapB on STANDBY is cleared
        assertThat(mapBStandby.count()).isEqualTo(0);
    }

    /**
     * This test will verify the streams to stream tags map is correctly rebuilt during
     * snapshot sync.
     *
     * (1) Open table with TAG_ONE on ACTIVE and STANDBY
     * (2) Initiate a testing stream listener and subscribe to TAG_ONE on STANDBY
     * (3) Write some entries to the table on ACTIVE
     * (4) Start log replication snapshot sync and verify the test listener received all the updates
     */
    @Test
    public void testStandbyStreamingSnapshotSync() throws Exception {
        // Open testing map on ACTIVE and STANDBY
        String streamName = "TableStreaming001";

        Table<StringKey, IntValue, Metadata> mapTagOne = corfuStoreActive.openTable(
                NAMESPACE,
                streamName,
                StringKey.class,
                IntValue.class,
                Metadata.class,
                TableOptions.fromProtoSchema(OptionTagOne.class)
        );

        Table<StringKey, IntValue, Metadata> mapTagOneStandby = corfuStoreStandby.openTable(
                NAMESPACE,
                streamName,
                StringKey.class,
                IntValue.class,
                Metadata.class,
                TableOptions.fromProtoSchema(OptionTagOne.class)
        );

        // Subscribe the testing stream listener
        UUID streamId = CorfuRuntime.getStreamID(mapTagOne.getFullyQualifiedTableName());
        CountDownLatch streamingStandbySnapshotCompletion = new CountDownLatch(numWrites);
        StreamingStandbyListener listener = new StreamingStandbyListener(streamingStandbySnapshotCompletion,
                Collections.singleton(streamId));
        corfuStoreStandby.subscribeListener(listener, NAMESPACE, DefaultLogReplicationConfigAdapter.TAG_ONE);

        writeToMap(mapTagOne, true, 0, numWrites);

        // The stream to tags map is rebuilt upon receiving the replicated data
        startLogReplicatorServers();

        // Verify snapshot sync is succeed and stream listener received all the changes
        verifyDataOnStandby(mapTagOneStandby, numWrites);

        streamingStandbySnapshotCompletion.await();
        assertThat(listener.messages.size()).isEqualTo(numWrites);
    }

    /**
     * This test will verify the streams to stream tags map is correctly rebuilt during
     * log entry sync.
     *
     * (1) Perform a basic snapshot sync and log entry sync, verify the cluster is in log entry sync state
     * (2) Open a new map on both ACTIVE and STANDBY with TAG_ONE
     * (3) Initiate a testing stream listener and subscribe to TAG_ONE on STANDBY
     * (4) Write some new entries to this new map on ACTIVE
     * (5) Verify new data get replicated and the stream listener received all the updates
     */
    @Test
    public void testStandbyStreamingLogEntrySync() throws Exception {
        // perform basic snapshot and log entry sync, the cluster should be in log entry sync state now
        testSnapshotAndLogEntrySync();

        // Verify log replication status is log entry sync
        verifyLogEntrySyncStatus();

        // Open testing map on ACTIVE and STANDBY
        String streamName = "TableStreaming001";

        Table<StringKey, IntValue, Metadata> mapTagOne = corfuStoreActive.openTable(
                NAMESPACE,
                streamName,
                StringKey.class,
                IntValue.class,
                Metadata.class,
                TableOptions.fromProtoSchema(OptionTagOne.class)
        );

        Table<StringKey, IntValue, Metadata> mapTagOneStandby = corfuStoreStandby.openTable(
                NAMESPACE,
                streamName,
                StringKey.class,
                IntValue.class,
                Metadata.class,
                TableOptions.fromProtoSchema(OptionTagOne.class)
        );

        // Subscribe the testing stream listener
        UUID streamId = CorfuRuntime.getStreamID(mapTagOne.getFullyQualifiedTableName());
        CountDownLatch streamingStandbySnapshotCompletion = new CountDownLatch(numWrites);
        StreamingStandbyListener listener = new StreamingStandbyListener(streamingStandbySnapshotCompletion,
                Collections.singleton(streamId));
        corfuStoreStandby.subscribeListener(listener, NAMESPACE, DefaultLogReplicationConfigAdapter.TAG_ONE);

        writeToMap(mapTagOne, true, 0, numWrites);

        // Verify snapshot sync succeeded and stream listener received all the changes
        verifyDataOnStandby(mapTagOneStandby, numWrites);

        streamingStandbySnapshotCompletion.await();
        assertThat(listener.messages.size()).isEqualTo(numWrites);
    }

    /**
     * Similar to {@link LogReplicationDynamicStreamIT#testLogEntrySyncPendingEntries()}, this test will
     * verify STANDBY streaming against pending entries.
     *
     * (1) Perform basic snapshot sync and log entry sync, verify the cluster is in log entry sync status
     * (2) Stop ACTIVE log replicator, such that the new entries in registry table will not be replicated immediately
     * (3) Open a new map with TAG_ONE
     * (4) Remove their registration from registry table and restart ACTIVE log replicator
     * (5) Sleep the thread for a while and reopen the table, verify replication and streaming status
     */
    @SuppressWarnings("checkstyle:magicnumber")
    @Test
    public void testStandbyStreamingOnPendingEntries() throws Exception {
        testSnapshotAndLogEntrySync();

        stopActiveLogReplicator();

        verifyLogEntrySyncStatus();

        String streamName = "TableStreaming001";
        // open mapTagOne at ACTIVE
        Table<StringKey, IntValue, Metadata> mapTagOne = corfuStoreActive.openTable(
                NAMESPACE,
                streamName,
                StringKey.class,
                IntValue.class,
                Metadata.class,
                TableOptions.fromProtoSchema(OptionTagOne.class)
        );

        // open mapB at STANDBY
        Table<StringKey, IntValue, Metadata> mapTagOneStandby = corfuStoreStandby.openTable(
                NAMESPACE,
                streamName,
                StringKey.class,
                IntValue.class,
                Metadata.class,
                TableOptions.fromProtoSchema(OptionTagOne.class)
        );

        writeToMap(mapTagOne, true, 0, numWrites);

        // Remove the entry of mapTagOne from registry table, such that we can make sure the
        // entries of registry table will not be replicated before the entries of mapTagOne
        activeRuntime.getTableRegistry().getRegistryTable().remove(TableName.newBuilder()
                .setNamespace(NAMESPACE).setTableName(streamName).build());

        // Subscribe the testing stream listener
        UUID streamId = CorfuRuntime.getStreamID(mapTagOne.getFullyQualifiedTableName());
        CountDownLatch streamingStandbySnapshotCompletion = new CountDownLatch(numWrites);
        StreamingStandbyListener listener = new StreamingStandbyListener(streamingStandbySnapshotCompletion,
                Collections.singleton(streamId));
        corfuStoreStandby.subscribeListener(listener, NAMESPACE, DefaultLogReplicationConfigAdapter.TAG_ONE);

        // Remove the entry of mapTagOne from STANDBY registry table to make sure the log entries
        // went to pending entries list.
        standbyRuntime.getTableRegistry().getRegistryTable().remove(TableName.newBuilder()
                .setNamespace(NAMESPACE).setTableName(streamName).build());

        startActiveLogReplicator();

        verifyLogEntrySyncStatus();

        Thread.sleep(3000);
        // open mapTagOne at ACTIVE to register it to registry table again
        corfuStoreActive.openTable(
                NAMESPACE,
                streamName,
                StringKey.class,
                IntValue.class,
                Metadata.class,
                TableOptions.fromProtoSchema(OptionTagOne.class)
        );

        // Verify log entry sync succeeded and stream listener received all the changes
        verifyDataOnStandby(mapTagOneStandby, numWrites);

        streamingStandbySnapshotCompletion.await();
        assertThat(listener.messages.size()).isEqualTo(numWrites);
    }
}
