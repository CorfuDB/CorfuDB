package org.corfudb.integration;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationStatusKey;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationStatusVal;
import org.corfudb.infrastructure.logreplication.proto.Sample;
import org.corfudb.infrastructure.logreplication.proto.Sample.IntValueTag;
import org.corfudb.infrastructure.logreplication.proto.Sample.Metadata;
import org.corfudb.infrastructure.logreplication.proto.Sample.StringKey;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.CorfuStoreMetadata.TableName;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStoreEntry;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager.REPLICATION_STATUS_TABLE;
import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

/**
 * This suite of tests validates the behavior of Log Replication when the set of streams to replicated is dynamically
 * built by querying registry table.
 */
@Slf4j
public class LogReplicationDynamicStreamIT extends LogReplicationAbstractIT {

    private Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> mapA;
    private Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> mapAStandby;
    private final int numWrites = 2000;

    private final String TEST_TAG = "test";

    /**
     * Sets the plugin path before starting any test
     */
    @Before
    public void setupPluginPath() throws Exception {
        String nettyConfigDynamic = "src/test/resources/transport/nettyConfig.properties";
        if(runProcess) {
            File f = new File(nettyConfigDynamic);
            this.pluginConfigFilePath = f.getAbsolutePath();
        } else {
            this.pluginConfigFilePath = nettyConfigDynamic;
        }

        // Initiate Source and Sink runtime and CorfuStore
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

    private void openMapAOnActive() throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        // Open to StreamA on Source Site
        mapA = corfuStoreActive.openTable(
                NAMESPACE,
                streamA,
                StringKey.class,
                IntValueTag.class,
                Metadata.class,
                TableOptions.fromProtoSchema(IntValueTag.class)
        );
        assertThat(mapA.count()).isEqualTo(0);
    }

    private void openMapAOnStandby() throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        // Open StreamA on Source Site
        mapAStandby = corfuStoreStandby.openTable(
                NAMESPACE,
                streamA,
                StringKey.class,
                IntValueTag.class,
                Metadata.class,
                TableOptions.fromProtoSchema(IntValueTag.class)
        );
    }

    private void writeToMap(Table<StringKey, IntValueTag, Metadata> map, boolean isActive,
                           int startIndex, int totalEntries) {
        int maxIndex = totalEntries + startIndex;
        CorfuStore corfuStore = isActive ? corfuStoreActive : corfuStoreStandby;
        for (int i = startIndex; i < maxIndex; i++) {
            try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
                txn.putRecord(map, StringKey.newBuilder().setKey(String.valueOf(i)).build(),
                        IntValueTag.newBuilder().setValue(i).build(), null);
                txn.commit();
            }
        }
    }

    private void verifyDataOnStandby(Table<StringKey, IntValueTag, Metadata> mapStandby,
                                    int expectNumEntries) {
        // Wait until data is fully replicated
        while (mapStandby.count() != expectNumEntries) {
            log.trace("Current map size on Sink:: {}", mapStandby.count());
            // Block until expected number of entries is reached
        }

        // Verify data is present in Sink Site
        assertThat(mapStandby.count()).isEqualTo(expectNumEntries);

        try (TxnContext txn = corfuStoreStandby.txn(CORFU_SYSTEM_NAMESPACE)) {
            for (int i = 0; i < expectNumEntries; i++) {
                StringKey key = StringKey.newBuilder().setKey(String.valueOf(i)).build();
                CorfuStoreEntry<StringKey, IntValueTag, Metadata> entry = txn.getRecord(mapStandby, key);
                assertThat(entry.getPayload()).isNotNull();
                assertThat(entry.getPayload()).isEqualTo(IntValueTag.newBuilder().setValue(i).build());
            }
            txn.commit();
        }
    }

    /*
     * Helper methods section end
     */

    /**
     * Note that we already have basic end-to-end test for snapshot sync
     * and log entry sync. {@link CorfuReplicationE2EIT} This test here
     * is mainly for validating their behavior when tables / streams are
     * only opened at Source side.
     *
     * (1) Set up Source and Sink CorfuRuntime and CorfuStore
     * (2) Open mapA only at Source, write some entries to it
     * (3) Start log replication server for snapshot sync
     * (4) Open mapA at Sink and verify data replicated successfully
     * (5) Write more entries to mapA at Source and verify log entry sync at Sink
     */
    @Test
    public void testSnapshotAndLogEntrySync() throws Exception {
        // Open mapA on Source
        openMapAOnActive();

        // writeToSource for initial snapshot sync
        writeToMap(mapA, true, 0, numWrites);

        // Confirm data does exist on Source Cluster
        assertThat(mapA.count()).isEqualTo(numWrites);

        startLogReplicatorServers();

        // Verify snapshot sync complete by checking log replication status is log entry sync state
        verifyInLogEntrySyncState();

        // Open mapA on Sink after log replication started
        openMapAOnStandby();

        // Verify succeed of snapshot sync
        verifyDataOnStandby(mapAStandby, numWrites);

        // Add Delta's for Log Entry Sync
        writeToMap(mapA, true, numWrites, numWrites / 2);

        // Verify log replication status is log entry sync
        verifyInLogEntrySyncState();

        // Verify succeed of log entry sync
        verifyDataOnStandby(mapAStandby, numWrites + (numWrites / 2));
    }

    /**
     * We cannot get the list of streams to replicate in advance. This test will verify it works
     * correctly when new streams are opened during log entry sync.
     *
     * (1) Perform basic snapshot and log entry sync, leave the clusters in log entry sync state
     * (2) Open a new stream mapB at Source, write some entries to it
     * (3) Verify at the Sink that mapB is replicated successfully during log entry sync
     */
    @Test
    public void testNewStreamsInLogEntrySync() throws Exception {
        // perform basic snapshot and log entry sync, the cluster should be in log entry sync state now
        testSnapshotAndLogEntrySync();

        // Verify log replication status is log entry sync
        verifyInLogEntrySyncState();

        String streamB = "Table002";
        // open mapB at Source
        Table<StringKey, IntValueTag, Metadata> mapB = corfuStoreActive.openTable(
                NAMESPACE,
                streamB,
                StringKey.class,
                IntValueTag.class,
                Metadata.class,
                TableOptions.fromProtoSchema(IntValueTag.class)
        );

        writeToMap(mapB, true, 0, numWrites);

        // open mapB at Sink
        Table<StringKey, IntValueTag, Metadata> mapBStandby = corfuStoreStandby.openTable(
                NAMESPACE,
                streamB,
                StringKey.class,
                IntValueTag.class,
                Metadata.class,
                TableOptions.fromProtoSchema(IntValueTag.class)
        );

        verifyDataOnStandby(mapBStandby, numWrites);
    }

    /**
     * We need to clear local writes on Sink for streams (for replication) which are not MERGE_ONLY and have local writes.
     *
     * (1) Set up Source and Sink corfu, open mapA in both of them
     * (2) Write some entries to Source mapA, and write some different entries to Sink mapA
     * (3) Open mapB on Sink, write some entries to it
     * (4) Start log replication servers, verify Sink mapA is consistent with Source mapA and Sink mapB is cleared
     */
    @Test
    public void testStandbyLocalWritesClearing() throws Exception {
        // Open mapA on Source and write entries
        openMapAOnActive();
        // Open mapA on Sink
        openMapAOnStandby();

        // Write to mapA on both sides before log replication starts
        writeToMap(mapA, true, 0, numWrites);
        writeToMap(mapAStandby, false, 2 * numWrites, numWrites / 2);

        // Open mapB on Sink and write entries
        String streamB = "Table002";
        Table<StringKey, IntValueTag, Metadata> mapBStandby = corfuStoreStandby.openTable(
                NAMESPACE,
                streamB,
                StringKey.class,
                IntValueTag.class,
                Metadata.class,
                TableOptions.fromProtoSchema(IntValueTag.class)
        );
        writeToMap(mapBStandby, false, 0, numWrites);

        // Start log replication
        startLogReplicatorServers();

        // Verify mapA is successfully replicated
        verifyDataOnStandby(mapAStandby, numWrites);
        // Verify old entries of mapA are cleared on Sink
        try (TxnContext txn = corfuStoreStandby.txn(CORFU_SYSTEM_NAMESPACE)) {
            for (int i = 2 * numWrites; i < 2 * numWrites + numWrites / 2; i++) {
                StringKey key = StringKey.newBuilder().setKey(String.valueOf(i)).build();
                CorfuStoreEntry<StringKey, IntValueTag, Metadata> entry = txn.getRecord(mapAStandby, key);
                assertThat(entry.getPayload()).isNull();
            }
            txn.commit();
        }
        // Verify mapB on Sink is cleared
        assertThat(mapBStandby.count()).isEqualTo(0);
    }

    /**
     * This test will verify the stream listener could correctly receive the updates during snapshot sync.
     *
     * (1) Open table with TEST_TAG on Source and Sink
     * (2) Initiate a testing stream listener and subscribe to TEST_TAG on Sink
     * (3) Write some entries to the table on Source
     * (4) Start log replication snapshot sync and verify the test listener received all the updates
     */
    @Test
    public void testStandbyStreamingSnapshotSync() throws Exception {
        // Open testing map on Source and Sink
        String streamName = "TableStreaming001";

        Table<StringKey, IntValueTag, Metadata> mapTagOne = corfuStoreActive.openTable(
                NAMESPACE,
                streamName,
                StringKey.class,
                IntValueTag.class,
                Metadata.class,
                TableOptions.fromProtoSchema(IntValueTag.class)
        );

        Table<StringKey, IntValueTag, Metadata> mapTagOneStandby = corfuStoreStandby.openTable(
                NAMESPACE,
                streamName,
                StringKey.class,
                IntValueTag.class,
                Metadata.class,
                TableOptions.fromProtoSchema(IntValueTag.class)
        );

        // Subscribe the testing stream listener
        UUID streamId = CorfuRuntime.getStreamID(mapTagOne.getFullyQualifiedTableName());
        // The plus one is for the clear entry before applying the stream updates
        int expectedMessageSize = numWrites + 1;
        int numTables = 1;
        CountDownLatch streamingStandbySnapshotCompletion = new CountDownLatch(expectedMessageSize);
        CountDownLatch numTxLatch = new CountDownLatch(numTables);
        StreamingStandbyListener listener = new StreamingStandbyListener(streamingStandbySnapshotCompletion,
                numTxLatch, Collections.singleton(streamId));
        corfuStoreStandby.subscribeListener(listener, NAMESPACE, TEST_TAG);

        writeToMap(mapTagOne, true, 0, numWrites);

        startLogReplicatorServers();

        // Verify snapshot sync is succeed and stream listener received all the changes
        verifyDataOnStandby(mapTagOneStandby, numWrites);

        streamingStandbySnapshotCompletion.await();
        numTxLatch.await();
        assertThat(listener.messages.size()).isEqualTo(expectedMessageSize);
    }

    /**
     * This test will verify the stream listener could correctly receive the updates during log entry sync.
     *
     * (1) Perform a basic snapshot sync and log entry sync, verify the cluster is in log entry sync state
     * (2) Open a new map on both Source and Sink with TEST_TAG
     * (3) Initiate a testing stream listener and subscribe to TEST_TAG on Sink
     * (4) Write some new entries to this new map on Source
     * (5) Verify new data get replicated and the stream listener received all the updates
     */
    @Test
    public void testStandbyStreamingLogEntrySync() throws Exception {
        // perform basic snapshot and log entry sync, the cluster should be in log entry sync state now
        testSnapshotAndLogEntrySync();

        // Verify log replication status is log entry sync
        verifyInLogEntrySyncState();

        // Open testing map on Source and Sink
        String streamName = "TableStreaming001";

        Table<StringKey, IntValueTag, Metadata> mapTagOne = corfuStoreActive.openTable(
                NAMESPACE,
                streamName,
                StringKey.class,
                IntValueTag.class,
                Metadata.class,
                TableOptions.fromProtoSchema(IntValueTag.class)
        );

        Table<StringKey, IntValueTag, Metadata> mapTagOneStandby = corfuStoreStandby.openTable(
                NAMESPACE,
                streamName,
                StringKey.class,
                IntValueTag.class,
                Metadata.class,
                TableOptions.fromProtoSchema(IntValueTag.class)
        );

        // Subscribe the testing stream listener
        UUID streamId = CorfuRuntime.getStreamID(mapTagOne.getFullyQualifiedTableName());
        CountDownLatch streamingStandbySnapshotCompletion = new CountDownLatch(numWrites);
        CountDownLatch numTxLatch = new CountDownLatch(numWrites);
        StreamingStandbyListener listener = new StreamingStandbyListener(streamingStandbySnapshotCompletion,
                numTxLatch, Collections.singleton(streamId));
        corfuStoreStandby.subscribeListener(listener, NAMESPACE, TEST_TAG);

        writeToMap(mapTagOne, true, 0, numWrites);

        // Verify snapshot sync succeeded and stream listener received all the changes
        verifyDataOnStandby(mapTagOneStandby, numWrites);

        streamingStandbySnapshotCompletion.await();
        numTxLatch.await();
        assertThat(listener.messages.size()).isEqualTo(numWrites);
    }

    /**
     * This test will verify that if a stream to replicate is not opened on Sink, it could still be applied successfully
     * during snapshot sync, and its stream tags map could be correctly rebuilt.
     *
     * (1) Open a new map only on Source side with TEST_TAG
     * (2) Write some new entries to this new map on Source
     * (3) Start log replication
     * (4) Initiate a testing stream listener and subscribe to TEST_TAG on Sink from timestamp 0
     * (5) Verify new data get replicated and the stream listener received all the updates
     */
    @Test
    public void testStandbyStreamingSnapshotSyncNewTable() throws Exception {
        // Open testing map on Source and Sink
        String streamName = "TableStreaming001";

        Table<StringKey, IntValueTag, Metadata> mapTagOne = corfuStoreActive.openTable(
                NAMESPACE,
                streamName,
                StringKey.class,
                IntValueTag.class,
                Metadata.class,
                TableOptions.fromProtoSchema(IntValueTag.class)
        );

        writeToMap(mapTagOne, true, 0, numWrites);

        // The stream to tags map will be rebuilt upon receiving the replicated data
        startLogReplicatorServers();

        // Make sure Sink side record comes from snapshot sync instead of opening table by itself.
        while (!standbyRuntime.getTableRegistry().getRegistryTable().containsKey(
                TableName.newBuilder().setTableName(streamName).setNamespace(NAMESPACE).build())) {
            log.trace("Test table record hasn't been replicated");
        }

        Table<StringKey, IntValueTag, Metadata> mapTagOneStandby = corfuStoreStandby.openTable(
                NAMESPACE,
                streamName,
                StringKey.class,
                IntValueTag.class,
                Metadata.class,
                TableOptions.fromProtoSchema(IntValueTag.class)
        );

        // Subscribe the testing stream listener
        UUID streamId = CorfuRuntime.getStreamID(mapTagOne.getFullyQualifiedTableName());
        // The plus one is for the clear entry before applying the stream updates
        int expectedMessageSize = numWrites + 1;
        int numTables = 1;
        CountDownLatch streamingStandbySnapshotCompletion = new CountDownLatch(expectedMessageSize);
        CountDownLatch numTxLatch = new CountDownLatch(numTables);
        StreamingStandbyListener listener = new StreamingStandbyListener(streamingStandbySnapshotCompletion,
                numTxLatch, Collections.singleton(streamId));
        // Subscription is done from the beginning of the log (ts=0), so all updates written before the
        // subscription should be received.
        corfuStoreStandby.subscribeListener(listener, NAMESPACE, TEST_TAG,
                CorfuStoreMetadata.Timestamp.newBuilder().setEpoch(0).setSequence(0).build());

        // Verify the stream listener received all the updates.
        streamingStandbySnapshotCompletion.await();
        numTxLatch.await();
        assertThat(listener.messages.size()).isEqualTo(expectedMessageSize);

        // Verify snapshot sync succeeded and stream listener received all the changes
        verifyDataOnStandby(mapTagOneStandby, numWrites);
    }

    /**
     * This test will verify that if a stream to replicate is not opened on Sink, it could still be applied successfully
     * during log entry sync, and its stream tags map could be correctly rebuilt.
     *
     * (1) Perform a basic snapshot sync and log entry sync, verify the cluster is in log entry sync state
     * (2) Open a new map only on Source side with TEST_TAG
     * (3) Write some new entries to this new map on Source
     * (4) Initiate a testing stream listener and subscribe to TEST_TAG on Sink from timestamp 0
     * (5) Verify new data get replicated and the stream listener received all the updates
     */
    @Test
    public void testStandbyStreamingLogEntrySyncNewTable() throws Exception {
        // perform basic snapshot and log entry sync, the cluster should be in log entry sync state now
        testSnapshotAndLogEntrySync();

        // Verify log replication status is log entry sync
        verifyInLogEntrySyncState();

        // Open testing map on Source and Sink
        String streamName = "TableStreaming001";

        Table<StringKey, IntValueTag, Metadata> mapTagOne = corfuStoreActive.openTable(
                NAMESPACE,
                streamName,
                StringKey.class,
                IntValueTag.class,
                Metadata.class,
                TableOptions.fromProtoSchema(IntValueTag.class)
        );
        writeToMap(mapTagOne, true, 0, numWrites);

        // Make sure Sink side record comes from log entry sync instead of opening table by itself.
        while (!standbyRuntime.getTableRegistry().getRegistryTable().containsKey(
                TableName.newBuilder().setTableName(streamName).setNamespace(NAMESPACE).build())) {
            log.trace("Test table record hasn't been replicated");
        }

        // Open table through CorfuStore is required before creating the streaming task.
        Table<StringKey, IntValueTag, Metadata> mapTagOneStandby = corfuStoreStandby.openTable(
                NAMESPACE,
                streamName,
                StringKey.class,
                IntValueTag.class,
                Metadata.class,
                TableOptions.fromProtoSchema(IntValueTag.class)
        );

        // Subscribe the testing stream listener
        UUID streamId = CorfuRuntime.getStreamID(mapTagOne.getFullyQualifiedTableName());
        CountDownLatch streamingStandbySnapshotCompletion = new CountDownLatch(numWrites);
        CountDownLatch numTxLatch = new CountDownLatch(numWrites);
        StreamingStandbyListener listener = new StreamingStandbyListener(streamingStandbySnapshotCompletion,
                numTxLatch, Collections.singleton(streamId));
        // Subscription is done from the beginning of the log (ts=0), so all updates written before the
        // subscription should be received.
        corfuStoreStandby.subscribeListener(listener, NAMESPACE, TEST_TAG,
                CorfuStoreMetadata.Timestamp.newBuilder().setEpoch(0).setSequence(0).build());

        // Verify the stream listener received all the updates.
        streamingStandbySnapshotCompletion.await();
        numTxLatch.await();
        assertThat(listener.messages.size()).isEqualTo(numWrites);

        // Verify snapshot sync succeeded and stream listener received all the changes
        verifyDataOnStandby(mapTagOneStandby, numWrites);
    }
}
