package org.corfudb.integration;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.LogReplication.ReplicationStatus;
import org.corfudb.infrastructure.logreplication.proto.Sample;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.runtime.LogReplicationUtils;
import org.corfudb.runtime.collections.CorfuStreamEntries;
import org.corfudb.runtime.collections.CorfuStreamEntry;
import org.corfudb.runtime.collections.StreamListener;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TableSchema;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.corfudb.runtime.view.ObjectsView;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.test.SampleSchema;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test the behavior of transaction batching on the Sink.
 * During snapshot sync, all entries to be applied are grouped in a single
 * transaction and applied atomically.  However, if the size of
 * updates to apply increases the runtime's maxWriteSize threshold, they are
 * applied in multiple transactions in chunks.
 *
 * This suite tests this behavior and verifies that snapshot writes on the Sink
 * contain multiple(possibly all) updates in a single transaction.
 */
@Slf4j
@SuppressWarnings("checkstyle:magicnumber")
public class CorfuReplicationLargeTxIT extends LogReplicationAbstractIT {

    private Map<String, Table<Sample.StringKey, SampleSchema.ValueFieldTagOne,
        Sample.Metadata>> mapNameToMapSource = new HashMap<>();

    private Map<String, Table<Sample.StringKey, SampleSchema.ValueFieldTagOne,
        Sample.Metadata>> mapNameToMapSink = new HashMap<>();

    private static final int NUM_ENTRIES_PER_TABLE = 20;

    // Max uncompressed transaction size(in bytes) for applying snapshot sync updates
    // It was observed that a single transaction on the Protobuf descriptor table was >9k bytes.  The Snapshot reader
    // currently does not have the ability to split a single transaction before sending it.  So the max write size
    // must be able to accommodate this much data as it will not be split when writing to the shadow stream on the
    // Sink also.
    private static final int MAX_WRITE_SIZE_BYTES = 10000;

    // Max number of entries applied in a single transaction during snapshot sync
    private static final int MAX_SNAPSHOT_ENTRIES_APPLIED = 1;

    /**
     * In LR Snapshot sync, the max payload size transferred is 85% of MAX_WRITE_SIZE_BYTES, i.e., 8500 bytes.
     * The Sink cluster also applies the same number of bytes in a single transaction during the apply phase.
     * It was empirically determined that NUM_ENTRIES_PER_TABLE had a serialized size of 5.5k bytes approx.  Hence, a
     * snapshot sync with this much data will be applied in a single transaction on the Sink
     * @throws Exception
     */
    @Test
    public void testAtomicSnapshotSyncWithoutChunking() throws Exception {
        testTxChunking(NUM_ENTRIES_PER_TABLE, 1);
    }

    /**
     * In LR Snapshot sync, the max payload size transferred is 85% of MAX_WRITE_SIZE_BYTES, i.e., 8500 bytes.
     * The Sink cluster also applies the same number of bytes in a single transaction during the apply phase.
     * It was empirically determined that NUM_ENTRIES_PER_TABLE had a serialized size of 5.5k bytes approx.  Hence, a
     * snapshot sync with twice this much data will be applied in 2 transactions on the Sink.
     * @throws Exception
     */
    @Test
    public void testSnapshotSyncWithChunking() throws Exception {
        testTxChunking(2*NUM_ENTRIES_PER_TABLE, 2);
    }

    private void testTxChunking(int numEntriesToWrite,
                                int expectedStreamingUpdatesPerTable) throws Exception {
        log.debug("Setup Source and Sink Corfu's");
        setupSourceAndSinkCorfu();
        initSingleSourceSinkCluster();

        log.debug("Open map on Source and Sink");
        openMaps(2, false);

        log.debug("Write data to Source CorfuDB before LR is started ...");
        writeOnSender(0, numEntriesToWrite);

        log.debug("Verify data exists on the Source and none on the Sink");
        // Confirm data does exist on source Cluster
        verifyDataOnSender(numEntriesToWrite);

        // Confirm data does not exist on Sink Cluster
        verifyDataOnReceiver(0);

        // Subscribe to replication status table on Sink (to be sure data
        // change on status are captured)
        int totalSinkStatusUpdates = 2;
        corfuStoreSink.openTable(LogReplicationMetadataManager.NAMESPACE,
            LogReplicationUtils.REPLICATION_STATUS_TABLE_NAME,
            LogReplicationSession.class,
            ReplicationStatus.class,
            null,
            TableOptions.fromProtoSchema(ReplicationStatus.class));

        CountDownLatch statusUpdateLatch = new CountDownLatch(totalSinkStatusUpdates);
        ReplicationStatusListener sinkListener =
            new ReplicationStatusListener(statusUpdateLatch, false);
        corfuStoreSink.subscribeListener(sinkListener, LogReplicationMetadataManager.NAMESPACE,
                LogReplicationUtils.LR_STATUS_STREAM_TAG);

        // Calculate the expected total number of streaming updates across all tables
        int totalStreamingUpdates =
            expectedStreamingUpdatesPerTable * mapNameToMapSink.size();

        // MAX_SNAPSHOT_ENTRIES_APPLIED = 1.  So entries to the ProtobufDescriptor table will be applied in a batch
        // of 1.  Hence, the number of transactions made = number of entries received from the Source Cluster(1 TX
        // per entry).
        int numExpectedTxOnProtobufDescriptorTable = sourceRuntime.getTableRegistry().getProtobufDescriptorTable().size();

        // The number of entries in the protobuf descriptor table on Source(or numExpectedTxOnProtobufDescriptorTable)
        // must not be equal to MAX_SNAPSHOT_ENTRIES_APPLIED.  Otherwise we cannot verify that it was applied in
        // batches.
        Assert.assertNotEquals(numExpectedTxOnProtobufDescriptorTable, MAX_SNAPSHOT_ENTRIES_APPLIED);
        CountDownLatch protobufDescriptorTxLatch = new CountDownLatch(numExpectedTxOnProtobufDescriptorTable);
        StreamingUpdateListener protobufDescriptorTxListener = new StreamingUpdateListener(protobufDescriptorTxLatch);
        corfuStoreSink.subscribeListener(protobufDescriptorTxListener, TableRegistry.CORFU_SYSTEM_NAMESPACE,
            ObjectsView.getLogReplicatorStreamId().toString(),
            Arrays.asList(TableRegistry.PROTOBUF_DESCRIPTOR_TABLE_NAME));

        CountDownLatch streamingUpdatesLatch = new CountDownLatch(totalStreamingUpdates);
        StreamingUpdateListener streamingUpdateListener = new StreamingUpdateListener(streamingUpdatesLatch);
        corfuStoreSink.subscribeListener(streamingUpdateListener, NAMESPACE, TAG_ONE);

        // Start LR on both clusters with custom write sizes.
        startLogReplicatorServersWithCustomMaxTxSize();

        log.debug("Wait for snapshot sync to finish");
        statusUpdateLatch.await();

        log.debug("Wait for the expected number of updates to replicated tables, other than the protobuf descriptor " +
            "table");
        streamingUpdatesLatch.await();

        log.debug("Wait for the expected number of updates to the Protobuf Descriptor table");
        protobufDescriptorTxLatch.await();

        // Verify that updates were received for all replicated tables with data
        Assert.assertEquals(mapNameToMapSink.size(), streamingUpdateListener.getTableNameToUpdatesMap().size());

        mapNameToMapSink.keySet().forEach(key ->
            Assert.assertTrue(streamingUpdateListener.getTableNameToUpdatesMap().containsKey(key)));

        // Verify that the right number of entries are contained in the
        // streaming update.  Also verify that the first entry is a 'clear'
        // followed by all updates.
        for (List<List<CorfuStreamEntry<Sample.StringKey,
            SampleSchema.ValueFieldTagOne, Sample.Metadata>>> outerList :
            streamingUpdateListener.getTableNameToUpdatesMap().values()) {

            Assert.assertEquals(expectedStreamingUpdatesPerTable, outerList.size());

            int totalEntriesReceived = 0;
            List<CorfuStreamEntry<Sample.StringKey,
                SampleSchema.ValueFieldTagOne, Sample.Metadata>> updateEntries =
                new ArrayList<>();

            for (int i=0; i<outerList.size(); i++) {
                // If the snapshot apply was chunked, the first entry in the
                // first chunk must be a 'clear'
                if (i == 0) {
                    Assert.assertEquals(CorfuStreamEntry.OperationType.CLEAR,
                        outerList.get(i).get(0).getOperation());
                    updateEntries.addAll(outerList.get(i).subList(1,
                        outerList.get(i).size()));
                } else {
                  updateEntries.addAll(outerList.get(i));
                }
                totalEntriesReceived += outerList.get(i).size();
            }
            // The total entries received must be equal to (numUpdates + clear)
            Assert.assertEquals(numEntriesToWrite+1, totalEntriesReceived);

            updateEntries.forEach(entry -> Assert.assertEquals(
                CorfuStreamEntry.OperationType.UPDATE, entry.getOperation()));
        }

        verifyDataOnReceiver(numEntriesToWrite);

        corfuStoreSink.unsubscribeListener(sinkListener);
        corfuStoreSink.unsubscribeListener(streamingUpdateListener);
        shutDown();
    }

    @Override
    public void openMaps(int mapCount, boolean diskBased) throws Exception {
        mapNameToMapSource = new HashMap<>();
        mapNameToMapSink = new HashMap<>();

        for (int i = 1; i <= mapCount; i++) {
            String mapName = TABLE_PREFIX + i;

            Table<Sample.StringKey, SampleSchema.ValueFieldTagOne, Sample.Metadata> mapSource =
                corfuStoreSource.openTable(NAMESPACE, mapName,
                    Sample.StringKey.class, SampleSchema.ValueFieldTagOne.class,
                    Sample.Metadata.class, TableOptions.fromProtoSchema(
                        SampleSchema.ValueFieldTagOne.class));

            Table<Sample.StringKey, SampleSchema.ValueFieldTagOne, Sample.Metadata> mapSink =
                corfuStoreSink.openTable(NAMESPACE, mapName,
                    Sample.StringKey.class, SampleSchema.ValueFieldTagOne.class,
                    Sample.Metadata.class, TableOptions.fromProtoSchema(
                        SampleSchema.ValueFieldTagOne.class));

            mapNameToMapSource.put(mapName, mapSource);
            mapNameToMapSink.put(mapName, mapSink);

            assertThat(mapSource.count()).isEqualTo(0);
            assertThat(mapSink.count()).isEqualTo(0);
        }
    }

    private void writeOnSender(int startIndex, int totalEntries) {
        int maxIndex = totalEntries + startIndex;

        for (Map.Entry<String, Table<Sample.StringKey,
            SampleSchema.ValueFieldTagOne, Sample.Metadata>> entry :
            mapNameToMapSource.entrySet()) {

            Table<Sample.StringKey, SampleSchema.ValueFieldTagOne,
                    Sample.Metadata> map = entry.getValue();

            for (int i = startIndex; i < maxIndex; i++) {
                Sample.StringKey stringKey = Sample.StringKey.newBuilder()
                        .setKey(String.valueOf(i)).build();
                SampleSchema.ValueFieldTagOne value = SampleSchema
                        .ValueFieldTagOne.newBuilder()
                        .setPayload(String.valueOf(i)).build();
                Sample.Metadata metadata = Sample.Metadata.newBuilder()
                        .setMetadata("Metadata_" + i).build();
                try (TxnContext txn = corfuStoreSource.txn(NAMESPACE)) {
                    txn.putRecord(map, stringKey, value, metadata);
                    txn.commit();
                }
            }
        }
    }

    private void verifyDataOnSender(int expectedSize) {
        for(Map.Entry<String, Table<Sample.StringKey,
            SampleSchema.ValueFieldTagOne, Sample.Metadata>> entry :
            mapNameToMapSource.entrySet()) {
            Table<Sample.StringKey, SampleSchema.ValueFieldTagOne,
                Sample.Metadata> map = entry.getValue();
            Assert.assertEquals(expectedSize, map.count());
        }
    }

    private void verifyDataOnReceiver(long expectedSize) {
        for(Map.Entry<String, Table<Sample.StringKey,
            SampleSchema.ValueFieldTagOne, Sample.Metadata>> entry :
            mapNameToMapSink.entrySet()) {

            Table<Sample.StringKey, SampleSchema.ValueFieldTagOne,
                Sample.Metadata> map = entry.getValue();
            Assert.assertEquals(expectedSize, map.entryStream().count());
        }
    }

    private void startLogReplicatorServersWithCustomMaxWriteSize() throws Exception {
        sourceReplicationServer =
            runReplicationServerCustomMaxWriteSize(sourceReplicationServerPort, sourceSiteCorfuPort,
                pluginConfigFilePath, MAX_WRITE_SIZE_BYTES, MAX_SNAPSHOT_ENTRIES_APPLIED, transportType);

        // Start Log Replication Server on Sink Site
        sinkReplicationServer =
            runReplicationServerCustomMaxWriteSize(sinkReplicationServerPort, sinkSiteCorfuPort,
                pluginConfigFilePath, MAX_WRITE_SIZE_BYTES, MAX_SNAPSHOT_ENTRIES_APPLIED, transportType);
    }

    private void shutDown() {
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

    private class StreamingUpdateListener implements StreamListener {
        private CountDownLatch countdownLatch;

        @Getter
        private Map<String,
            List<List<CorfuStreamEntry<Sample.StringKey,
                SampleSchema.ValueFieldTagOne, Sample.Metadata>>>>
            tableNameToUpdatesMap = new HashMap<>();

        StreamingUpdateListener(CountDownLatch latch) {
            countdownLatch = latch;
        }

        @Override
        public void onNext(CorfuStreamEntries results) {
            for (Map.Entry<TableSchema, List<CorfuStreamEntry>> entry :
                results.getEntries().entrySet()) {

                if (entry.getKey().getTableName().equals(TableRegistry.PROTOBUF_DESCRIPTOR_TABLE_NAME)) {
                    // Updates on the protobuf descriptor table are not accumulated in the test
                    continue;
                }

                List<CorfuStreamEntry<Sample.StringKey,
                    SampleSchema.ValueFieldTagOne, Sample.Metadata>> tableEntries =
                    new ArrayList<>();
                entry.getValue().forEach(tableEntry -> tableEntries.add(tableEntry));

                List<List<CorfuStreamEntry<Sample.StringKey,
                    SampleSchema.ValueFieldTagOne, Sample.Metadata>>>
                    existingEntries =
                    tableNameToUpdatesMap.getOrDefault(
                        entry.getKey().getTableName(), new ArrayList<>());

                existingEntries.add(tableEntries);
                tableNameToUpdatesMap.putIfAbsent(entry.getKey().getTableName(),
                    existingEntries);
            }
            countdownLatch.countDown();
        }

        @Override
        public void onError(Throwable throwable) {
            log.error("Error in stream listener", throwable);
        }
    }
}
