package org.corfudb.integration;

import com.google.protobuf.Message;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.LogReplication.ReplicationStatusKey;
import org.corfudb.runtime.LogReplication.ReplicationStatusVal;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStoreEntry;
import org.corfudb.runtime.collections.CorfuStreamEntries;
import org.corfudb.runtime.collections.CorfuStreamEntry;
import org.corfudb.runtime.collections.StreamListener;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TableSchema;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.test.SampleSchema;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import static org.corfudb.runtime.view.ObjectsView.REPLICATION_STATUS_TABLE;
import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;
import static org.junit.Assert.fail;

@Slf4j
public class LogReplicationMultiNamespaceStreamingIT extends AbstractIT {

    private final String corfuSingleNodeHost;
    private final int corfuStringNodePort;
    private final String singleNodeEndpoint;
    private Process corfuServer;
    private CorfuStore store;

    private final String namespace = "test_namespace";
    private final String userTableName = "data_table";
    private final String userTag = "sample_streamer_1";
    private final String defaultClusterId = UUID.randomUUID().toString();


    Table<SampleSchema.Uuid, SampleSchema.SampleTableAMsg, SampleSchema.Uuid> userDataTable;
    Table<ReplicationStatusKey, ReplicationStatusVal, Message> replicationStatusTable;

    public LogReplicationMultiNamespaceStreamingIT() {
        corfuSingleNodeHost = PROPERTIES.getProperty("corfuSingleNodeHost");
        corfuStringNodePort = Integer.valueOf(PROPERTIES.getProperty("corfuSingleNodePort"));
        singleNodeEndpoint = String.format("%s:%d", corfuSingleNodeHost, corfuStringNodePort);
    }

    private void initializeCorfu() throws Exception {
        corfuServer = new AbstractIT.CorfuServerRunner()
            .setHost(corfuSingleNodeHost)
            .setPort(corfuStringNodePort)
            .setLogPath(getCorfuServerLogPath(corfuSingleNodeHost, corfuStringNodePort))
            .setSingle(true)
            .runServer();
        runtime = createRuntime(singleNodeEndpoint);
        store = new CorfuStore(runtime);
    }

    /**
     * This test verifies that data written to both tables is captured by the streaming listener.  It also verifies
     * that the number of streaming updates and data received in them was as expected.
     * @throws Exception
     */
    @Test
    public void testDataWrittenToBothTables() throws Exception {
        initializeCorfu();
        openTables();

        final int numUpdates = 10;
        CountDownLatch countDownLatch = new CountDownLatch(numUpdates);
        LRCrossNamespaceTestListener listener = new LRCrossNamespaceTestListener(countDownLatch);
        store.subscribeLRCrossNamespaceListener(listener, namespace, userTag, Arrays.asList(userTableName));

        log.info("Write data on both tables");
        writeUpdatesToBothTables(numUpdates, 0);

        log.info("Wait for data to arrive");
        countDownLatch.await();

        log.info("Verify the sequence of updates and received data");
        verifyUpdatesSequence(listener.getUpdates());
        verifyData(listener.getUpdates(), true);

        store.unsubscribeListener(listener);
    }

    /**
     * This test verifies that all expected updates are received regardless of the number of updates in a given
     * transaction on a table.
     */
    @Test
    public void testDataWrittenToBothTablesMultipleUpdatesInTx() throws Exception {
        initializeCorfu();
        openTables();

        final int numWrites = 10;

        // Each transaction will write half of the entries.  So the number of onNext() callbacks invoked = 2
        final int numStreamingUpdates = 2;
        CountDownLatch countDownLatch = new CountDownLatch(numStreamingUpdates);
        LRCrossNamespaceTestListener listener = new LRCrossNamespaceTestListener(countDownLatch);
        store.subscribeLRCrossNamespaceListener(listener, namespace, userTag, Arrays.asList(userTableName));

        log.info("Write data on both tables");
        writeToBothTablesMultipleUpdatesInATx(numWrites, 0);

        log.info("Wait for data to arrive");
        countDownLatch.await();

        log.info("Verify the sequence of updates and received data");
        verifyUpdatesSequence(listener.getUpdates());
        verifyData(listener.getUpdates(), true);

        store.unsubscribeListener(listener);
    }

    /**
     * This test verifies that the expected updates are received with the correct order with a custom buffer size of 10.
     * This means that the buffer in LRDeltaStream can accommodate max 10 updates at any time.
     */
    @Test
    public void testDataWrittenToBothTablesCustomBufferSize() throws Exception {
        initializeCorfu();
        openTables();

        // Obtain the latest timestamp
        CorfuStoreMetadata.Timestamp timestamp;
        try (TxnContext txn = store.txn(namespace)) {
            txn.executeQuery(userTableName, p -> true);
            timestamp = txn.commit();
        }

        final int bufferSize = 10;
        final int numUpdates = 50;
        CountDownLatch countDownLatch = new CountDownLatch(numUpdates);
        LRCrossNamespaceTestListener listener = new LRCrossNamespaceTestListener(countDownLatch);
        store.subscribeLRCrossNamespaceListener(listener, namespace, userTag, timestamp, bufferSize);

        log.info("Write data on both tables");
        writeUpdatesToBothTables(numUpdates, 0);

        log.info("Wait for data to arrive");
        countDownLatch.await();

        log.info("Verify the sequence of updates and received data");
        verifyUpdatesSequence(listener.getUpdates());
        verifyData(listener.getUpdates(), true);

        store.unsubscribeListener(listener);
    }

    /**
     * This test simulates a scenario where unequal number of updates are written to both(data and system) tables
     * concurrently.  For every X writes to the data table, there is 1 write to the System table.  This is repeated
     * several times.  In the end, all writes must be received through streaming and in the right order.
     * @throws Exception
     */
    @Test
    public void testConcurrentUnequalWritesInBothTables() throws Exception {
        initializeCorfu();
        openTables();

        final int numIterations = 10;
        final int numWritesToDataTable = 10;

        // In each iteration, numWritesToDataTable are made + 1 write to the System Table
        final int numExpectedStreamingUpdates = numIterations * (numWritesToDataTable + 1);

        CountDownLatch countDownLatch = new CountDownLatch(numExpectedStreamingUpdates);
        LRCrossNamespaceTestListener listener = new LRCrossNamespaceTestListener(countDownLatch);
        store.subscribeLRCrossNamespaceListener(listener, namespace, userTag, Arrays.asList(userTableName));

        log.info("Write data on both tables");

        writeUnequalDataConcurrently(numIterations, numWritesToDataTable);

        log.info("Wait for data to arrive");
        countDownLatch.await();

        log.info("Verify the sequence of updates and received data");
        verifyUpdatesSequence(listener.getUpdates());
        verifyData(listener.getUpdates(), true);

        store.unsubscribeListener(listener);
    }

    /**
     * This test tests the behavior of the subscription api at a specific timestamp.  It writes to both tables,
     * captures the timestamp of the commit and subscribes a listener after this timestamp.  It then verifies
     * that updates from the later timestamp onwards were received.
     * @throws Exception
     */
    @Test
    public void testSubscriptionAfterDataWritten() throws Exception {
        initializeCorfu();
        openTables();

        final int numUpdates = 10;
        writeUpdatesToBothTables(numUpdates, 0);

        // Fetch the timestamp of the above writes and entries written so far
        List<CorfuStoreEntry<SampleSchema.Uuid, SampleSchema.SampleTableAMsg, SampleSchema.Uuid>> existingEntries;
        CorfuStoreMetadata.Timestamp timestamp;
        try (TxnContext txn = store.txn(namespace)) {
            existingEntries = txn.executeQuery(userDataTable, p -> true);
            timestamp = txn.commit();
        }

        final int newUpdates = 5;
        CountDownLatch countDownLatch = new CountDownLatch(newUpdates);
        LRCrossNamespaceTestListener listener = new LRCrossNamespaceTestListener(countDownLatch);
        store.subscribeLRCrossNamespaceListener(listener, namespace, userTag, Arrays.asList(userTableName), timestamp);

        writeToDataTable(newUpdates, numUpdates);
        countDownLatch.await();

        verifyUpdatesSequence(listener.getUpdates(), timestamp.getSequence());
        verifyData(listener.getUpdates(), existingEntries);
        store.unsubscribeListener(listener);
    }

    /**
     * This test verifies that no updates are received from any other system table which the LR listener does not
     * subscribe to.  It writes updates to the LR Metadata table.  This table is not subscribed to so no updates on
     * it must be received.
     * @throws Exception
     */
    @Test
    public void testNoUpdatesReceivedFromNonSubscribedTables() throws Exception {
        initializeCorfu();
        openTables();

        LRCrossNamespaceTestListener listener = new LRCrossNamespaceTestListener();
        store.subscribeLRCrossNamespaceListener(listener, namespace, userTag, Arrays.asList(userTableName));

        writeToNonSubscribedSystemTable();

        // Sleep for 1 second and verify that no updates were received on the listener
        Thread.sleep(PARAMETERS.TIMEOUT_SHORT.toMillis());

        Assert.assertEquals(0, listener.getUpdates().size());
        store.unsubscribeListener(listener);
    }

    private void openTables() throws Exception {
        userDataTable = store.openTable(namespace, userTableName, SampleSchema.Uuid.class,
                SampleSchema.SampleTableAMsg.class, SampleSchema.Uuid.class,
                TableOptions.fromProtoSchema(SampleSchema.SampleTableAMsg.class)
        );

        replicationStatusTable = store.openTable(CORFU_SYSTEM_NAMESPACE,
                REPLICATION_STATUS_TABLE, ReplicationStatusKey.class,
                ReplicationStatusVal.class, null,
                TableOptions.fromProtoSchema(ReplicationStatusVal.class));
    }


    private void writeUpdatesToBothTables(int totalUpdates, int offset) {
        for (int index = offset; index < offset + totalUpdates; index++) {
            if (index % 2 == 0) {
                // Write to the user data table
                SampleSchema.Uuid uuid = SampleSchema.Uuid.newBuilder().setMsb(index).setLsb(index).build();
                SampleSchema.SampleTableAMsg msgA =
                    SampleSchema.SampleTableAMsg.newBuilder().setPayload(String.valueOf(index))
                        .build();
                try (TxnContext tx = store.txn(namespace)) {
                    tx.putRecord(userDataTable, uuid, msgA, uuid);
                    tx.commit();
                }
            } else {
                // Write to the LR Status table
                ReplicationStatusKey key =
                    ReplicationStatusKey.newBuilder().setClusterId(defaultClusterId + index).build();
                ReplicationStatusVal val = ReplicationStatusVal.newBuilder().setRemainingEntriesToSend(index).build();
                try (TxnContext tx = store.txn(CORFU_SYSTEM_NAMESPACE)) {
                    tx.putRecord(replicationStatusTable, key, val, null);
                    tx.commit();
                }
            }
        }
    }

    private void writeToBothTablesMultipleUpdatesInATx(int totalUpdates, int offset) {
        final int lastIndex = totalUpdates + offset;

        // Write the first half records to user data table and the rest to the system table
        try (TxnContext txn = store.txn(namespace)) {
            for (int index = offset; index < lastIndex / 2; index++) {
                SampleSchema.Uuid uuid = SampleSchema.Uuid.newBuilder().setMsb(index).setLsb(index).build();
                SampleSchema.SampleTableAMsg msgA =
                    SampleSchema.SampleTableAMsg.newBuilder().setPayload(String.valueOf(index))
                        .build();
                txn.putRecord(userDataTable, uuid, msgA, uuid);
            }
            txn.commit();
        }

        try (TxnContext txn = store.txn(CORFU_SYSTEM_NAMESPACE)) {
            for (int index = lastIndex/2; index < lastIndex; index++) {
                ReplicationStatusKey key =
                    ReplicationStatusKey.newBuilder().setClusterId(defaultClusterId + index).build();
                ReplicationStatusVal val = ReplicationStatusVal.newBuilder().setRemainingEntriesToSend(index).build();
                txn.putRecord(replicationStatusTable, key, val, null);
            }
            txn.commit();
        }
    }

    private void writeUnequalDataConcurrently(int numIterations, int numUpdatesToDataTable) throws Exception {
        for (int i = 0; i < numIterations; i++) {
            int systemTableKeyIndex = i;
            int dataTableStart = i * numUpdatesToDataTable;
            int dataTableEnd = dataTableStart + numUpdatesToDataTable;

            // For every 'numUpdatesToDataTable', there is 1 update to the System Table.
            scheduleConcurrently(f -> {
                    for (int index = dataTableStart; index < dataTableEnd; index++) {
                        try (TxnContext txn = store.txn(namespace)) {
                            SampleSchema.Uuid uuid = SampleSchema.Uuid.newBuilder().setMsb(index).setLsb(index).build();
                            SampleSchema.SampleTableAMsg msgA =
                                SampleSchema.SampleTableAMsg.newBuilder().setPayload(String.valueOf(index))
                                    .build();
                            txn.putRecord(userDataTable, uuid, msgA, uuid);
                            txn.commit();
                        }
                    }
                });

            scheduleConcurrently(f -> {
                try (TxnContext txn = store.txn(CORFU_SYSTEM_NAMESPACE)) {
                    ReplicationStatusKey key =
                            ReplicationStatusKey.newBuilder().setClusterId(defaultClusterId + systemTableKeyIndex).build();
                    ReplicationStatusVal val = ReplicationStatusVal.newBuilder()
                            .setRemainingEntriesToSend(systemTableKeyIndex).build();
                    txn.putRecord(replicationStatusTable, key, val, null);
                    txn.commit();
                }
            });
            executeScheduled(2, PARAMETERS.TIMEOUT_NORMAL);
        }
    }

    private void writeToDataTable(int numUpdates, int offset) {
        for (int i = offset; i < offset + numUpdates; i++) {
            try (TxnContext tx = store.txn(namespace)) {
                SampleSchema.Uuid uuid = SampleSchema.Uuid.newBuilder().setMsb(i).setLsb(i).build();
                SampleSchema.SampleTableAMsg msgA = SampleSchema.SampleTableAMsg.newBuilder()
                        .setPayload(String.valueOf(i)).build();
                tx.putRecord(userDataTable, uuid, msgA, uuid);
                tx.commit();
            }
        }
    }

    private void writeToNonSubscribedSystemTable() throws Exception {
        Table<LogReplicationMetadata.LogReplicationMetadataKey, LogReplicationMetadata.LogReplicationMetadataVal,
                Message> metadataTable = store.openTable(CORFU_SYSTEM_NAMESPACE,
                LogReplicationMetadataManager.getPersistedWriterMetadataTableName(defaultClusterId),
                LogReplicationMetadata.LogReplicationMetadataKey.class,
                LogReplicationMetadata.LogReplicationMetadataVal.class,
                null,
                TableOptions.fromProtoSchema(LogReplicationMetadata.LogReplicationMetadataVal.class));

        final int numUpdates = 5;
        for (int i = 0; i < numUpdates; i++) {
            try (TxnContext txn = store.txn(CORFU_SYSTEM_NAMESPACE)) {
                LogReplicationMetadata.LogReplicationMetadataKey key = LogReplicationMetadata.LogReplicationMetadataKey
                        .newBuilder().setKey(Integer.toString(i)).build();
                LogReplicationMetadata.LogReplicationMetadataVal val = LogReplicationMetadata.LogReplicationMetadataVal
                        .newBuilder().setVal(Integer.toString(i)).build();
                txn.putRecord(metadataTable, key, val, null);
                txn.commit();
            }
        }
    }

    private void verifyUpdatesSequence(LinkedList<CorfuStreamEntries> updates) {
        long startTs = -1;

        for (CorfuStreamEntries update : updates) {
            if(startTs == -1) {
                startTs = update.getTimestamp().getSequence();
                continue;
            }
            Assert.assertTrue(startTs < update.getTimestamp().getSequence());
            startTs = update.getTimestamp().getSequence();
        }
    }

    private void verifyUpdatesSequence(LinkedList<CorfuStreamEntries> updates, long floorTs) {
        Assert.assertTrue(floorTs < updates.getFirst().getTimestamp().getSequence());
        verifyUpdatesSequence(updates);
    }

    private void verifyData(LinkedList<CorfuStreamEntries> updates, boolean allEntriesExpected) {
        Map<String, List<CorfuStreamEntry>> updatesPerTable = new HashMap<>();

        // Group the incoming updates as per the table name
        for (CorfuStreamEntries update : updates) {
            for (Map.Entry<TableSchema, List<CorfuStreamEntry>> entry : update.getEntries().entrySet()) {
                List<CorfuStreamEntry> existingEntries = updatesPerTable.getOrDefault(entry.getKey().getTableName(),
                        new ArrayList<>());
                existingEntries.addAll(entry.getValue());
                updatesPerTable.put(entry.getKey().getTableName(), existingEntries);
            }
        }

        // Verify that the streaming data received for each table with the corresponding original table
        for (String tableName : updatesPerTable.keySet()) {

            if (allEntriesExpected) {
                // Verify the number of entries
                if (tableName.equals(REPLICATION_STATUS_TABLE)) {
                    Assert.assertEquals(store.getTable(CORFU_SYSTEM_NAMESPACE, tableName).count(),
                        updatesPerTable.get(tableName).size());
                } else {
                    Assert.assertEquals(store.getTable(namespace, tableName).count(), updatesPerTable.get(tableName).size());
                }
            }

            // Verify the actual data
            for (CorfuStreamEntry entry : updatesPerTable.get(tableName)) {
                if (tableName.equals(REPLICATION_STATUS_TABLE)) {
                    ReplicationStatusKey key = (ReplicationStatusKey)entry.getKey();
                    ReplicationStatusVal val = (ReplicationStatusVal)entry.getPayload();
                    try (TxnContext txn = store.txn(CORFU_SYSTEM_NAMESPACE)) {
                        Assert.assertEquals(txn.getRecord(tableName, key).getPayload(), val);
                        txn.commit();
                    }
                } else {
                    SampleSchema.Uuid key = (SampleSchema.Uuid)entry.getKey();
                    SampleSchema.SampleTableAMsg val = (SampleSchema.SampleTableAMsg)entry.getPayload();
                    try (TxnContext txn = store.txn(namespace)) {
                        Assert.assertEquals(txn.getRecord(tableName, key).getPayload(), val);
                        txn.commit();
                    }
                }
            }
        }
    }

    private void verifyData(LinkedList<CorfuStreamEntries> updates, List<CorfuStoreEntry<SampleSchema.Uuid,
            SampleSchema.SampleTableAMsg, SampleSchema.Uuid>> entriesNotExpectedInUpdate) {
        verifyData(updates, false);

        Map<String, List<CorfuStreamEntry>> updatesPerTable = new HashMap<>();

        // Group the incoming updates as per the table name
        for (CorfuStreamEntries update : updates) {
            for (Map.Entry<TableSchema, List<CorfuStreamEntry>> entry : update.getEntries().entrySet()) {
                List<CorfuStreamEntry> entriesFound = updatesPerTable.getOrDefault(entry.getKey().getTableName(),
                    new ArrayList<>());
                entriesFound.addAll(entry.getValue());
                updatesPerTable.put(entry.getKey().getTableName(), entriesFound);
            }
        }

        for (CorfuStoreEntry<SampleSchema.Uuid, SampleSchema.SampleTableAMsg, SampleSchema.Uuid> entryNotExpected :
                entriesNotExpectedInUpdate) {
            for (CorfuStreamEntry entryFound : updatesPerTable.get(userTableName)) {
                SampleSchema.Uuid key = (SampleSchema.Uuid)entryFound.getKey();
                Assert.assertNotEquals(key, entryNotExpected.getKey());
            }
        }
    }

    private class LRCrossNamespaceTestListener implements StreamListener {
        private CountDownLatch countDownLatch;

        @Getter
        private final LinkedList<CorfuStreamEntries> updates = new LinkedList<>();

        LRCrossNamespaceTestListener() { }

        LRCrossNamespaceTestListener(CountDownLatch countDownLatch) {
            this.countDownLatch = countDownLatch;
        }

        @Override
        public void onNext(CorfuStreamEntries results) {
            results.getEntries().forEach((key, val) -> countDownLatch.countDown());
            updates.add(results);
        }

        @Override
        public void onError(Throwable throwable) {
            log.error("Error on listener", throwable);
            fail("onError for CrossNamespaceListener: " + throwable.toString());
        }
    }
}
