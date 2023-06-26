package org.corfudb.integration;

import com.google.protobuf.Message;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.corfudb.runtime.LogReplication.ReplicationModel;
import org.corfudb.runtime.LogReplication.ReplicationStatus;
import org.corfudb.runtime.LogReplication.ReplicationSubscriber;
import org.corfudb.runtime.LogReplication.SinkReplicationStatus;
import org.corfudb.runtime.LogReplicationListener;
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
import org.junit.After;
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

import static org.corfudb.runtime.LogReplicationUtils.REPLICATION_STATUS_TABLE_NAME;
import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;
import static org.junit.Assert.fail;

@Slf4j
public class LogReplicationListenerIT extends AbstractIT {

    private final String corfuSingleNodeHost;
    private final int corfuStringNodePort;
    private final String singleNodeEndpoint;
    private CorfuStore store;

    private final String namespace = "test_namespace";
    private final String userTableName = "data_table";
    private final String userTag = "sample_streamer_1";
    private final String defaultClusterId = UUID.randomUUID().toString();
    private final String testClientName = "lr_test_client";

    // LR Listeners for data and system tables
    private LRTestListener lrListener;
    private LRTestListener newListener;

    // Regular(non-LR) listener for the data table
    private TestListener listener;

    Table<SampleSchema.Uuid, SampleSchema.SampleTableAMsg, SampleSchema.Uuid> userDataTable;
    Table<LogReplicationSession, ReplicationStatus, Message> replicationStatusTable;

    public LogReplicationListenerIT() {
        corfuSingleNodeHost = PROPERTIES.getProperty("corfuSingleNodeHost");
        corfuStringNodePort = Integer.valueOf(PROPERTIES.getProperty("corfuSingleNodePort"));
        singleNodeEndpoint = String.format("%s:%d", corfuSingleNodeHost, corfuStringNodePort);
    }

    private void initializeCorfu() throws Exception {
        new AbstractIT.CorfuServerRunner()
            .setHost(corfuSingleNodeHost)
            .setPort(corfuStringNodePort)
            .setLogPath(getCorfuServerLogPath(corfuSingleNodeHost, corfuStringNodePort))
            .setSingle(true)
            .runServer();
        CorfuRuntime runtime = createRuntime(singleNodeEndpoint);
        managed(runtime);
        store = new CorfuStore(runtime);
    }

    @Test
    public void testWritesToDataTableStartInLogEntrySync() throws Exception {
        testWritesToDataTable(false);
    }

    @Test
    public void testWritesToDataTableStartInSnapshotSync() throws Exception {
        testWritesToDataTable(true);
    }

    /**
     * This test verifies that all data written to the user table is received in increasing order of the timestamps and
     * the streaming updates match the data in the table.  The final number of entries seen by performFullSyncAndMerge(if
     * starting in snapshot sync) + streaming updates must be equal to the total
     * number of writes to the table.
     * @throws Exception
     */
    private void testWritesToDataTable(boolean startInSnapshotSync) throws Exception {
        initializeCorfu();
        openAndInitializeTables(testClientName);

        // Start snapshot sync if requested
        if (startInSnapshotSync) {
            writeToStatusTable(false, testClientName);
        }

        final int numUpdates = 10;
        CountDownLatch countDownLatch = new CountDownLatch(numUpdates);
        lrListener = new LRTestListener(store, namespace, countDownLatch);

        // Subscribe the listener
        store.subscribeLogReplicationListener(lrListener, namespace, userTag);

        // End snapshot sync if it had been requested
        if (startInSnapshotSync) {
            writeToStatusTable(true, testClientName);
        }

        // Write numUpdates records in the data table
        writeToDataTable(numUpdates, 0);

        // Wait for the data to arrive.  Since performFullSyncAndMerge() is executed later if a snapshot sync is ongoing, the
        // number of streaming updates in that case will be lesser.  But the final number of entries seen by
        // performFullSyncAndMerge() + streaming updates must be equal to numUpdates
        countDownLatch.await();

        // If the test started when in log entry sync, performFullSyncAndMerge() should not find any existing data as no
        // updates have been written prior to subscription.
        if (!startInSnapshotSync) {
            Assert.assertTrue(lrListener.getExistingEntries().isEmpty());
        }

        // Verify the sequence(timestamp) of the streaming updates
        verifyUpdatesSequence(lrListener.getUpdates());

        // Verify all the data observed by the listener - existing entries before subscription + updates afterwards
        verifyData(lrListener.getUpdates(), lrListener.getExistingEntries());
    }

    @Test
    public void testMultipleWritesToDataTableInTxStartInLogEntrySync() throws Exception {
        testMultipleWritesToDataTableInTx(false);
    }

    @Test
    public void testMultipleWritesToDataTableInTxStartInSnapshotSync() throws Exception {
        testMultipleWritesToDataTableInTx(true);
    }

    /**
     * This test verifies that all data written to the user table is received in increasing order of the timestamps and
     * the streaming updates match the data in the table.  Multiple entries to the table are written in a single
     * transaction (batch size >1).
     * The final number of entries seen by performFullSyncAndMerge(if starting in snapshot sync) + streaming updates must be
     * equal to the total number of writes to the table.
     */
    private void testMultipleWritesToDataTableInTx(boolean startInSnapshotSync) throws Exception {
        initializeCorfu();
        openAndInitializeTables(testClientName);

        // Start snapshot sync if requested
        if (startInSnapshotSync) {
            writeToStatusTable(false, testClientName);
        }

        final int numUpdates = 10;
        final int numStreamingUpdates = 1;
        CountDownLatch countDownLatch = new CountDownLatch(numStreamingUpdates);

        // As all updates are written in the same transaction, set this countdown latch to 1
        CountDownLatch numTxLatch = new CountDownLatch(1);

        lrListener = new LRTestListener(store, namespace, countDownLatch);
        lrListener.setNumTxLatch(numTxLatch);
        store.subscribeLogReplicationListener(lrListener, namespace, userTag);

        // End snapshot sync if it had been requested
        if (startInSnapshotSync) {
            writeToStatusTable(true, testClientName);
        }

        writeToDataTableMultipleUpdatesInATx(numUpdates, 0);

        countDownLatch.await();
        numTxLatch.await();

        // If the test started when in log entry sync, performFullSyncAndMerge() should not find any existing data as no
        // updates have been written prior to subscription.
        if (!startInSnapshotSync) {
            Assert.assertTrue(lrListener.getExistingEntries().isEmpty());
        }

        verifyUpdatesSequence(lrListener.getUpdates());
        verifyData(lrListener.getUpdates(), lrListener.getExistingEntries());
    }

    @Test
    public void testSubscriptionWithCustomBufferSizeStartInLogEntrySync() throws Exception {
        testSubscriptionWithCustomBufferSize(false);
    }

    @Test
    public void testSubscriptionWithCustomBufferSizeStartInSnapshotSync() throws Exception {
        testSubscriptionWithCustomBufferSize(true);
    }

    /**
     * This test verifies that a listener with a custom buffer size works as expected.  All data written to the
     * user table is received in increasing order of timestamps and the streaming updates match the data in the table.
     * The final number of entries seen by performFullSyncAndMerge(if starting in snapshot sync) + streaming updates must be
     * equal to the total number of writes to the table.
     */
    private void testSubscriptionWithCustomBufferSize(boolean startInSnapshotSync) throws Exception {
        initializeCorfu();
        openAndInitializeTables(testClientName);

        // Start snapshot sync if requested
        if (startInSnapshotSync) {
            writeToStatusTable(false, testClientName);
        }

        final int bufferSize = 10;
        final int numUpdates = 50;

        CountDownLatch countDownLatch = new CountDownLatch(numUpdates);
        lrListener = new LRTestListener(store, namespace, countDownLatch);

        // Subscribe a listener with a buffer size of 10
        store.subscribeLogReplicationListener(lrListener, namespace, userTag, bufferSize);

        // End snapshot sync if it had been requested
        if (startInSnapshotSync) {
            writeToStatusTable(true, testClientName);
        }

        log.info("Write updates to the data table");
        writeToDataTable(numUpdates, 0);

        log.info("Wait for data to arrive");
        countDownLatch.await();

        // If the test started when in log entry sync, performFullSyncAndMerge() should not find any existing data as no
        // updates have been written prior to subscription.
        if (!startInSnapshotSync) {
            Assert.assertTrue(lrListener.getExistingEntries().isEmpty());
        }

        log.info("Verify the sequence of updates and received data");
        verifyUpdatesSequence(lrListener.getUpdates());
        verifyData(lrListener.getUpdates(), lrListener.getExistingEntries());
    }

    /**
     * This test simultates a concurrent toggling of the 'dataConsistent' flag along with writes to the data
     * table.  For every X writes to the data table, there is a concurrent write toggling this flag on the System
     * table.  This is repeated several times.  In the end, it is expected that the number of entries seen by
     * performFullSyncAndMerge() + streaming updates must be equal to the total writes across all iterations.  The updates
     * must also be in increasing order of timestamps.
     * @throws Exception
     */
    @Test
    public void testConcurrentDataWritesAndDataConsistentToggle() throws Exception {
        initializeCorfu();
        openAndInitializeTables(testClientName);

        final int numIterations = 10;
        final int numWritesToDataTable = 10;

        // In each iteration, numWritesToDataTable are made
        final int numExpectedStreamingUpdates = numIterations * numWritesToDataTable;

        CountDownLatch countDownLatch = new CountDownLatch(numExpectedStreamingUpdates);
        lrListener = new LRTestListener(store, namespace, countDownLatch);
        store.subscribeLogReplicationListener(lrListener, namespace, userTag);

        log.info("Write data concurrently on both tables");
        writeDataAndToggleDataConsistentConcurrently(numIterations, numWritesToDataTable, testClientName);

        log.info("Wait for data to arrive");
        countDownLatch.await();

        log.info("Verify the sequence of updates and received data");
        verifyUpdatesSequence(lrListener.getUpdates());
        verifyData(lrListener.getUpdates(), lrListener.getExistingEntries());
    }

    @Test
    public void testSubscriptionAfterDataWrittenStartInLogEntrySync() throws Exception {
        testSubscriptionAfterDataWritten(false);
    }

    @Test
    public void testSubscriptionAfterDataWrittenStartInSnapshotSync() throws Exception {
        testSubscriptionAfterDataWritten(true);
    }

    /**
     * This test tests the behavior of subscription done after some data is written.  It has the following workflow:
     * 1. Write data to the table
     * 2. Subscribe for streaming updates
     * 3. Write more data
     * In the end, it verifies that the number of entries observed in performFullSyncAndMerge(if starting in snapshot sync) +
     * streaming updates is the total of the writes in 1 and 3.
     * Also verifies the order of streaming updates and the data seen in the listener.
     * @throws Exception
     */
    private void testSubscriptionAfterDataWritten(boolean startInSnapshotSync) throws Exception {
        initializeCorfu();
        openAndInitializeTables(testClientName);

        // Start snapshot sync if requested
        if (startInSnapshotSync) {
            writeToStatusTable(false, testClientName);
        }

        final int numUpdates = 10;
        writeToDataTable(numUpdates, 0);

        final int newUpdates = 5;
        final int totalUpdates = numUpdates + newUpdates;
        CountDownLatch countDownLatch = new CountDownLatch(totalUpdates);
        lrListener = new LRTestListener(store, namespace, countDownLatch);

        // Subscribe the listener at the obtained timestamp
        store.subscribeLogReplicationListener(lrListener, namespace, userTag);

        // End snapshot sync if it had been requested
        if (startInSnapshotSync) {
            writeToStatusTable(true, testClientName);
        }

        log.info("Write updates to the data table after subscription");
        writeToDataTable(newUpdates, numUpdates);

        log.info("Wait for subscription and for all updates to be received");
        countDownLatch.await();

        // If the test started when in log entry sync, performFullSyncAndMerge() should only find the data written prior to
        // subscription
        if (!startInSnapshotSync) {
            Assert.assertEquals(numUpdates, lrListener.getExistingEntries().size());
        }

        log.info("Verify the sequence of updates");
        verifyUpdatesSequence(lrListener.getUpdates());

        log.info("Verify that updates made only after the subscription are received");
        verifyData(lrListener.getUpdates(), lrListener.getExistingEntries());
    }

    /**
     * This test verifies that no updates are received from any other table which the LR listener does not
     * subscribe to.  It writes updates to the LR Metadata table.  This table is not subscribed to so no updates on
     * it must be received.
     * @throws Exception
     */
    @Test
    public void testNoUpdatesReceivedFromNonSubscribedTables() throws Exception {
        initializeCorfu();
        openAndInitializeTables(testClientName);

        lrListener = new LRTestListener(store, namespace, new CountDownLatch(0));
        store.subscribeLogReplicationListener(lrListener, namespace, userTag);

        // Write to a redundant table to which the listener has not subscribed
        writeToNonSubscribedSystemTable();

        // Sleep for 1 second and verify that no updates were received on the listener
        Thread.sleep(PARAMETERS.TIMEOUT_SHORT.toMillis());

        Assert.assertEquals(0, lrListener.getUpdates().size());
    }

    /**
     * This test verifies that both LR and non-LR streaming tasks can exist together and function as expected.  The
     * test subscribes a non-LR listener an LR listener and verifies that the expected updates were received on each.
     * @throws Exception
     */
    @Test
    public void testNonLRStreamingTaskCoexistence() throws Exception {
        initializeCorfu();
        openAndInitializeTables(testClientName);

        final int numUpdates = 10;

        CountDownLatch countDownLatch = new CountDownLatch(numUpdates);
        listener = new TestListener(countDownLatch);

        CountDownLatch lrCountDownLatch = new CountDownLatch(numUpdates);
        lrListener = new LRTestListener(store, namespace, lrCountDownLatch);

        store.subscribeListener(listener, namespace, userTag, Arrays.asList(userTableName));
        store.subscribeLogReplicationListener(lrListener, namespace, userTag);

        writeToDataTable(numUpdates, 0);

        log.info("Wait for the expected number of updates to arrive");
        countDownLatch.await();
        lrCountDownLatch.await();

        log.info("Verify the sequence of updates and received data on both listeners");
        verifyUpdatesSequence(lrListener.getUpdates());
        verifyData(lrListener.getUpdates(), lrListener.getExistingEntries());

        verifyUpdatesSequence(listener.getUpdates());
        verifyData(listener.getUpdates(), new ArrayList<>());
    }

    /**
     * This test verifies that multiple clients using the same replication model(Logical Group) do not interfere with
     * each other.  The test simulates the following workflow:
     * 1. Start snapshot sync on test_client
     * 2. Start snapshot sync on new_client
     * 3. Subscribe listeners for both clients
     * 4. Write data to a table which is read during performFullSyncAndMerge()
     * 5. End snapshot sync on test_client
     * 6. Write more data which will be received as updates on the listener for test_client
     * 7. Verify that performFullSyncAndMerge() was invoked on the listener for test_client and it read the expected data.
     * Also verify the updates in 6. were received
     * 8. Verify that the listener for new_client did not receive any data
     * @throws Exception
     */
    @Test
    public void testMultipleClients() throws Exception {
        initializeCorfu();
        openTables();

        // Set snapshot sync ongoing on test_client
        writeToStatusTable(false, testClientName);

        // Set snapshot sync ongoing on new_client
        final String newClientName = "new_client";
        writeToStatusTable(false, newClientName);

        final int numUpdates = 10;
        CountDownLatch countDownLatch = new CountDownLatch(numUpdates);

        // Create a listener for test_client
        lrListener = new LRTestListener(store, namespace, countDownLatch);

        // Create a listener for new_client
        newListener = new LRTestListener(store, namespace, null);
        newListener.setClientName(newClientName);

        // Subscribe both the listeners
        store.subscribeLogReplicationListener(lrListener, namespace, userTag);

        final String newTag = "new_tag";
        store.subscribeLogReplicationListener(newListener, namespace, newTag);

        // Write numUpdates records
        writeToDataTable(numUpdates, 0);

        // End snapshot sync on test_client
        writeToStatusTable(true, testClientName);

        // Wait for the written data to be observed through performFullSyncAndMerge()
        countDownLatch.await();

        // Write numUpdates more records.  They will be observed as part of LR log entry sync on the listener
        countDownLatch = new CountDownLatch(numUpdates);
        lrListener.setCountDownLatch(countDownLatch);

        writeToDataTable(numUpdates, numUpdates);

        // Wait for the new updates to arrive
        countDownLatch.await();

        // Verify that the listener corresponding to test_client read the expected data during performFullSyncAndMerge().
        // Also verify that it received the subsequent updates after snapshot sync ended.
        Assert.assertEquals(numUpdates, lrListener.getExistingEntries().size());
        Assert.assertEquals(numUpdates, lrListener.getUpdates().size());
        verifyData(lrListener.getUpdates(), lrListener.getExistingEntries());

        // Verify that no data was observed on the listener corresponding to new_client
        Assert.assertTrue(newListener.getUpdates().isEmpty());
        Assert.assertTrue(newListener.getExistingEntries().isEmpty());
    }

    private void openAndInitializeTables(String clientName) throws Exception {
        openTables();
        try (TxnContext tx = store.txn(CORFU_SYSTEM_NAMESPACE)) {
            LogReplicationSession key = getTestSession(clientName);
            ReplicationStatus val = getTestReplicationStatus(true);
            tx.putRecord(replicationStatusTable, key, val, null);
            tx.commit();
        }
    }

    private void openTables() throws Exception {
        userDataTable = store.openTable(namespace, userTableName, SampleSchema.Uuid.class,
            SampleSchema.SampleTableAMsg.class, SampleSchema.Uuid.class,
            TableOptions.fromProtoSchema(SampleSchema.SampleTableAMsg.class)
        );

        replicationStatusTable = store.openTable(CORFU_SYSTEM_NAMESPACE,
            REPLICATION_STATUS_TABLE_NAME, LogReplicationSession.class,
            ReplicationStatus.class, null,
            TableOptions.fromProtoSchema(ReplicationStatus.class));
    }

    private LogReplicationSession getTestSession(String clientName) {
        ReplicationSubscriber subscriber = ReplicationSubscriber.newBuilder().setClientName(clientName)
                .setModel(ReplicationModel.LOGICAL_GROUPS)
                .build();
        return LogReplicationSession.newBuilder().setSourceClusterId(defaultClusterId)
                .setSinkClusterId(defaultClusterId).setSubscriber(subscriber)
                .build();
    }

    private ReplicationStatus getTestReplicationStatus(boolean dataConsistent) {
        return ReplicationStatus.newBuilder()
                .setSinkStatus(SinkReplicationStatus.newBuilder().setDataConsistent(dataConsistent))
                .build();
    }

    private void writeDataAndToggleDataConsistentConcurrently(int numIterations, int numUpdatesToDataTable,
                                                              String clientName) throws Exception {
        for (int i = 0; i < numIterations; i++) {
            boolean dataConsistent;
            // Toggle Data Consistent in each iteration
            if (i % 2 == 0) {
                dataConsistent = false;
            } else {
                dataConsistent = true;
            }
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
                    LogReplicationSession key = getTestSession(clientName);
                    ReplicationStatus val = getTestReplicationStatus(dataConsistent);
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

    private void writeToStatusTable(boolean dataConsistent, String clientName) {
        try (TxnContext txn = store.txn(CORFU_SYSTEM_NAMESPACE)) {
            LogReplicationSession key = getTestSession(clientName);
            ReplicationStatus val = getTestReplicationStatus(dataConsistent);
            txn.putRecord(replicationStatusTable, key, val, null);
            txn.commit();
        }
    }

    private void writeToDataTableMultipleUpdatesInATx(int numUpdates, int offset) {
        try (TxnContext txn = store.txn(namespace)) {
            for (int index = offset; index < (offset + numUpdates); index++) {
                SampleSchema.Uuid uuid = SampleSchema.Uuid.newBuilder().setMsb(index).setLsb(index).build();
                SampleSchema.SampleTableAMsg msgA =
                    SampleSchema.SampleTableAMsg.newBuilder().setPayload(String.valueOf(index))
                        .build();
                txn.putRecord(userDataTable, uuid, msgA, uuid);
            }
            txn.commit();
        }
    }

    private void writeToNonSubscribedSystemTable() throws Exception {
        Table<LogReplicationMetadata.LogReplicationMetadataKey, LogReplicationMetadata.LogReplicationMetadataVal,
                Message> metadataTable = store.openTable(CORFU_SYSTEM_NAMESPACE,
                LogReplicationMetadataManager.METADATA_TABLE_NAME,
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

    private void verifyData(LinkedList<CorfuStreamEntries> updates,
                            List<CorfuStoreEntry<SampleSchema.Uuid, SampleSchema.SampleTableAMsg, SampleSchema.Uuid>> existingEntries) {
        Map<String, List<CorfuStreamEntry>> updatesPerTable = new HashMap<>();

        // Group the incoming updates as per the table name
        for (CorfuStreamEntries update : updates) {
            for (Map.Entry<TableSchema, List<CorfuStreamEntry>> entry : update.getEntries().entrySet()) {
                List<CorfuStreamEntry> entries = updatesPerTable.getOrDefault(entry.getKey().getTableName(),
                    new ArrayList<>());
                entries.addAll(entry.getValue());
                updatesPerTable.put(entry.getKey().getTableName(), entries);
            }
        }

        // Verify that the streaming data received for each table with the corresponding original table
        for (String tableName : updatesPerTable.keySet()) {
            log.info("TableName {}", tableName);
            // Verify the actual data
            for (CorfuStreamEntry entry : updatesPerTable.get(tableName)) {
                    SampleSchema.Uuid key = (SampleSchema.Uuid) entry.getKey();
                    SampleSchema.SampleTableAMsg val = (SampleSchema.SampleTableAMsg) entry.getPayload();
                    try (TxnContext txn = store.txn(namespace)) {
                        Assert.assertEquals(txn.getRecord(tableName, key).getPayload(), val);
                        txn.commit();
                    }

            }
        }

        for (CorfuStoreEntry entry : existingEntries) {
            try (TxnContext txn = store.txn(namespace)) {
                SampleSchema.Uuid key = (SampleSchema.Uuid) entry.getKey();
                SampleSchema.SampleTableAMsg val = (SampleSchema.SampleTableAMsg) entry.getPayload();
                Assert.assertEquals(txn.getRecord(userTableName, key).getPayload(), val);
            }
        }
    }

    @After
    @Override
    public void cleanUp() throws Exception {
        if (lrListener != null) {
            store.unsubscribeListener(lrListener);
        }

        if (newListener != null) {
            store.unsubscribeListener(newListener);
        }

        if (listener != null) {
            store.unsubscribeListener(listener);
        }
        super.cleanUp();
    }

    private class TestListener implements StreamListener {
        private CountDownLatch countDownLatch;

        @Getter
        private final LinkedList<CorfuStreamEntries> updates = new LinkedList<>();

        TestListener(CountDownLatch countDownLatch) {
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

    private class LRTestListener extends LogReplicationListener {

        @Setter
        private String clientName = testClientName;

        @Setter
        private CountDownLatch countDownLatch;

        // CountDown latch which checks the number of transactions received.
        // Depending on the type of update, a single overridden method is invoked on the listener per update received
        // in onNext().  So this latch can be counted down in each method.
        @Setter
        CountDownLatch numTxLatch = null;

        // Updates received through streaming
        @Getter
        private final LinkedList<CorfuStreamEntries> updates = new LinkedList<>();

        // Entries discovered in performFullSyncAndMerge() before streaming updates are received
        @Getter
        private final List<CorfuStoreEntry<SampleSchema.Uuid, SampleSchema.SampleTableAMsg, SampleSchema.Uuid>>
                existingEntries = new ArrayList<>();

        LRTestListener(CorfuStore corfuStore, String namespace, CountDownLatch countDownLatch) {
            super(corfuStore, namespace);
            this.countDownLatch = countDownLatch;
        }

        @Override
        public void onError(Throwable throwable) {
            log.error("Error on listener", throwable);
            fail("onError for LRTestListener: " + throwable.toString());
        }

        @Override
        protected void onSnapshotSyncStart() {
            log.info("Snapshot sync started");
            countDownNumTxLatch();
        }

        @Override
        protected void onSnapshotSyncComplete() {
            log.info("Snapshot sync complete");
            countDownNumTxLatch();
        }

        @Override
        protected void processUpdatesInSnapshotSync(CorfuStreamEntries results) {
            log.info("Processing updates in snapshot sync.");
            updates.add(results);
            results.getEntries().forEach((key, val) -> countDownLatch.countDown());
            countDownNumTxLatch();
        }

        @Override
        protected void processUpdatesInLogEntrySync(CorfuStreamEntries results) {
            log.info("Processing updates in log entry sync.");
            updates.add(results);
            results.getEntries().forEach((key, val) -> countDownLatch.countDown());
            countDownNumTxLatch();
        }

        @Override
        protected void performFullSyncAndMerge(TxnContext txnContext) {
            List<CorfuStoreEntry<SampleSchema.Uuid, SampleSchema.SampleTableAMsg, SampleSchema.Uuid>> entries =
                    txnContext.executeQuery(userTableName, p -> true);
            existingEntries.addAll(entries);

            existingEntries.forEach(existingEntry -> countDownLatch.countDown());
            countDownNumTxLatch();
        }

        @Override
        protected String getClientName() {
            return clientName;
        }

        private void countDownNumTxLatch() {
            if (numTxLatch != null) {
                numTxLatch.countDown();
            }
        }
    }
}
