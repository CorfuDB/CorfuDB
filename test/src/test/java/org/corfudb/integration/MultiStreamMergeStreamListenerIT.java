package org.corfudb.integration;

import com.google.protobuf.Message;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.collections.CorfuStreamEntries;
import org.corfudb.runtime.collections.CorfuStoreEntry;
import org.corfudb.runtime.collections.LrMultiStreamMergeStreamListener;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.test.SampleSchema;
import org.junit.After;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

@Slf4j
public class MultiStreamMergeStreamListenerIT extends AbstractIT{

    private final String corfuSingleNodeHost;
    private final int corfuStringNodePort;
    private final String singleNodeEndpoint;
    private Process corfuServer;
    private CorfuStore store;

    private final String namespace = "test_namespace";
    private final String defaultTableName = "table_testA";
    private final String defaultTag = "sample_streamer_1";

    private final int sleepTime = 100;

    private final int deltaStreamBufferSize = 10;

    Table<SampleSchema.Uuid, SampleSchema.SampleTableAMsg, SampleSchema.Uuid> tableA;
    Table<LogReplicationMetadata.ReplicationStatusKey, LogReplicationMetadata.ReplicationStatusVal, Message> replicationStatusTable;


    public MultiStreamMergeStreamListenerIT() {
        corfuSingleNodeHost = PROPERTIES.getProperty("corfuSingleNodeHost");
        corfuStringNodePort = Integer.valueOf(PROPERTIES.getProperty("corfuSingleNodePort"));
        singleNodeEndpoint = String.format("%s:%d",
                corfuSingleNodeHost,
                corfuStringNodePort);
    }

    @After
    public void cleanup() {
        try {
            shutdownCorfuServer(corfuServer);
        } catch (Exception e) {
            log.error("clean up failed", e);
        }
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

    private void openTable() throws Exception {
        tableA = store.openTable(
                namespace, defaultTableName,
                SampleSchema.Uuid.class, SampleSchema.SampleTableAMsg.class, SampleSchema.Uuid.class,
                TableOptions.fromProtoSchema(SampleSchema.SampleTableAMsg.class)
        );

        replicationStatusTable = store.openTable(LogReplicationMetadataManager.NAMESPACE,
                LogReplicationMetadataManager.REPLICATION_STATUS_TABLE,
                LogReplicationMetadata.ReplicationStatusKey.class,
                LogReplicationMetadata.ReplicationStatusVal.class,
                null,
                TableOptions.fromProtoSchema(LogReplicationMetadata.ReplicationStatusVal.class));
    }

    private void writeUpdatesToDefaultTable(int numUpdates, int offset) throws Exception {

        // Make some updates to tableA
        for (int index = offset; index < offset + numUpdates; index++) {
            // Update TableA
            try (TxnContext tx = store.txn(namespace)) {
                SampleSchema.Uuid uuid = SampleSchema.Uuid.newBuilder().setMsb(index).setLsb(index).build();
                SampleSchema.SampleTableAMsg msgA = SampleSchema.SampleTableAMsg.newBuilder().setPayload(String.valueOf(index)).build();
                tx.putRecord(tableA, uuid, msgA, uuid);
                tx.commit();
            }
        }
    }

    private void writeUpdatesToBothTable(int numUpdates, int offset) throws Exception {

        // Make some updates to tableA
        for (int index = offset; index < offset + numUpdates; index++) {
            if (index % 2 == 0) {
                // Update TableA
                try (TxnContext tx = store.txn(namespace)) {
                    SampleSchema.Uuid uuid = SampleSchema.Uuid.newBuilder().setMsb(index).setLsb(index).build();
                    SampleSchema.SampleTableAMsg msgA = SampleSchema.SampleTableAMsg.newBuilder().setPayload("tableA " + String.valueOf(index)).build();
                    tx.putRecord(tableA, uuid, msgA, uuid);
                    tx.commit();
                }
            } else {
                // Update LRStatus table
                LogReplicationMetadata.ReplicationStatusKey key = LogReplicationMetadata.ReplicationStatusKey
                        .newBuilder().setClusterId("lrstatus" + Integer.toString(index)).build();
                LogReplicationMetadata.ReplicationStatusVal val = LogReplicationMetadata.ReplicationStatusVal
                        .newBuilder().setRemainingEntriesToSend(index).build();
                try (TxnContext tx = store.txn(CORFU_SYSTEM_NAMESPACE)) {
                    tx.putRecord(replicationStatusTable, key, val, null);
                    tx.commit();
                }
            }
        }
    }

    private void verifyUpdates(LinkedList<CorfuStreamEntries> updates) {
        long startTs = -1;
        for (CorfuStreamEntries update : updates) {
            if(startTs == -1) {
                startTs = update.getTimestamp().getSequence();
                continue;
            }
            assertThat(startTs < update.getTimestamp().getSequence());
            startTs = update.getTimestamp().getSequence();
        }
    }

    /*
    * This tests the scenario where streamListener is subscribed but only one table has data, the
    * other table has no data. After sometime, data gets written to both the tables.
    * Verifies that streamListener sees the updates in the correct sequence.
    * */
    @Test
    public void testWriteClientTableSyncWriteBothTables() throws Exception {
        // Run a corfu server & initialize CorfuStore
        initializeCorfu();

        openTable();
        StreamListenerImplMultiStream listener = new StreamListenerImplMultiStream();
        store.subscribeLrMergeListener(listener, namespace, defaultTag, Arrays.asList(defaultTableName));

        StreamListenerImplMultiStream listenerBufferDefined = new StreamListenerImplMultiStream();
        store.subscribeLrMergeListener(listenerBufferDefined, namespace, defaultTag,
                Arrays.asList(defaultTableName), CorfuStoreMetadata.Timestamp.getDefaultInstance(),
                deltaStreamBufferSize);

        final int numUpdates = 10;
        // Write updates to table_A only
        writeUpdatesToDefaultTable(numUpdates, 0);

        // Give time for deltas to be processed
        TimeUnit.MILLISECONDS.sleep(sleepTime);
        LinkedList<CorfuStreamEntries> updates = listener.getUpdates();
        assertThat(updates.size()).isEqualTo(numUpdates);

        // Write more updates to table_A and LrStatus table
        writeUpdatesToBothTable(numUpdates/2, numUpdates);
        TimeUnit.MILLISECONDS.sleep(sleepTime);
        assertThat(updates.size()).isEqualTo(numUpdates + numUpdates/2);
        verifyUpdates(updates);

        assertThat(shutdownCorfuServer(corfuServer)).isTrue();

        assertThat(listenerBufferDefined.updates.size()).isEqualTo(numUpdates + numUpdates/2);
    }


    /*
     * This tests the scenario where streamListener is subscribed after initial write to one table, the
     * other table has no data at this point. After sometime, data gets written to the same table.
     * Verifies that the streamListener sees the updates from one table in the correct sequence.
     * */
    @Test
    public void testStreamingClientTableFromTsCorfuTableEmpty() throws Exception {
        // Run a corfu server & initialize CorfuStore
        initializeCorfu();
        openTable();

        final int numUpdates = 10;
        final int indexDefault= 0;
        // Write updates to table_A only
        writeUpdatesToDefaultTable(numUpdates, 0);

        CorfuStoreMetadata.Timestamp snapshotTs;
        SampleSchema.Uuid firstKey = SampleSchema.Uuid.newBuilder().setMsb(indexDefault).setLsb(indexDefault).build();
        SampleSchema.SampleTableAMsg firstValue = SampleSchema.SampleTableAMsg.newBuilder().setPayload(String.valueOf(indexDefault)).build();

        // read tail of table_A
        try (TxnContext txn = store.txn(namespace)) {
            CorfuStoreEntry<SampleSchema.Uuid, SampleSchema.SampleTableAMsg, SampleSchema.Uuid> entry = txn.getRecord(defaultTableName, firstKey);
            assertThat(entry.getPayload()).isEqualTo(firstValue);
            snapshotTs = txn.commit();
            assertThat(snapshotTs.getSequence()).isEqualTo(runtime.getSequencerView()
                    .query(CorfuRuntime.getStreamID(TableRegistry.getFullyQualifiedTableName(namespace, defaultTableName))));
        }

        // Subscribe from latest Ts. The listener starts listening to both the LrStatus table and table_A from the Ts onwards.
        StreamListenerImplMultiStream listener = new StreamListenerImplMultiStream();
        store.subscribeLrMergeListener(listener, namespace, defaultTag, Arrays.asList(defaultTableName), snapshotTs);

        // Write more updates to table_A
        writeUpdatesToDefaultTable(numUpdates/2, 0);

        // Give time for deltas to be processed
        TimeUnit.MILLISECONDS.sleep(sleepTime);
        LinkedList<CorfuStreamEntries> updates = listener.getUpdates();
        assertThat(updates.size()).isEqualTo(numUpdates/2);
        verifyUpdates(updates);

        assertThat(shutdownCorfuServer(corfuServer)).isTrue();
    }

    /*
     * This tests the scenario where streamListener is subscribed but there is no data in either of
     * the tables
     * */
    @Test
    public void testBothTablesEmpty() throws Exception {
        // Run a corfu server & initialize CorfuStore
        initializeCorfu();
        openTable();

        // Subscribe from latest full sync
        StreamListenerImplMultiStream listener = new StreamListenerImplMultiStream();
        store.subscribeLrMergeListener(listener, namespace, defaultTag, Arrays.asList(defaultTableName));

        // Give time for deltas to be processed
        TimeUnit.MILLISECONDS.sleep(sleepTime);
        LinkedList<CorfuStreamEntries> updates = listener.getUpdates();
        assertThat(updates.size()).isEqualTo(0);
        verifyUpdates(updates);

        assertThat(shutdownCorfuServer(corfuServer)).isTrue();
    }

    /*
     * This tests the scenario where data is written to both the tables in interleaving fashion.
     * Verifies that the streamListener sees the data in the correct sequence
     * */
    @Test
    public void testStreamingWritesInterleaved() throws Exception {
        // Run a corfu server & initialize CorfuStore
        initializeCorfu();

        openTable();
        StreamListenerImplMultiStream listener = new StreamListenerImplMultiStream();
        store.subscribeLrMergeListener(listener, namespace, defaultTag, Arrays.asList(defaultTableName));

        final int numUpdates = 10;
        // Write updates to table_A and LrStatus table
        writeUpdatesToBothTable(numUpdates, 0);

        // Give time for deltas to be processed
        TimeUnit.MILLISECONDS.sleep(sleepTime);
        LinkedList<CorfuStreamEntries> updates = listener.getUpdates();
        assertThat(updates.size()).isEqualTo(numUpdates);

        // Write more updates to table_A and LrStatus table
        writeUpdatesToBothTable(numUpdates/2, numUpdates);
        TimeUnit.MILLISECONDS.sleep(sleepTime);
        assertThat(updates.size()).isEqualTo(numUpdates + numUpdates/2);
        verifyUpdates(updates);

        assertThat(shutdownCorfuServer(corfuServer)).isTrue();
    }

    private class StreamListenerImplMultiStream implements LrMultiStreamMergeStreamListener {

        @Getter
        private final LinkedList<CorfuStreamEntries> updates = new LinkedList<>();

        @Override
        public void onNext(CorfuStreamEntries results) {
            updates.add(results);
        }

        @Override
        public void onError(Throwable throwable) {
            //Ignore
        }
    }

}
