package org.corfudb.integration;

import com.google.protobuf.Message;
import lombok.Getter;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultLogReplicationConfigAdapter;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.runtime.CorfuRuntime;
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
import org.mockito.exceptions.misusing.NotAMockException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.fail;

public class LogReplicationPoCIT extends AbstractIT {

    private final int sourceSiteCorfuPort1 = 9000;
    private final int sourceSiteCorfuPort2 = 9002;
    private final int sourceSiteCorfuPort3 = 9004;
    private final int sinkSiteCorfuPort = 9001;
    private final int sourceReplicationPort1 = 9010;
    private final int sourceReplicationPort2 = 9011;
    private final int sourceReplicationPort3 = 9012;

    private final int sinkReplicationPort = 9020;
    private Process sourceCorfu1 = null;
    private Process sourceCorfu2 = null;
    private Process sourceCorfu3 = null;
    private Process sinkCorfu = null;
    private Process sourceReplicationServer1 = null;
    private Process sourceReplicationServer2 = null;
    private Process sourceReplicationServer3 = null;
    private Process sinkReplicationServer = null;

    private CorfuRuntime sourceRuntime1;
    private CorfuRuntime sourceRuntime2;
    private CorfuRuntime sourceRuntime3;
    private CorfuRuntime sinkRuntime;

    private CorfuStore corfuStoreSource1;
    private CorfuStore corfuStoreSource2;
    private CorfuStore corfuStoreSource3;
    private CorfuStore corfuStoreSink;

    private final String sourceEndpoint1 = DEFAULT_HOST + ":" + sourceSiteCorfuPort1;
    private final String sourceEndpoint2 = DEFAULT_HOST + ":" + sourceSiteCorfuPort2;
    private final String sourceEndpoint3 = DEFAULT_HOST + ":" + sourceSiteCorfuPort3;
    private final String sinkEndpoint = DEFAULT_HOST + ":" + sinkSiteCorfuPort;

    private final String TABLE_1 = "Table_Demo_LM-1";
    private final String TABLE_2 = "Table_Demo_LM-2";
    private final String TABLE_3 = "Table_Demo_LM-3";
    private final String TABLE_MERGED = "Table_Demo";
    private final String NAMESPACE = DefaultLogReplicationConfigAdapter.NAMESPACE;
    private final String STREAM_TAG = DefaultLogReplicationConfigAdapter.TAG_ONE;

    private final int THREE = 3;
    private final int NINE = 9;
    private final int FOUR = 4;
    private final int TWO_THOUSAND = 2000;
    private final int SIX = 6;
    private final int TEN = 10;

    public String pluginConfigFilePath;

    private Table<SampleSchema.KeyToMerge, SampleSchema.PayloadToMerge,
        Message> table1;
    private Table<SampleSchema.KeyToMerge, SampleSchema.PayloadToMerge,
        Message> table2;
    private Table<SampleSchema.KeyToMerge, SampleSchema.PayloadToMerge,
        Message> table3;
    private Table<SampleSchema.KeyToMerge, SampleSchema.PayloadToMerge,
        Message> mergedTable;

    @Test
    public void testMergeTable() throws Exception {
        // Setup Corfu on 3 LR Source Sites and 1 LR Sink Site
        setupSourceAndSinkCorfu();

        // Open maps on the Source and Destination Sites
        openMaps();

        System.out.println();
        System.out.println("==============================================");
        System.out.println("Step 1: Write 3 records locally on each LM");
        System.out.println("==============================================");

        // Write data to all the source sites
        writeData(corfuStoreSource1, TABLE_1, table1);
        writeData(corfuStoreSource2, TABLE_2, table2);
        writeData(corfuStoreSource3, TABLE_3, table3);

        System.out.println("\n\n");
        System.out.println("==============================================");
        System.out.println("Step 2: Verify No Data Found on the GM");
        // Assert no data on the sink site
        Assert.assertEquals(0, mergedTable.count());
        System.out.println("Size Of Table on GM: " + mergedTable.count());
        System.out.println("==============================================");


        // Implement and register a stream listener for the necessary stream tag
        Table<SampleSchema.KeyToMerge, SampleSchema.PayloadToMerge,
            Message> testTable1 = corfuStoreSink.openTable(NAMESPACE, TABLE_1,
            SampleSchema.KeyToMerge.class,
            SampleSchema.PayloadToMerge.class, null,
            TableOptions.fromProtoSchema(SampleSchema.PayloadToMerge.class));

        Table<SampleSchema.KeyToMerge, SampleSchema.PayloadToMerge,
            Message> testTable2 = corfuStoreSink.openTable(NAMESPACE, TABLE_2,
            SampleSchema.KeyToMerge.class, SampleSchema.PayloadToMerge.class,
            null,
            TableOptions.fromProtoSchema(SampleSchema.PayloadToMerge.class));

        Table<SampleSchema.KeyToMerge, SampleSchema.PayloadToMerge,
            Message> testTable3 = corfuStoreSink.openTable(NAMESPACE, TABLE_3,
            SampleSchema.KeyToMerge.class,
            SampleSchema.PayloadToMerge.class, null,
            TableOptions.fromProtoSchema(SampleSchema.PayloadToMerge.class));

        MergeTableListener mergeTableListener = new MergeTableListener();
        corfuStoreSink.subscribeListener(mergeTableListener, NAMESPACE, STREAM_TAG);

        Assert.assertEquals(0, mergedTable.count());

        // Subscribe to replication status table on Standby (to be sure data change on status are captured)
        corfuStoreSink.openTable(LogReplicationMetadataManager.NAMESPACE,
            LogReplicationMetadataManager.REPLICATION_STATUS_TABLE,
            LogReplicationMetadata.ReplicationStatusKey.class,
            LogReplicationMetadata.ReplicationStatusVal.class,
            null,
            TableOptions.fromProtoSchema(LogReplicationMetadata.ReplicationStatusVal.class));

        CountDownLatch statusUpdateLatch = new CountDownLatch(SIX);
        ReplicationStatusListener statusListener =
            new ReplicationStatusListener(statusUpdateLatch);
        corfuStoreSink.subscribeListener(statusListener,
            LogReplicationMetadataManager.NAMESPACE,
            LogReplicationMetadataManager.LR_STATUS_STREAM_TAG);

        System.out.println("\n\n");
        System.out.println("==============================================");
        System.out.println("Step 3: Start Log Replication on LMs and GM");
        System.out.println("==============================================");
        // Start Log Replication Servers
        startReplicationServers();

        statusUpdateLatch.await();

        while(mergedTable.count() != NINE) {

        }

        System.out.println("\n\n");
        System.out.println("==============================================");
        System.out.println("Step 4: Verify All Records were written on GM");
        System.out.println("==============================================");
        Assert.assertEquals(NINE, mergedTable.count());
        System.out.println("Size of Table on GM: " + mergedTable.count());
        System.out.println("Table Contents:");
        System.out.println("-----------------");

        List<CorfuStoreEntry<SampleSchema.KeyToMerge, SampleSchema.PayloadToMerge,
            Message>> entries = new ArrayList<>();
        try (TxnContext txn = corfuStoreSink.txn(NAMESPACE)) {
          entries = txn.executeQuery(TABLE_MERGED, p -> true);
          txn.commit();
        }
        entries.forEach(entry -> {
            System.out.print(entry.getKey());
            System.out.println(entry.getPayload());
        });

        System.out.println("\n\n");
        System.out.println("==============================================");
        System.out.println("Step 5: Add a record on table on LM-1");
        System.out.println("==============================================");
        System.out.println("Add (Key4, Payload4) on the table");
        writeMoreData(corfuStoreSource1, TABLE_1, table1);
        while(mergedTable.count() != TEN) {

        }

        System.out.println("\n\n");
        System.out.println("==============================================");
        System.out.println("Step 6: Verify the record was replicated on GM");
        System.out.println("==============================================");
        Assert.assertEquals(TEN, mergedTable.count());
        System.out.println("Size of Table on GM: " + mergedTable.count());
        System.out.println("Table Contents:");
        System.out.println("-----------------");
        try (TxnContext txn = corfuStoreSink.txn(NAMESPACE)) {
            entries = txn.executeQuery(TABLE_MERGED, p -> true);
            txn.commit();
        }
        entries.forEach(entry -> {
            System.out.print(entry.getKey());
            System.out.println(entry.getPayload());
        });

        System.out.println("\n\n");
        System.out.println("==============================================");
        System.out.println("Step 7: Delete a record from LM-2");
        System.out.println("==============================================");
        System.out.println("Delete Key2");
        deleteRecord(corfuStoreSource2, TABLE_2, 2);
        while(mergedTable.count() != NINE) {

        }

        System.out.println("\n\n");
        System.out.println("==============================================");
        System.out.println("Step 8: Verify the record was deleted on GM");
        System.out.println("==============================================");
        Assert.assertEquals(NINE, mergedTable.count());
        System.out.println("Size of Table on GM: " + mergedTable.count());
        System.out.println("Table Contents:");
        System.out.println("-----------------");
        try (TxnContext txn = corfuStoreSink.txn(NAMESPACE)) {
            entries = txn.executeQuery(TABLE_MERGED, p -> true);
            txn.commit();
        }
        entries.forEach(entry -> {
            System.out.print(entry.getKey());
            System.out.println(entry.getPayload());
        });
        corfuStoreSink.unsubscribeListener(mergeTableListener);
        corfuStoreSink.unsubscribeListener(statusListener);

        shutdownCorfuServers();
        shutdownLogReplicationServers();
        sourceRuntime1.shutdown();
        sourceRuntime2.shutdown();
        sourceRuntime3.shutdown();
        sinkRuntime.shutdown();
    }

    private void setupSourceAndSinkCorfu() throws Exception {
        sourceCorfu1 = runServer(sourceSiteCorfuPort1, true);
        sourceCorfu2 = runServer(sourceSiteCorfuPort2, true);
        sourceCorfu3 = runServer(sourceSiteCorfuPort3, true);

        sinkCorfu = runServer(sinkSiteCorfuPort, true);

        // Setup the runtimes to each Corfu server
        CorfuRuntime.CorfuRuntimeParameters params = CorfuRuntime.CorfuRuntimeParameters
            .builder()
            .build();

        sourceRuntime1 = CorfuRuntime.fromParameters(params);
        sourceRuntime1.parseConfigurationString(sourceEndpoint1);
        sourceRuntime1.connect();

        sourceRuntime2 = CorfuRuntime.fromParameters(params);
        sourceRuntime2.parseConfigurationString(sourceEndpoint2);
        sourceRuntime2.connect();

        sourceRuntime3 = CorfuRuntime.fromParameters(params);
        sourceRuntime3.parseConfigurationString(sourceEndpoint3);
        sourceRuntime3.connect();

        sinkRuntime = CorfuRuntime.fromParameters(params);
        sinkRuntime.parseConfigurationString(sinkEndpoint);
        sinkRuntime.connect();

        corfuStoreSource1 = new CorfuStore(sourceRuntime1);
        corfuStoreSource2 = new CorfuStore(sourceRuntime2);
        corfuStoreSource3 = new CorfuStore(sourceRuntime3);
        corfuStoreSink = new CorfuStore(sinkRuntime);
    }

    private void shutdownCorfuServers() {
        if (sourceCorfu1 != null) {
            sourceCorfu1.destroy();
        }

        if (sourceCorfu2 != null) {
            sourceCorfu2.destroy();
        }

        if (sourceCorfu3 != null) {
            sourceCorfu3.destroy();
        }

        if (sinkCorfu != null) {
            sinkCorfu.destroy();
        }
    }

    private void shutdownLogReplicationServers() {
        if (sourceReplicationServer1 != null) {
            sourceReplicationServer1.destroy();
        }

        if (sourceReplicationServer2 != null) {
            sourceReplicationServer2.destroy();
        }

        if (sourceReplicationServer3 != null) {
            sourceReplicationServer3.destroy();
        }

        if (sinkReplicationServer != null) {
            sinkReplicationServer.destroy();
        }
    }

    private void openMaps() throws Exception {
        table1 = corfuStoreSource1.openTable(NAMESPACE, TABLE_1,
            SampleSchema.KeyToMerge.class,
            SampleSchema.PayloadToMerge.class, null,
            TableOptions.fromProtoSchema(SampleSchema.PayloadToMerge.class));

        table2 = corfuStoreSource2.openTable(NAMESPACE, TABLE_2,
            SampleSchema.KeyToMerge.class, SampleSchema.PayloadToMerge.class,
            null,
            TableOptions.fromProtoSchema(SampleSchema.PayloadToMerge.class));

        table3 = corfuStoreSource3.openTable(NAMESPACE, TABLE_3,
            SampleSchema.KeyToMerge.class,
            SampleSchema.PayloadToMerge.class, null,
            TableOptions.fromProtoSchema(SampleSchema.PayloadToMerge.class));

        mergedTable = corfuStoreSink.openTable(NAMESPACE, TABLE_MERGED,
            SampleSchema.KeyToMerge.class, SampleSchema.PayloadToMerge.class,
            null,
            TableOptions.builder().build());
    }

    private void writeData(CorfuStore corfuStore, String tableName,
                           Table table) {
        for (int i=1; i<=THREE; i++) {
            SampleSchema.KeyToMerge key =
                SampleSchema.KeyToMerge.newBuilder().setKey(tableName + " key" +
                    " " + i).build();

            SampleSchema.PayloadToMerge payload =
                SampleSchema.PayloadToMerge.newBuilder().setPayload(
                    tableName + " payload " + i).build();
            try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
                txn.putRecord(table, key, payload, null);
                txn.commit();
            }
        }
    }

    private void writeMoreData(CorfuStore corfuStore, String tableName, Table table) {
        SampleSchema.KeyToMerge key =
            SampleSchema.KeyToMerge.newBuilder().setKey(tableName + " key" +
                " " + FOUR).build();

        SampleSchema.PayloadToMerge payload =
            SampleSchema.PayloadToMerge.newBuilder().setPayload(
                tableName + " payload " + FOUR).build();

        try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
            txn.putRecord(table, key, payload, null);
            txn.commit();
        }
    }

    private void deleteRecord(CorfuStore corfuStore, String tableName,
        int index) {
        SampleSchema.KeyToMerge key =
            SampleSchema.KeyToMerge.newBuilder().setKey(tableName + " key " + index).build();

        try (TxnContext txnContext = corfuStore.txn(NAMESPACE)) {
            txnContext.delete(tableName, key);
            txnContext.commit();
        }
    }

    private void startReplicationServers() throws Exception {
        sourceReplicationServer1 =
            runReplicationServer(sourceReplicationPort1, pluginConfigFilePath);
        sourceReplicationServer2 =
            runReplicationServer(sourceReplicationPort2, pluginConfigFilePath);
        sourceReplicationServer3 =
            runReplicationServer(sourceReplicationPort3, pluginConfigFilePath);
        sinkReplicationServer = runReplicationServer(sinkReplicationPort, pluginConfigFilePath);
    }

    private class MergeTableListener implements StreamListener {

        @Override
        public void onNext(CorfuStreamEntries results) {
            List<CorfuStreamEntry> entries = new ArrayList<>();
            for (List<CorfuStreamEntry> streamEntries :
                results.getEntries().values()) {
                entries.addAll(streamEntries);
            }

            for (CorfuStreamEntry entry : entries) {
                try(TxnContext txn = corfuStoreSink.txn(NAMESPACE)) {
                    if (entry.getOperation() == CorfuStreamEntry.OperationType.DELETE) {
                        txn.delete(TABLE_MERGED, entry.getKey());
                    } else if (entry.getOperation() == CorfuStreamEntry.OperationType.UPDATE) {
                        txn.putRecord(mergedTable,
                            (SampleSchema.KeyToMerge)entry.getKey(),
                            (SampleSchema.PayloadToMerge)entry.getPayload(),
                            (Message)entry.getMetadata());
                    } else if (entry.getOperation() == CorfuStreamEntry.OperationType.CLEAR){
                    } else {
                        throw new RuntimeException("Unexpected Op type " +
                            entry.getOperation());
                    }
                    txn.commit();
                }
            }
        }

        @Override
        public void onError(Throwable t) {
            System.out.print("Error:");
            t.printStackTrace();
        }
    }

    private class ReplicationStatusListener implements StreamListener {

        @Getter
        List<Boolean> accumulatedStatus = new ArrayList<>();

        private final CountDownLatch countDownLatch;

        public ReplicationStatusListener(CountDownLatch countdownLatch) {
            this.countDownLatch = countdownLatch;
        }

        @Override
        public void onNext(CorfuStreamEntries results) {
            results.getEntries().forEach((schema, entries) -> entries.forEach(e ->
                accumulatedStatus.add(((LogReplicationMetadata.ReplicationStatusVal)e.getPayload()).getDataConsistent())));
            countDownLatch.countDown();
        }

        @Override
        public void onError(Throwable throwable) {
            fail("onError for ReplicationStatusListener");
        }
    }
}
