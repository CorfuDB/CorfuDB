package org.corfudb.integration;

import com.google.protobuf.Message;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.proto.Sample;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LogReplicationListener;
import org.corfudb.runtime.collections.*;
import org.corfudb.test.SampleSchema;

import java.lang.reflect.InvocationTargetException;
import java.util.*;

import static org.corfudb.integration.LogReplicationAbstractIT.NAMESPACE;
import static org.junit.Assert.fail;

/**
 * This class is for LogicalGroup E2E IT
 *
 *  (1) Open a merged table that store all the existing data and keys,
 *      add keys to a set that store all the existing keys
 *  (2) Start snapshot sync
 *  (3) Process updates for snapshot sync, perform the diff by using
 *      the set in (1) when processing replicated tables
 *  (4) Snapshot sync ends, add the keys to the existing keys' set
 *      and apply the keys to merged table
 *  (5) Process updates for log entry sync
 */
@Slf4j
public class MergeTestListener extends LogReplicationListener{


    public CorfuRuntime runtime;


    private CorfuStore corfuStoreSink;

    private final String namespace = "LR-Test";

    String mergedTableName = "MERGED_TABLE";


    // Updates received through streaming
    @Getter
    public static LinkedList<CorfuStreamEntry> updates = new LinkedList<>();

    public Set<Message> deletedKeys = new HashSet<>();

    public Map<Message, CorfuStoreEntry> addedKeys = new HashMap<>();

    public Set<Message> existingKeysInMergedTable = new HashSet<>();

    public static Table<Sample.StringKey, SampleSchema.SampleMergedTable, Message> mergedTable;

    List<Table<Sample.StringKey, SampleSchema.SampleGroupMsgA, Message>> tableListForGroupA;

    List<Table<Sample.StringKey, SampleSchema.SampleGroupMsgA, Message>> tableListForLocalTable;

    public static List<CorfuStoreEntry<Sample.StringKey, SampleSchema.SampleGroupMsgA, Message>> entries
            = new ArrayList<>();

    MergeTestListener(CorfuStore corfuStoreSink, String namespace, List<Table<Sample.StringKey, SampleSchema.SampleGroupMsgA, Message>>
                      tableListForGroupA, List<Table<Sample.StringKey, SampleSchema.SampleGroupMsgA, Message>> tableListForLocalTable
    ) throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        super(corfuStoreSink, namespace);
        this.corfuStoreSink = corfuStoreSink;
        openMergedTable();
        this.tableListForGroupA = tableListForGroupA;
        this.tableListForLocalTable = tableListForLocalTable;
    }

    @Override
    public void onError(Throwable throwable) {
        log.error("Error on listener", throwable);
        fail("onError for LRTestListener: " + throwable.toString());
    }

    /**
     * Invoke when snapshot sync starts
     */
    @Override
    protected void onSnapshotSyncStart() {
        log.info("Snapshot sync started");
    }

    /**
     * Apply addedKeys and deletedKeys to merged table, add keys to
     * existingKeysInMergedTable from addedKeys and deletedKeys.
     */
    @Override
    protected void onSnapshotSyncComplete() {
        try (TxnContext tx = corfuStoreSink.txn(NAMESPACE)){
            existingKeysInMergedTable.addAll(addedKeys.keySet());
            addedKeys.forEach((addedKey, e) -> {
                tx.putRecord(mergedTable, (Sample.StringKey) addedKey,
                        SampleSchema.SampleMergedTable.newBuilder().setPayload(e.getPayload().toString()).build(), e.getMetadata());
            });

            existingKeysInMergedTable.removeAll(deletedKeys);
            deletedKeys.forEach(deletedKey -> {
                tx.delete(mergedTable, (Sample.StringKey) deletedKey);
            });

            tx.commit();
        }

        log.info("Snapshot sync complete");
    }

    /**
     * Write local tables to merged table, process updates and add the keys to addedKeys
     * and deletedKeys for replicated table.
     *
     * @param results Entries received in a single transaction as part of a snapshot sync
     */
    @Override
    protected void processUpdatesInSnapshotSync(CorfuStreamEntries results) {
        log.info("Processing updates in snapshot sync");
        results.getEntries().forEach((schema, entries) -> {
            entries.forEach(e -> {
                Message key = e.getKey();
                if (schema.getTableName().contains("Local_Table00")) {
                    System.out.println(schema.getTableName());
                    System.out.println(mergedTable.count());
                    if (!(e.getOperation() == CorfuStreamEntry.OperationType.CLEAR) &&
                            !(existingKeysInMergedTable.contains(key))) {
                        try (TxnContext tx = corfuStoreSink.txn(namespace)){
                            if (e.getOperation() == CorfuStreamEntry.OperationType.UPDATE) {
                                tx.putRecord(mergedTable, (Sample.StringKey) key,
                                                SampleSchema.SampleMergedTable.newBuilder().setPayload(e.getPayload().toString()).build(),
                                         e.getMetadata());
                                existingKeysInMergedTable.add(key);
                            }
                            else {
                                tx.delete(mergedTable, (Sample.StringKey) key);
                            }
                            tx.commit();
                        }
                    }
                }
                else {
                    if ((e.getOperation() == CorfuStreamEntry.OperationType.UPDATE) &&
                            !(existingKeysInMergedTable.contains(key))) {
                        updates.add(e);
                        addedKeys.put(key, e);
                    }
                    else if ((e.getOperation() == CorfuStreamEntry.OperationType.DELETE) &&
                            !(existingKeysInMergedTable.contains(key))) {
                        updates.add(e);
                        deletedKeys.add(key);
                    }
                }
            });

        });
    }

    /**
     * Write records to merged table, and process updates from replicated
     * tables.
     *
     * @param results Entries received in a single transaction as part of a log entry sync
     */
    @Override
    protected void processUpdatesInLogEntrySync(CorfuStreamEntries results) {
        log.info("Processing updates in log entry sync");
        results.getEntries().forEach((schema, entries) -> {
            entries.forEach(e -> {
                updates.add(e);
                Message key = e.getKey();
                // Add the records to idfwMergedTable.  Add to existingKeysInMergedTable
                // Any other processing specific to the local table
                existingKeysInMergedTable.add(key);
                try (TxnContext tx = corfuStoreSink.txn(NAMESPACE)){
                    tx.putRecord(mergedTable, (Sample.StringKey) key,
                            SampleSchema.SampleMergedTable.newBuilder().setPayload(e.getPayload().toString()).build(),
                             e.getMetadata());
                    tx.commit();

                }
            });
        });
    }

    /**
     * Read and write replicated and local tables to the merged table,
     * construct existingKeysInMergedTable and add the existing keys to it.
     *
     * @param txnContext transaction context in which the operation must be performed
     */
    @Override
    protected void performFullSync(TxnContext txnContext) {
        log.info("Processing existing updates");
        tableListForGroupA.forEach((t) -> {
            entries.addAll((txnContext.executeQuery(t.getFullyQualifiedTableName().substring(8), p -> true)));
        });

        tableListForLocalTable.forEach((t) -> {
            entries.addAll((txnContext.executeQuery(t.getFullyQualifiedTableName().substring(8), p -> true)));
        });

        System.out.println(entries.size());

        entries.addAll(txnContext.executeQuery(mergedTable.getFullyQualifiedTableName().substring(8), p -> true));

        entries.forEach((entry) -> {
            txnContext.putRecord(mergedTable, entry.getKey(),
                    SampleSchema.SampleMergedTable.newBuilder().setPayload(entry.getPayload().getPayload()).build(),
                    entry.getMetadata());
            existingKeysInMergedTable.add(entry.getKey());
        });
        System.out.println(mergedTable.count());
    }

    private void openMergedTable() throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        mergedTable = corfuStoreSink.openTable(
                NAMESPACE,
                mergedTableName,
                Sample.StringKey.class,
                SampleSchema.SampleMergedTable.class,
                null,
                TableOptions.fromProtoSchema(SampleSchema.SampleMergedTable.class)
        );
    }


}
