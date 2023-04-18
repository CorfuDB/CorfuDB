package org.corfudb.integration;

import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.proto.Sample;
import org.corfudb.runtime.LogReplicationListener;
import org.corfudb.runtime.collections.*;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.test.SampleSchema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.List;
import java.util.Set;


import static org.corfudb.integration.LogReplicationAbstractIT.NAMESPACE;
import static org.corfudb.integration.MergeTestListenerIT.GROUPA_TABLE_PREFIX;
import static org.corfudb.integration.MergeTestListenerIT.LOCAL_TABLE_PREFIX;
import static org.corfudb.integration.MergeTestListenerIT.MERGED_TABLE_NAME;
import static org.junit.Assert.fail;

@Slf4j
public class MergeTestListener extends LogReplicationListener{

    private static CorfuStore corfuStoreSink;

    // Keys not present in the replicated tables and hence to be deleted from the merged table
    private static Set<Sample.StringKey> deletedKeys = new HashSet<>();

    // Records newly added or updated in the replicated tables and hence to be added/updated in the merged table
    private static Map<Sample.StringKey, CorfuStoreEntry> newRecords = new HashMap<>();

    // Keys in the merged table which were received from the replicated tables
    private static Set<Sample.StringKey> existingKeysInMergedTable = new HashSet<>();

    private static List<CorfuStoreEntry<Sample.StringKey, SampleSchema.SampleGroupMsgA, Message>> entries
            = new ArrayList<>();

    private final int numTableForGroupA;

    private final int numTableForLocalTables;

    MergeTestListener(CorfuStore corfuStoreSink, String namespace, int numTableForGroupA, int numTableForLocalTables) {
        super(corfuStoreSink, namespace);
        this.corfuStoreSink = corfuStoreSink;
        this.numTableForGroupA = numTableForGroupA;
        this.numTableForLocalTables = numTableForLocalTables;
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
     * Apply newRecords and deletedKeys to merged table
     * when Snapshot Sync ends.
     */
    @Override
    protected void onSnapshotSyncComplete() {
        try (TxnContext tx = corfuStoreSink.txn(NAMESPACE)){
            // Compute the diff
            // diff: deletedKeys = deletedKeys + (existingKeysInMergedTable - (unchangedKeys + newRecords))
            Set<Sample.StringKey> diff = new HashSet<>(existingKeysInMergedTable);
            diff.removeAll(newRecords.keySet());
            deletedKeys.addAll(diff);

            // Apply deletedKeys to merged table
            existingKeysInMergedTable.removeAll(deletedKeys);
            deletedKeys.forEach(deletedKey -> {
                tx.delete(tx.getTable(MERGED_TABLE_NAME), deletedKey);
            });

            // Apply newRecords to merged table
            existingKeysInMergedTable.addAll(newRecords.keySet());
            newRecords.forEach((addedKey, e) -> {
                tx.putRecord(tx.getTable(MERGED_TABLE_NAME), addedKey,
                        SampleSchema.SampleMergedTable.newBuilder().setPayload(e.getPayload().toString().
                                replaceAll("[^0-9]", "")).build(), e.getMetadata());
            });
            tx.commit();
        } catch  (TransactionAbortedException tae) {
            log.error("Fail to update existingKeysInMergedTable");
            throw tae;
        }
        log.info("Snapshot sync complete");
    }

    /**
     * @param results Entries received in a single transaction as part of a snapshot sync
     */
    @Override
    protected void processUpdatesInSnapshotSync(CorfuStreamEntries results) {
        log.debug("Processing updates in snapshot sync");
        results.getEntries().forEach((schema, entries) -> {
            entries.forEach(e -> {
                if (schema.getTableName().contains(LOCAL_TABLE_PREFIX)) {
                        try (TxnContext tx = corfuStoreSink.txn(NAMESPACE)){
                            if ((e.getOperation() == CorfuStreamEntry.OperationType.UPDATE) &&
                                    !(existingKeysInMergedTable.contains((Sample.StringKey) e.getKey()))) {
                                // Add the records to idfwMergedTable
                                tx.putRecord(tx.getTable(MERGED_TABLE_NAME), (Sample.StringKey) e.getKey(),
                                        SampleSchema.SampleMergedTable.newBuilder().setPayload(e.getPayload().
                                                toString().replaceAll("[^0-9]", "")).build(),
                                        e.getMetadata());
                            }
                            else if ((e.getOperation() == CorfuStreamEntry.OperationType.DELETE)) {
                                // Delete the records from idfwMergedTable
                                tx.delete(tx.getTable(MERGED_TABLE_NAME), (Sample.StringKey) e.getKey());
                            }
                            tx.commit();
                        }
                        catch  (TransactionAbortedException tae) {
                            log.error("Fail to process entry {}, in table {}", e.getKey(), schema.getTableName());
                            throw tae;
                        }
                    }
                else {
                    if ((e.getOperation() == CorfuStreamEntry.OperationType.UPDATE) &&
                            !(existingKeysInMergedTable.contains((Sample.StringKey) e.getKey()))) {
                        // Add the keys to newRecords
                        newRecords.put((Sample.StringKey) e.getKey(), e);
                    }
                    else if ((e.getOperation() == CorfuStreamEntry.OperationType.DELETE)) {
                        // Add the keys to deletedKeys
                        deletedKeys.add((Sample.StringKey) e.getKey());
                    }
                }
            });
        });
    }

    /**
     * @param results Entries received in a single transaction as part of a log entry sync
     */
    @Override
    protected void processUpdatesInLogEntrySync(CorfuStreamEntries results) {
        log.debug("Processing updates in log entry sync");
        results.getEntries().forEach((schema, entries) -> {
            entries.forEach(e -> {
                try (TxnContext tx = corfuStoreSink.txn(NAMESPACE)){
                    if (schema.getTableName().contains(LOCAL_TABLE_PREFIX)) {
                        if ((e.getOperation() == CorfuStreamEntry.OperationType.UPDATE) &&
                                !(existingKeysInMergedTable.contains((Sample.StringKey) e.getKey()))) {
                            // Add the records to idfwMergedTable
                            tx.putRecord(tx.getTable(MERGED_TABLE_NAME), (Sample.StringKey) e.getKey(),
                                    SampleSchema.SampleMergedTable.newBuilder().setPayload(e.getPayload().
                                            toString().replaceAll("[^0-9]", "")).build(),
                                    e.getMetadata());
                        }
                        else if ((e.getOperation() == CorfuStreamEntry.OperationType.DELETE)) {
                            // Delete the records from idfwMergedTable
                            tx.delete(tx.getTable(MERGED_TABLE_NAME), (Sample.StringKey) e.getKey());
                        }
                    }
                    else {
                        if ((e.getOperation() == CorfuStreamEntry.OperationType.UPDATE) &&
                                !(existingKeysInMergedTable.contains((Sample.StringKey) e.getKey()))) {
                            // Add the records to idfwMergedTable
                            // Update existingKeysInMergedTable
                            tx.putRecord(tx.getTable(MERGED_TABLE_NAME), (Sample.StringKey) e.getKey(),
                                    SampleSchema.SampleMergedTable.newBuilder().
                                            setPayload(e.getPayload().
                                                    toString().replaceAll("[^0-9]", "")).build(),
                                    e.getMetadata());
                            existingKeysInMergedTable.add((Sample.StringKey) e.getKey());
                        }
                        else if ((e.getOperation() == CorfuStreamEntry.OperationType.DELETE)) {
                            // Delete the records from idfwMergedTable
                            // Update existingKeysInMergedTable
                            tx.delete(tx.getTable(MERGED_TABLE_NAME), (Sample.StringKey) e.getKey());
                            existingKeysInMergedTable.remove((Sample.StringKey) e.getKey());
                        }
                    }
                tx.commit();
                }  catch  (TransactionAbortedException tae) {
                    log.error("Fail to process entry {}, in table {}", e.getKey(), schema.getTableName());
                    throw tae;
                }

            });
        });
    }

    /**
     * Read the replicated and local tables and write them to idfwMergedTable.
     * Construct existingKeysInMergedTable from the entries in replicated tables.
     *
     * @param txnContext transaction context in which the operation must be performed
     */
    @Override
    protected void performFullSync(TxnContext txnContext) {
        log.info("Processing existing updates");

        // Read the existing entries from replicated tables
        for (int i = 1; i <= numTableForGroupA; i++) {
            entries.addAll(txnContext.executeQuery(GROUPA_TABLE_PREFIX + i, p -> true));
        }

        // Construct existingKeysInMergedTable from the entries in replicated tables
        entries.forEach((entryInTableForGroupA) -> {
                existingKeysInMergedTable.add(entryInTableForGroupA.getKey());
            }
        );

        // Read the existing entries from local tables
        for (int j = 1; j <= numTableForLocalTables; j++) {
            entries.addAll(txnContext.executeQuery(LOCAL_TABLE_PREFIX + j, p -> true));
        }

        // Read the existing entries from merged table
        entries.addAll(txnContext.executeQuery(MERGED_TABLE_NAME, p -> true));

        // Write entries to idfwMergedTable
        entries.forEach((entry) -> {
            txnContext.putRecord(txnContext.getTable(MERGED_TABLE_NAME), entry.getKey(),
                    SampleSchema.SampleMergedTable.newBuilder().setPayload(entry.getPayload().getPayload()).build(),
                    entry.getMetadata());
        });
    }
}
