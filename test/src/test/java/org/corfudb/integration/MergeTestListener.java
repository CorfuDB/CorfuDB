package org.corfudb.integration;

import com.google.protobuf.Message;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.proto.Sample;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LogReplicationListener;
import org.corfudb.runtime.collections.*;
import org.corfudb.test.SampleSchema;

import java.util.*;

import static org.corfudb.integration.LogReplicationAbstractIT.NAMESPACE;
import static org.corfudb.integration.MergeTestListenerIT.GROUPA_TABLE_PREFIX;
import static org.corfudb.integration.MergeTestListenerIT.LOCAL_TABLE_PREFIX;
import static org.corfudb.integration.MergeTestListenerIT.MERGED_TABLE_NAME;
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

    // Updates received through streaming
    @Getter
    public static LinkedList<CorfuStreamEntry> updates = new LinkedList<>();

    public Set<Sample.StringKey> deletedKeys = new HashSet<>();

    public Map<Sample.StringKey, CorfuStoreEntry> addedKeys = new HashMap<>();

    public Set<Sample.StringKey> existingKeysInMergedTable = new HashSet<>();


    public static List<CorfuStoreEntry<Sample.StringKey, SampleSchema.SampleGroupMsgA, Message>> entries
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
     * Apply addedKeys and deletedKeys to merged table, add keys to
     * existingKeysInMergedTable from addedKeys and deletedKeys.
     */
    @Override
    protected void onSnapshotSyncComplete() {
        try (TxnContext tx = corfuStoreSink.txn(NAMESPACE)){
            existingKeysInMergedTable.addAll(addedKeys.keySet());
            addedKeys.forEach((addedKey, e) -> {
                tx.putRecord(tx.getTable(MERGED_TABLE_NAME), addedKey,
                        SampleSchema.SampleMergedTable.newBuilder().setPayload(e.getPayload().toString().replaceAll("[^0-9]", "")).build(), e.getMetadata());
            });

            existingKeysInMergedTable.removeAll(deletedKeys);
            deletedKeys.forEach(deletedKey -> {
                tx.delete(tx.getTable(MERGED_TABLE_NAME), deletedKey);
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
                Sample.StringKey key = (Sample.StringKey) e.getKey();
                if (schema.getTableName().contains("Local_Table00")) {
                    if (!(e.getOperation() == CorfuStreamEntry.OperationType.CLEAR) &&
                            !(existingKeysInMergedTable.contains(key))) {
                        try (TxnContext tx = corfuStoreSink.txn(namespace)){
                            if (e.getOperation() == CorfuStreamEntry.OperationType.UPDATE) {
                                tx.putRecord(tx.getTable(MERGED_TABLE_NAME), key,
                                                SampleSchema.SampleMergedTable.newBuilder().setPayload(e.getPayload().toString().replaceAll("[^0-9]", "")).build(),
                                         e.getMetadata());
                                existingKeysInMergedTable.add(key);
                            }
                            else {
                                tx.delete(tx.getTable(MERGED_TABLE_NAME), key);
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
                Sample.StringKey key = (Sample.StringKey) e.getKey();
                // Add the records to idfwMergedTable.  Add to existingKeysInMergedTable
                // Any other processing specific to the local table
                existingKeysInMergedTable.add(key);
                try (TxnContext tx = corfuStoreSink.txn(NAMESPACE)){
                    tx.putRecord(tx.getTable(MERGED_TABLE_NAME), key,
                            SampleSchema.SampleMergedTable.newBuilder().setPayload(e.getPayload().toString().replaceAll("[^0-9]", "")).build(),
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
        for (int i = 1; i <= numTableForGroupA; i++) {
            entries.addAll(txnContext.executeQuery(GROUPA_TABLE_PREFIX + i, p -> true));
        }

        for (int j = 1; j <= numTableForLocalTables; j++) {
            entries.addAll(txnContext.executeQuery(LOCAL_TABLE_PREFIX + j, p -> true));
        }

        entries.addAll(txnContext.executeQuery(MERGED_TABLE_NAME, p -> true));

        entries.forEach((entry) -> {
            txnContext.putRecord(txnContext.getTable(MERGED_TABLE_NAME), entry.getKey(),
                    SampleSchema.SampleMergedTable.newBuilder().setPayload(entry.getPayload().getPayload()).build(),
                    entry.getMetadata());
            existingKeysInMergedTable.add(entry.getKey());
        });
    }

}
