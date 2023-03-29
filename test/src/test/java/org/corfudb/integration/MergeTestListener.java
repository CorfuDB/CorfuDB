package org.corfudb.integration;

import com.google.protobuf.Message;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LogReplicationListener;
import org.corfudb.runtime.collections.*;
import org.corfudb.test.SampleSchema;
import org.junit.Before;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

import static org.corfudb.integration.AbstractIT.*;
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

    private final String corfuSingleNodeHost;
    private final int corfuStringNodePort;

    public CorfuRuntime runtime;

    private final String singleNodeEndpoint;

    private CorfuStore store;

    private final String namespace = "test_namespace";

    private final String userTableName = "data_table";

    // Updates received through streaming
    @Getter
    private final LinkedList<CorfuStreamEntry> updates = new LinkedList<>();

    private Set<Message> deletedKeys = new HashSet<>();

    private Map<Message, CorfuStoreEntry> addedKeys = new HashMap<>();

    private Set<Message> existingKeysInMergedTable = new HashSet<>();

    Table<SampleSchema.Uuid, SampleSchema.ValueFieldTagOne, SampleSchema.Uuid> mergedTable;

    MergeTestListener(CorfuStore corfuStore, String namespace) {
        super(corfuStore, namespace);

        corfuSingleNodeHost = PROPERTIES.getProperty("corfuSingleNodeHost");
        corfuStringNodePort = Integer.valueOf(PROPERTIES.getProperty("corfuSingleNodePort"));
        singleNodeEndpoint = String.format("%s:%d", corfuSingleNodeHost, corfuStringNodePort);
    }

    @Before
    public void init() throws IOException, InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        new AbstractIT.CorfuServerRunner()
                .setHost(corfuSingleNodeHost)
                .setPort(corfuStringNodePort)
                .setLogPath(getCorfuServerLogPath(corfuSingleNodeHost, corfuStringNodePort))
                .setSingle(true)
                .runServer();
        runtime = createRuntime(singleNodeEndpoint);
        store = new CorfuStore(runtime);
        openMergedTable();
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
        existingKeysInMergedTable.addAll(addedKeys.keySet());
        addedKeys.forEach((addedKey, e) -> {
            try (TxnContext tx = store.txn(namespace)){
                tx.putRecord(mergedTable, (SampleSchema.Uuid) addedKey,
                        (SampleSchema.ValueFieldTagOne) e.getPayload(), (SampleSchema.Uuid) e.getMetadata());
                tx.commit();
            }
        });

        existingKeysInMergedTable.addAll(deletedKeys);
        deletedKeys.forEach(deletedKey -> {
            try (TxnContext tx = store.txn(namespace)){
                tx.delete(mergedTable, (SampleSchema.Uuid) deletedKey);
                tx.commit();
            }
        });
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
                if (schema.getTableName().equals("local_table_name")) {
                    existingKeysInMergedTable.add(key);
                    try (TxnContext tx = store.txn(namespace)){
                        tx.putRecord(mergedTable, (SampleSchema.Uuid) key, (SampleSchema.ValueFieldTagOne)
                                        e.getPayload(),
                                (SampleSchema.Uuid) e.getMetadata());
                        tx.commit();
                    }
                }
                else {
                    updates.add(e);
                    if ((e.getOperation() == CorfuStreamEntry.OperationType.UPDATE) &&
                            !(existingKeysInMergedTable.contains(key))) {
                        addedKeys.put(key, e);
                    }
                    else if ((e.getOperation() == CorfuStreamEntry.OperationType.DELETE) &&
                            !(existingKeysInMergedTable.contains(key))) {
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
                if (!schema.getTableName().equals("local_table_name")) {
                    updates.add(e);
                }
                Message key = e.getKey();
                // Add the records to idfwMergedTable.  Add to existingKeysInMergedTable
                // Any other processing specific to the local table
                existingKeysInMergedTable.add(key);
                try (TxnContext tx = store.txn(namespace)){
                    tx.putRecord(mergedTable, (SampleSchema.Uuid) key, (SampleSchema.ValueFieldTagOne)
                                    e.getPayload(),
                            (SampleSchema.Uuid) e.getMetadata());
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
        List<CorfuStoreEntry<SampleSchema.Uuid, SampleSchema.ValueFieldTagOne, SampleSchema.Uuid>> entries =
                txnContext.executeQuery(userTableName, p -> true);
        entries.forEach((entry) -> {
            txnContext.putRecord(mergedTable, entry.getKey(),
                            entry.getPayload(),
                     entry.getMetadata());
            existingKeysInMergedTable.add(entry.getKey());
        });
    }

    /**
     * Open merged table.
     *
     * @throws InvocationTargetException
     * @throws NoSuchMethodException
     * @throws IllegalAccessException
     */
    protected void openMergedTable() throws InvocationTargetException, NoSuchMethodException, IllegalAccessException{
        String mergedTableName = "MERGED_TABLE";
        mergedTable = store.openTable(
                NAMESPACE,
                mergedTableName,
                SampleSchema.Uuid.class,
                SampleSchema.ValueFieldTagOne.class,
                SampleSchema.Uuid.class,
                TableOptions.fromProtoSchema(SampleSchema.ValueFieldTagOne.class)
        );
    }
}
