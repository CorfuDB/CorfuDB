package org.corfudb.runtime.collections;

import com.google.protobuf.Message;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.object.transactions.TransactionalContext;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.corfudb.runtime.Queue;
import org.corfudb.runtime.view.TableRegistry;

import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static org.corfudb.runtime.collections.QueryOptions.DEFAULT_OPTIONS;

/**
 * TxnContext is the access layer for binding all the CorfuStore CRUD operations.
 * It can help reduce the footprint of a CorfuStore transaction by only having writes in it.
 * All mutations/writes are aggregated and applied at once a the time of commit() call
 * where a real corfu transaction is started.
 * <p>
 * Created by hisundar, @wenbinzhu, @pankti-m on 2020-09-15
 */
@Slf4j
public class TransactionCrud<T extends StoreTransaction<T>>
        implements StoreTransaction<T> {

    @Getter
    protected final String namespace;
    private final Runnable txAbort;
    protected final Map<UUID, Table<?, ?, ?>> tablesUpdated;

    /**
     * Creates a new TxnContext.
     *
     * @param namespace               Namespace boundary defined for the transaction.
     */
    public TransactionCrud(@Nonnull final String namespace, @Nonnull Runnable txAbort) {
        this.namespace = namespace;
        this.txAbort = txAbort;
        this.tablesUpdated = new HashMap<>();
    }

    /*
     *************************** WRITE APIs *****************************************
     */

    /**
     * All write api must be validate to ensure that the table belongs to the namespace.
     *
     * @param table       - table being written to
     * @param key         - key used in the transaction to check for null.
     * @param validateKey - should key be validated for null.
     * @param <K>         - type of the key
     * @param <V>         - type of the payload/value
     * @param <M>         - type of the metadata
     */
    private <K extends Message, V extends Message, M extends Message>
    void validateWrite(@Nonnull Table<K, V, M> table, K key, boolean validateKey) {
        baseValidateWrite(table, key, validateKey);
    }

    /**private
     * All write api must be validate to ensure that the table belongs to the namespace.
     *
     * @param table       - table being written to
     * @param key         - key used in the transaction to check for null.
     * @param validateKey - should key be validated for null.
     * @param <K>         - type of the key
     * @param <V>         - type of the payload/value
     * @param <M>         - type of the metadata
     */
    private <K extends Message, V extends Message, M extends Message>
    void validateWrite(@Nonnull Table<K, V, M> table, K key, M metadata, boolean validateKey) {
        baseValidateWrite(table, key, validateKey);
        if (table.getMetadataClass() == null && metadata != null) {
            throw new IllegalArgumentException("Metadata schema for table " + table.getFullyQualifiedTableName() + " is defined as NULL, non-null metadata is not allowed.");
        }
    }

    private <K extends Message, V extends Message, M extends Message>
    void baseValidateWrite(@Nonnull Table<K, V, M> table, K key, boolean validateKey) {
        if (!table.getNamespace().equals(TableRegistry.CORFU_SYSTEM_NAMESPACE) &&
                !table.getNamespace().equals(namespace)) {
            throw new IllegalArgumentException("TxnContext can't apply table from namespace "
                    + table.getNamespace() + " to transaction on namespace " + namespace);
        }
        if (!TransactionalContext.isInTransaction()) {
            throw new IllegalStateException( // Do not allow transactions after commit() or abort()
                    "TxnContext cannot be used after a transaction has ended on " +
                            table.getFullyQualifiedTableName());
        }
        if (validateKey && key == null) {
            throw new IllegalArgumentException("Key cannot be null on "
                    + table.getFullyQualifiedTableName() + " in transaction on namespace " + namespace);
        }
    }

    private <K extends Message, V extends Message, M extends Message>
    void validateWrite(@Nonnull Table<K, V, M> table) {
        validateWrite(table, null, false);
    }

    private <K extends Message, V extends Message, M extends Message>
    void validateWrite(@Nonnull Table<K, V, M> table, K key) {
        validateWrite(table, key, true);
    }

    private <K extends Message, V extends Message, M extends Message>
    void validateWrite(@Nonnull Table<K, V, M> table, K key, M metadata) {
        validateWrite(table, key, metadata, true);
    }

    /**
     * put the value on the specified key create record if it does not exist.
     *
     * @param table    Table object to perform the create/update on.
     * @param key      Key of the record.
     * @param value    Value or payload of the record.
     * @param metadata Metadata associated with the record.
     * @param <K>      Type of Key.
     * @param <V>      Type of Value.
     * @param <M>      Type of Metadata.
     */
    @Override
    public <K extends Message, V extends Message, M extends Message>
    void putRecord(@Nonnull Table<K, V, M> table,
                   @Nonnull final K key,
                   @Nonnull final V value,
                   @Nullable final M metadata) {
        validateWrite(table, key, metadata);
        table.put(key, value, metadata);
        tablesUpdated.putIfAbsent(table.getStreamUUID(), table);
    }

    /**
     * Merges the delta value with the old value by applying a caller specified BiFunction and writes
     * the final value.
     *
     * @param table         Table object to perform the merge operation on.
     * @param key           Key
     * @param mergeCallback Function to apply to get the new value
     * @param recordDelta   Argument to pass to the mutation function
     * @param <K>           Type of Key.
     * @param <V>           Type of Value.
     * @param <M>           Type of Metadata.
     */
    @Override
    public <K extends Message, V extends Message, M extends Message>
    void merge(@Nonnull Table<K, V, M> table,
               @Nonnull final K key,
               @Nonnull MergeCallback mergeCallback,
               @Nonnull final CorfuRecord<V, M> recordDelta) {
        validateWrite(table, key);
        CorfuRecord<V, M> oldRecord = table.get(key);
        CorfuRecord<V, M> mergedRecord;
        try {
            mergedRecord = mergeCallback.doMerge(table, key, oldRecord, recordDelta);
        } catch (Exception ex) {
            txAbort.run(); // explicitly abort this transaction and then throw the abort manually
            log.error("TX Abort merge: {}", table.getFullyQualifiedTableName(), ex);
            throw ex;
        }
        if (mergedRecord == null) {
            table.deleteRecord(key);
        } else {
            table.put(key, mergedRecord.getPayload(), mergedRecord.getMetadata());
        }

        tablesUpdated.putIfAbsent(table.getStreamUUID(), table);
    }

    /**
     * touch() is a call to create a conflict on a read in a write-only transaction
     *
     * @param table Table object to perform the create/update on.
     * @param key   Key of the record to touch.
     * @param <K>   Type of Key.
     * @param <V>   Type of Value.
     * @param <M>   Type of Metadata.
     */
    @Override
    public <K extends Message, V extends Message, M extends Message>
    void touch(@Nonnull Table<K, V, M> table,
               @Nonnull final K key) {
        validateWrite(table, key);
        CorfuRecord<V, M> touchedObject = table.get(key);
        if (touchedObject != null) {
            table.put(key, touchedObject.getPayload(), touchedObject.getMetadata());
            tablesUpdated.putIfAbsent(table.getStreamUUID(), table);
        } else { // TODO: add support for touch()ing an object that hasn't been created.
            txAbort.run(); // explicitly abort this transaction and then throw the abort manually
            log.error("TX Abort touch on non-existing object: in " + table.getFullyQualifiedTableName());
            throw new UnsupportedOperationException(
                    "Attempt to touch() a non-existing object in "
                            + table.getFullyQualifiedTableName());
        }
    }

    /**
     * Clears the entire table.
     *
     * @param table Table object to perform the clear on.
     * @param <K>   Type of Key.
     * @param <V>   Type of Value.
     * @param <M>   Type of Metadata.
     */
    @Override
    public <K extends Message, V extends Message, M extends Message>
    void clear(@Nonnull Table<K, V, M> table) {
        validateWrite(table);
        table.clearAll();
        tablesUpdated.putIfAbsent(table.getStreamUUID(), table);
    }

    /**
     * Deletes the specified key.
     *
     * @param table Table object to perform the delete on.
     * @param key   Key of the record to be deleted.
     * @param <K>   Type of Key.
     * @param <V>   Type of Value.
     * @param <M>   Type of Metadata.
     * @return TxnContext instance.
     */
    @Nonnull
    public <K extends Message, V extends Message, M extends Message>
    T delete(@Nonnull Table<K, V, M> table,
                           @Nonnull final K key) {
        validateWrite(table);
        table.deleteRecord(key);
        tablesUpdated.putIfAbsent(table.getStreamUUID(), table);
        return (T) this;
    }

    // ************************** Queue API ***************************************/
    class QueueEntryAddressGetter<V extends Message, M extends Message>
            implements TransactionalContext.PreCommitListener {
        final int smrIndexOfValue = 1;
        public QueueEntryAddressGetter() {}
        /**
         * If we are in a transaction, determine the commit address and fix it up in
         * the queue entry's metadata.
         * @param tokenResponse - the sequencer's token response returned.
         */
        @Override
        public void preCommitCallback(TokenResponse tokenResponse) {
            tablesUpdated.entrySet().forEach(e -> {
                if (e.getValue().getGuidGenerator() == null) {
                    return; // Transaction has an update to a table which is not a Queue, so ignore.
                }
                TransactionalContext.getRootContext().getWriteSetInfo().getWriteSet().getSMRUpdates(e.getKey())
                        .forEach(smrEntry -> {
                            if (smrEntry.getSMRArguments().length > smrIndexOfValue) {
                                CorfuRecord<V, M> queueRecord =
                                        (CorfuRecord<V, M>) smrEntry.getSMRArguments()[smrIndexOfValue];
                                queueRecord.setMetadata((M)
                                        Queue.CorfuQueueMetadataMsg
                                                .newBuilder().setTxSequence(tokenResponse.getSequence())
                                                .build());
                            }
                        });
            });
        }
    }

    /**
     * Enqueue a message object into the CorfuQueue.
     *
     * @param table  Table object to perform the delete on.
     * @param record Record to be inserted into the Queue.
     * @param <K>    Type of Key.
     * @param <V>    Type of Value.
     * @param <M>    Type of Metadata.
     * @return K the type of key this queue table was created with.
     */
    @Override
    @Nonnull
    public <K extends Message, V extends Message, M extends Message>
    K enqueue(@Nonnull Table<K, V, M> table,
              @Nonnull final V record) {
        validateWrite(table);
        if (TransactionalContext.getRootContext().getPreCommitListeners().isEmpty()) {
            TransactionalContext.getCurrentContext().addPreCommitListener(new QueueEntryAddressGetter());
        }
        K ret = table.enqueue(record);
        tablesUpdated.putIfAbsent(table.getStreamUUID(), table);
        return ret;
    }

    /**
     * This API is used to add an entry to the CorfuQueue without materializing the queue in memory.
     *
     * @param table  Table object to operate on the queue.
     * @param record Record to be added.
     * @param streamTags  - stream tags associated to the given stream id
     * @param corfuStore CorfuStore that gets the runtime for the serializer.
     * @param <K>    Type of Key.
     * @param <V>    Type of Value.
     * @param <M>    Type of Metadata.
     * @return K the type of key this queue table was created with.
     */
    @Nonnull
    public <K extends Message, V extends Message, M extends Message>
    K logUpdateEnqueue(@Nonnull Table<K, V, M> table,
                       @Nonnull final V record, List<UUID> streamTags, CorfuStore corfuStore) {

        if (TransactionalContext.getRootContext().getPreCommitListeners().isEmpty()) {
            TransactionalContext.getCurrentContext().addPreCommitListener(new QueueEntryAddressGetter());
        }
        K ret = table.logUpdateEnqueue(record, streamTags, corfuStore);
        tablesUpdated.putIfAbsent(table.getStreamUUID(), table);
        return ret;
    }

    /**
     * This API is used to add an entry to the CorfuQueue without materializing the queue in memory.
     *
     * @param table  Table object to operate on the queue.
     * @param key key of the record to be deleted.
     * @param streamTags  - stream tags associated to the given stream id
     * @param corfuStore CorfuStore that gets the runtime for the serializer.
     * @param <K>    Type of Key.
     * @param <V>    Type of Value.
     * @param <M>    Type of Metadata.
     * @return K the type of key this queue table was created with.
     */
    @Nonnull
    public <K extends Message, V extends Message, M extends Message>
    void logUpdateDelete(@Nonnull Table<K, V, M> table,
                         @Nonnull final K key, List<UUID> streamTags, CorfuStore corfuStore) {
        table.logUpdateDelete(key, streamTags, corfuStore);
        tablesUpdated.putIfAbsent(table.getStreamUUID(), table);
    }

    // *************************** READ API *****************************************

    /**
     * get the full record from the table given a key.
     * If this is invoked on a Read-Your-Writes transaction, it will result in starting a corfu transaction
     * and applying all the updates done so far.
     *
     * @param table Table object to retrieve the record from
     * @param key   Key of the record.
     * @return CorfuStoreEntry<Key, Value, Metadata> instance.
     */
    @Override
    @Nonnull
    public <K extends Message, V extends Message, M extends Message>
    CorfuStoreEntry<K, V, M> getRecord(@Nonnull Table<K, V, M> table,
                                       @Nonnull final K key) {
        CorfuRecord<V, M> record = table.get(key);
        if (record == null) {
            return new CorfuStoreEntry<K, V, M>(key, null, null);
        }
        return new CorfuStoreEntry<K, V, M>(key, record.getPayload(), record.getMetadata());
    }

    /**
     * Query by a secondary index.
     *
     * @param table     Table object.
     * @param indexName Index name. In case of protobuf-defined secondary index it is the field name.
     * @param indexKey  Key to query.
     * @param <K>       Type of Key.
     * @param <V>       Type of Value.
     * @param <I>       Type of index/secondary key.
     * @return Result of the query.
     */
    @Override
    @Nonnull
    public <K extends Message, V extends Message, M extends Message, I>
    List<CorfuStoreEntry<K, V, M>> getByIndex(@Nonnull Table<K, V, M> table,
                                              @Nonnull final String indexName,
                                              @Nonnull final I indexKey) {
        return table.getByIndex(indexName, indexKey);
    }

    /**
     * Gets the count of records in the table at a particular timestamp.
     *
     * @param table - the table whose count is requested.
     * @return Count of records.
     */
    @Override
    public <K extends Message, V extends Message, M extends Message>
    int count(@Nonnull final Table<K, V, M> table) {
        return table.count();
    }

    /**
     * Gets all the keys of a table.
     *
     * @param table - the table whose keys are requested.
     * @return keyset of the table
     */
    @Override
    public <K extends Message, V extends Message, M extends Message>
    Set<K> keySet(@Nonnull final Table<K, V, M> table) {
        return table.keySet();
    }

    /**
     * Gets all entries in the table in form of a stream.
     *
     * @param table - the table whose entrires are requested.
     * @return stream of entries in the table
     */
    @Override
    public <K extends Message, V extends Message, M extends Message>
    Stream<CorfuStoreEntry<K, V, M>> entryStream(@Nonnull final Table<K, V, M> table) {
        return table.entryStream();
    }

    /**
     * Scan and filter by entry.
     *
     * @param table          Table< K, V, M > object on which the scan must be done.
     * @param entryPredicate Predicate to filter the entries.
     * @return Collection of filtered entries.
     */
    @Override
    public <K extends Message, V extends Message, M extends Message>
    List<CorfuStoreEntry<K, V, M>> executeQuery(@Nonnull final Table<K, V, M> table,
                                                @Nonnull final Predicate<CorfuStoreEntry<K, V, M>> entryPredicate) {
        return table.scanAndFilterByEntry(entryPredicate);
    }

    /**
     * Execute a join of 2 tables.
     *
     * @param table1         First table in the join query.
     * @param table2         Second table to join with the first.
     * @param query1         Predicate to filter entries in table 1.
     * @param query2         Predicate to filter entries in table 2.
     * @param joinPredicate  Predicate to filter entries during the join.
     * @param joinFunction   Function to merge entries.
     * @param joinProjection Project the merged entries.
     * @param <V1>           Type of Value in table 1.
     * @param <V2>           Type of Value in table 2.
     * @param <T>            Type of resultant value after merging type V and type W.
     * @param <U>            Type of value projected from T.
     * @return Result of query.
     */
    @Override
    @Nonnull
    public <K1 extends Message, K2 extends Message,
            V1 extends Message, V2 extends Message,
            M1 extends Message, M2 extends Message, T, U>
    QueryResult<U> executeJoinQuery(
            @Nonnull final Table<K1, V1, M1> table1,
            @Nonnull final Table<K2, V2, M2> table2,
            @Nonnull final Predicate<CorfuStoreEntry<K1, V1, M1>> query1,
            @Nonnull final Predicate<CorfuStoreEntry<K2, V2, M2>> query2,
            @Nonnull final BiPredicate<V1, V2> joinPredicate,
            @Nonnull final BiFunction<V1, V2, T> joinFunction,
            final Function<T, U> joinProjection) {
        return executeJoinQuery(table1, table2, query1, query2,
                DEFAULT_OPTIONS, DEFAULT_OPTIONS, joinPredicate,
                joinFunction, joinProjection);
    }

    /**
     * Execute a join of 2 tables.
     *
     * @param table1         First table object.
     * @param table2         Second table to join with the first one.
     * @param query1         Predicate to filter entries in table 1.
     * @param query2         Predicate to filter entries in table 2.
     * @param queryOptions1  Query options to transform table 1 filtered values.
     * @param queryOptions2  Query options to transform table 2 filtered values.
     * @param joinPredicate  Predicate to filter entries during the join.
     * @param joinFunction   Function to merge entries.
     * @param joinProjection Project the merged entries.
     * @param <V1>           Type of Value in table 1.
     * @param <V2>           Type of Value in table 2.
     * @param <R>            Type of projected values from table 1 from type V.
     * @param <S>            Type of projected values from table 2 from type W.
     * @param <T>            Type of resultant value after merging type R and type S.
     * @param <U>            Type of value projected from T.
     * @return Result of query.
     */
    @Override
    @Nonnull
    public <K1 extends Message, K2 extends Message,
            V1 extends Message, V2 extends Message,
            M1 extends Message, M2 extends Message,
            R, S, T, U>
    QueryResult<U> executeJoinQuery(
            @Nonnull final Table<K1, V1, M1> table1,
            @Nonnull final Table<K2, V2, M2> table2,
            @Nonnull final Predicate<CorfuStoreEntry<K1, V1, M1>> query1,
            @Nonnull final Predicate<CorfuStoreEntry<K2, V2, M2>> query2,
            @Nonnull final QueryOptions<K1, V1, M1, R> queryOptions1,
            @Nonnull final QueryOptions<K2, V2, M2, S> queryOptions2,
            @Nonnull final BiPredicate<R, S> joinPredicate,
            @Nonnull final BiFunction<R, S, T> joinFunction,
            final Function<T, U> joinProjection) {
        return JoinQuery.executeJoinQuery(table1, table2,
                query1, query2, queryOptions1,
                queryOptions2, joinPredicate, joinFunction, joinProjection);
    }

    /**
     * Test if a record exists in a table.
     *
     * @param table - table object to test if record exists
     * @param key   - key or identifier to test for existence.
     * @param <K>   - type of the key
     * @param <V>   - type of payload or value
     * @param <M>   - type of metadata
     * @return true if record exists and false if record does not exist.
     */
    @Override
    public <K extends Message, V extends Message, M extends Message>
    boolean isExists(@Nonnull Table<K, V, M> table, @Nonnull final K key) {
        CorfuStoreEntry<K, V, M> record = getRecord(table, key);
        return record.getPayload() != null;
    }

    /**
     * Return all the Queue entries ordered by their parent transaction.
     * <p>
     * Note that the key in these entries would be the CorfuQueueIdMsg.
     *
     * @param table Table< K, V, M > object aka queue on which the scan must be done.
     * @return Collection of filtered entries.
     */
    @Override
    public <K extends Message, V extends Message, M extends Message>
    List<Table.CorfuQueueRecord> entryList(@Nonnull final Table<K, V, M> table) {
        return table.entryList();
    }
}
