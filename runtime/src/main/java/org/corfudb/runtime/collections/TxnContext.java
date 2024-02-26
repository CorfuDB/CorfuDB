package org.corfudb.runtime.collections;

import com.google.protobuf.Message;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.MultiObjectSMREntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuStoreMetadata.Timestamp;
import org.corfudb.runtime.exceptions.TransactionAlreadyStartedException;
import org.corfudb.runtime.object.transactions.AbstractTransactionalContext;
import org.corfudb.runtime.object.transactions.Transaction;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.ObjectsView;
import org.corfudb.runtime.view.TableRegistry;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
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
public class TxnContext implements AutoCloseable {

    private final ObjectsView objectsView;
    private final TableRegistry tableRegistry;
    @Getter
    private final String namespace;
    private final Token txnSnapshot;

    private final List<CommitCallback> commitCallbacks;

    private boolean iDidNotStartCorfuTxn;

    private final Map<UUID, Table> tablesUpdated;

    /**
     * Creates a new TxnContext.
     *
     * @param objectsView             ObjectsView from the Corfu client.
     * @param tableRegistry           Table Registry.
     * @param namespace               Namespace boundary defined for the transaction.
     * @param isolationLevel          How should this transaction be applied/evaluated.
     * @param allowNestedTransactions Is it ok to re-use another transaction context.
     */
    @Nonnull
    public TxnContext(@Nonnull final ObjectsView objectsView,
                      @Nonnull final TableRegistry tableRegistry,
                      @Nonnull final String namespace,
                      @Nonnull final IsolationLevel isolationLevel,
                      boolean allowNestedTransactions) {
        this.objectsView = objectsView;
        this.tableRegistry = tableRegistry;
        this.namespace = namespace;
        this.commitCallbacks = new ArrayList<>();
        this.tablesUpdated = new HashMap<>();
        this.txnSnapshot = txBeginInternal( // May throw exception if transaction was already started
                allowNestedTransactions,
                isolationLevel);
    }

    /**
     * @param allowNestedTransactions - is it ok to re-use thread's corfu transaction?
     *                                Start the actual corfu transaction.
     *                                Ensure there isn't one already in the same thread.
     * @param isolationLevel - the requested snapshot to start transaction (or UNINITIALIZED if none)
     * @return the snapshot Token of this transaction
     */
    private Token txBeginInternal(boolean allowNestedTransactions, IsolationLevel isolationLevel) {
        if (TransactionalContext.isInTransaction()) {
            TxnContext txnContext = TransactionalContext.getRootContext().getTxnContext();
            if (!allowNestedTransactions) {
                throw new TransactionAlreadyStartedException(TransactionalContext.getRootContext().toString());
            }
            if (txnContext != null) {
                throw new TransactionAlreadyStartedException(TransactionalContext.getRootContext().toString());
            }
            log.warn("Reusing the transactional context created outside this layer!");
            this.iDidNotStartCorfuTxn = true;
            TransactionalContext.getRootContext().setTxnContext(this);
            return TransactionalContext.getRootContext().getSnapshotTimestamp();
        }

        log.trace("TxnContext: begin transaction in namespace {}", namespace);
        Transaction.TransactionBuilder transactionBuilder = this.objectsView
                .TXBuild()
                .type(TransactionType.WRITE_AFTER_WRITE);
        Token snapshotToken;
        if (isolationLevel.getTimestamp() != Token.UNINITIALIZED) {
            transactionBuilder.snapshot(isolationLevel.getTimestamp());
            transactionBuilder.build().begin();
            // Since transaction was requested at a particular snapshot
            // return that as the transaction's snapshot address.
            snapshotToken = new Token(isolationLevel.getTimestamp().getEpoch(),
                    isolationLevel.getTimestamp().getSequence());
        } else {
            transactionBuilder.snapshot(isolationLevel.getTimestamp());
            transactionBuilder.build().begin();
            snapshotToken = TransactionalContext.getCurrentContext().getSnapshotTimestamp();
        }
        TransactionalContext.getRootContext().setTxnContext(this);
        return snapshotToken;
    }

    public <K extends Message, V extends Message, M extends Message>
    Table<K, V, M> getTable(@Nonnull final String tableName) {
        return this.tableRegistry.getTable(this.namespace, tableName);
    }

    /**
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
    void validateWrite(@Nonnull Table<K, V, M> table, K key, M metadata, boolean validateKey) {
        baseValidateWrite(table, key, validateKey);
        if (table.getMetadataClass() == null && metadata != null) {
            throw new IllegalArgumentException("Metadata schema for table " + table.getFullyQualifiedTableName() + " is defined as NULL, non-null metadata is not allowed.");
        }
    }

    private <K extends Message, V extends Message, M extends Message>
    void baseValidateWrite(@Nonnull Table<K, V, M> table, K key, boolean validateKey) {
        if (!table.getNamespace().equals(namespace)) {
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
    public <K extends Message, V extends Message, M extends Message>
    void putRecord(@Nonnull Table<K, V, M> table,
                   @Nonnull final K key,
                   @Nonnull final V value,
                   @Nullable final M metadata) {
        validateWrite(table, key, metadata);
        table.put(key, value, metadata);
        tablesUpdated.putIfAbsent(table.getStreamUUID(), table);
    }

    public long getEpoch() {
        return txnSnapshot.getEpoch();
    }

    public long getTxnSequence() {
        return txnSnapshot.getSequence();
    }

    /**
     * A user callback that will take previous value of the record along with its new value
     * and return the merged record which is to be inserted into the table.
     */
    public interface MergeCallback {
        /**
         * @param table     table the merge is being done one that will be returned.
         * @param key       key of the record on which merge is being done.
         * @param oldRecord previous record extracted from the table for the same key.
         * @param newRecord new record that user is currently inserting into table.
         * @param <K>       type of the key
         * @param <V>       type of value or payload
         * @param <M>       type of metadata
         * @return
         */
        <K extends Message, V extends Message, M extends Message>
        CorfuRecord<V, M> doMerge(Table<K, V, M> table,
                                  K key,
                                  CorfuRecord<V, M> oldRecord,
                                  CorfuRecord<V, M> newRecord);
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
            txAbort(); // explicitly abort this transaction and then throw the abort manually
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
    public <K extends Message, V extends Message, M extends Message>
    void touch(@Nonnull Table<K, V, M> table,
               @Nonnull final K key) {
        validateWrite(table, key);
        CorfuRecord<V, M> touchedObject = table.get(key);
        if (touchedObject != null) {
            table.put(key, touchedObject.getPayload(), touchedObject.getMetadata());
            tablesUpdated.putIfAbsent(table.getStreamUUID(), table);
        } else { // TODO: add support for touch()ing an object that hasn't been created.
            txAbort(); // explicitly abort this transaction and then throw the abort manually
            log.error("TX Abort touch on non-existing object: in " + table.getFullyQualifiedTableName());
            throw new UnsupportedOperationException(
                    "Attempt to touch() a non-existing object in "
                            + table.getFullyQualifiedTableName());
        }
    }

    /**
     * touch() a key to generate a conflict on it given tableName.
     *
     * @param tableName Table object to perform the touch() in.
     * @param key       Key of the record.
     * @param <K>       Type of Key.
     * @throws UnsupportedOperationException if attempted on a non-existing object.
     */
    public <K extends Message, V extends Message, M extends Message>
    void touch(@Nonnull String tableName,
               @Nonnull final K key) {
        this.touch(getTable(tableName), key);
    }

    /**
     * Apply a Corfu SMREntry directly to a stream. This can be used for replaying the mutations
     * directly into the underlying stream bypassing the object layer entirely.
     *
     * @param streamId    - UUID of the stream on which the logUpdate is being added to.
     * @param updateEntry - the actual State Machine Replicated entry.
     */
    public void logUpdate(UUID streamId, SMREntry updateEntry) {
        TransactionalContext.getCurrentContext().logUpdate(streamId, updateEntry);
    }

    /**
     * Apply a Corfu SMREntry directly to a stream. This can be used for replaying the mutations
     * directly into the underlying stream bypassing the object layer entirely.
     * <p>
     * This API is used for LR feature, as streaming is selectively required on sink (receiver)
     * by means of a static configuration file.
     *
     * @param streamId    - UUID of the stream on which the logUpdate is being added to.
     * @param updateEntry - the actual State Machine Replicated entry.
     * @param streamTags  - stream tags associated to the given stream id
     */
    public void logUpdate(UUID streamId, SMREntry updateEntry, List<UUID> streamTags) {
        if (streamTags != null) {
            TransactionalContext.getCurrentContext().logUpdate(streamId, updateEntry, streamTags);
        } else {
            TransactionalContext.getCurrentContext().logUpdate(streamId, updateEntry);
        }
    }

    /**
     * Apply a list of Corfu SMREntries directly to a stream. This can be used for replaying the mutations
     * directly into the underlying stream bypassing the object layer entirely.
     *
     * @param streamId      - UUID of the stream on which the logUpdate is being added to.
     * @param updateEntries - the actual State Machine Replicated entries.
     */
    public void logUpdate(UUID streamId, List<SMREntry> updateEntries) {
        TransactionalContext.getCurrentContext().logUpdate(streamId, updateEntries);
    }

    /**
     * Clears the entire table.
     *
     * @param table Table object to perform the clear on.
     * @param <K>   Type of Key.
     * @param <V>   Type of Value.
     * @param <M>   Type of Metadata.
     */
    public <K extends Message, V extends Message, M extends Message>
    void clear(@Nonnull Table<K, V, M> table) {
        validateWrite(table);
        table.clearAll();
        tablesUpdated.putIfAbsent(table.getStreamUUID(), table);
    }

    /**
     * Clears the entire table given the table name.
     *
     * @param tableName Full table name of table to be cleared.
     * @param <K>       Type of Key.
     * @param <V>       Type of Value.
     * @param <M>       Type of Metadata.
     */
    public <K extends Message, V extends Message, M extends Message>
    void clear(@Nonnull String tableName) {
        this.clear(getTable(tableName));
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
    TxnContext delete(@Nonnull Table<K, V, M> table,
                      @Nonnull final K key) {
        validateWrite(table);
        table.deleteRecord(key);
        tablesUpdated.putIfAbsent(table.getStreamUUID(), table);
        return this;
    }

    /**
     * Deletes the specified key on a table given its full name.
     *
     * @param tableName Table object to perform the delete on.
     * @param key       Key of the record to be deleted.
     * @param <K>       Type of Key.
     * @param <V>       Type of Value.
     * @param <M>       Type of Metadata.
     */
    public <K extends Message, V extends Message, M extends Message>
    void delete(@Nonnull String tableName,
                @Nonnull final K key) {
        this.delete(getTable(tableName), key);
    }

    // ************************** Queue API ***************************************/

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
    @Nonnull
    public <K extends Message, V extends Message, M extends Message>
    K enqueue(@Nonnull Table<K, V, M> table,
              @Nonnull final V record) {
        validateWrite(table);
        K ret = table.enqueue(record);
        tablesUpdated.putIfAbsent(table.getStreamUUID(), table);
        return ret;
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
     * get the full record from the table given a key.
     * If this is invoked on a Read-Your-Writes transaction, it will result in starting a corfu transaction
     * and applying all the updates done so far.
     *
     * @param tableName Table object to retrieve the record from
     * @param key       Key of the record.
     * @return CorfuStoreEntry<Key, Value, Metadata> instance.
     */
    @Nonnull
    public <K extends Message, V extends Message, M extends Message>
    CorfuStoreEntry getRecord(@Nonnull final String tableName,
                              @Nonnull final K key) {
        return this.getRecord(getTable(tableName), key);
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
    @Nonnull
    public <K extends Message, V extends Message, M extends Message, I>
    List<CorfuStoreEntry<K, V, M>> getByIndex(@Nonnull Table<K, V, M> table,
                                              @Nonnull final String indexName,
                                              @Nonnull final I indexKey) {
        return table.getByIndex(indexName, indexKey);
    }

    /**
     * Query by a secondary index given just the full tableName.
     *
     * @param tableName fullyQualified name of the table.
     * @param indexName Index name. In case of protobuf-defined secondary index it is the field name.
     * @param indexKey  Key to query.
     * @param <K>       Type of Key.
     * @param <V>       Type of Value.
     * @param <I>       Type of index/secondary key.
     * @return Result of the query.
     */
    @Nonnull
    public <K extends Message, V extends Message, M extends Message, I>
    List<CorfuStoreEntry<K, V, M>> getByIndex(@Nonnull String tableName,
                                              @Nonnull final String indexName,
                                              @Nonnull final I indexKey) {
        return this.getByIndex(this.getTable(tableName), indexName, indexKey);
    }

    /**
     * Gets the count of records in the table at a particular timestamp.
     *
     * @param table - the table whose count is requested.
     * @return Count of records.
     */
    public <K extends Message, V extends Message, M extends Message>
    int count(@Nonnull final Table<K, V, M> table) {
        return table.count();
    }

    /**
     * Gets the count of records in the table at a particular timestamp.
     *
     * @param tableName - the namespace+table name of the table.
     * @return Count of records.
     */
    public int count(@Nonnull final String tableName) {
        return this.count(this.getTable(tableName));
    }

    /**
     * Gets all the keys of a table.
     *
     * @param table - the table whose keys are requested.
     * @return keyset of the table
     */
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
    public <K extends Message, V extends Message, M extends Message>
    Stream<CorfuStoreEntry<K, V, M>> entryStream(@Nonnull final Table<K, V, M> table) {
        return table.entryStream();
    }

    /**
     * Get all the keys of a table just given its tableName.
     *
     * @param tableName fullyQualifiedTableName whose keys are requested.
     * @return keyset of the table
     */
    public <K extends Message, V extends Message, M extends Message>
    Set<K> keySet(@Nonnull final String tableName) {
        return this.keySet(this.getTable(tableName));
    }

    /**
     * Scan and filter by entry.
     *
     * @param table          Table< K, V, M > object on which the scan must be done.
     * @param entryPredicate Predicate to filter the entries.
     * @return Collection of filtered entries.
     */
    public <K extends Message, V extends Message, M extends Message>
    List<CorfuStoreEntry<K, V, M>> executeQuery(@Nonnull final Table<K, V, M> table,
                                                @Nonnull final Predicate<CorfuStoreEntry<K, V, M>> entryPredicate) {
        return table.scanAndFilterByEntry(entryPredicate);
    }

    /**
     * Scan and filter by entry.
     *
     * @param tableName      fullyQualified tablename to filter the entries on.
     * @param entryPredicate Predicate to filter the entries.
     * @return Collection of filtered entries.
     */
    public <K extends Message, V extends Message, M extends Message>
    List<CorfuStoreEntry<K, V, M>> executeQuery(@Nonnull final String tableName,
                                                @Nonnull final Predicate<CorfuStoreEntry<K, V, M>> entryPredicate) {
        return this.executeQuery(this.getTable(tableName), entryPredicate);
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
    public <K extends Message, V extends Message, M extends Message>
    boolean isExists(@Nonnull Table<K, V, M> table, @Nonnull final K key) {
        CorfuStoreEntry<K, V, M> record = getRecord(table, key);
        return record.getPayload() != null;
    }

    /**
     * Variant of isExists that works on tableName instead of the table object.
     *
     * @param tableName - namespace + tablename of table being tested
     * @param key       - key to check for existence
     * @param <K>       - type of the key
     * @param <V>       - type of payload or value
     * @param <M>       - type of metadata
     * @return - true if record exists and false if record does not exist.
     */
    public <K extends Message, V extends Message, M extends Message>
    boolean isExists(@Nonnull String tableName, @Nonnull final K key) {
        return this.isExists(getTable(tableName), key);
    }

    /**
     * Return all the Queue entries ordered by their parent transaction.
     * <p>
     * Note that the key in these entries would be the CorfuQueueIdMsg.
     *
     * @param table Table< K, V, M > object aka queue on which the scan must be done.
     * @return Collection of filtered entries.
     */
    public <K extends Message, V extends Message, M extends Message>
    List<Table.CorfuQueueRecord> entryList(@Nonnull final Table<K, V, M> table) {
        return table.entryList();
    }

    /**
     * @return The the thread local's TxnContext, null if not in a transaction.
     */
    public static TxnContext getMyTxnContext() {
        if (!TransactionalContext.isInTransaction()) {
            return null;
        }
        return TransactionalContext.getRootContext().getTxnContext();
    }

    /**
     * @return true if the transaction was started by this layer, false otherwise
     */
    public boolean isInMyTransaction() {
        TxnContext txnContext = getMyTxnContext();
        return txnContext == this;
    }

    /** -------------------------- internal private methods ------------------------------*/

    /**
     * Protobuf objects are immutable. So any metadata modifications made by any merge() callback
     * won't be reflected back into the caller's in-memory object directly.
     * The caller is only really interested in the modified values of those transactions
     * that successfully commit.
     * To reflect metadata changes made here, we modify commit() to accept a callback
     * that carries all the final values of the changes made by this transaction.
     */
    public interface CommitCallback {
        /**
         * This callback returns a list of stream entries as opposed to CorfuStoreEntries
         * because if this transaction had operations like clear() then the CorfuStoreEntry
         * would just be empty.
         *
         * @param mutations - A group of all tables touched by this transaction along with
         *                  the updates made in each table.
         */
        void onCommit(Map<String, List<CorfuStreamEntry>> mutations);
    }

    /**
     * Commit the transaction.
     * For a transaction that only has write operations, this method will start and end
     * a corfu transaction keeping the "critical" section small while batching up all updates.
     * For a transaction that has reads, this will end the transaction.
     * The commit returns successfully if the write transaction was committed.
     * Otherwise this throws a TransactionAbortedException with a cause.
     * The cause and the caller's intent of the transaction can determine if this aborted
     * Transaction can be retried.
     * If there are any post-commit callbacks registered, they will be invoked.
     *
     * @return - address at which the commit of this transaction occurred.
     */
    public Timestamp commit() {
        if (!isInMyTransaction()) {
            throw new IllegalStateException("commit() called without a transaction!");
        }

        // CorfuStore should have only one transactional context since nesting is prohibited.
        AbstractTransactionalContext rootContext = TransactionalContext.getRootContext();

        long commitAddress = Address.NON_ADDRESS;
        if (iDidNotStartCorfuTxn) {
            log.warn("commit() called on an inner transaction not started by CorfuStore");
        } else {
            commitAddress = this.objectsView.TXEnd();
        }

        MultiObjectSMREntry writeSet = rootContext.getWriteSetInfo().getWriteSet();
        final Map<String, List<CorfuStreamEntry>> mutations = new HashMap<>(tablesUpdated.size());
        tablesUpdated.forEach((uuid, table) -> {
            List<CorfuStreamEntry> writesInTable = writeSet.getSMRUpdates(uuid).stream()
                    .map(CorfuStreamEntry::fromSMREntry).collect(Collectors.toList());
            mutations.put(table.getFullyQualifiedTableName(), writesInTable);
        });

        commitCallbacks.forEach(cb -> cb.onCommit(mutations));

        return Timestamp.newBuilder()
                .setEpoch(getEpoch())
                .setSequence(commitAddress)
                .build();
    }


    /**
     * To allow nested transactions, we need to track all commit callbacks
     *
     * @param commitCallback
     */
    public void addCommitCallback(@Nonnull CommitCallback commitCallback) {
        log.trace("TxnContext:addCommitCallback in transaction on namespace {}", namespace);
        this.commitCallbacks.add(commitCallback);
    }

    /**
     * Explicitly abort a transaction in case of an external failure
     */
    public void txAbort() {
        if (TransactionalContext.isInTransaction()) {
            this.objectsView.TXAbort();
        }
    }

    /**
     * Cleanup the transaction resources.
     * If invoked on transaction with just queries, record the time taken.
     */
    @Override
    public void close() {
        if (TransactionalContext.isInTransaction()) {
            AbstractTransactionalContext rootContext = TransactionalContext.getRootContext();
            log.trace("closing {} transaction without calling commit()!", rootContext);
            if (iDidNotStartCorfuTxn) {
                log.warn("close() called on an inner transaction not started by CorfuStore");
            } else {
                this.objectsView.TXAbort();
            }
        }
    }

    /**
     * @param streamId - UUID of the stream
     * @return Return the table name given the stream UUID
     */
    public String getTableNameFromUuid(UUID streamId) {
        if (tablesUpdated.containsKey(streamId)) {
            return tablesUpdated.get(streamId).getFullyQualifiedTableName();
        }
        return streamId.toString();
    }
}
