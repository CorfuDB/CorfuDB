package org.corfudb.runtime.collections;

import com.google.protobuf.Message;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.Queue.CorfuQueueIdMsg;
import org.corfudb.runtime.exceptions.TransactionAlreadyStartedException;
import org.corfudb.runtime.object.transactions.Transaction;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.corfudb.runtime.object.transactions.TransactionalContext;
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

import static org.corfudb.runtime.collections.QueryOptions.DEFAULT_OPTIONS;

/**
 * TxnContext is the access layer for binding all the CorfuStore CRUD operations.
 * It can help reduce the footprint of a CorfuStore transaction by only having writes in it.
 * All mutations/writes are aggregated and applied at once a the time of commit() call
 * where a real corfu transaction is started.
 *
 * Created by hisundar, @wenbinzhu, @pankti-m on 2020-09-15
 */
@Slf4j
public class TxnContext implements AutoCloseable {

    private final ObjectsView objectsView;
    private final TableRegistry tableRegistry;
    private final String namespace;
    private final IsolationLevel isolationLevel;
    private final List<Runnable> operations;
    @Getter
    private final Map<UUID, Table> tablesInTxn;
    private long txnStartTime = 0L;
    private static final byte READ_ONLY  = 0x01;
    private static final byte WRITE_ONLY = 0x02;
    private static final byte READ_WRITE = 0x03;
    private byte txnType;

    /**
     * Creates a new TxnContext.
     *
     * @param objectsView   ObjectsView from the Corfu client.
     * @param tableRegistry Table Registry.
     * @param namespace     Namespace boundary defined for the transaction.
     * @param isolationLevel How should this transaction be applied/evaluated.
     */
    @Nonnull
    TxnContext(@Nonnull final ObjectsView objectsView,
               @Nonnull final TableRegistry tableRegistry,
               @Nonnull final String namespace,
               @Nonnull final IsolationLevel isolationLevel) {
        this.objectsView = objectsView;
        this.tableRegistry = tableRegistry;
        this.namespace = namespace;
        this.isolationLevel = isolationLevel;
        this.operations = new ArrayList<>();
        this.tablesInTxn = new HashMap<>();
        txBeginInternal(); // May throw exception if transaction was already started
    }

    public  <K extends Message, V extends Message, M extends Message>
    Table<K, V, M> getTable(@Nonnull final String tableName) {
        return this.tableRegistry.getTable(this.namespace, tableName);
    }

    /**
     *************************** WRITE APIs *****************************************
     */

    /**
     * All write api must be validate to ensure that the table belongs to the namespace.
     *
     * @param table - table being written to
     * @param <K> -type of the key
     * @param <V> - type of the payload/value
     * @param <M> - type of the metadata
     */
    private <K extends Message, V extends Message, M extends Message>
    void validateTableWrittenIsInNamespace(@Nonnull Table<K, V, M> table) {
        if (!table.getNamespace().equals(namespace)) {
            throw new IllegalArgumentException("TxnContext can't apply table from namespace "
                    + table.getNamespace() + " to transaction on namespace " + namespace);
        }
        if (!TransactionalContext.isInTransaction()) {
            throw new IllegalStateException( // Do not allow transactions after commit() or abort()
                    "TxnContext cannot be used after a transaction has ended on "+
                    table.getFullyQualifiedTableName());
        }
        txnType |= WRITE_ONLY;
        tablesInTxn.putIfAbsent(table.getStreamUUID(), table);
    }

    /**
     * put the value on the specified key create record if it does not exist.
     *
     * @param table Table object to perform the create/update on.
     * @param key   Key of the record.
     * @param value Value or payload of the record.
     * @param metadata Metadata associated with the record.
     * @param <K>   Type of Key.
     * @param <V>   Type of Value.
     * @param <M>   Type of Metadata.
     */
    public <K extends Message, V extends Message, M extends Message>
    void putRecord(@Nonnull Table<K, V, M> table,
                   @Nonnull final K key,
                   @Nonnull final V value,
                   @Nullable final M metadata) {
        validateTableWrittenIsInNamespace(table);
        operations.add(() -> {
            table.put(key, value, metadata);
            table.getMetrics().incNumPuts();
        });
    }

    /**
     * A user callback that will take previous value of the record along with its new value
     * and return the merged record which is to be inserted into the table.
     */
    public interface MergeCallback {
        /**
         *
         * @param table     table the merge is being done one that will be returned.
         * @param oldRecord previous record extracted from the table for the same key.
         * @param newRecord new record that user is currently inserting into table.
         * @param <K>       type of the key
         * @param <V>       type of value or payload
         * @param <M>       type of metadata
         * @return
         */
        <K extends Message, V extends Message, M extends Message>
        CorfuRecord<V, M> doMerge(Table<K, V, M> table,
                                  CorfuRecord<V, M> oldRecord,
                                  CorfuRecord<V, M> newRecord);
    }

    /**
     * Merges the delta value with the old value by applying a caller specified BiFunction and writes
     * the final value.
     *
     * @param table Table object to perform the merge operation on.
     * @param key            Key
     * @param mergeCallback  Function to apply to get the new value
     * @param recordDelta    Argument to pass to the mutation function
     * @param <K>            Type of Key.
     * @param <V>            Type of Value.
     * @param <M>            Type of Metadata.
     */
    public <K extends Message, V extends Message, M extends Message>
    void merge(@Nonnull Table<K, V, M> table,
                     @Nonnull final K key,
                     @Nonnull MergeCallback mergeCallback,
                     @Nonnull final CorfuRecord<V,M> recordDelta) {
        validateTableWrittenIsInNamespace(table);
        operations.add(() -> {
            CorfuRecord<V,M> oldRecord = table.get(key);
            CorfuRecord<V, M> mergedRecord;
            try {
                mergedRecord = mergeCallback.doMerge(table, oldRecord, recordDelta);
            } catch (Exception ex) {
                txAbort(); // explicitly abort this transaction and then throw the abort manually
                log.error("TX Abort merge: {}", table.getFullyQualifiedTableName(), ex);
                throw ex;
            }
            table.put(key, mergedRecord.getPayload(), mergedRecord.getMetadata());
            table.getMetrics().incNumMerges();
        });
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
        validateTableWrittenIsInNamespace(table);
        operations.add(() -> {
            CorfuRecord<V, M> touchedObject = table.get(key);
            if (touchedObject != null) {
                table.put(key, touchedObject.getPayload(), touchedObject.getMetadata());
            } else { // TODO: add support for touch()ing an object that hasn't been created.
                txAbort(); // explicitly abort this transaction and then throw the abort manually
                log.error("TX Abort touch on non-existing object: in "+ table.getFullyQualifiedTableName());
                throw new UnsupportedOperationException(
                        "Attempt to touch() a non-existing object in "
                                + table.getFullyQualifiedTableName());
            }
            table.getMetrics().incNumTouches();
        });
    }

    /**
     * touch() a key to generate a conflict on it given tableName.
     *
     * @param tableName    Table object to perform the touch() in.
     * @param key          Key of the record.
     * @param <K>          Type of Key.
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
     * @param streamId - UUID of the stream on which the logUpdate is being added to.
     * @param updateEntry - the actual State Machine Replicated entry.
     */
    public void logUpdate(UUID streamId, SMREntry updateEntry) {
        operations.add(() -> {
            TransactionalContext.getCurrentContext().logUpdate(streamId, updateEntry);
        });
    }

    /**
     * Clears the entire table.
     *
     * @param table Table object to perform the delete on.
     * @param <K>       Type of Key.
     * @param <V>       Type of Value.
     * @param <M>       Type of Metadata.
     */
    public <K extends Message, V extends Message, M extends Message>
    void clear(@Nonnull Table<K, V, M> table) {
        validateTableWrittenIsInNamespace(table);
        operations.add(() -> {
            table.clearAll();
            table.getMetrics().incNumClears();
        });
    }

    /**
     * Clears the entire table given the table name.
     *
     * @param tableName Full table name of table to be cleared.
     * @param <K>   Type of Key.
     * @param <V>   Type of Value.
     * @param <M>   Type of Metadata.
     */
    public <K extends Message, V extends Message, M extends Message>
    void clear(@Nonnull String tableName) {
        this.clear(getTable(tableName));
    }

    /**
     * Deletes the specified key.
     *
     * @param table Table object to perform the delete on.
     * @param key       Key of the record to be deleted.
     * @param <K>       Type of Key.
     * @param <V>       Type of Value.
     * @param <M>       Type of Metadata.
     * @return TxnContext instance.
     */
    @Nonnull
    public <K extends Message, V extends Message, M extends Message>
    TxnContext delete(@Nonnull Table<K, V, M> table,
                      @Nonnull final K key) {
        validateTableWrittenIsInNamespace(table);
        table.getMetrics().incNumDeletes();
        operations.add(() -> table.deleteRecord(key));
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
     * @param table Table object to perform the delete on.
     * @param record    Record to be inserted into the Queue.
     * @param <K>       Type of Key.
     * @param <V>       Type of Value.
     * @param <M>       Type of Metadata.
     * @return K the type of key this queue table was created with.
     */
    @Nonnull
    public <K extends Message, V extends Message, M extends Message>
    K enqueue(@Nonnull Table<K, V, M> table,
              @Nonnull final V record) {
        validateTableWrittenIsInNamespace(table);
        applyWritesForReadOnTable(table);
        /********* TEMPORARY FIX UNTIL REAL IMPLEMENTATION********/
        UUID todoReplaceMe = UUID.randomUUID();
        CorfuQueueIdMsg key = CorfuQueueIdMsg.newBuilder()
                .setEntryId(todoReplaceMe.timestamp())
                .setTxSequence(todoReplaceMe.clockSequence()).build();
        this.putRecord(table, (K)key, record, null);
        return (K) key;
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
        applyWritesForReadOnTable(table);
        CorfuRecord<V, M> record = table.get(key);
        table.getMetrics().incNumGets();
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
     * @param key   Key of the record.
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
     * @param table Table object.
     * @param indexName Index name. In case of protobuf-defined secondary index it is the field name.
     * @param indexKey  Key to query.
     * @param <K>       Type of Key.
     * @param <V>       Type of Value.
     * @param <I>       Type of index/secondary key.
     * @return Result of the query.
     */
    @Nonnull
    public <K extends Message, V extends Message, M extends Message, I extends Comparable<I>>
    List<CorfuStoreEntry<K, V, M>> getByIndex(@Nonnull Table<K, V, M> table,
                                              @Nonnull final String indexName,
                                              @Nonnull final I indexKey) {
        applyWritesForReadOnTable(table);
        table.getMetrics().incNumGetByIndexes();
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
    public <K extends Message, V extends Message, M extends Message, I extends Comparable<I>>
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
        applyWritesForReadOnTable(table);
        table.getMetrics().incNumCounts();
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
        applyWritesForReadOnTable(table);
        table.getMetrics().incNumKeySets();
        return table.keySet();
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
     * @param table Table< K, V, M > object on which the scan must be done.
     * @param entryPredicate Predicate to filter the entries.
     * @return Collection of filtered entries.
     */
    public <K extends Message, V extends Message, M extends Message>
    List<CorfuStoreEntry<K, V, M>> executeQuery(@Nonnull final Table<K, V, M> table,
                                                @Nonnull final Predicate<CorfuStoreEntry<K, V, M>> entryPredicate) {
        applyWritesForReadOnTable(table);
        table.getMetrics().incNumScans();
        return table.scanAndFilterByEntry(entryPredicate);
    }

    /**
     * Scan and filter by entry.
     *
     * @param tableName fullyQualified tablename to filter the entries on.
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
        applyWritesForReadOnTable(table1);
        table1.getMetrics().incNumJoins();
        applyWritesForReadOnTable(table1);
        table2.getMetrics().incNumJoins();
        return Query.executeJoinQuery(table1, table2,
                query1, query2, queryOptions1,
                queryOptions2, joinPredicate, joinFunction, joinProjection);
    }

    /**
     * Test if a record exists in a table.
     *
     * @param table - table object to test if record exists
     * @param key - key or identifier to test for existence.
     * @param <K> - type of the key
     * @param <V> - type of payload or value
     * @param <M> - type of metadata
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
     * @param key - key to check for existence
     * @param <K> - type of the key
     * @param <V> - type of payload or value
     * @param <M> - type of metadata
     * @return - true if record exists and false if record does not exist.
     */
    public <K extends Message, V extends Message, M extends Message>
    boolean isExists(@Nonnull String tableName, @Nonnull final K key) {
        return this.isExists(getTable(tableName), key);
    }

    /**
     * Return all the Queue entries ordered by their parent transaction.
     *
     * Note that the key in these entries would be the CorfuQueueIdMsg.
     *
     * @param table Table< K, V, M > object aka queue on which the scan must be done.
     * @return Collection of filtered entries.
     */
    public <K extends Message, V extends Message, M extends Message>
    List<CorfuStoreEntry<K, V, M>> entryList(@Nonnull final Table<K, V, M> table) {
        applyWritesForReadOnTable(table);
        /***** TODO FIX ME WITH REAL IMPLEMENTATION *******/
        return table.scanAndFilterByEntry(record -> true);
    }

    /** -------------------------- internal private methods ------------------------------*/

    /**
     * Apply all pending writes (if any) to serve read queries.
     */
    private <K extends Message, V extends Message, M extends Message>
    void applyWritesForReadOnTable(Table<K, V, M> tableBeingRead) {
        tablesInTxn.putIfAbsent(tableBeingRead.getStreamUUID(), tableBeingRead);
        txnType |= READ_ONLY;
        if (!operations.isEmpty()) {
            operations.forEach(Runnable::run);
            operations.clear();
        }
    }

    /**
     * Start the actual corfu transaction. Ensure there isn't one already in the same thread.
     */
    private void txBeginInternal() {
        if (TransactionalContext.isInTransaction()) {
            log.error("Cannot start new transaction in this thread without ending previous one");
            throw new TransactionAlreadyStartedException(TransactionalContext.getRootContext().toString());
        }
        this.txnStartTime = System.nanoTime();
        Transaction.TransactionBuilder transactionBuilder = this.objectsView
                .TXBuild()
                .type(TransactionType.WRITE_AFTER_WRITE);
        if (isolationLevel.getTimestamp() != Token.UNINITIALIZED) {
            transactionBuilder.snapshot(isolationLevel.getTimestamp());
        }
        transactionBuilder.build().begin();
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
     * @return - address at which the commit of this transaction occurred.
     */
    public long commit() {
        if (!TransactionalContext.isInTransaction()) {
            throw new IllegalStateException("commit() called without a transaction!");
        }
        operations.forEach(Runnable::run);
        long commitAddress;
        try {
            commitAddress = this.objectsView.TXEnd();
        } catch (Exception ex) {
            tablesInTxn.values().forEach(t -> t.getMetrics().incNumTxnAborts());
            tablesInTxn.clear();
            throw ex;
        }
        long timeElapsed = System.nanoTime() - txnStartTime;
        switch (txnType) {
            case READ_ONLY:
                tablesInTxn.values().forEach(t -> t.getMetrics().setReadOnlyTxnTimes(timeElapsed));
                break;
            case WRITE_ONLY:
                tablesInTxn.values().forEach(t -> t.getMetrics().setWriteOnlyTxnTimes(timeElapsed));
                break;
            case READ_WRITE:
                tablesInTxn.values().forEach(t -> t.getMetrics().setReadWriteTxnTimes(timeElapsed));
                break;
            default:
                log.error("UNKNOWN TxnType!!");
                break;
        }
        operations.clear();
        return commitAddress;
    }

    /**
     * Explicitly abort a transaction in case of an external failure
     */
    public void txAbort() {
        operations.clear();
        if (TransactionalContext.isInTransaction()) {
            this.objectsView.TXAbort();
            tablesInTxn.values().forEach(t -> t.getMetrics().incNumTxnAborts());
        }
        tablesInTxn.clear();
    }

    /**
     * Cleanup the transaction resources.
     * If invoked on transaction with just queries, record the time taken.
     */
    @Override
    public void close() {
        if (TransactionalContext.isInTransaction()) {
            log.warn("close()ing a {} transaction without calling commit()!", txnType);
            long timeElapsed = System.nanoTime() - txnStartTime;
            if (txnType == READ_ONLY) {
                tablesInTxn.values().forEach(t -> t.getMetrics().setReadOnlyTxnTimes(timeElapsed));
            } else {
                tablesInTxn.values().forEach(t -> t.getMetrics().incNumTxnAborts());
            }
            this.objectsView.TXAbort();
        }
        operations.clear();
        tablesInTxn.clear();
    }
}
