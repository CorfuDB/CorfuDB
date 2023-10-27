package org.corfudb.runtime.collections;

import com.google.protobuf.Message;
import lombok.Getter;
import lombok.Setter;
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

/**
 * TxnContext is the access layer for binding all the CorfuStore CRUD operations.
 * It can help reduce the footprint of a CorfuStore transaction by only having writes in it.
 * All mutations/writes are aggregated and applied at once a the time of commit() call
 * where a real corfu transaction is started.
 * <p>
 * Created by hisundar, @wenbinzhu, @pankti-m on 2020-09-15
 */
@Slf4j
public class TxnContext
        implements StoreTransaction<TxnContext>,
        TableStringApi, AutoCloseable, CommitApi {

    private final TransactionCrud<TxnContext> crud;
    private final ObjectsView objectsView;
    private final TableRegistry tableRegistry;
    private final Token txnSnapshot;
    private final List<CommitCallback> commitCallbacks;
    private boolean iDidNotStartCorfuTxn;

    @Getter
    private final String namespace;

    /**
     * A transaction id to be embedded into Queue's id for parallel unique id generation.
     */
    @Getter
    @Setter
    private long txnIdForQueues = 0;

    /**
     * Creates a new TxnContext.
     *
     * @param objectsView             ObjectsView from the Corfu client.
     * @param tableRegistry           Table Registry.
     * @param namespace               Namespace boundary defined for the transaction.
     * @param isolationLevel          How should this transaction be applied/evaluated.
     * @param allowNestedTransactions Is it ok to re-use another transaction context.
     */
    public TxnContext(@Nonnull final ObjectsView objectsView,
                      @Nonnull final TableRegistry tableRegistry,
                      @Nonnull final String namespace,
                      @Nonnull final IsolationLevel isolationLevel,
                      boolean allowNestedTransactions) {
        this.crud = new TransactionCrud<>(namespace, this::txAbort);
        this.objectsView = objectsView;
        this.tableRegistry = tableRegistry;
        this.namespace = namespace;
        this.commitCallbacks = new ArrayList<>();
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

    /*
     *************************** WRITE APIs *****************************************
     */

    public long getEpoch() {
        return txnSnapshot.getEpoch();
    }

    public long getTxnSequence() {
        return txnSnapshot.getSequence();
    }

    /**
     * touch() a key to generate a conflict on it given tableName.
     *
     * @param tableName Table object to perform the touch() in.
     * @param key       Key of the record.
     * @param <K>       Type of Key.
     * @throws UnsupportedOperationException if attempted on a non-existing object.
     */
    @Override
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
     * Clears the entire table given the table name.
     *
     * @param tableName Full table name of table to be cleared.
     * @param <K>       Type of Key.
     * @param <V>       Type of Value.
     * @param <M>       Type of Metadata.
     */
    @Override
    public <K extends Message, V extends Message, M extends Message>
    void clear(@Nonnull String tableName) {
        this.clear(getTable(tableName));
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
    @Override
    public <K extends Message, V extends Message, M extends Message>
    void delete(@Nonnull String tableName,
                @Nonnull final K key) {
        this.delete(getTable(tableName), key);
    }

    // ************************** Queue API ***************************************/

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
                       @Nonnull final V value, List<UUID> streamTags, CorfuStore corfuStore) {
        return crud.logUpdateEnqueue(table, value, streamTags, corfuStore);
    }

    /**
     * This API is used to delete a record without materializing in-memory
     *
     * @param table  Table object to operate on the queue.
     * @param key - key of the record to be deleted
     * @param streamTags  - stream tags associated to the given stream id
     * @param corfuStore CorfuStore that gets the runtime for the serializer.
     * @param <K>    Type of Key.
     * @param <V>    Type of Value.
     * @param <M>    Type of Metadata.
     */
    public <K extends Message, V extends Message, M extends Message>
    void logUpdateDelete(@Nonnull Table<K, V, M> table,
                         @Nonnull final K key, List<UUID> streamTags, CorfuStore corfuStore) {

        crud.logUpdateDelete(table, key, streamTags, corfuStore);
    }

    // *************************** READ API *****************************************

    /**
     * get the full record from the table given a key.
     * If this is invoked on a Read-Your-Writes transaction, it will result in starting a corfu transaction
     * and applying all the updates done so far.
     *
     * @param tableName Table object to retrieve the record from
     * @param key       Key of the record.
     * @return CorfuStoreEntry<Key, Value, Metadata> instance.
     */
    @Override
    @Nonnull
    public <K extends Message, V extends Message, M extends Message>
    CorfuStoreEntry getRecord(@Nonnull final String tableName,
                              @Nonnull final K key) {
        return this.getRecord(getTable(tableName), key);
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
    @Override
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
     * @param tableName - the namespace+table name of the table.
     * @return Count of records.
     */
    @Override
    public int count(@Nonnull final String tableName) {
        return this.count(this.getTable(tableName));
    }

    /**
     * Get all the keys of a table just given its tableName.
     *
     * @param tableName fullyQualifiedTableName whose keys are requested.
     * @return keyset of the table
     */
    @Override
    public <K extends Message, V extends Message, M extends Message>
    Set<K> keySet(@Nonnull final String tableName) {
        return this.keySet(this.getTable(tableName));
    }

    @Override
    public <K extends Message, V extends Message, M extends Message>
    boolean isExists(@Nonnull String tableName, @Nonnull final K key) {
        return this.isExists(getTable(tableName), key);
    }



    /**
     * Scan and filter by entry.
     *
     * @param tableName      fullyQualified tablename to filter the entries on.
     * @param entryPredicate Predicate to filter the entries.
     * @return Collection of filtered entries.
     */
    @Override
    public <K extends Message, V extends Message, M extends Message>
    List<CorfuStoreEntry<K, V, M>> executeQuery(@Nonnull final String tableName,
                                                @Nonnull final Predicate<CorfuStoreEntry<K, V, M>> entryPredicate) {
        return this.executeQuery(this.getTable(tableName), entryPredicate);
    }

    /**
     * @return true if the transaction was started by this layer, false otherwise
     */
    public boolean isInMyTransaction() {
        TxnContext txnContext = StoreTransaction.getMyTxnContext();
        return txnContext == this;
    }

    /** -------------------------- internal private methods ------------------------------*/

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
    @Override
    public Timestamp commit() {
        if (!isInMyTransaction()) {
            throw new IllegalStateException("commit() called without a transaction!");
        }

        // CorfuStore should have only one transactional context since nesting is prohibited.
        AbstractTransactionalContext rootContext = TransactionalContext.getRootContext();
        // Regardless of transaction outcome remove any TxnContext association from ThreadLocal.
        rootContext.setTxnContext(null);

        long commitAddress = Address.NON_ADDRESS;
        if (iDidNotStartCorfuTxn) {
            log.warn("commit() called on an inner transaction not started by CorfuStore");
        } else {
            commitAddress = this.objectsView.TXEnd();
        }

        // These can be moved to trace once stability improves.
        log.trace("Txn committed on namespace {}", namespace);

        MultiObjectSMREntry writeSet = rootContext.getWriteSetInfo().getWriteSet();
        final Map<String, List<CorfuStreamEntry>> mutations = new HashMap<>(crud.tablesUpdated.size());
        crud.tablesUpdated.forEach((uuid, table) -> {
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



    public void addCommitCallback(@Nonnull CommitCallback commitCallback) {
        log.trace("TxnContext:addCommitCallback in transaction on namespace {}", namespace);
        this.commitCallbacks.add(commitCallback);
    }

    /**
     * Explicitly abort a transaction in case of an external failure
     */
    @Override
    public void txAbort() {
        if (TransactionalContext.isInTransaction()) {
            // Regardless of transaction outcome remove any TxnContext association from ThreadLocal.
            TransactionalContext.getRootContext().setTxnContext(null);
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
            rootContext.setTxnContext(null);
            log.trace("closing {} transaction without calling commit()!", rootContext);

            if (iDidNotStartCorfuTxn) {
                log.warn("close() called on an inner transaction not started by CorfuStore");
            } else {
                this.objectsView.TXAbort();
            }
        }
    }

    @Override
    public <K extends Message, V extends Message, M extends Message>
    void putRecord(@Nonnull Table<K, V, M> table,
                   @Nonnull K key,
                   @Nonnull V value,
                   @Nullable M metadata) {
        crud.putRecord(table, key, value, metadata);
    }

    @Override
    public <K extends Message, V extends Message, M extends Message>
    void merge(@Nonnull Table<K, V, M> table,
               @Nonnull K key,
               @Nonnull MergeCallback mergeCallback,
               @Nonnull CorfuRecord<V, M> recordDelta) {
        crud.merge(table, key, mergeCallback, recordDelta);
    }

    @Override
    public <K extends Message, V extends Message, M extends Message>
    void touch(@Nonnull Table<K, V, M> table, @Nonnull K key) {
        crud.touch(table, key);
    }

    @Override
    public <K extends Message, V extends Message, M extends Message>
    void clear(@Nonnull Table<K, V, M> table) {
        crud.clear(table);
    }

    @Nonnull
    @Override
    public <K extends Message, V extends Message, M extends Message>
    TxnContext delete(@Nonnull Table<K, V, M> table, @Nonnull K key) {
        crud.delete(table, key);
        return this;
    }

    @Nonnull
    @Override
    public <K extends Message, V extends Message, M extends Message>
    K enqueue(@Nonnull Table<K, V, M> table, @Nonnull V value) {
        return crud.enqueue(table, value);
    }

    @Nonnull
    @Override
    public <K extends Message, V extends Message, M extends Message> CorfuStoreEntry<K, V, M>
    getRecord(@Nonnull Table<K, V, M> table, @Nonnull K key) {
        return crud.getRecord(table, key);
    }

    @Nonnull
    @Override
    public <K extends Message, V extends Message, M extends Message, I> List<CorfuStoreEntry<K, V, M>>
    getByIndex(@Nonnull Table<K, V, M> table, @Nonnull String indexName, @Nonnull I indexKey) {
        return crud.getByIndex(table, indexName, indexKey);
    }

    @Override
    public <K extends Message, V extends Message, M extends Message>
    int count(@Nonnull Table<K, V, M> table) {
        return crud.count(table);
    }

    @Override
    public <K extends Message, V extends Message, M extends Message> Set<K>
    keySet(@Nonnull Table<K, V, M> table) {
        return crud.keySet(table);
    }

    @Override
    public <K extends Message, V extends Message, M extends Message> Stream<CorfuStoreEntry<K, V, M>>
    entryStream(@Nonnull Table<K, V, M> table) {
        return crud.entryStream(table);
    }

    @Override
    public <K extends Message, V extends Message, M extends Message> List<CorfuStoreEntry<K, V, M>>
    executeQuery(@Nonnull Table<K, V, M> table,
                 @Nonnull Predicate<CorfuStoreEntry<K, V, M>> corfuStoreEntryPredicate) {
        return crud.executeQuery(table, corfuStoreEntryPredicate);
    }

    @Nonnull
    @Override
    public <K1 extends Message, K2 extends Message,
            V1 extends Message, V2 extends Message,
            M1 extends Message, M2 extends Message, T, U> QueryResult<U>
    executeJoinQuery(@Nonnull Table<K1, V1, M1> table1,
                     @Nonnull Table<K2, V2, M2> table2,
                     @Nonnull Predicate<CorfuStoreEntry<K1, V1, M1>> query1,
                     @Nonnull Predicate<CorfuStoreEntry<K2, V2, M2>> query2,
                     @Nonnull BiPredicate<V1, V2> joinPredicate,
                     @Nonnull BiFunction<V1, V2, T> joinFunction,
                     Function<T, U> joinProjection) {
        return crud.executeJoinQuery(table1, table2, query1, query2, joinPredicate, joinFunction, joinProjection);
    }

    @Nonnull
    @Override
    public <K1 extends Message, K2 extends Message,
            V1 extends Message, V2 extends Message,
            M1 extends Message, M2 extends Message, R, S, T, U>
    QueryResult<U> executeJoinQuery(@Nonnull Table<K1, V1, M1> table1,
                                    @Nonnull Table<K2, V2, M2> table2,
                                    @Nonnull Predicate<CorfuStoreEntry<K1, V1, M1>> query1,
                                    @Nonnull Predicate<CorfuStoreEntry<K2, V2, M2>> query2,
                                    @Nonnull QueryOptions<K1, V1, M1, R> queryOptions1,
                                    @Nonnull QueryOptions<K2, V2, M2, S> queryOptions2,
                                    @Nonnull BiPredicate<R, S> joinPredicate,
                                    @Nonnull BiFunction<R, S, T> joinFunction,
                                    Function<T, U> joinProjection) {
        return crud.executeJoinQuery(
                table1, table2, query1, query2, queryOptions1, queryOptions2,
                joinPredicate, joinFunction, joinProjection);
    }

    @Override
    public <K extends Message, V extends Message, M extends Message>
    boolean isExists(@Nonnull Table<K, V, M> table, @Nonnull K key) {
        return crud.isExists(table, key);
    }

    @Override
    public <K extends Message, V extends Message, M extends Message> List<Table.CorfuQueueRecord>
    entryList(@Nonnull Table<K, V, M> table) {
        return crud.entryList(table);
    }
}
