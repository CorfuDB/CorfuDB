package org.corfudb.runtime.collections;

import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.exceptions.TransactionAlreadyStartedException;
import org.corfudb.runtime.object.transactions.Transaction;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.corfudb.runtime.view.ObjectsView;
import org.corfudb.runtime.view.TableRegistry;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Predicate;

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
    private final Set<Table> tablesInTxn;
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
        this.tablesInTxn = new HashSet<>();
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
        txnType |= WRITE_ONLY;
        tablesInTxn.add(table);
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
     * @return TxnContext instance.
     */
    @Nonnull
    public <K extends Message, V extends Message, M extends Message>
    TxnContext put(@Nonnull Table<K, V, M> table,
                   @Nonnull final K key,
                   @Nonnull final V value,
                   @Nullable final M metadata) {
        validateTableWrittenIsInNamespace(table);
        operations.add(() -> {
            table.put(key, value, metadata);
            table.getMetrics().incNumPuts();
        });
        return this;
    }

    /**
     * Merges the delta value with the old value by applying a caller specified BiFunction and writes
     * the final value.
     *
     * @param table Table object to perform the merge operation on.
     * @param key            Key
     * @param mergeOperator  Function to apply to get the new value
     * @param recordDelta    Argument to pass to the mutation function
     * @param <K>            Type of Key.
     * @param <V>            Type of Value.
     * @param <M>            Type of Metadata.
     * @return TxnContext instance
     */
    @Nonnull
    public <K extends Message, V extends Message, M extends Message>
    TxnContext merge(@Nonnull Table<K, V, M> table,
                     @Nonnull final K key,
                     @Nonnull BiFunction<CorfuRecord<V, M>, CorfuRecord<V,M>, CorfuRecord<V,M>> mergeOperator,
                     @Nonnull final CorfuRecord<V,M> recordDelta) {
        validateTableWrittenIsInNamespace(table);
        operations.add(() -> {
            CorfuRecord<V,M> oldRecord = table.get(key);
            CorfuRecord<V, M> mergedRecord;
            try {
                mergedRecord = mergeOperator.apply(oldRecord, recordDelta);
            } catch (Exception ex) {
                txAbort(); // explicitly abort this transaction and then throw the abort manually
                log.error("TX Abort merge: {}", table.getFullyQualifiedTableName(), ex);
                throw ex;
            }
            table.put(key, mergedRecord.getPayload(), mergedRecord.getMetadata());
            table.getMetrics().incNumMerges();
        });
        return this;
    }

    /**
     * touch() is a call to create a conflict on a read in a write-only transaction
     *
     * @param table Table object to perform the create/update on.
     * @param key   Key of the record to touch.
     * @param <K>   Type of Key.
     * @param <V>   Type of Value.
     * @param <M>   Type of Metadata.
     * @return TxnContext instance.
     */
    @Nonnull
    public <K extends Message, V extends Message, M extends Message>
    TxnContext touch(@Nonnull Table<K, V, M> table,
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
        return this;
    }

    /**
     * Apply a Corfu SMREntry directly to a stream. This can be used for replaying the mutations
     * directly into the underlying stream bypassing the object layer entirely.
     * @param streamId - UUID of the stream on which the logUpdate is being added to.
     * @param updateEntry - the actual State Machine Replicated entry.
     * @return
     */
    public TxnContext logUpdate(UUID streamId, SMREntry updateEntry) {
        operations.add(() -> {
            TransactionalContext.getCurrentContext().logUpdate(streamId, updateEntry);
        });
        return this;
    }

    /**
     * Clears the entire table.
     *
     * @param table Table object to perform the delete on.
     * @param <K>       Type of Key.
     * @param <V>       Type of Value.
     * @param <M>       Type of Metadata.
     * @return TxnContext instance.
     */
    @Nonnull
    public <K extends Message, V extends Message, M extends Message>
    TxnContext clear(@Nonnull Table<K, V, M> table) {
        validateTableWrittenIsInNamespace(table);
        operations.add(() -> {
            table.clearAll();
            table.getMetrics().incNumClears();
        });
        return this;
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
     *************************** READ API *****************************************
     */

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
     * Apply all pending writes (if any) to serve read queries.
     */
    private <K extends Message, V extends Message, M extends Message>
    void applyWritesForReadOnTable(Table<K, V, M> tableBeingRead) {
        tablesInTxn.add(tableBeingRead);
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
            tablesInTxn.forEach(t -> t.getMetrics().incNumTxnAborts());
            tablesInTxn.clear();
            throw ex;
        }
        long timeElapsed = System.nanoTime() - txnStartTime;
        switch (txnType) {
            case READ_ONLY:
                tablesInTxn.forEach(t -> t.getMetrics().setReadOnlyTxnTimes(timeElapsed));
                break;
            case WRITE_ONLY:
                tablesInTxn.forEach(t -> t.getMetrics().setWriteOnlyTxnTimes(timeElapsed));
                break;
            case READ_WRITE:
                tablesInTxn.forEach(t -> t.getMetrics().setReadWriteTxnTimes(timeElapsed));
                break;
            default:
                log.error("UNKNOWN TxnType!!");
                break;
        }
        tablesInTxn.clear();
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
            tablesInTxn.forEach(t -> t.getMetrics().incNumTxnAborts());
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
                tablesInTxn.forEach(t -> t.getMetrics().setReadOnlyTxnTimes(timeElapsed));
            } else {
                tablesInTxn.forEach(t -> t.getMetrics().incNumTxnAborts());
            }
            this.objectsView.TXAbort();
        }
        operations.clear();
        tablesInTxn.clear();
    }
}