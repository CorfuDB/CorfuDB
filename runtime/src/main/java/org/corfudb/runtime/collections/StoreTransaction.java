package org.corfudb.runtime.collections;

import com.google.protobuf.Message;
import org.corfudb.runtime.object.transactions.TransactionalContext;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

public interface StoreTransaction<T extends StoreTransaction<T>> {
    /**
     * @return ThreadLocal TxnContext, null if not in a transaction.
     */
    static TxnContext getMyTxnContext() {
        if (!TransactionalContext.isInTransaction()) {
            return null;
        }
        return TransactionalContext.getRootContext().getTxnContext();
    }

    <K extends Message, V extends Message, M extends Message>
    void putRecord(@Nonnull Table<K, V, M> table,
                   @Nonnull K key,
                   @Nonnull V value,
                   @Nullable M metadata);

    <K extends Message, V extends Message, M extends Message>
    void merge(@Nonnull Table<K, V, M> table,
               @Nonnull K key,
               @Nonnull MergeCallback mergeCallback,
               @Nonnull CorfuRecord<V, M> recordDelta);

    <K extends Message, V extends Message, M extends Message>
    void touch(@Nonnull Table<K, V, M> table,
               @Nonnull K key);

    <K extends Message, V extends Message, M extends Message>
    void clear(@Nonnull Table<K, V, M> table);

    @Nonnull
    <K extends Message, V extends Message, M extends Message>
    T delete(@Nonnull Table<K, V, M> table,
                            @Nonnull K key);

    @Nonnull
    <K extends Message, V extends Message, M extends Message>
    K enqueue(@Nonnull Table<K, V, M> table,
              @Nonnull V value);

    @Nonnull
    <K extends Message, V extends Message, M extends Message>
    CorfuStoreEntry<K, V, M> getRecord(@Nonnull Table<K, V, M> table,
                                       @Nonnull K key);

    @Nonnull
    <K extends Message, V extends Message, M extends Message, I>
    List<CorfuStoreEntry<K, V, M>> getByIndex(@Nonnull Table<K, V, M> table,
                                              @Nonnull String indexName,
                                              @Nonnull I indexKey);

    <K extends Message, V extends Message, M extends Message>
    int count(@Nonnull Table<K, V, M> table);

    <K extends Message, V extends Message, M extends Message>
    Set<K> keySet(@Nonnull Table<K, V, M> table);

    <K extends Message, V extends Message, M extends Message>
    Stream<CorfuStoreEntry<K, V, M>> entryStream(@Nonnull Table<K, V, M> table);

    <K extends Message, V extends Message, M extends Message>
    List<CorfuStoreEntry<K, V, M>> executeQuery(@Nonnull Table<K, V, M> table,
                                                @Nonnull Predicate<CorfuStoreEntry<K, V, M>> entryPredicate);

    @Nonnull
    <K1 extends Message, K2 extends Message,
            V1 extends Message, V2 extends Message,
            M1 extends Message, M2 extends Message, T, U>
    QueryResult<U> executeJoinQuery(
            @Nonnull Table<K1, V1, M1> table1,
            @Nonnull Table<K2, V2, M2> table2,
            @Nonnull Predicate<CorfuStoreEntry<K1, V1, M1>> query1,
            @Nonnull Predicate<CorfuStoreEntry<K2, V2, M2>> query2,
            @Nonnull BiPredicate<V1, V2> joinPredicate,
            @Nonnull BiFunction<V1, V2, T> joinFunction,
            Function<T, U> joinProjection);

    @Nonnull
    <K1 extends Message, K2 extends Message,
            V1 extends Message, V2 extends Message,
            M1 extends Message, M2 extends Message,
            R, S, T, U>
    QueryResult<U> executeJoinQuery(
            @Nonnull Table<K1, V1, M1> table1,
            @Nonnull Table<K2, V2, M2> table2,
            @Nonnull Predicate<CorfuStoreEntry<K1, V1, M1>> query1,
            @Nonnull Predicate<CorfuStoreEntry<K2, V2, M2>> query2,
            @Nonnull QueryOptions<K1, V1, M1, R> queryOptions1,
            @Nonnull QueryOptions<K2, V2, M2, S> queryOptions2,
            @Nonnull BiPredicate<R, S> joinPredicate,
            @Nonnull BiFunction<R, S, T> joinFunction,
            Function<T, U> joinProjection);

    <K extends Message, V extends Message, M extends Message>
    boolean isExists(@Nonnull Table<K, V, M> table, @Nonnull K key);

    <K extends Message, V extends Message, M extends Message>
    List<Table.CorfuQueueRecord> entryList(@Nonnull Table<K, V, M> table);

    /**
     * A user callback that will take previous value of the record along with its new value
     * and return the merged record which is to be inserted into the table.
     */
    interface MergeCallback {
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
     * Protobuf objects are immutable. So any metadata modifications made by any merge() callback
     * won't be reflected back into the caller's in-memory object directly.
     * The caller is only really interested in the modified values of those transactions
     * that successfully commit.
     * To reflect metadata changes made here, we modify commit() to accept a callback
     * that carries all the final values of the changes made by this transaction.
     */
    interface CommitCallback {
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
}
