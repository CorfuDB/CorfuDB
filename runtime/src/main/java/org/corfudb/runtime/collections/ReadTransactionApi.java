package org.corfudb.runtime.collections;

import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

public interface ReadTransactionApi {

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
    <K extends Message, V extends Message, M extends Message>
    CorfuStoreEntry<K, V, M> getRecord(@Nonnull Table<K, V, M> table,
                                       @Nonnull K key);

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
    <K extends Message, V extends Message, M extends Message, I>
    List<CorfuStoreEntry<K, V, M>> getByIndex(@Nonnull Table<K, V, M> table,
                                              @Nonnull String indexName,
                                              @Nonnull I indexKey);

    /**
     * Gets the count of records in the table at a particular timestamp.
     *
     * @param table - the table whose count is requested.
     * @return Count of records.
     */
    <K extends Message, V extends Message, M extends Message>
    int count(@Nonnull Table<K, V, M> table);

    /**
     * Gets all the keys of a table.
     *
     * @param table - the table whose keys are requested.
     * @return keyset of the table
     */
    <K extends Message, V extends Message, M extends Message>
    Set<K> keySet(@Nonnull Table<K, V, M> table);

    /**
     * Gets all entries in the table in form of a stream.
     *
     * @param table - the table whose entrires are requested.
     * @return stream of entries in the table
     */
    <K extends Message, V extends Message, M extends Message>
    Stream<CorfuStoreEntry<K, V, M>> entryStream(@Nonnull Table<K, V, M> table);

    /**
     * Scan and filter by entry.
     *
     * @param table          Table< K, V, M > object on which the scan must be done.
     * @param entryPredicate Predicate to filter the entries.
     * @return Collection of filtered entries.
     */
    <K extends Message, V extends Message, M extends Message>
    List<CorfuStoreEntry<K, V, M>> executeQuery(@Nonnull Table<K, V, M> table,
                                                @Nonnull Predicate<CorfuStoreEntry<K, V, M>> entryPredicate);

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
    <K extends Message, V extends Message, M extends Message>
    boolean isExists(@Nonnull Table<K, V, M> table, @Nonnull K key);

    /**
     * Return all the Queue entries ordered by their parent transaction.
     * <p>
     * Note that the key in these entries would be the CorfuQueueIdMsg.
     *
     * @param table Table< K, V, M > object aka queue on which the scan must be done.
     * @return Collection of filtered entries.
     */
    <K extends Message, V extends Message, M extends Message>
    List<Table.CorfuQueueRecord> entryList(@Nonnull Table<K, V, M> table);
}
