package org.corfudb.runtime.collections;

import static org.corfudb.runtime.collections.QueryOptions.DEFAULT_OPTIONS;

import com.google.protobuf.Message;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuStoreMetadata.Timestamp;
import org.corfudb.runtime.object.transactions.Transaction.TransactionBuilder;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.corfudb.runtime.view.ObjectsView;
import org.corfudb.runtime.view.TableRegistry;

/**
 * Query class provides methods to query the CorfuStore tables.
 * It has several methods to query by secondary index, to perform predicate joins and apply merge filters.
 * <p>
 * Created by zlokhandwala on 2019-08-09.
 */
public class Query {

    private final TableRegistry tableRegistry;

    private final ObjectsView objectsView;

    private final String namespace;

    /**
     * Creates a Query interface.
     *
     * @param tableRegistry Table registry from the corfu client.
     * @param objectsView   Objects View from the corfu client.
     * @param namespace     Namespace to perform the queries within.
     */
    @Deprecated
    public Query(final TableRegistry tableRegistry, final ObjectsView objectsView, final String namespace) {
        this.tableRegistry = tableRegistry;
        this.objectsView = objectsView;
        this.namespace = namespace;
    }

    private Token getToken(Timestamp timestamp) {
        return new Token(timestamp.getEpoch(), timestamp.getSequence());
    }

    private void txBegin(Timestamp timestamp) {
        TransactionBuilder transactionBuilder = objectsView.TXBuild()
                .type(TransactionType.SNAPSHOT);

        if (timestamp != null) {
            transactionBuilder.snapshot(getToken(timestamp));
        }

        transactionBuilder.build().begin();
    }

    private void txEnd() {
        objectsView.TXEnd();
    }

    private <K extends Message, V extends Message, M extends Message>
    Table<K, V, M> getTable(@Nonnull final String tableName) {
        return tableRegistry.getTable(this.namespace, tableName);
    }

    /**
     * Fetch the CorfuRecord for the specified key at the LATEST timestamp (default).
     *
     * @param tableName Table name.
     * @param key       Key.
     * @param <K>       Type of key.
     * @param <V>       Type of the payload/value.
     * @param <M>       Type of the metadata.
     * @return CorfuRecord encapsulating the payload and the metadata.
     */
    @Nullable
    @Deprecated
    public <K extends Message, V extends Message, M extends Message>
    CorfuRecord<V, M> getRecord(@Nonnull final String tableName,
                                @Nonnull final K key) {
        return getRecord(tableName, null, key);
    }

    /**
     * Fetch the Value payload for the specified key at the specified snapshot/timestamp.
     *
     * @param tableName Table name.
     * @param timestamp Timestamp to perform the query.
     * @param key       Key.
     * @param <K>       Type of key.
     * @param <V>       Type of value/payload.
     * @param <M>       Type of metadata.
     * @return Value.
     */
    @Nullable
    @Deprecated
    public <K extends Message, V extends Message, M extends Message>
    CorfuRecord<V, M> getRecord(@Nonnull final String tableName,
                                @Nullable final Timestamp timestamp,
                                @Nonnull final K key) {
        if (tableName == null) {
            throw new IllegalArgumentException("Query::getRecord needs a valid tableName");
        }
        try {
            txBegin(timestamp);
            Table<K, V, M> table = getTable(tableName);
            return table.get(key);
        } finally {
            txEnd();
        }
    }

    /**
     * Gets the count of records in the table.
     *
     * @param tableName Table name.
     * @return Count of records.
     */
    @Deprecated
    public int count(@Nonnull final String tableName) {
        return count(tableName, null);
    }

    /**
     * Gets the count of records in the table at a particular timestamp.
     *
     * @param tableName Table name.
     * @param timestamp Timestamp to perform the query on.
     * @return Count of records.
     */
    @Deprecated
    public int count(@Nonnull final String tableName,
                     @Nullable final Timestamp timestamp) {
        try {
            txBegin(timestamp);
            return getTable(tableName).count();
        } finally {
            txEnd();
        }
    }

    /**
     * Returns the keySet of the Table.
     *
     * @param tableName TableName to query.
     * @param timestamp Timestamp to query at. If null, latest timestamp is used.
     * @param <K>       Type of Key.
     * @return Set of keys.
     */
    @Nonnull
    @Deprecated
    public <K extends Message> Set<K> keySet(@Nonnull String tableName,
                                             @Nullable Timestamp timestamp) {
        try {
            txBegin(timestamp);
            return (Set<K>) getTable(tableName).keySet();
        } finally {
            txEnd();
        }
    }

    @Nonnull
    @Deprecated
    private <K extends Message, V extends Message, M extends Message>
    List<CorfuStoreEntry<K, V, M>> scanAndFilterByEntry(
            @Nonnull final String tableName,
            @Nullable Timestamp timestamp,
            @Nonnull final Predicate<CorfuStoreEntry<K, V, M>> predicate) {
        try {
            txBegin(timestamp);
            return ((Table<K, V, M>) getTable(tableName)).scanAndFilterByEntry(predicate);
        } finally {
            txEnd();
        }
    }

    /**
     * Query by a secondary index.
     *
     * @param tableName Table name.
     * @param indexName Index name. In case of protobuf-defined secondary index it is the field name.
     * @param indexKey  Key to query.
     * @param <K>       Type of Key.
     * @param <V>       Type of Value.
     * @param <I>       Type of index/secondary key.
     * @return Result of the query.
     */
    @Nonnull
    @Deprecated
    public <K extends Message, V extends Message, M extends Message, I>
    QueryResult<Entry<K, V>> getByIndex(@Nonnull final String tableName,
                                        @Nonnull final String indexName,
                                        @Nonnull final I indexKey) {
        return new QueryResult<>(((Table<K, V, M>) getTable(tableName)).getByIndexAsQueryResult(indexName, indexKey));
    }

    private static <K extends Message, V extends Message, M extends Message, R>
    Collection<R> initializeResultCollection(QueryOptions<K, V, M, R> queryOptions) {
        if (!queryOptions.isDistinct()) {
            return new ArrayList<>();
        }
        if (queryOptions.getComparator() != null) {
            return new TreeSet<>(queryOptions.getComparator());
        }
        return new HashSet<>();
    }

    private static <K extends Message, V extends Message, M extends Message, R>
    Collection<R> transform(Collection<CorfuStoreEntry<K, V, M>> queryResult,
                            Collection<R> resultCollection,
                            Function<CorfuStoreEntry<K, V, M>, R> projection) {
        return queryResult.stream()
                .map(v -> Optional.ofNullable(projection)
                        .map(function -> function.apply(v))
                        .orElse((R) v))
                .collect(Collectors.toCollection(() -> resultCollection));
    }

    /**
     * Execute a scan and filter query.
     *
     * @param tableName Table name.
     * @param query     Predicate to filter the values.
     * @param <K>       Type of Key.
     * @param <V>       Type of Value.
     * @param <R>       Type of returned projected values.
     * @return Result of the query.
     */
    @Nonnull
    @Deprecated
    public <K extends Message, V extends Message, M extends Message, R>
    QueryResult<R> executeQuery(@Nonnull final String tableName,
                                @Nonnull final Predicate<CorfuStoreEntry<K, V, M>> query) {
        return executeQuery(tableName, query, DEFAULT_OPTIONS);
    }

    /**
     * Execute a scan and filter query.
     * The query options enables to define:
     * a projection function,
     * timestamp at which the table needs to be queried,
     * Flag to return only distinct results.
     *
     * @param tableName    Table name.
     * @param query        Predicate to filter the values.
     * @param queryOptions Query options.
     * @param <V>          Type of Value.
     * @param <R>          Type of returned projected values.
     * @return Result of the query.
     */
    @Nonnull
    @Deprecated
    public <K extends Message, V extends Message, M extends Message, R>
    QueryResult<R> executeQuery(@Nonnull final String tableName,
                                @Nonnull final Predicate<CorfuStoreEntry<K, V, M>> query,
                                @Nonnull final QueryOptions<K, V, M, R> queryOptions) {

        List<CorfuStoreEntry<K, V, M>> filterResult
                = scanAndFilterByEntry(tableName, queryOptions.getTimestamp(), query);
        Collection<R> result = initializeResultCollection(queryOptions);
        return new QueryResult<>(transform(filterResult, result, queryOptions.getProjection()));
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
    public static <K1 extends Message, K2 extends Message,
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
        return executeJoinQuery(
                table1,
                table2,
                query1,
                query2,
                DEFAULT_OPTIONS,
                DEFAULT_OPTIONS,
                joinPredicate,
                joinFunction,
                joinProjection);
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
    public static <K1 extends Message, K2 extends Message,
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

        List<CorfuStoreEntry<K1, V1, M1>> filterResult1
                = table1.scanAndFilterByEntry(query1);
        Collection<R> queryResult1 = transform(
                filterResult1,
                initializeResultCollection(queryOptions1),
                queryOptions1.getProjection());

        List<CorfuStoreEntry<K2, V2, M2>> filterResult2
                = table2.scanAndFilterByEntry(query2);
        Collection<S> queryResult2 = transform(
                filterResult2,
                initializeResultCollection(queryOptions2),
                queryOptions2.getProjection());

        Collection<T> joinResult = new ArrayList<>();

        for (R value1 : queryResult1) {
            for (S value2 : queryResult2) {
                if (!joinPredicate.test(value1, value2)) {
                    continue;
                }

                T resultValue = joinFunction.apply(value1, value2);
                joinResult.add(resultValue);
            }
        }

        return new QueryResult<>(joinResult.stream()
                .map(v -> Optional.ofNullable(joinProjection)
                        .map(function -> function.apply(v))
                        .orElse((U) v))
                .collect(Collectors.toList()));
    }

    /**
     * Merge Function which combines the result two tables at a time.
     *
     * @param <R> Type of result.
     */
    @FunctionalInterface
    @Deprecated
    public interface MergeFunction<R> {

        /**
         * Merges the specified argument list.
         *
         * @param arguments Objects across tables to be merged.
         * @return Resultant object.
         */
        R merge(List<Object> arguments);
    }

    /**
     * Combines the result of a multi table join, two tables at a time
     * This function can be optimized without recursions if required.
     *
     * @param list         List of collection of values across tables.
     * @param mergePayload Payload for the next merge level. Starts with an empty list.
     * @param func         Merge function specified by the user.
     * @param depth        Current depth. Starts at 0.
     * @param <R>          Return type.
     * @return List of values after merge.
     */
    private <R> List<R> merge(@Nonnull List<Collection<?>> list,
                              @Nonnull List<Object> mergePayload,
                              @Nonnull MergeFunction<R> func,
                              int depth) {
        Collection<?> collection = list.get(depth);
        if (list.size() - 1 == depth) {
            List<R> result = new ArrayList<>();
            for (Object o : collection) {
                List<Object> finalMergeList = new ArrayList<>(mergePayload);
                finalMergeList.add(o);
                R mergeResult = func.merge(finalMergeList);
                if (mergeResult != null) {
                    result.add(mergeResult);
                }
            }
            return result;
        }

        List<R> result = new ArrayList<>();
        for (Object o : collection) {
            List<Object> mergeList = new ArrayList<>(mergePayload);
            mergeList.add(o);
            result.addAll(merge(list, mergeList, func, depth + 1));
        }
        return result;
    }

    /**
     * Performs join of multiple tables.
     *
     * @param tableNames   Collection of table names to be joined.
     * @param joinFunction MergeFunction to perform the join across the specified tables.
     * @param <R>          Type of resultant Object.
     * @return Result of the query.
     */
    @Nonnull
    @Deprecated
    public <R> QueryResult<R> executeMultiJoinQuery(@Nonnull final Collection<String> tableNames,
                                                    @Nonnull final MergeFunction<R> joinFunction) {

        List<Collection<?>> values = new ArrayList<>();
        for (String tableName : tableNames) {
            Collection<Message> messages = scanAndFilterByEntry(tableName, null, corfuStoreEntry -> true)
                    .stream()
                    .map(CorfuStoreEntry::getPayload)
                    .collect(Collectors.toList());
            values.add(messages);
        }

        int mergeJoinDepth = 0; // shallow merges for now
        Collection<R> mergedResults = merge(values, new ArrayList<>(), joinFunction, mergeJoinDepth);
        return new QueryResult<>(mergedResults);
    }
}
