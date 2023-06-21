package org.corfudb.runtime.collections;

import com.google.protobuf.Message;
import org.corfudb.runtime.view.ObjectsView;
import org.corfudb.runtime.view.TableRegistry;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.TreeSet;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.corfudb.runtime.collections.QueryOptions.DEFAULT_OPTIONS;

/**
 * JoinQuery class provides methods to query multiple tables in the CorfuStore tables.
 * <p>
 * Created by zlokhandwala on 2019-08-09.
 */
public class JoinQuery {

    private final TableRegistry tableRegistry;

    private final ObjectsView objectsView;

    private final String namespace;

    /**
     * Creates a JoinQuery interface.
     *
     * @param tableRegistry Table registry from the corfu client.
     * @param objectsView   Objects View from the corfu client.
     * @param namespace     Namespace to perform the queries within.
     */
    @Deprecated
    public JoinQuery(final TableRegistry tableRegistry, final ObjectsView objectsView, final String namespace) {
        this.tableRegistry = tableRegistry;
        this.objectsView = objectsView;
        this.namespace = namespace;
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
     * @param queryOptions1  JoinQuery options to transform table 1 filtered values.
     * @param queryOptions2  JoinQuery options to transform table 2 filtered values.
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
}
