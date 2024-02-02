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

import static org.corfudb.runtime.collections.QueryOptions.DEFAULT_OPTIONS;

public class ReadTransaction implements ReadTransactionApi {

    @Nonnull
    public <K extends Message, V extends Message, M extends Message>
    CorfuStoreEntry<K, V, M> getRecord(@Nonnull Table<K, V, M> table,
                                       @Nonnull K key) {
        CorfuRecord<V, M> record = table.get(key);
        if (record == null) {
            return new CorfuStoreEntry<K, V, M>(key, null, null);
        }
        return new CorfuStoreEntry<K, V, M>(key, record.getPayload(), record.getMetadata());
    }

    @Nonnull
    @Override
    public <K extends Message, V extends Message, M extends Message, I>
    List<CorfuStoreEntry<K, V, M>> getByIndex(@Nonnull Table<K, V, M> table,
                                              @Nonnull String indexName,
                                              @Nonnull I indexKey) {
        return table.getByIndex(indexName, indexKey);
    }

    @Override
    public <K extends Message, V extends Message, M extends Message>
    int count(@Nonnull Table<K, V, M> table) {
        return table.count();
    }

    @Override
    public <K extends Message, V extends Message, M extends Message>
    Set<K> keySet(@Nonnull Table<K, V, M> table) {
        return table.keySet();
    }

    @Override
    public <K extends Message, V extends Message, M extends Message>
    Stream<CorfuStoreEntry<K, V, M>> entryStream(@Nonnull Table<K, V, M> table) {
        return table.entryStream();
    }

    @Override
    public <K extends Message, V extends Message, M extends Message>
    List<CorfuStoreEntry<K, V, M>> executeQuery(@Nonnull Table<K, V, M> table,
                                                @Nonnull Predicate<CorfuStoreEntry<K, V, M>> entryPredicate) {
        return table.scanAndFilterByEntry(entryPredicate);
    }

    @Override
    @Nonnull
    public <K1 extends Message, K2 extends Message,
            V1 extends Message, V2 extends Message,
            M1 extends Message, M2 extends Message, T, U>
    QueryResult<U> executeJoinQuery(
            @Nonnull Table<K1, V1, M1> table1,
            @Nonnull Table<K2, V2, M2> table2,
            @Nonnull Predicate<CorfuStoreEntry<K1, V1, M1>> query1,
            @Nonnull Predicate<CorfuStoreEntry<K2, V2, M2>> query2,
            @Nonnull BiPredicate<V1, V2> joinPredicate,
            @Nonnull BiFunction<V1, V2, T> joinFunction,
            Function<T, U> joinProjection) {
        return executeJoinQuery(table1, table2, query1, query2,
                DEFAULT_OPTIONS, DEFAULT_OPTIONS, joinPredicate,
                joinFunction, joinProjection);
    }

    @Override
    @Nonnull
    public <K1 extends Message, K2 extends Message,
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
            Function<T, U> joinProjection) {
        return JoinQuery.executeJoinQuery(table1, table2,
                query1, query2, queryOptions1,
                queryOptions2, joinPredicate, joinFunction, joinProjection);
    }

    @Override
    public <K extends Message, V extends Message, M extends Message>
    boolean isExists(@Nonnull Table<K, V, M> table, @Nonnull K key) {
        CorfuStoreEntry<K, V, M> record = getRecord(table, key);
        return record.getPayload() != null;
    }

    @Override
    public <K extends Message, V extends Message, M extends Message>
    List<Table.CorfuQueueRecord> entryList(@Nonnull Table<K, V, M> table) {
        return table.entryList();
    }
}
