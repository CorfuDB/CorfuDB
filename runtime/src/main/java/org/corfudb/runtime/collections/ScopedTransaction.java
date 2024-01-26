package org.corfudb.runtime.collections;

import com.google.protobuf.Message;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.IntervalRetry;
import org.corfudb.util.retry.RetryNeededException;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class ScopedTransaction implements
        ReadTransactionApi, AutoCloseable {

    private final ReadTransactionApi crud;
    private final CorfuRuntime runtime;
    private final String namespace;

    @Getter
    private final Token txnSnapshot;
    private final Map<
            Table<? extends Message, ? extends Message, ? extends Message>,
            Table> mapping;

    ScopedTransaction(
            @Nonnull final CorfuRuntime runtime,
            @Nonnull final String namespace,
            @Nonnull final IsolationLevel isolationLevel,
            Table<?, ?, ?>... tables) {
        this.crud = new ReadTransaction();
        this.runtime = runtime;
        this.namespace = namespace;
        this.txnSnapshot = txBeginInternal(isolationLevel);
        this.mapping = Arrays.stream(tables).collect(Collectors.toMap(
                        table -> table,
                        table -> table.generateImmutableView(txnSnapshot.getSequence())));
    }

    public static ScopedTransaction newScopedTransaction(
            @Nonnull final CorfuRuntime runtime,
            @Nonnull final String namespace,
            @Nonnull final IsolationLevel isolationLevel,
            Table<?, ?, ?>... tables) {
        try {
            return IRetry.build(IntervalRetry.class, () -> {
                try {
                    return new ScopedTransaction(
                            runtime,
                            namespace,
                            isolationLevel,
                            tables);
                } catch (TrimmedException e) {
                    log.error("Error while attempting to snapshot tables {}.", tables, e);
                    throw new RetryNeededException();
                }
            }).run();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private Token txBeginInternal(IsolationLevel isolationLevel) {
        log.trace("TxnContext: begin transaction in namespace {}", namespace);

        if (isolationLevel.getTimestamp() != Token.UNINITIALIZED) {
            return isolationLevel.getTimestamp();
        }

        return runtime
                .getSequencerView()
                .query()
                .getToken();
    }

    @Nonnull
    @Override
    public <K extends Message, V extends Message, M extends Message> CorfuStoreEntry<K, V, M>
    getRecord(@Nonnull Table<K, V, M> table, @Nonnull K key) {
        return crud.getRecord(getTableSnapshot(table), key);
    }

    @Override
    public void close() {
    }

    private <K extends Message, V extends Message, M extends Message>
    Table<K, V, M> getTableSnapshot(Table<K, V, M> table) {
        return Optional.of(mapping.get(table)).orElseThrow(
                () -> new IllegalStateException("Table not specified during TX build."));
    }

    @Nonnull
    @Override
    public <K extends Message, V extends Message, M extends Message, I> List<CorfuStoreEntry<K, V, M>>
    getByIndex(@Nonnull Table<K, V, M> table, @Nonnull String indexName, @Nonnull I indexKey) {
        return crud.getByIndex(getTableSnapshot(table), indexName, indexKey);
    }

    @Override
    public <K extends Message, V extends Message, M extends Message>
    int count(@Nonnull Table<K, V, M> table) {
        return crud.count(getTableSnapshot(table));
    }

    @Override
    public <K extends Message, V extends Message, M extends Message> Set<K>
    keySet(@Nonnull Table<K, V, M> table) {
        return crud.keySet(getTableSnapshot(table));
    }

    @Override
    public <K extends Message, V extends Message, M extends Message> Stream<CorfuStoreEntry<K, V, M>>
    entryStream(@Nonnull Table<K, V, M> table) {
        return crud.entryStream(getTableSnapshot(table));
    }

    @Override
    public <K extends Message, V extends Message, M extends Message> List<CorfuStoreEntry<K, V, M>>
    executeQuery(@Nonnull Table<K, V, M> table,
                 @Nonnull Predicate<CorfuStoreEntry<K, V, M>> corfuStoreEntryPredicate) {
        return crud.executeQuery(getTableSnapshot(table), corfuStoreEntryPredicate);
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
        return crud.executeJoinQuery(
                getTableSnapshot(table1), getTableSnapshot(table2),
                query1, query2, joinPredicate, joinFunction, joinProjection);
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
                getTableSnapshot(table1), getTableSnapshot(table2),
                query1, query2, queryOptions1, queryOptions2,
                joinPredicate, joinFunction, joinProjection);
    }

    @Override
    public <K extends Message, V extends Message, M extends Message>
    boolean isExists(@Nonnull Table<K, V, M> table, @Nonnull K key) {
        return crud.isExists(getTableSnapshot(table), key);
    }

    @Override
    public <K extends Message, V extends Message, M extends Message> List<Table.CorfuQueueRecord>
    entryList(@Nonnull Table<K, V, M> table) {
        return crud.entryList(getTableSnapshot(table));
    }
}
