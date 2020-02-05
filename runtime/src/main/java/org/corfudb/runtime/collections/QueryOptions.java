package org.corfudb.runtime.collections;

import com.google.protobuf.Message;

import java.util.Comparator;
import java.util.function.Function;

import org.corfudb.runtime.CorfuStoreMetadata.Timestamp;
import lombok.Getter;

/**
 * QueryOptions enables querying the CorfuStore with projections and custom comparators.
 * <p>
 * Created by zlokhandwala on 2019-08-10.
 */
@Getter
public class QueryOptions<K extends Message, V extends Message, M extends Message, R> {

    public static final QueryOptions DEFAULT_OPTIONS = QueryOptionsBuilder.newBuilder().build();

    private final Timestamp timestamp;
    private final boolean distinct;
    private final Comparator<R> comparator;
    private final Function<CorfuStoreEntry<K, V, M>, R> projection;

    private QueryOptions(Timestamp timestamp,
                         boolean distinct,
                         Comparator<R> comparator,
                         Function<CorfuStoreEntry<K, V, M>, R> projection) {
        this.timestamp = timestamp;
        this.distinct = distinct;
        this.comparator = comparator;
        this.projection = projection;
    }

    public static class QueryOptionsBuilder<K extends Message, V extends Message, M extends Message, R> {

        private Timestamp timestamp;
        private boolean distinct;
        private Comparator<R> comparator;
        private Function<CorfuStoreEntry<K, V, M>, R> projection;

        public static <KEY extends Message, VALUE extends Message, META extends Message, S>
        QueryOptionsBuilder<KEY, VALUE, META, S> newBuilder() {
            return new QueryOptionsBuilder<>();
        }

        public QueryOptionsBuilder<K, V, M, R> setTimestamp(Timestamp timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public QueryOptionsBuilder<K, V, M, R> setDistinct(boolean distinct) {
            this.distinct = distinct;
            return this;
        }

        public QueryOptionsBuilder<K, V, M, R> setComparator(Comparator<R> comparator) {
            this.comparator = comparator;
            return this;
        }

        public QueryOptionsBuilder<K, V, M, R> setProjection(Function<CorfuStoreEntry<K, V, M>, R> projection) {
            this.projection = projection;
            return this;
        }

        public QueryOptions<K, V, M, R> build() {
            return new QueryOptions<>(
                    timestamp,
                    distinct,
                    comparator,
                    projection);
        }
    }
}
