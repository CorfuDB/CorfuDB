package org.corfudb.runtime.collections;

import com.google.protobuf.Message;

import java.util.Comparator;
import java.util.function.Function;

import org.corfudb.runtime.CorfuStoreMetadata.Timestamp;
import lombok.Getter;

/**
 * QueryOptions enables querying the CorfuStore with projections and custom comparators.
 *
 * Created by zlokhandwala on 2019-08-10.
 */
@Getter
public class QueryOptions<V extends Message, M extends Message, R> {

    public static final QueryOptions DEFAULT_OPTIONS = QueryOptionsBuilder.newBuilder().build();

    private final Timestamp timestamp;
    private final boolean distinct;
    private final Comparator<R> comparator;
    private final Function<CorfuRecord<V, M>, R> projection;

    private QueryOptions(Timestamp timestamp,
                         boolean distinct,
                         Comparator<R> comparator,
                         Function<CorfuRecord<V, M>, R> projection) {
        this.timestamp = timestamp;
        this.distinct = distinct;
        this.comparator = comparator;
        this.projection = projection;
    }

    public static class QueryOptionsBuilder<V extends Message, M extends Message, R> {

        private Timestamp timestamp;
        private boolean distinct;
        private Comparator<R> comparator;
        private Function<CorfuRecord<V, M>, R> projection;

        public static <VALUE extends Message, META extends Message, S>
        QueryOptionsBuilder<VALUE, META, S> newBuilder() {
            return new QueryOptionsBuilder<>();
        }

        public QueryOptionsBuilder<V, M, R> setTimestamp(Timestamp timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public QueryOptionsBuilder<V, M, R> setDistinct(boolean distinct) {
            this.distinct = distinct;
            return this;
        }

        public QueryOptionsBuilder<V, M, R> setComparator(Comparator<R> comparator) {
            this.comparator = comparator;
            return this;
        }

        public QueryOptionsBuilder<V, M, R> setProjection(Function<CorfuRecord<V, M>, R> projection) {
            this.projection = projection;
            return this;
        }

        public QueryOptions<V, M, R> build() {
            return new QueryOptions<>(
                    timestamp,
                    distinct,
                    comparator,
                    projection);
        }
    }
}
