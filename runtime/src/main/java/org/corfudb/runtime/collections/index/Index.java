package org.corfudb.runtime.collections.index;

import com.google.common.collect.Streams;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.collections.DiskBackedCorfuTable;
import org.corfudb.runtime.collections.ICorfuTable;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.object.RocksDbStore;
import org.corfudb.util.serializer.ISerializer;
import org.rocksdb.RocksDBException;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class Index {

    /**
     * Denotes a function that supplies the unique name of an index registered to
     * {@link ICorfuTable}.
     */
    @FunctionalInterface
    public interface Name extends Supplier<String> {
    }

    /**
     * Denotes a function that takes as input the key and value of an {@link ICorfuTable}
     * record, and computes the associated index value for the record.
     *
     * @param <K> type of the record key.
     * @param <V> type of the record value.
     * @param <I> type of the index value computed.
     */
    @FunctionalInterface
    public interface Function<K, V, I> extends BiFunction<K, V, I> {
    }

    @FunctionalInterface
    public interface MultiValueFunction<K, V, I>
            extends BiFunction<K, V, Iterable<I>> {
    }

    /**
     * Descriptor of named indexing function entry. The indexing function can
     * be single indexer {@link Function} mapping a value to single
     * secondary index value, or a multi indexer {@link Function}
     * mapping a value to multiple secondary index values.
     *
     * @param <K> type of the record key associated with {@code IndexKey}.
     * @param <V> type of the record value associated with {@code IndexKey}.
     * @param <I> type of the index value computed using the {@code IndexKey}.
     */
    public static class Spec<K, V, I> {
        private final Name name;
        private final Name alias;
        private final MultiValueFunction<K, V, I> indexFunction;

        public Spec(Name name, Function<K, V, I> indexFunction) {
            this(name, name, indexFunction);
        }

        public Spec(Name name, Name alias, Function<K, V, I> indexFunction) {
            this.name = name;
            this.alias = alias;
            this.indexFunction =
                    (k, v) -> Collections.singletonList(indexFunction.apply(k, v));
        }

        public Spec(Name name, Name alias, MultiValueFunction<K, V, I> indexFunction) {
            this.name = name;
            this.alias = alias;
            this.indexFunction = indexFunction;
        }

        public Spec(Name name, MultiValueFunction<K, V, I> indexFunction) {
            this(name, name, indexFunction);
        }

        public Name getName() {
            return name;
        }

        public Name getAlias() {
            return alias;
        }

        public MultiValueFunction<K, V, I> getMultiValueIndexFunction() {
            return indexFunction;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Index)) return false;
            Spec<?, ?, ?> index = (Spec<?, ?, ?>) o;
            return Objects.equals(name.get(), index.name.get());
        }

        @Override
        public int hashCode() {
            return Objects.hash(name.get());
        }
    }

    /**
     * Registry hosting of a collection of {@link Index}.
     *
     * @param <K> type of the record key associated with {@code Index}.
     * @param <V> type of the record value associated with {@code Index}.
     */
    public interface Registry<K, V> extends Iterable<Spec<K, V, ?>> {

        Registry<?, ?> EMPTY = new Registry<Object, Object>() {
            @Override
            public <I> Optional<Spec<Object, Object, I>> get(Name name) {
                return Optional.empty();
            }

            @Override
            public Iterator<Spec<Object, Object, ? >> iterator() {
                return Collections.emptyIterator();
            }
        };

        /**
         * Obtain the {@link Function} via its registered {@link Name}.
         *
         * @param name name of the {@code IndexKey} previously registered.
         * @return the instance of {@link Function} registered to the lookup name.
         */
        <I> Optional<Spec<K, V, I>> get(Name name);

        /**
         * Obtain a static {@link Registry} with no registered {@link Function}s.
         *
         * @param <K> type of the record key associated with {@code Index}.
         * @param <V> type of the record value associated with {@code Index}.
         * @return a static instance of {@link Registry}.
         */
        static <K, V> Registry<K, V> empty() {
            @SuppressWarnings("unchecked")
            Registry<K, V> result = (Registry<K, V>) EMPTY;
            return result;
        }
    }

}
