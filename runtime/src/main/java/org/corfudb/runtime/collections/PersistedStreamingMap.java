package org.corfudb.runtime.collections;

import com.google.common.collect.Streams;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.util.serializer.ISerializer;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.Options;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.io.File;
import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

/**
 * A concrete implementation of {@link StreamingMap} that is capable of storing data
 * off-heap. The location for the off-heap data is provided by {@link File} dataPath,
 * while the resource policy (memory and storage limits) are defined in {@link Options}.
 *
 * @param <K> key type
 * @param <V> value type
 */
@Slf4j
public class PersistedStreamingMap<K, V> implements ContextAwareMap<K, V> {

    static {
        RocksDB.loadLibrary();
    }

    private final ContextAwareMap<K, V> optimisticMap = new StreamingMapDecorator<>();
    private final AtomicInteger dataSetSize = new AtomicInteger();
    private final CorfuRuntime corfuRuntime;
    private final ISerializer serializer;
    private final RocksDB rocksDb;

    public PersistedStreamingMap(@NonNull Path dataPath,
                                 @NonNull Options options,
                                 @NonNull ISerializer serializer,
                                 @NonNull CorfuRuntime corfuRuntime) {
        try {
            RocksDB.destroyDB(dataPath.toFile().getAbsolutePath(), options);
            this.rocksDb = RocksDB.open(options, dataPath.toFile().getAbsolutePath());
        } catch (RocksDBException e) {
            throw new UnrecoverableCorfuError(e);
        }
        this.serializer = serializer;
        this.corfuRuntime = corfuRuntime;
    }

    /**
     * A Java compatible {@link RocksIterator} implementation
     */
    public class RocksDbIterator implements Iterator<Entry<K, V>> {
        private RocksIterator iterator;
        private Entry<K, V> current;
        private Entry<K, V> next;

        RocksDbIterator(RocksIterator iterator) {
            this.iterator = iterator;
        }

        /**
         * Ensure that this iterator is operating under the correct assumptions.
         */
        private void checkInvariants() {
            // RocksDB does not support multi-threaded access.
            // If the iterator was created by some thread, it also has to be consumed by it.
            if (!iterator.isOwningHandle()) {
                throw new IllegalStateException("Detected multi-threaded access to this iterator.");
            }

            try {
                iterator.status();
            } catch (RocksDBException e) {
                throw new UnrecoverableCorfuError(
                        "There was an error reading the persisted map.", e);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean hasNext() {
            // If we have the next element pipelined, go ahead and return true.
            if (next != null) {
                return true;
            }

            // If the iterator is valid, this means that the next entry exists.
            checkInvariants();
            if (iterator.isValid()) {
                // Go ahead and cache that entry.
                next = new AbstractMap.SimpleEntry(
                        serializer.deserialize(Unpooled.wrappedBuffer(iterator.key()), corfuRuntime),
                        serializer.deserialize(Unpooled.wrappedBuffer(iterator.value()), corfuRuntime));
                // Advance the underlying iterator.
                iterator.next();
            }

            return next != null;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Entry<K, V> next() {
            if (hasNext()) {
                current = next;
                next = null;
                return current;
            } else {
                throw new NoSuchElementException();
            }

        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int size() {
        return dataSetSize.get();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isEmpty() {
        return dataSetSize.get() == 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean containsKey(@NonNull Object key) {
        final ByteBuf keyPayload = Unpooled.buffer();
        serializer.serialize(key, keyPayload);
        try {
            byte[] value = rocksDb.get(
                    keyPayload.array(), keyPayload.arrayOffset(), keyPayload.readableBytes());
            return value != null;
        } catch (RocksDBException ex) {
            throw new UnrecoverableCorfuError(ex);
        } finally {
            keyPayload.release();
        }
    }

    /**
     * {@inheritDoc}
     *
     * Please use {@link StreamingMap#entryStream()}.
     */
    @Override
    public boolean containsValue(@NonNull Object value) {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public V get(@NonNull Object key) {
        final ByteBuf keyPayload = Unpooled.buffer();
        serializer.serialize(key, keyPayload);

        try {
            byte[] value = rocksDb.get(
                    keyPayload.array(), keyPayload.arrayOffset(), keyPayload.readableBytes());
            if (value == null) {
                return null;
            }
            return (V) serializer.deserialize(Unpooled.wrappedBuffer(value), corfuRuntime);
        } catch (RocksDBException ex) {
            throw new UnrecoverableCorfuError(ex);
        } finally {
            keyPayload.release();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public V put(@NonNull K key, @NonNull V value) {
        final ByteBuf keyPayload = Unpooled.buffer();
        final ByteBuf valuePayload = Unpooled.buffer();
        serializer.serialize(key, keyPayload);
        serializer.serialize(value, valuePayload);

        // Only increment the count if the value is not present. In other words,
        // increment the count if this is an update operation.
        final boolean keyExists = rocksDb.keyMayExist(keyPayload.array(),
                keyPayload.arrayOffset(), keyPayload.readableBytes(), new StringBuilder());
        if (!keyExists) {
            dataSetSize.incrementAndGet();
        }

        try {
            rocksDb.put(
                    keyPayload.array(), keyPayload.arrayOffset(), keyPayload.readableBytes(),
                    valuePayload.array(), valuePayload.arrayOffset(), valuePayload.readableBytes());
        } catch (RocksDBException ex) {
            throw new UnrecoverableCorfuError(ex);
        } finally {
            keyPayload.release();
            valuePayload.release();
        }

        return value;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public V remove(@NonNull Object key) {
        final ByteBuf keyPayload = Unpooled.buffer();
        serializer.serialize(key, keyPayload);
        try {
            V value = get(key);
            if (value != null) {
                rocksDb.delete(
                        keyPayload.array(), keyPayload.arrayOffset(), keyPayload.readableBytes());
                dataSetSize.decrementAndGet();
                return value;
            } else {
                return null;
            }
        } catch (RocksDBException ex) {
            throw new UnrecoverableCorfuError(ex);
        } finally {
            keyPayload.release();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void putAll(@NonNull Map<? extends K, ? extends V> map) {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clear() {
        entryStream().map(Entry::getKey).forEach(this::remove);
        dataSetSize.set(0);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<K> keySet() {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     *
     * Please use {@link StreamingMap#entryStream()}.
     */
    @Override
    public Collection<V> values() {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     *
     * Please use {@link StreamingMap#entryStream()}.
     */
    @Override
    public Set<Entry<K, V>> entrySet() {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ContextAwareMap<K, V> getOptimisticMap() {
        return optimisticMap;
    }

    /**
     * Return a new iterator that iterates over a data snapshot.
     *
     * @return snapshot iterator
     */
    protected RocksDbIterator newRocksDbIterator() {
        final ReadOptions readOptions = new ReadOptions();
        // If ReadOptions.snapshot is given, the iterator will return data as of the snapshot.
        // If it is nullptr, the iterator will read from an implicit snapshot as of the time the
        // iterator is created. The implicit snapshot is preserved by pinning resource.
        readOptions.setSnapshot(null);
        final RocksIterator rocksIterator = rocksDb.newIterator(readOptions);
        rocksIterator.seekToFirst();
        return new RocksDbIterator(rocksIterator);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Stream<Entry<K, V>> entryStream() {
        return Streams.stream(newRocksDbIterator());
    }

    /**
     * Close the underlying database.
     */
    @Override
    public void close() {
        this.rocksDb.close();
    }
}
