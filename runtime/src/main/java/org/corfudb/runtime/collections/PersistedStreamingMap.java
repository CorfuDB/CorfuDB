package org.corfudb.runtime.collections;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MicroMeterUtils;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.util.serializer.ISerializer;
import org.rocksdb.CompactionOptionsUniversal;
import org.rocksdb.CompressionType;
import org.rocksdb.Holder;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;

import java.io.File;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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

    public static final String DISK_BACKED = "diskBacked";
    public static final String TRUE = "true";
    public static final int SAMPLING_RATE = 1000;

    private final Optional<Counter> getCounter;
    private final Optional<Counter> putCounter;

    private final WriteOptions writeOptions = new WriteOptions()
            .setDisableWAL(true)
            .setSync(false);

    static {
        RocksDB.loadLibrary();
    }

    /**
     * A set of options defined for disk-backed {@link PersistedStreamingMap}.
     *
     * For a set of options that dictate RocksDB memory usage can be found here:
     * https://github.com/facebook/rocksdb/wiki/Memory-usage-in-RocksDB
     *
     * Block Cache:  Which can be set via Options::setTableFormatConfig.
     *               Out of box, RocksDB will use LRU-based block cache
     *               implementation with 8MB capacity.
     * Index/Filter: Is a function of the block cache. Generally it inflates
     *               the block cache by about 50%. The exact number can be
     *               retrieved via "rocksdb.estimate-table-readers-mem"
     *               property.
     * Write Buffer: Also known as memtable is defined by the ColumnFamilyOptions
     *               option. The default is 64 MB.
     */
    public static Options getPersistedStreamingMapOptions() {
        final int maxSizeAmplificationPercent = 50;
        final Options options = new Options();

        options.setCreateIfMissing(true);
        options.setCompressionType(CompressionType.LZ4_COMPRESSION);

        // Set a threshold at which full compaction will be triggered.
        // This is important as it purges tombstoned entries.
        final CompactionOptionsUniversal compactionOptions = new CompactionOptionsUniversal();
        compactionOptions.setMaxSizeAmplificationPercent(maxSizeAmplificationPercent);
        options.setCompactionOptionsUniversal(compactionOptions);
        return options;
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
        getCounter = MicroMeterUtils.counter("persisted_map.get.counter");
        putCounter = MicroMeterUtils.counter("persisted_map.put.counter");
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
     * <p>
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
        getCounter.ifPresent(Counter::increment);
        Optional<Timer.Sample> recordSample = MicroMeterUtils.startTimer(getCounter, SAMPLING_RATE);
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
            MicroMeterUtils.time(recordSample, "corfu_table.read.timer", DISK_BACKED, TRUE);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public V put(@NonNull K key, @NonNull V value) {
        putCounter.ifPresent(Counter::increment);
        Optional<Timer.Sample> recordSample = MicroMeterUtils.startTimer(putCounter, SAMPLING_RATE);
        final ByteBuf keyPayload = Unpooled.buffer();
        final ByteBuf valuePayload = Unpooled.buffer();
        serializer.serialize(key, keyPayload);
        serializer.serialize(value, valuePayload);

        // Only increment the count if the value is not present. In other words,
        // increment the count if this is an update operation.
        final Holder<byte[]> holder = new Holder<>();
        final boolean keyExists = rocksDb.keyMayExist(keyPayload.array(),
                keyPayload.arrayOffset(), keyPayload.readableBytes(), holder);
        if (!keyExists) {
            dataSetSize.incrementAndGet();
        }

        try {
            rocksDb.put(writeOptions,
                    keyPayload.array(), keyPayload.arrayOffset(), keyPayload.readableBytes(),
                    valuePayload.array(), valuePayload.arrayOffset(), valuePayload.readableBytes());
        } catch (RocksDBException ex) {
            throw new UnrecoverableCorfuError(ex);
        } finally {
            keyPayload.release();
            valuePayload.release();
            MicroMeterUtils.time(recordSample, "corfu_table.write.timer", DISK_BACKED, TRUE);
        }

        // Although keyMayExist is probabilistic, the found value should be returned to better approximate
        // the Map.java interface semantics
        if (Objects.nonNull(holder.getValue())) {
            return (V) serializer.deserialize(Unpooled.wrappedBuffer(holder.getValue()), corfuRuntime);
        }

        return null;
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
                rocksDb.delete(writeOptions,
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
        try (final RocksDbEntryIterator<K, V> entryIterator =
                     new RocksDbEntryIterator<>(rocksDb, serializer, false)) {
            Set<K> keySet = new HashSet<>();
            while (entryIterator.hasNext()) {
                keySet.add(entryIterator.next().getKey());
            }
            return keySet;
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Please use {@link StreamingMap#entryStream()}.
     */
    @Override
    public Collection<V> values() {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     * <p>
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
     * {@inheritDoc}
     */
    @Override
    public Stream<Entry<K, V>> entryStream() {
        final RocksDbEntryIterator<K, V> entryIterator = new RocksDbEntryIterator<>(rocksDb, serializer);
        Stream<Entry<K, V>> resStream = StreamSupport.stream(Spliterators.spliteratorUnknownSize(entryIterator,
                Spliterator.ORDERED), false);
        resStream.onClose(entryIterator::close);
        return resStream;
    }

    /**
     * Close the underlying database.
     */
    @Override
    public void close() {
        this.rocksDb.close();
    }
}
