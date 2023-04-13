package org.corfudb.runtime.collections;

import io.micrometer.core.instrument.Timer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MeterRegistryProvider;
import org.corfudb.common.metrics.micrometer.MicroMeterUtils;
import org.corfudb.runtime.CorfuOptions;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.object.ConsistencyView;
import org.corfudb.runtime.object.PersistenceOptions;
import org.corfudb.runtime.object.RocksDbApi;
import org.corfudb.runtime.object.RocksDbSnapshotGenerator;
import org.corfudb.runtime.object.SMRSnapshot;
import org.corfudb.runtime.object.RocksDbStore;
import org.corfudb.runtime.object.SnapshotGenerator;
import org.corfudb.runtime.object.VersionedObjectIdentifier;
import org.corfudb.runtime.object.ViewGenerator;
import org.corfudb.runtime.view.ObjectOpenOption;
import org.corfudb.util.serializer.ISerializer;
import org.rocksdb.CompressionType;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Statistics;
import org.rocksdb.StatsLevel;
import org.rocksdb.WriteOptions;

import java.util.Map;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.corfudb.runtime.CorfuOptions.ConsistencyModel.READ_COMMITTED;

@Slf4j
@AllArgsConstructor
@Builder(toBuilder=true)
public class DiskBackedCorfuTable<K, V> implements
        SnapshotGenerator<DiskBackedCorfuTable<K, V>>,
        ViewGenerator<DiskBackedCorfuTable<K, V>>,
        ConsistencyView {

    public static final String DISK_BACKED = "diskBacked";
    public static final String TRUE = "true";
    public static final int BOUND = 100;
    public static final int SAMPLING_RATE = 40;

    private final WriteOptions writeOptions = new WriteOptions()
            .setDisableWAL(true)
            .setSync(false);

    static {
        RocksDB.loadLibrary();
    }

    /**
     * A set of options defined for {@link DiskBackedCorfuTable}.
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
    public static Options getDiskBackedCorfuTableOptions() {
        final Options options = new Options();

        options.setCreateIfMissing(true);
        options.setCompressionType(CompressionType.LZ4_COMPRESSION);

        return options;
    }

    private final PersistenceOptions persistenceOptions;
    private final ISerializer serializer;
    private final RocksDbApi<DiskBackedCorfuTable<K, V>> rocksApi;
    private final RocksDbSnapshotGenerator<DiskBackedCorfuTable<K, V>> rocksDbSnapshotGenerator;
    private final String metricsId;
    @Getter
    private final Statistics statistics;

    public DiskBackedCorfuTable(@NonNull PersistenceOptions persistenceOptions,
                                @NonNull Options rocksDbOptions,
                                @NonNull ISerializer serializer) {
        this.persistenceOptions = persistenceOptions;
        try {
            this.statistics = new Statistics();
            this.statistics.setStatsLevel(StatsLevel.ALL);
            rocksDbOptions.setStatistics(statistics);

            final RocksDbStore<DiskBackedCorfuTable<K, V>> rocksDbStore = new RocksDbStore<>(
                    persistenceOptions.getDataPath(),
                    rocksDbOptions, writeOptions, persistenceOptions);
            this.rocksApi = rocksDbStore;
            this.rocksDbSnapshotGenerator = rocksDbStore;
            this.metricsId = String.format("%s.%s.",
                    persistenceOptions.getDataPath().getFileName(), System.identityHashCode(this));
            MeterRegistryProvider.registerExternalSupplier(metricsId, this.statistics::toString);
        } catch (RocksDBException e) {
            throw new UnrecoverableCorfuError(e);
        }

        this.serializer = serializer;
    }

    public DiskBackedCorfuTable(@NonNull PersistenceOptions persistenceOptions,
                                @NonNull ISerializer serializer) {
        this(persistenceOptions, getDiskBackedCorfuTableOptions(), serializer);
    }

    public V get(@NonNull Object key) {
        Optional<Timer.Sample> recordSample = MicroMeterUtils.startTimer(
                SAMPLING_RATE > ThreadLocalRandom.current().nextInt(BOUND));

        final ByteBuf keyPayload = Unpooled.buffer();

        try {
            // Serialize in the try-catch block to release ByteBuf when an exception occurs.
            serializer.serialize(key, keyPayload);
            byte[] value = rocksApi.get(keyPayload);
            if (value == null) {
                return null;
            }
            return (V) serializer.deserialize(Unpooled.wrappedBuffer(value), null);
        } catch (RocksDBException ex) {
            throw new UnrecoverableCorfuError(ex);
        } finally {
            keyPayload.release();
            MicroMeterUtils.time(recordSample, "corfu_table.read.timer", DISK_BACKED, TRUE);
        }
    }

    public boolean containsKey(@NonNull Object key) {
        final ByteBuf keyPayload = Unpooled.buffer();

        try {
            // Serialize in the try-catch block to release ByteBuf when an exception occurs.
            serializer.serialize(key, keyPayload);
            byte[] value = rocksApi.get(keyPayload);
            return value != null;
        } catch (RocksDBException ex) {
            throw new UnrecoverableCorfuError(ex);
        } finally {
            keyPayload.release();
        }
    }

    public DiskBackedCorfuTable<K, V> put(@NonNull K key, @NonNull V value) {
        Optional<Timer.Sample> recordSample = MicroMeterUtils.startTimer(
                SAMPLING_RATE > ThreadLocalRandom.current().nextInt(BOUND));

        final ByteBuf keyPayload = Unpooled.buffer();
        final ByteBuf valuePayload = Unpooled.buffer();

        try {
            // Serialize in the try-catch block to release ByteBuf when an exception occurs.
            serializer.serialize(key, keyPayload);
            serializer.serialize(value, valuePayload);
            rocksApi.insert(keyPayload, valuePayload);
            return this;
        } catch (RocksDBException ex) {
            throw new UnrecoverableCorfuError(ex);
        } finally {
            keyPayload.release();
            valuePayload.release();
            MicroMeterUtils.time(recordSample, "corfu_table.write.timer", DISK_BACKED, TRUE);
        }
    }

    public DiskBackedCorfuTable<K, V> remove(@NonNull K key) {
        final ByteBuf keyPayload = Unpooled.buffer();

        try {
            // Serialize in the try-catch block to release ByteBuf when an exception occurs.
            serializer.serialize(key, keyPayload);
            rocksApi.delete(keyPayload);
            return this;
        } catch (RocksDBException ex) {
            throw new UnrecoverableCorfuError(ex);
        } finally {
            keyPayload.release();
        }
    }

    public DiskBackedCorfuTable<K, V> clear() {
        try {
            rocksApi.clear();
        } catch (RocksDBException e) {
            throw new UnrecoverableCorfuError(e);
        }
        return this;
    }

    public Stream<Map.Entry<K, V>> entryStream() {
        final RocksDbEntryIterator<K, V> entryIterator = rocksApi.getIterator(serializer);
        Stream<Map.Entry<K, V>> resStream = StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(entryIterator, Spliterator.ORDERED), false);
        return resStream.onClose(entryIterator::close);
    }

    public long size() {
        return rocksApi.exactSize();
    }

    @Override
    public void close() {
        try {
            rocksApi.close();
        } catch (RocksDBException e) {
            throw new UnrecoverableCorfuError(e);
        } finally {
            if (isRoot()) {
                MeterRegistryProvider.unregisterExternalSupplier(metricsId);
            }
        }
    }

    @Override
    public SMRSnapshot<DiskBackedCorfuTable<K, V>> generateSnapshot(VersionedObjectIdentifier version) {
        if (persistenceOptions.getConsistencyModel() == READ_COMMITTED) {
            return rocksDbSnapshotGenerator.getImplicitSnapshot(this, version);
        }
        return rocksDbSnapshotGenerator.getSnapshot(this, version);
    }

    @Override
    public Optional<SMRSnapshot<DiskBackedCorfuTable<K, V>>> generateTargetSnapshot(
            VersionedObjectIdentifier version,
            ObjectOpenOption objectOpenOption,
            SMRSnapshot<DiskBackedCorfuTable<K, V>> previousSnapshot) {
        return Optional.empty();
    }

    @Override
    public Optional<SMRSnapshot<DiskBackedCorfuTable<K, V>>> generateIntermediarySnapshot(
            VersionedObjectIdentifier version,
            ObjectOpenOption objectOpenOption) {
        return Optional.of(generateSnapshot(version));
    }

    @Override
    public DiskBackedCorfuTable<K, V> newView(@NonNull RocksDbApi<DiskBackedCorfuTable<K, V>> rocksApi) {
        if (!isRoot()) {
            throw new IllegalStateException("Only the root object cen generate new views.");
        }
        return toBuilder().rocksApi(rocksApi).build();
    }

    private boolean isRoot() {
        return rocksApi == rocksDbSnapshotGenerator;
    }

    @Override
    public CorfuOptions.ConsistencyModel getConsistencyModel() {
        return persistenceOptions.getConsistencyModel();
    }
}
