package org.corfudb.runtime.collections;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import io.micrometer.core.instrument.Timer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MicroMeterUtils;
import org.corfudb.runtime.CorfuOptions;
import org.corfudb.runtime.collections.index.Index;
import org.corfudb.runtime.collections.index.IndexStore;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.object.ColumnFamilyRegistry;
import org.corfudb.runtime.object.DiskBackedSMRSnapshot;
import org.corfudb.runtime.object.RocksDbApi;
import org.corfudb.runtime.object.RocksDbSnapshotGenerator;
import org.corfudb.runtime.object.RocksDbStore;
import org.corfudb.runtime.object.SMRSnapshot;
import org.corfudb.runtime.object.SnapshotGenerator.SnapshotGeneratorWithConsistency;
import org.corfudb.runtime.object.SnapshotProxy;
import org.corfudb.runtime.object.VersionedObjectIdentifier;
import org.corfudb.runtime.object.ViewGenerator;
import org.corfudb.runtime.view.ObjectOpenOption;
import org.corfudb.util.serializer.ISerializer;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.CompressionType;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;

import java.util.Map;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.corfudb.runtime.CorfuOptions.ConsistencyModel.READ_COMMITTED;
import static org.corfudb.runtime.CorfuOptions.SizeComputationModel.EXACT_SIZE;

/**
 * This is the underlying implementation of {@link PersistedCorfuTable}.
 * <p>
 * There are two higher level constructs that incorporate {@link DiskBackedCorfuTable}:
 * {@link DiskBackedSMRSnapshot}: A particular snapshot in time associated with some version.
 * {@link SnapshotProxy}: A particular view of the above snapshot.
 * <p>
 * As we are moving the object/table forward, we end up creating new snapshots. When a client
 * wants to consume that snapshot, we create a new view. The hierarchy can be represented
 * as follows:
 * <p>
 * MVO
 * |------ Version 1: DiskBackedSMRSnapshot
 * |                  |------ DiskBackedCorfuTable(RocksDbTx1) (SnapshotProxy)
 * |                  |------ DiskBackedCorfuTable(RocksDbTx2) (SnapshotProxy)
 * |                  |------ ...
 * |------ Version 2: DiskBackedSMRSnapshot
 * |------ Version 3: DiskBackedSMRSnapshot
 * |                  |------ DiskBackedCorfuTable(RocksDbTx3) (SnapshotProxy)
 * |                  |------ DiskBackedCorfuTable(RocksDbTx4) (SnapshotProxy)
 * |                  |------ ...
 *
 * @param <K> key type
 * @param <V> value type
 */
@Slf4j
@AllArgsConstructor
@Builder(toBuilder = true)
public class DiskBackedCorfuTable<K, V> implements
        SnapshotGeneratorWithConsistency<DiskBackedCorfuTable<K, V>>,
        ViewGenerator<DiskBackedCorfuTable<K, V>> {

    public static final Options DEFAULT_OPTIONS = getDiskBackedCorfuTableOptions();
    private static final HashFunction murmurHash3 = Hashing.murmur3_32();
    private static final String DISK_BACKED = "diskBacked";
    private static final String TRUE = "true";
    private static final int BOUND = 100;
    private static final int SAMPLING_RATE = 40;

    // Optimization: We never perform crash-recovery, so we can disable
    // Write Ahead Log and disable explicit calls to sync().
    public static final WriteOptions WRITE_OPTIONS = new WriteOptions()
            .setDisableWAL(true)
            .setSync(false);

    static {
        RocksDB.loadLibrary();
    }

    private final RocksDbApi rocksApi;
    private final RocksDbSnapshotGenerator<DiskBackedCorfuTable<K, V>> rocksDbSnapshotGenerator;
    private final ColumnFamilyRegistry columnFamilyRegistry;
    private final ISerializer serializer;

    private final RocksDbStore<DiskBackedCorfuTable<K, V>> rocksDbStore;

    @Getter
    private final Optional<IndexStore<K, V>> indexStore;

    public DiskBackedCorfuTable(
            @NonNull ISerializer serializer,
            @NonNull RocksDbStore<DiskBackedCorfuTable<K, V>> rocksDbStore
    ) {
        this(serializer, rocksDbStore, Optional.of(new IndexStore<>(Index.Registry.empty(), serializer, rocksDbStore)));
    }

    public DiskBackedCorfuTable(
            @NonNull ISerializer serializer,
            @NonNull RocksDbStore<DiskBackedCorfuTable<K, V>> rocksDbStore,
            @NonNull Optional<IndexStore<K, V>> indexStore
    ) {

        this.rocksDbStore = rocksDbStore;
        this.rocksApi = rocksDbStore;
        this.columnFamilyRegistry = rocksDbStore;
        this.rocksDbSnapshotGenerator = rocksDbStore;

        this.indexStore = indexStore;
        this.serializer = serializer;
    }

    /**
     * A set of options defined for {@link DiskBackedCorfuTable}.
     * <p>
     * For a set of options that dictate RocksDB memory usage can be found here:
     * <a href="https://github.com/facebook/rocksdb/wiki/Memory-usage-in-RocksDB">...</a>
     * <p>
     * Block Cache:  Which can be set via Options::setTableFormatConfig.
     * Out of box, RocksDB will use LRU-based block cache
     * implementation with 8MB capacity.
     * Index/Filter: Is a function of the block cache. Generally it inflates
     * the block cache by about 50%. The exact number can be
     * retrieved via "rocksdb.estimate-table-readers-mem"
     * property.
     * Write Buffer: Also known as memtable is defined by the ColumnFamilyOptions
     * option. The default is 64 MB.
     */
    private static Options getDiskBackedCorfuTableOptions() {
        final Options options = new Options();

        options.setCreateIfMissing(true);
        options.setCompressionType(CompressionType.LZ4_COMPRESSION);

        BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();
        blockBasedTableConfig.setFilterPolicy(new BloomFilter(10));
        options.setTableFormatConfig(blockBasedTableConfig);

        return options;
    }

    public static int hashBytes(byte[] serializedObject, int offset, int length) {
        return murmurHash3.hashBytes(serializedObject, offset, length).asInt();
    }

    public V get(@NonNull Object key) {
        Optional<Timer.Sample> recordSample = MicroMeterUtils
                .startTimer(SAMPLING_RATE > ThreadLocalRandom.current().nextInt(BOUND));

        try {
            // Serialize in the try-catch block to release ByteBuf when an exception occurs.
            AtomicReference<byte[]> value = new AtomicReference<>();
            managedPayload(key, keyPayload -> {
                byte[] data = rocksApi.get(columnFamilyRegistry.getDefaultColumnFamily(), keyPayload);
                value.set(data);
            });

            byte[] resultValue = value.get();
            if (resultValue == null) {
                return null;
            }

            return serializer.deserializeTyped(Unpooled.wrappedBuffer(value.get()), null);
        } finally {
            MicroMeterUtils.time(recordSample, "corfu_table.read.timer", DISK_BACKED, TRUE);
        }
    }

    public boolean containsKey(@NonNull Object key) {
        final ByteBuf keyPayload = Unpooled.buffer();

        try {
            // Serialize in the try-catch block to release ByteBuf when an exception occurs.
            serializer.serialize(key, keyPayload);
            byte[] value = rocksApi.get(columnFamilyRegistry.getDefaultColumnFamily(), keyPayload);
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

        try {
            V previous = get(key);

            // Insert the primary key-value mapping into the default column family.
            managedPayload(key, keyPayload -> {
                managedPayload(value, valuePayload -> {
                    if (indexStore.isPresent()) {
                        IndexStore<K, V> registry = indexStore.get();
                        registry.remap(key, value, previous, valuePayload);
                    }

                    rocksApi.insert(columnFamilyRegistry.getDefaultColumnFamily(), keyPayload, valuePayload);
                });
            });

            return this;
        } finally {
            MicroMeterUtils.time(recordSample, "corfu_table.write.timer", DISK_BACKED, TRUE);
        }
    }


    /**
     * Prevents the code that uses serialized values from resource leaks
     * by providing managed ByteBuf which is closed automatically
     *
     * @param data           data that will be serialized
     * @param payloadHandler the code that uses serialized payload
     * @param <T>            data type
     */
    private <T> void managedPayload(T data, ManagedPayloadHandler payloadHandler) {
        final ByteBuf payload = Unpooled.buffer();
        try {
            serializer.serialize(data, payload);
            payloadHandler.handle(payload);
        } catch (RocksDBException ex) {
            close();
            throw new UnrecoverableCorfuError(ex);
        } finally {
            payload.release();
        }
    }

    public DiskBackedCorfuTable<K, V> remove(@NonNull K key) {
        // Delete the primary key-value mapping from the default column family.
        V previous = get(key);

        managedPayload(key, keyPayload -> {
            if (indexStore.isPresent()) {
                indexStore.get().unmapSecondaryIndexes(key, previous);
            }
            rocksApi.delete(columnFamilyRegistry.getDefaultColumnFamily(), keyPayload);
        });
        return this;
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
        if (rocksDbStore.getPersistenceOptions().getSizeComputationModel() == EXACT_SIZE) {
            return rocksApi.exactSize();
        }

        return rocksApi.estimateSize();
    }

    @Override
    public void close() {

        // Do not call close on WriteOptions and Options, as they are
        // either statically defined or owned by the client.

        try {
            this.rocksApi.close();
            this.rocksDbStore.close();
        } catch (RocksDBException e) {
            throw new UnrecoverableCorfuError(e);
        }
    }

    @Override
    public SMRSnapshot<DiskBackedCorfuTable<K, V>> generateSnapshot(VersionedObjectIdentifier version) {
        if (getConsistencyModel() == READ_COMMITTED) {
            return rocksDbSnapshotGenerator.getImplicitSnapshot(this);
        }
        return rocksDbSnapshotGenerator.getSnapshot(this, version);
    }

    @Override
    public Optional<SMRSnapshot<DiskBackedCorfuTable<K, V>>> generateTargetSnapshot(
            VersionedObjectIdentifier version,
            ObjectOpenOption objectOpenOption,
            SMRSnapshot<DiskBackedCorfuTable<K, V>> previousSnapshot) {
        // We always generate an intermediary snapshot, therefore,
        // there is no reason for generating a target snapshot.
        return Optional.empty();
    }

    @Override
    public Optional<SMRSnapshot<DiskBackedCorfuTable<K, V>>> generateIntermediarySnapshot(
            VersionedObjectIdentifier version,
            ObjectOpenOption objectOpenOption) {
        return Optional.of(generateSnapshot(version));
    }

    @Override
    public DiskBackedCorfuTable<K, V> newView(@NonNull RocksDbApi rocksApi) {
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
        return rocksDbStore.getPersistenceOptions().getConsistencyModel();
    }

    @FunctionalInterface
    private interface ManagedPayloadHandler {
        void handle(ByteBuf payload) throws RocksDBException;
    }
}
