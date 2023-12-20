package org.corfudb.runtime.collections;

import com.google.common.collect.Streams;
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
import org.corfudb.common.metrics.micrometer.MeterRegistryProvider;
import org.corfudb.common.metrics.micrometer.MicroMeterUtils;
import org.corfudb.runtime.CorfuOptions;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.object.ColumnFamilyRegistry;
import org.corfudb.runtime.object.ConsistencyView;
import org.corfudb.runtime.object.DiskBackedSMRSnapshot;
import org.corfudb.runtime.object.PersistenceOptions;
import org.corfudb.runtime.object.RocksDbApi;
import org.corfudb.runtime.object.RocksDbSnapshotGenerator;
import org.corfudb.runtime.object.RocksDbStore;
import org.corfudb.runtime.object.SMRSnapshot;
import org.corfudb.runtime.object.SnapshotGenerator;
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
import org.rocksdb.Statistics;
import org.rocksdb.StatsLevel;
import org.rocksdb.WriteOptions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
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
        SnapshotGenerator<DiskBackedCorfuTable<K, V>>,
        ViewGenerator<DiskBackedCorfuTable<K, V>>,
        ConsistencyView {

    public static final Options defaultOptions = getDiskBackedCorfuTableOptions();
    private static final HashFunction murmurHash3 = Hashing.murmur3_32();
    private static final long NUM_LOG_FILES = 2;
    private static final long LOG_FILE_SIZE = 1024 * 1024 * 5; // 5MB.

    private static final String DISK_BACKED = "diskBacked";
    private static final String TRUE = "true";
    private static final int BOUND = 100;
    private static final int SAMPLING_RATE = 40;

    // Optimization: We never perform crash-recovery, so we can disable
    // Write Ahead Log and disable explicit calls to sync().
    private static final WriteOptions writeOptions = new WriteOptions()
                    .setDisableWAL(true)
                    .setSync(false);

    static {
        RocksDB.loadLibrary();
    }

    @Getter
    private final Statistics statistics;
    private final RocksDbApi rocksApi;
    private final RocksDbSnapshotGenerator<DiskBackedCorfuTable<K, V>> rocksDbSnapshotGenerator;
    private final ColumnFamilyRegistry columnFamilyRegistry;
    private final ISerializer serializer;
    // Index.
    private final Map<String, String> secondaryIndexesAliasToPath;
    private final Map<String, Byte> indexToId;
    private final Set<Index.Spec<K, V, ?>> indexSpec;
    // Metrics.
    private final String metricsId;
    // Options.
    private final PersistenceOptions persistenceOptions;

    public DiskBackedCorfuTable(@NonNull PersistenceOptions persistenceOptions,
                                @NonNull Options rocksDbOptions,
                                @NonNull ISerializer serializer,
                                @Nonnull Index.Registry<K, V> indices) {

        this.persistenceOptions = persistenceOptions;
        this.secondaryIndexesAliasToPath = new HashMap<>();
        this.indexToId = new HashMap<>();
        this.indexSpec = new HashSet<>();

        byte indexId = 0;
        for (Index.Spec<K, V, ?> index : indices) {
            this.secondaryIndexesAliasToPath.put(index.getAlias().get(), index.getName().get());
            this.indexSpec.add(index);
            this.indexToId.put(index.getName().get(), indexId++);
        }

        try {
            this.statistics = new Statistics();
            this.statistics.setStatsLevel(StatsLevel.ALL);
            rocksDbOptions.setStatistics(statistics);
            persistenceOptions.getWriteBufferSize().map(rocksDbOptions::setWriteBufferSize);

            final RocksDbStore<DiskBackedCorfuTable<K, V>> rocksDbStore = new RocksDbStore<>(
                    persistenceOptions.getDataPath(), rocksDbOptions, writeOptions);

            this.rocksApi = rocksDbStore;
            this.columnFamilyRegistry = rocksDbStore;
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
                                @NonNull Options rocksDbOptions,
                                @NonNull ISerializer serializer) {
        this(persistenceOptions, rocksDbOptions, serializer, Index.Registry.empty());
    }

    public DiskBackedCorfuTable(@NonNull PersistenceOptions persistenceOptions,
                                @NonNull ISerializer serializer) {
        this(persistenceOptions, defaultOptions, serializer);
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

        // For long-running processes, limit the amount of space
        // that the log files can occupy.
        options.setKeepLogFileNum(NUM_LOG_FILES);
        options.setMaxLogFileSize(LOG_FILE_SIZE);

        BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();
        blockBasedTableConfig.setFilterPolicy(new BloomFilter(10));
        options.setTableFormatConfig(blockBasedTableConfig);

        return options;
    }

    public static int hashBytes(byte[] serializedObject, int offset, int length) {
        return murmurHash3.hashBytes(serializedObject, offset, length).asInt();
    }

    public V get(@NonNull Object key) {
        Optional<Timer.Sample> recordSample = MicroMeterUtils.startTimer(
                SAMPLING_RATE > ThreadLocalRandom.current().nextInt(BOUND));

        final ByteBuf keyPayload = Unpooled.buffer();

        try {
            // Serialize in the try-catch block to release ByteBuf when an exception occurs.
            serializer.serialize(key, keyPayload);
            byte[] value = rocksApi.get(columnFamilyRegistry.getDefaultColumnFamily(), keyPayload);
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

        final ByteBuf keyPayload = Unpooled.buffer();
        final ByteBuf valuePayload = Unpooled.buffer();

        try {
            // Serialize in the try-catch block to release ByteBuf when an exception occurs.
            serializer.serialize(key, keyPayload);
            serializer.serialize(value, valuePayload);

            if (!indexSpec.isEmpty()) {
                V previous = get(key);

                // Update secondary indexes with new mappings.
                unmapSecondaryIndexes(key, previous);
                mapSecondaryIndexes(key, value, valuePayload.readableBytes());
            }

            // Insert the primary key-value mapping into the default column family.
            rocksApi.insert(columnFamilyRegistry.getDefaultColumnFamily(), keyPayload, valuePayload);
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

            if (!indexSpec.isEmpty()) {
                V previous = get(key);
                // Remove stale secondary indexes mappings.
                unmapSecondaryIndexes(key, previous);
            }

            // Delete the primary key-value mapping from the default column family.
            rocksApi.delete(columnFamilyRegistry.getDefaultColumnFamily(), keyPayload);
            return this;
        } catch (RocksDBException ex) {
            throw new UnrecoverableCorfuError(ex);
        } finally {
            keyPayload.release();
        }
    }

    /**
     * Return a compound key consisting of: Index ID (1 byte) + Secondary Key Hash (4 bytes) +
     * Serialized Secondary Key (Arbitrary) + Serialized Primary Key (Arbitrary)
     *
     * @param indexId      a mapping (byte) that represents the specific index name/spec
     * @param secondaryKey secondary key
     * @param primaryKey   primary key
     * @return byte representation of the compound key
     */
    private ByteBuf getCompoundKey(byte indexId, Object secondaryKey, K primaryKey) {
        final ByteBuf compositeKey = Unpooled.buffer();

        // Write the index ID (1 byte).
        compositeKey.writeByte(indexId);
        final int hashStart = compositeKey.writerIndex();

        // Move the index beyond the hash (4 bytes).
        compositeKey.writerIndex(compositeKey.writerIndex() + Integer.BYTES);

        // Serialize and write the secondary key and save the offset.
        final int secondaryStart = compositeKey.writerIndex();
        serializer.serialize(secondaryKey, compositeKey);
        final int secondaryLength = compositeKey.writerIndex() - secondaryStart;

        // Serialize and write the primary key and save the offset.
        serializer.serialize(primaryKey, compositeKey);
        final int end = compositeKey.writerIndex();

        // Move the pointer to the hash offset and write the hash.
        compositeKey.writerIndex(hashStart);
        compositeKey.writeInt(hashBytes(compositeKey.array(), secondaryStart, secondaryLength));

        // Move the pointer to the end.
        compositeKey.writerIndex(end);

        return compositeKey;
    }

    private void unmapSecondaryIndexes(@NonNull K primaryKey, @Nullable V value) throws RocksDBException {
        if (Objects.isNull(value)) {
            return;
        }

        try {
            for (Index.Spec<K, V, ?> index : indexSpec) {
                Iterable<?> mappedValues = index.getMultiValueIndexFunction().apply(primaryKey, value);
                for (Object secondaryKey : mappedValues) {
                    // Protobuf 3 does not allow for optional fields, so the secondary
                    // key should never be null.
                    if (Objects.isNull(secondaryKey)) {
                        log.warn("{}: null secondary keys are not supported.", index.getName());
                        continue;
                    }

                    final ByteBuf serializedCompoundKey = getCompoundKey(
                            indexToId.get(index.getName().get()), secondaryKey, primaryKey);
                    try {
                        rocksApi.delete(columnFamilyRegistry.getSecondaryIndexColumnFamily(), serializedCompoundKey);
                    } finally {
                        serializedCompoundKey.release();
                    }
                }
            }
        } catch (Exception fatal) {
            log.error("Received an exception while computing the index. " +
                    "This is most likely an issue with the client's indexing function.", fatal);

            close(); // Do not leave the table in an inconsistent state.

            // In case of both a transactional and non-transactional operation, the client
            // is going to receive UnrecoverableCorfuError along with the appropriate cause.
            throw fatal;
        }
    }

    private void mapSecondaryIndexes(@NonNull K primaryKey, @NonNull V value, int valueSize) throws RocksDBException {
        try {
            for (Index.Spec<K, V, ?> index : indexSpec) {
                Iterable<?> mappedValues = index.getMultiValueIndexFunction().apply(primaryKey, value);
                for (Object secondaryKey : mappedValues) {
                    // Protobuf 3 does not allow for optional fields, so the secondary
                    // key should never be null.
                    if (Objects.isNull(secondaryKey)) {
                        log.warn("{}: null secondary keys are not supported.", index.getName());
                        continue;
                    }

                    final ByteBuf serializedCompoundKey = getCompoundKey(
                            indexToId.get(index.getName().get()), secondaryKey, primaryKey);

                    // We need to persist the actual value size, since multi-get API
                    // requires an allocation of a direct buffer.
                    final ByteBuf serializedIndexValue = Unpooled.buffer();
                    serializedIndexValue.writeInt(valueSize);

                    try {
                        rocksApi.insert(columnFamilyRegistry.getSecondaryIndexColumnFamily(),
                                serializedCompoundKey, serializedIndexValue);
                    } finally {
                        serializedCompoundKey.release();
                        serializedIndexValue.release();
                    }
                }
            }
        } catch (Exception fatal) {
            log.error("Received an exception while computing the index. " +
                    "This is most likely an issue with the client's indexing function.", fatal);

            close(); // Do not leave the table in an inconsistent state.

            // In case of both a transactional and non-transactional operation, the client
            // is going to receive UnrecoverableCorfuError along with the appropriate cause.
            throw new UnrecoverableCorfuError(fatal);
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
        if (persistenceOptions.getSizeComputationModel() == EXACT_SIZE) {
            return rocksApi.exactSize();
        }

        return rocksApi.estimateSize();
    }

    public <I> Iterable<Map.Entry<K, V>> getByIndex(@NonNull final Index.Name indexName,
                                                    @NonNull I indexKey) {
        final List<ByteBuffer> keys = new ArrayList<>();
        final List<ByteBuffer> values = new ArrayList<>();

        try {
            String secondaryIndex = indexName.get();
            if (!secondaryIndexesAliasToPath.containsKey(secondaryIndex)) {
                return null;
            }

            byte indexId = indexToId.get(secondaryIndexesAliasToPath.get(secondaryIndex));
            rocksApi.prefixScan(columnFamilyRegistry.getSecondaryIndexColumnFamily(),
                    indexId, indexKey, serializer, keys, values);

            // Prevent the keys from being consumed.
            final List<ByteBuffer> duplicateKeys = keys.stream().map(ByteBuffer::duplicate)
                    .collect(Collectors.toList());
            rocksApi.multiGet(columnFamilyRegistry.getDefaultColumnFamily(), duplicateKeys, values);

            return Streams.zip(keys.stream(), values.stream(), (key, value) ->
                    new AbstractMap.SimpleEntry<>(
                            (K) serializer.deserialize(Unpooled.wrappedBuffer(key), null),
                            (V) serializer.deserialize(Unpooled.wrappedBuffer(value), null)
                    )).collect(Collectors.toList());
        } catch (RocksDBException e) {
            throw new UnrecoverableCorfuError(e);
        }
    }

    @Override
    public void close() {

        // Do not call close on WriteOptions and Options, as they are
        // either statically defined or owned by the client.

        try {
            this.rocksApi.close();
        } catch (RocksDBException e) {
            throw new UnrecoverableCorfuError(e);
        } finally {
            if (isRoot()) {
                MeterRegistryProvider.unregisterExternalSupplier(metricsId);
                this.statistics.close();
            }
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
        return persistenceOptions.getConsistencyModel();
    }
}
