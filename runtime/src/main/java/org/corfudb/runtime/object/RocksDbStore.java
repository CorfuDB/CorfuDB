package org.corfudb.runtime.object;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MeterRegistryProvider;
import org.corfudb.runtime.collections.RocksDbEntryIterator;
import org.corfudb.util.serializer.ISerializer;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.Filter;
import org.rocksdb.IndexType;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Statistics;
import org.rocksdb.StatsLevel;
import org.rocksdb.WriteOptions;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.StampedLock;
import java.util.stream.Collectors;

/**
 * A concrete class that implements {@link RocksDbApi} using
 * {@link OptimisticTransactionDB}.
 *
 * @param <S> extends SnapshotGenerator
 */
@Slf4j
public class RocksDbStore<S extends SnapshotGenerator<S>> implements
        RocksDbApi,
        RocksDbSnapshotGenerator<S>,
        ColumnFamilyRegistry,
        AutoCloseable {

    private final OptimisticTransactionDB rocksDb;
    private final WriteOptions writeOptions;
    private final Options rocksDbOptions;
    @Getter
    private final Statistics statistics;

    @Getter
    private final ColumnFamilyHandle defaultColumnFamily;

    private final Optional<ColumnFamilyHandle> secondaryIndexColumnFamily;

    // Options.
    @Getter
    private final PersistenceOptions persistenceOptions;

    // Metrics.
    private final String metricsId;

    public RocksDbStore(@NonNull Options rocksDbOptions, @NonNull WriteOptions writeOptions,
            @NonNull PersistenceOptions persistenceOptions) throws RocksDBException {
        this.rocksDbOptions = rocksDbOptions;
        this.writeOptions = writeOptions;

        this.statistics = new Statistics();
        this.statistics.setStatsLevel(StatsLevel.ALL);
        rocksDbOptions.setStatistics(statistics);

        this.persistenceOptions = persistenceOptions;
        persistenceOptions.getWriteBufferSize().map(rocksDbOptions::setWriteBufferSize);

        String absolutePathString = persistenceOptions.getAbsolutePathString();
        if (persistenceOptions.getStoreMode() == StoreMode.TEMPORARY) {
            // Open the RocksDB instance
            RocksDB.destroyDB(absolutePathString, this.rocksDbOptions);
        }
        this.rocksDb = OptimisticTransactionDB.open(rocksDbOptions, absolutePathString);
        this.defaultColumnFamily = this.rocksDb.getDefaultColumnFamily();

        // There is no need to override default options and customize
        // the behavior of individual column families.
        try (ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions()) {
            // The prefix is composed of Index ID (1 byte) and
            // the secondary key hash (4 bytes).
            columnFamilyOptions.useCappedPrefixExtractor(Byte.BYTES + Integer.BYTES);

            // Define prefix bloom filters, which can reduce read
            // amplification of prefix range queries.
            Filter bloomFilter = new BloomFilter(10);
            BlockBasedTableConfig tableConfig = new BlockBasedTableConfig();
            tableConfig.setFilterPolicy(bloomFilter);

            // The hash index, if enabled, will do the hash lookup when
            // prefix extractor is provided.
            tableConfig.setIndexType(IndexType.kHashSearch);
            columnFamilyOptions.setTableFormatConfig(tableConfig);


            // Use hash-map-based memtables to avoid binary search costs in memtables.
            // BUG: Memtable doesn't concurrent writes (allow_concurrent_memtable_write)
            // MemTableConfig memTableConfig = new HashLinkedListMemTableConfig();
            // columnFamilyOptions.setMemTableConfig(memTableConfig);

            if (persistenceOptions.getIndexMode() == IndexMode.INDEX) {
                ColumnFamilyDescriptor descriptor = new ColumnFamilyDescriptor(
                        "secondary-indexes".getBytes(),
                        columnFamilyOptions
                );
                this.secondaryIndexColumnFamily = Optional.of(this.rocksDb.createColumnFamily(descriptor));
            } else {
                this.secondaryIndexColumnFamily = Optional.empty();
            }
        }

        this.metricsId = String.format("%s.%s.",
                persistenceOptions.getDataPath().getFileName(), System.identityHashCode(this));
        MeterRegistryProvider.registerExternalSupplier(metricsId, this.statistics::toString);
    }

    @Override
    public byte[] get(@NonNull ColumnFamilyHandle columnFamilyHandle,
                      @NonNull ByteBuf keyPayload) throws RocksDBException {
        return rocksDb.get(
                columnFamilyHandle,
                keyPayload.array(), keyPayload.arrayOffset(), keyPayload.readableBytes());
    }

    @Override
    public void multiGet(
            @NonNull ColumnFamilyHandle columnFamilyHandle,
            @NonNull List<ByteBuffer> keys,
            @NonNull List<ByteBuffer> values) throws RocksDBException {
        final List<ColumnFamilyHandle> columFamilies = keys.stream()
                .map(ignore -> columnFamilyHandle).collect(Collectors.toList());
        rocksDb.multiGetByteBuffers(columFamilies, keys, values);
    }

    @Override
    public void insert(@NonNull ColumnFamilyHandle columnFamilyHandle,
                       @NonNull ByteBuf keyPayload, @NonNull ByteBuf valuePayload) throws RocksDBException {
        rocksDb.put(
                columnFamilyHandle,
                writeOptions,
                keyPayload.array(), keyPayload.arrayOffset(), keyPayload.readableBytes(),
                valuePayload.array(), valuePayload.arrayOffset(), valuePayload.readableBytes()
        );
    }

    @Override
    public void delete(@NonNull ColumnFamilyHandle columnFamilyHandle,
                       @NonNull ByteBuf keyPayload) throws RocksDBException {
        rocksDb.delete(columnFamilyHandle, writeOptions, keyPayload.array(), keyPayload.arrayOffset(), keyPayload.readableBytes());
    }

    @Override
    public <K, V> RocksDbEntryIterator<K, V> getIterator(@NonNull ISerializer serializer) {
        return new RocksDbEntryIterator<>(rocksDb.newIterator(), serializer, new ReadOptions(), new StampedLock(), true);
    }

    @Override
    public RocksIterator getRawIterator(ReadOptions readOptions, ColumnFamilyHandle columnFamilyHandle) {
        return rocksDb.newIterator(columnFamilyHandle, readOptions);
    }

    @Override
    public void prefixScan(
            ColumnFamilyHandle secondaryIndexesHandle, byte indexId,
            Object secondaryKey, ISerializer serializer,
            List<ByteBuffer> keys,
            List<ByteBuffer> values) {
        final ReadOptions readOptions = new ReadOptions();
        prefixScan(secondaryKey, secondaryIndexesHandle, indexId, serializer,
                readOptions, keys, values, ALLOCATE_DIRECT_BUFFERS);
        readOptions.close();
    }

    @Override
    public void clear() throws RocksDBException {
        try (RocksIterator entryIterator = this.rocksDb.newIterator(defaultColumnFamily)) {
            entryIterator.seekToFirst();
            while (entryIterator.isValid()) {
                rocksDb.delete(defaultColumnFamily, entryIterator.key());
                entryIterator.next();
            }
        }

        if (secondaryIndexColumnFamily.isPresent()) {
            ColumnFamilyHandle cf = secondaryIndexColumnFamily.get();
            try (RocksIterator entryIterator = this.rocksDb.newIterator(cf)) {
                entryIterator.seekToFirst();
                while (entryIterator.isValid()) {
                    rocksDb.delete(cf, entryIterator.key());
                    entryIterator.next();
                }
            }
        }
    }

    @Override
    public long exactSize() {
        long count = 0;

        try (RocksIterator entryIterator = this.rocksDb.newIterator()) {
            entryIterator.seekToFirst();
            while (entryIterator.isValid()) {
                entryIterator.next();
                count++;
            }
        }

        return count;
    }

    @Override
    public OptimisticTransactionDB getRocksDb() {
        return this.rocksDb;
    }

    @Override
    public void close() throws RocksDBException {
        MeterRegistryProvider.unregisterExternalSupplier(metricsId);
        this.statistics.close();

        rocksDb.close();

        if (persistenceOptions.getStoreMode() == StoreMode.TEMPORARY) {
            String absolutePathString = persistenceOptions.getAbsolutePathString();
            RocksDB.destroyDB(absolutePathString, rocksDbOptions);
            log.info("Cleared RocksDB data on {}", absolutePathString);
        }
    }

    /**
     * Generate a new snapshot based on the current state of
     * {@link OptimisticTransactionDB} instance.
     *
     * @param viewGenerator an instance that will be responsible for
     *                      generating new views based on this snapshot
     * @param version       a version that will be tied to this snapshot
     * @return a new snapshot
     */
    @Override
    public SMRSnapshot<S> getSnapshot(@NonNull ViewGenerator<S> viewGenerator,
                                      @NonNull VersionedObjectIdentifier version) {
        return new DiskBackedSMRSnapshot<>(rocksDb, writeOptions, version, viewGenerator, this);
    }

    /**
     * Generate a new snapshot that will follow read-committed
     * view of the {@link OptimisticTransactionDB} instance.
     *
     * @param viewGenerator an instance that will be responsible for
     *                      generating new views based on this snapshot
     * @return a new snapshot
     */
    @Override
    public SMRSnapshot<S> getImplicitSnapshot(
            @NonNull ViewGenerator<S> viewGenerator) {
        return new AlwaysLatestSnapshot<>(rocksDb, viewGenerator);
    }

    @Override
    public ColumnFamilyHandle getSecondaryIndexColumnFamily() {
        return secondaryIndexColumnFamily
                .orElseThrow(() -> new IllegalStateException("Secondary index disabled"));
    }

    public enum StoreMode {
        TEMPORARY, PERSISTENT
    }

    public enum IndexMode {
        INDEX, NON_INDEX
    }
}
