package org.corfudb.runtime.object;

import com.google.common.collect.ImmutableMap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.collections.RocksDbEntryIterator;
import org.corfudb.util.serializer.ISerializer;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.nio.file.Path;
import java.util.Map;

/**
 * A concrete class that implements {@link RocksDbApi} using
 * {@link OptimisticTransactionDB}.
 *
 * @param <S> extends SnapshotGenerator
 */
@Slf4j
public class RocksDbStore<S extends SnapshotGenerator<S>>
        implements RocksDbApi<S>, RocksDbSnapshotGenerator<S> {

    private final OptimisticTransactionDB rocksDb;
    private final String absolutePathString;
    private final WriteOptions writeOptions;
    private final Options rocksDbOptions;
    private final PersistenceOptions persistenceOptions;
    private final RocksDbColumnFamilyRegistry cfRegistry;

    public RocksDbStore(@NonNull Path dataPath,
                        @NonNull Options rocksDbOptions,
                        @NonNull WriteOptions writeOptions,
                        @NonNull PersistenceOptions persistenceOptions,
                        @NonNull Map<String, ColumnFamilyDescriptor> columnFamilyDescriptors) throws RocksDBException {
        this.absolutePathString = dataPath.toFile().getAbsolutePath();
        this.rocksDbOptions = rocksDbOptions;
        this.writeOptions = writeOptions;
        this.persistenceOptions = persistenceOptions;

        // Open the RocksDB instance
        RocksDB.destroyDB(this.absolutePathString, this.rocksDbOptions);
        this.rocksDb = OptimisticTransactionDB.open(rocksDbOptions, absolutePathString);

        // Create and register column families.
        final ImmutableMap.Builder<String, ColumnFamilyHandle> columnFamilyMapBuilder = ImmutableMap.builder();
        for (Map.Entry<String, ColumnFamilyDescriptor> entry : columnFamilyDescriptors.entrySet()) {
            columnFamilyMapBuilder.put(entry.getKey(), this.rocksDb.createColumnFamily(entry.getValue()));
        }

        this.cfRegistry = new RocksDbColumnFamilyRegistry(
                this.rocksDb.getDefaultColumnFamily(),
                columnFamilyMapBuilder.build()
        );
    }

    @Override
    public byte[] get(@NonNull ByteBuf keyPayload) throws RocksDBException {
        return rocksDb.get(keyPayload.array(), keyPayload.arrayOffset(), keyPayload.readableBytes());
    }

    @Override
    public void insert(@NonNull ByteBuf keyPayload, @NonNull ByteBuf valuePayload) throws RocksDBException {
        rocksDb.put(
                writeOptions,
                keyPayload.array(), keyPayload.arrayOffset(), keyPayload.readableBytes(),
                valuePayload.array(), valuePayload.arrayOffset(), valuePayload.readableBytes()
        );
    }

    @Override
    public void delete(@NonNull ByteBuf keyPayload) throws RocksDBException {
        rocksDb.delete(writeOptions, keyPayload.array(), keyPayload.arrayOffset(), keyPayload.readableBytes());
    }

    @Override
    public <K, V> RocksDbEntryIterator<K, V> getIterator(@NonNull ISerializer serializer) {
        return new RocksDbEntryIterator<>(rocksDb, serializer);
    }

    @Override
    public void clear() throws RocksDBException {
        try (RocksIterator entryIterator = this.rocksDb.newIterator()) {
            entryIterator.seekToFirst();
            while (entryIterator.isValid()) {
                rocksDb.delete(entryIterator.key());
                entryIterator.next();
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
    public void close() throws RocksDBException {
        cfRegistry.close();
        rocksDb.close();
        RocksDB.destroyDB(absolutePathString, rocksDbOptions);
        log.info("Cleared RocksDB data on {}", absolutePathString);
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
        return new DiskBackedSMRSnapshot<>(rocksDb, writeOptions,
                persistenceOptions.consistencyModel, version, viewGenerator, cfRegistry);
    }

    /**
     * Return the registry of column families associated with
     * this RocksDbStore instance.
     * @return The associated registry of column families.
     */
    @Override
    public RocksDbColumnFamilyRegistry getRegisteredColumnFamilies() {
        return cfRegistry;
    }

    @Override
    public SMRSnapshot<S> getImplicitSnapshot(
            @NonNull ViewGenerator<S> viewGenerator,
            @NonNull VersionedObjectIdentifier version) {
        return new AlwaysLatestSnapshot<>(rocksDb, viewGenerator, cfRegistry);
    }

    /**
     *
     * @return
     */
    @Override
    public BatchedUpdatesAdapter getBatchedUpdatesAdapter() {
        return new WriteBatchAdapter(rocksDb, writeOptions);
    }

    private static class WriteBatchAdapter implements BatchedUpdatesAdapter {
        private final OptimisticTransactionDB rocksDb;
        private final WriteOptions writeOptions;
        private final WriteBatch writeBatch;

        private boolean isProcessed;

        public WriteBatchAdapter(@NonNull OptimisticTransactionDB rocksDb,
                                 @NonNull WriteOptions writeOptions) {
            this.rocksDb = rocksDb;
            this.writeOptions = writeOptions;
            this.writeBatch = new WriteBatch();
            this.isProcessed = false;
        }

        @Override
        public void insert(@NonNull ColumnFamilyHandle cfh,
                           @NonNull ByteBuf keyPayload,
                           @NonNull ByteBuf valuePayload) throws RocksDBException {

            if (isProcessed) {
                throw new IllegalStateException();
            }

            writeBatch.put(cfh,
                    ByteBufUtil.getBytes(keyPayload, keyPayload.arrayOffset(), keyPayload.readableBytes(), false),
                    ByteBufUtil.getBytes(valuePayload, valuePayload.arrayOffset(), valuePayload.readableBytes(), false)
            );
        }

        @Override
        public void delete(@NonNull ColumnFamilyHandle cfh,
                           @NonNull ByteBuf keyPayload) throws RocksDBException {

            if (isProcessed) {
                throw new IllegalStateException();
            }

            writeBatch.delete(cfh,
                    ByteBufUtil.getBytes(keyPayload, keyPayload.arrayOffset(), keyPayload.readableBytes(), false)
            );
        }

        @Override
        public void process() throws RocksDBException {
            isProcessed = true;
            rocksDb.write(writeOptions, writeBatch);
        }

        @Override
        public void close() {
            writeBatch.close();
        }

    }

}
