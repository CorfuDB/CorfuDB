package org.corfudb.runtime.object;

import io.netty.buffer.ByteBuf;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.collections.RocksDbEntryIterator;
import org.corfudb.util.serializer.ISerializer;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;

import java.nio.file.Path;

@Slf4j
public class RocksDbStore<S extends SnapshotGenerator<S>> implements RocksDbApi<S> {

    private final OptimisticTransactionDB rocksDb;
    private final WriteOptions writeOptions;
    private final String absolutePathString;
    private final Options rocksDbOptions;
    private final ConsistencyOptions consistencyOptions;

    public RocksDbStore(@NonNull Path dataPath,
                        @NonNull Options rocksDbOptions,
                        @NonNull WriteOptions writeOptions,
                        @NonNull ConsistencyOptions consistencyOptions) throws RocksDBException {
        this.absolutePathString = dataPath.toFile().getAbsolutePath();
        this.rocksDbOptions = rocksDbOptions;
        this.writeOptions = writeOptions;
        this.consistencyOptions = consistencyOptions;

        RocksDB.destroyDB(this.absolutePathString, this.rocksDbOptions);
        this.rocksDb = OptimisticTransactionDB.open(rocksDbOptions, absolutePathString);
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
    public void close() throws RocksDBException {
        rocksDb.close();
        RocksDB.destroyDB(absolutePathString, rocksDbOptions);
        log.info("Cleared RocksDB data on {}", absolutePathString);
    }

    @Override
    public ISMRSnapshot<S> getSnapshot(@NonNull ViewGenerator<S> viewGenerator, VersionedObjectIdentifier version) {
        return new DiskBackedSMRSnapshot<>(rocksDb, writeOptions, consistencyOptions, version, viewGenerator);
    }
}
