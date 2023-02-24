package org.corfudb.runtime.object;

import io.netty.buffer.ByteBuf;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.collections.RocksDbEntryIterator;
import org.corfudb.util.serializer.ISerializer;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;

import java.util.function.Function;

@Slf4j
@AllArgsConstructor
public class RocksSource<T extends ICorfuSMR<T> & ViewGenerator<T>> implements RocksTableApi<T> {

    private final OptimisticTransactionDB rocksDb;
    private final WriteOptions writeOptions;

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
    public <K, V> RocksDbEntryIterator<K,V> getIterator(@NonNull ISerializer serializer) {
        return new RocksDbEntryIterator<>(rocksDb, serializer);
    }


    @Override
    public void close() throws RocksDBException {
    }

    public ISMRSnapshot<T> getSnapshot(@NonNull T instance) {
        return new DiskBackedSMRSnapshot<>(rocksDb, instance);
    }

    public RocksDB getRocksDb() {
        return this.rocksDb;
    }
}
