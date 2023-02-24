package org.corfudb.runtime.object;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import lombok.NonNull;
import org.corfudb.runtime.collections.RocksDbEntryIterator;
import org.corfudb.util.serializer.ISerializer;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.Snapshot;

import java.util.function.Function;

public class RocksStubTx<T extends ICorfuSMR<T>> implements RocksTableApi<T> {
    private final OptimisticTransactionDB rocksDb;
    private final Snapshot snapshot;
    private final ReadOptions readOptions;

    public RocksStubTx(@NonNull OptimisticTransactionDB rocksDb,
                       @NonNull Snapshot snapshot) {
        this.rocksDb = rocksDb;
        this.snapshot = snapshot;
        this.readOptions = new ReadOptions().setSnapshot(snapshot);
    }


    @Override
    public byte[] get(@NonNull ByteBuf keyPayload) throws RocksDBException {
        return this.rocksDb.get(readOptions, ByteBufUtil.getBytes(
                keyPayload, keyPayload.arrayOffset(), keyPayload.readableBytes(), false));
    }

    @Override
    public void insert(@NonNull ByteBuf keyPayload, @NonNull ByteBuf valuePayload) throws RocksDBException {
        // No-op.
    }

    @Override
    public void delete(@NonNull ByteBuf keyPayload) throws RocksDBException {
        // No-op
    }

    @Override
    public <K, V> RocksDbEntryIterator<K,V> getIterator(@NonNull ISerializer serializer) {
        return new RocksDbEntryIterator<>(rocksDb, serializer, snapshot);
    }

    @Override
    public void close() throws RocksDBException {
        // TODO(Zach): Anything else here?
        readOptions.close();
    }
}
