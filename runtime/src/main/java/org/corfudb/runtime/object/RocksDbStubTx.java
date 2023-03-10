package org.corfudb.runtime.object;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import lombok.NonNull;
import org.corfudb.runtime.collections.RocksDbEntryIterator;
import org.corfudb.util.serializer.ISerializer;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;

public class RocksDbStubTx<T extends SnapshotGenerator<T>> implements RocksDbApi<T> {
    private final OptimisticTransactionDB rocksDb;
    private final DiskBackedSMRSnapshot snapshot;
    private final ReadOptions readOptions;

    public RocksDbStubTx(@NonNull OptimisticTransactionDB rocksDb,
                         @NonNull DiskBackedSMRSnapshot snapshot) {
        this.rocksDb = rocksDb;
        this.snapshot = snapshot;
        this.readOptions = new ReadOptions();
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
        return new RocksDbEntryIterator<>(rocksDb, serializer, readOptions, true);
    }

    @Override
    public void close() throws RocksDBException {
        // TODO(Zach): How to make sure readOptions are not leaked if thread dies?
        readOptions.close();
    }

    @Override
    public ISMRSnapshot<T> getSnapshot(@NonNull ViewGenerator<T> viewGenerator, VersionedObjectIdentifier version) {
        throw new UnsupportedOperationException();
    }
}
