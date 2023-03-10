package org.corfudb.runtime.object;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import lombok.NonNull;
import org.corfudb.runtime.collections.RocksDbEntryIterator;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.util.serializer.ISerializer;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Transaction;
import org.rocksdb.WriteOptions;

public class RocksDbTx<T extends SnapshotGenerator<T>> implements RocksDbApi<T> {
    private final OptimisticTransactionDB rocksDb;
    private final DiskBackedSMRSnapshot<T> snapshot;
    private final Transaction txn;

    public RocksDbTx(@NonNull OptimisticTransactionDB rocksDb,
                     @NonNull WriteOptions writeOptions,
                     @NonNull DiskBackedSMRSnapshot<T> snapshot) {
        this.rocksDb = rocksDb;
        this.snapshot = snapshot;
        this.txn = rocksDb.beginTransaction(writeOptions);
    }


    @Override
    public byte[] get(@NonNull ByteBuf keyPayload) throws RocksDBException {
        return snapshot.executeInSnapshot(readOptions -> {
            try {
                return txn.get(readOptions, ByteBufUtil.getBytes(
                        keyPayload, keyPayload.arrayOffset(), keyPayload.readableBytes(), false));
            } catch (RocksDBException e) {
                throw new UnrecoverableCorfuError(e);
            }
        });
    }

    @Override
    public void insert(@NonNull ByteBuf keyPayload, @NonNull ByteBuf valuePayload) throws RocksDBException {
        // https://javadoc.io/static/org.rocksdb/rocksdbjni/7.9.2/org/rocksdb/Transaction.html#putUntracked-byte:A-byte:A-
        // No conflict checking since this context is always aborted.
        txn.putUntracked(
                ByteBufUtil.getBytes(keyPayload, keyPayload.arrayOffset(), keyPayload.readableBytes(), false),
                ByteBufUtil.getBytes(valuePayload, valuePayload.arrayOffset(), valuePayload.readableBytes(), false)
        );
    }

    @Override
    public void delete(@NonNull ByteBuf keyPayload) throws RocksDBException {
        // https://javadoc.io/static/org.rocksdb/rocksdbjni/7.9.2/org/rocksdb/Transaction.html#deleteUntracked-byte:A-
        // No conflict checking since this context is always aborted.
        txn.deleteUntracked(
                ByteBufUtil.getBytes(keyPayload, keyPayload.arrayOffset(), keyPayload.readableBytes(), false)
        );
    }

    @Override
    public <K, V> RocksDbEntryIterator<K,V> getIterator(@NonNull ISerializer serializer) {
        return this.snapshot.newIterator(serializer);
    }

    @Override
    public void close() throws RocksDBException {
        // TODO(Zach): How to make sure readOptions are not leaked if thread dies?
        txn.rollback();
    }

    @Override
    public ISMRSnapshot<T> getSnapshot(@NonNull ViewGenerator<T> viewGenerator, VersionedObjectIdentifier version) {
        throw new UnsupportedOperationException();
    }
}
