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
import org.rocksdb.Transaction;
import org.rocksdb.WriteOptions;

import java.util.function.Function;

public class RocksTx<T extends ICorfuSMR<T>> implements RocksTableApi<T> {

    private final OptimisticTransactionDB rocksDb;
    private final WriteOptions writeOptions;
    private final Snapshot snapshot;
    private final Transaction txn;
    private final ReadOptions readOptions;

    public RocksTx(@NonNull OptimisticTransactionDB rocksDb,
                   @NonNull WriteOptions writeOptions,
                   @NonNull Snapshot snapshot) {
        this.rocksDb = rocksDb;
        this.writeOptions = writeOptions;
        this.snapshot = snapshot;
        this.readOptions = new ReadOptions().setSnapshot(snapshot);
        this.txn = rocksDb.beginTransaction(writeOptions);
    }


    @Override
    public byte[] get(@NonNull ByteBuf keyPayload) throws RocksDBException {
        return txn.get(readOptions, ByteBufUtil.getBytes(
                keyPayload, keyPayload.arrayOffset(), keyPayload.readableBytes(), false));
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
        return new RocksDbEntryIterator<>(rocksDb, serializer, snapshot);
    }

    @Override
    public void close() throws RocksDBException {
        // TODO(Zach): Anything else here?
        txn.rollback();
        readOptions.close();
    }
}
