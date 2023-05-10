package org.corfudb.runtime.object;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import lombok.NonNull;
import org.corfudb.runtime.collections.RocksDbEntryIterator;
import org.corfudb.util.serializer.ISerializer;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Transaction;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.StampedLock;
import java.util.stream.Collectors;

/**
 * A concrete class that implements {@link RocksDbApi} using
 * {@link Transaction}. Unlike its cousin {@link RocksDbTx},
 *   1) All mutators are no-ops.
 *   2) All accessors operate directly on {@link OptimisticTransactionDB}.
 * This will effectively provide read-committed consistency.
 *
 * @param <S> extends SnapshotGenerator
 */
public class RocksDbReadCommittedTx<S extends SnapshotGenerator<S>> implements RocksDbApi<S> {
    private final OptimisticTransactionDB rocksDb;
    private final ReadOptions readOptions;

    public RocksDbReadCommittedTx(@NonNull OptimisticTransactionDB rocksDb) {
        this.rocksDb = rocksDb;
        this.readOptions = new ReadOptions();
    }
    
    @Override
    public byte[] get(@NonNull ColumnFamilyHandle columnFamilyHandle,
                      @NonNull ByteBuf keyPayload) throws RocksDBException {
        return rocksDb.get(columnFamilyHandle, readOptions, ByteBufUtil.getBytes(
                keyPayload, keyPayload.arrayOffset(), keyPayload.readableBytes(), false));
    }

    @Override
    public List<byte[]> multiGet(@NonNull ColumnFamilyHandle columnFamilyHandle,
                                 @NonNull List<byte[]> arrayKeys) throws RocksDBException {
        final List<ColumnFamilyHandle> columFamilies = arrayKeys.stream()
                .map(ignore -> columnFamilyHandle).collect(Collectors.toList());

        return rocksDb.multiGetAsList(columFamilies, arrayKeys);
    }

    @Override
    public void insert(@NonNull ColumnFamilyHandle columnFamilyHandle,
                       @NonNull ByteBuf keyPayload, @NonNull ByteBuf valuePayload) throws RocksDBException {
        // No-op.
    }

    @Override
    public void delete(@NonNull ColumnFamilyHandle columnFamilyHandle,
                       @NonNull ByteBuf keyPayload) throws RocksDBException {
        // No-op
    }

    @Override
    public void clear() {
        // No-op
    }

    public long exactSize() {
        long count = 0;
        try (RocksIterator entryIterator = rocksDb.newIterator()) {
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
    }

    @Override
    public <K, V> RocksDbEntryIterator<K,V> getIterator(@NonNull ISerializer serializer) {
        return new RocksDbEntryIterator<>(rocksDb.newIterator(), serializer, readOptions, new StampedLock());
    }

    @Override
    public RocksIterator getRawIterator(ReadOptions readOptions, ColumnFamilyHandle columnFamilyHandle) {
        return rocksDb.newIterator(columnFamilyHandle, readOptions);
    }

    @Override
    public Set<ByteBuf> prefixScan(ColumnFamilyHandle secondaryIndexesHandle, byte indexId, Object secondaryKey, ISerializer serializer) {
        return prefixScan(secondaryKey, secondaryIndexesHandle, indexId, serializer, new ReadOptions());
    }
}
