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

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.StampedLock;

import static org.corfudb.runtime.collections.RocksDbEntryIterator.LOAD_VALUES;

/**
 * A concrete class that implements {@link RocksDbApi} using
 * {@link Transaction}. Unlike its cousin {@link RocksDbTx},
 *   1) All mutators are no-ops.
 *   2) All accessors operate directly on {@link OptimisticTransactionDB}.
 * This will effectively provide read-committed consistency.
 */
public class RocksDbReadCommittedTx implements RocksDbApi {
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
    public void multiGet(
            @NonNull ColumnFamilyHandle columnFamilyHandle,
            @NonNull List<ByteBuffer> keys,
            @NonNull List<ByteBuffer> values) throws RocksDBException {
        final List<ColumnFamilyHandle> columFamilies = Collections.emptyList();
        rocksDb.multiGetByteBuffers(columFamilies, keys, values);
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

    @Override
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
    public OptimisticTransactionDB getRocksDb() {
        return this.rocksDb;
    }

    @Override
    public void close() {
        // This function should be called manually, or even better, called implicitly using a
        // try-with-resources statement, when you are finished with the object. It is no longer
        // called automatically during the regular Java GC process via
        // {@link AbstractNativeReference#finalize()}.</p>
        readOptions.close();
    }

    @Override
    public <K, V> RocksDbEntryIterator<K,V> getIterator(@NonNull ISerializer serializer) {
        return new RocksDbEntryIterator<>(rocksDb.newIterator(), serializer,
                readOptions, new StampedLock(), LOAD_VALUES);
    }

    @Override
    public RocksIterator getRawIterator(ReadOptions readOptions, ColumnFamilyHandle columnFamilyHandle) {
        return rocksDb.newIterator(columnFamilyHandle, readOptions);
    }

    @Override
    public void prefixScan(ColumnFamilyHandle secondaryIndexesHandle, byte indexId, Object secondaryKey, ISerializer serializer,
                           List<ByteBuffer> keys, List<ByteBuffer> values) {
        prefixScan(secondaryKey, secondaryIndexesHandle, indexId, serializer,
                readOptions, keys, values, ALLOCATE_DIRECT_BUFFERS);
    }
}
