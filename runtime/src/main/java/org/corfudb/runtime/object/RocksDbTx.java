package org.corfudb.runtime.object;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import lombok.NonNull;
import org.corfudb.runtime.collections.RocksDbEntryIterator;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.util.serializer.ISerializer;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Transaction;
import org.rocksdb.WriteOptions;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * A concrete class that implements {@link RocksDbApi} using
 * {@link Transaction}.
 *
 * @param <S> extends SnapshotGenerator
 */
public class RocksDbTx<S extends SnapshotGenerator<S>> implements RocksDbApi {

    private final OptimisticTransactionDB rocksDb;
    private final DiskBackedSMRSnapshot<S> snapshot;
    private final Transaction txn;
    private final ColumnFamilyRegistry columnFamilyRegistry;

    public RocksDbTx(@NonNull OptimisticTransactionDB rocksDb,
                     @NonNull WriteOptions writeOptions,
                     @NonNull DiskBackedSMRSnapshot<S> snapshot,
                     @NonNull ColumnFamilyRegistry columnFamilyRegistry) {
        this.rocksDb = rocksDb;
        this.snapshot = snapshot;
        this.columnFamilyRegistry = columnFamilyRegistry;
        this.txn = rocksDb.beginTransaction(writeOptions);
    }

    @Override
    public byte[] get(@NonNull ColumnFamilyHandle columnFamilyHandle,
                      @NonNull ByteBuf keyPayload) {
        return snapshot.executeInSnapshot(readOptions -> {
            try {
                return txn.get(columnFamilyHandle, readOptions, ByteBufUtil.getBytes(
                        keyPayload, keyPayload.arrayOffset(), keyPayload.readableBytes(), false));
            } catch (RocksDBException e) {
                throw new UnrecoverableCorfuError(e);
            }
        });
    }

    @Override
    public void multiGet(@NonNull ColumnFamilyHandle columnFamilyHandle,
                         @NonNull List<ByteBuffer> keys,
                         @NonNull List<ByteBuffer> values) {
        snapshot.executeInSnapshot(readOptions -> {
            try {
                final List<ColumnFamilyHandle> columFamilies = keys.stream()
                        .map(ignore -> columnFamilyHandle).collect(Collectors.toList());
                final List<byte[]> keysArray = keys.stream()
                        .map(buffer -> {
                            byte[] key = new byte[buffer.remaining()];
                            buffer.get(key);
                            return key;
                        })
                        .collect(Collectors.toList());
                List<byte[]> valuesArray = txn.multiGetAsList(readOptions, columFamilies, keysArray);
                valuesArray.stream().map(ByteBuffer::wrap).forEach(values::add);
            } catch (RocksDBException e) {
                throw new UnrecoverableCorfuError(e);
            }
        });
    }

    @Override
    public void insert(@NonNull ColumnFamilyHandle columnHandle,
                       @NonNull ByteBuf keyPayload, @NonNull ByteBuf valuePayload) throws RocksDBException {
        // https://javadoc.io/static/org.rocksdb/rocksdbjni/7.9.2/org/rocksdb/Transaction.html#putUntracked-byte:A-byte:A-
        // No conflict checking since this context is always aborted.
        txn.putUntracked(columnHandle,
                ByteBufUtil.getBytes(keyPayload, keyPayload.arrayOffset(), keyPayload.readableBytes(), false),
                ByteBufUtil.getBytes(valuePayload, valuePayload.arrayOffset(), valuePayload.readableBytes(), false)
        );
    }

    @Override
    public void delete(@NonNull ColumnFamilyHandle columnHandle,
                       @NonNull ByteBuf keyPayload) throws RocksDBException {
        // https://javadoc.io/static/org.rocksdb/rocksdbjni/7.9.2/org/rocksdb/Transaction.html#deleteUntracked-byte:A-
        // No conflict checking since this context is always aborted.
        txn.deleteUntracked(columnHandle,
                ByteBufUtil.getBytes(keyPayload, keyPayload.arrayOffset(), keyPayload.readableBytes(), false)
        );
    }

    @Override
    public <K, V> RocksDbEntryIterator<K, V> getIterator(@NonNull ISerializer serializer) {
        return this.snapshot.newIterator(serializer, txn);
    }

    @Override
    public RocksIterator getRawIterator(ReadOptions readOptions, ColumnFamilyHandle columnFamilyHandle) {
        return txn.getIterator(readOptions, columnFamilyHandle);
    }

    public void prefixScan(ColumnFamilyHandle secondaryIndexesHandle, byte indexId, Object secondaryKey,
                           ISerializer serializer, List<ByteBuffer> keys, List<ByteBuffer> values) {
        snapshot.executeInSnapshot(readOptions -> {
            prefixScan(secondaryKey, secondaryIndexesHandle, indexId, serializer, readOptions, keys, values,
                    !ALLOCATE_DIRECT_BUFFERS);
        });
    }

    @Override
    public void clear() throws RocksDBException {
        snapshot.executeInSnapshot(readOptions -> {
            try {
                // Access RocksIterator directly to avoid the cost of (de)serialization.
                try (RocksIterator entryIterator = txn.getIterator(readOptions,
                        columnFamilyRegistry.getDefaultColumnFamily())) {
                    entryIterator.seekToFirst();
                    while (entryIterator.isValid()) {
                        txn.delete(columnFamilyRegistry.getDefaultColumnFamily(), entryIterator.key());
                        entryIterator.next();
                    }
                }

                // Access RocksIterator directly to avoid the cost of (de)serialization.
                try (RocksIterator entryIterator = txn.getIterator(readOptions,
                        columnFamilyRegistry.getSecondaryIndexColumnFamily())) {
                    entryIterator.seekToFirst();
                    while (entryIterator.isValid()) {
                        txn.delete(columnFamilyRegistry.getSecondaryIndexColumnFamily(), entryIterator.key());
                        entryIterator.next();
                    }
                }
            } catch (RocksDBException e) {
                throw new UnrecoverableCorfuError(e);
            }
        });
    }

    @Override
    public long exactSize() {
        return snapshot.executeInSnapshot(readOptions -> {
            long count = 0;
            // Access RocksIterator directly to avoid the cost of (de)serialization.
            try (RocksIterator entryIterator = txn.getIterator(readOptions)) {
                entryIterator.seekToFirst();
                while (entryIterator.isValid()) {
                    entryIterator.next();
                    count++;
                }
            }
            return count;
        });
    }

    @Override
    public OptimisticTransactionDB getRocksDb() {
        return this.rocksDb;
    }

    @Override
    public void close() throws RocksDBException {
        txn.rollback();
        // This function should be called manually, or even better, called implicitly using a
        // try-with-resources statement, when you are finished with the object. It is no longer
        // called automatically during the regular Java GC process via
        // {@link AbstractNativeReference#finalize()}.</p>
        txn.close();
    }
}
