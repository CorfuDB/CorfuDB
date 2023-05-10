package org.corfudb.runtime.object;

import com.google.common.primitives.Bytes;
import com.google.common.primitives.Ints;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import lombok.NonNull;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.ArrayUtils;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.corfudb.util.Utils.startsWith;

/**
 * A concrete class that implements {@link RocksDbApi} using
 * {@link Transaction}.
 *
 * @param <S> extends SnapshotGenerator
 */
public class RocksDbTx<S extends SnapshotGenerator<S>> implements RocksDbApi<S> {

    private final DiskBackedSMRSnapshot<S> snapshot;
    private final Transaction txn;
    private final ColumnFamilyRegistry columnFamilyRegistry;

    public RocksDbTx(@NonNull OptimisticTransactionDB rocksDb,
                     @NonNull WriteOptions writeOptions,
                     @NonNull DiskBackedSMRSnapshot<S> snapshot,
                     @NonNull ColumnFamilyRegistry columnFamilyRegistry) {
        this.snapshot = snapshot;
        this.columnFamilyRegistry = columnFamilyRegistry;
        this.txn = rocksDb.beginTransaction(writeOptions);
    }

    @Override
    public byte[] get(@NonNull ColumnFamilyHandle columnFamilyHandle,
                      @NonNull ByteBuf keyPayload) throws RocksDBException {
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
    public List<byte[]> multiGet(@NonNull ColumnFamilyHandle columnFamilyHandle,
                                 @NonNull List<byte[]> arrayKeys) {
        return snapshot.executeInSnapshot(readOptions -> {
            try {
                final List<ColumnFamilyHandle> columFamilies = arrayKeys.stream()
                        .map(ignore -> columnFamilyHandle).collect(Collectors.toList());
                return txn.multiGetAsList(readOptions, columFamilies, arrayKeys);
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

    public Set<ByteBuf> prefixScan(ColumnFamilyHandle secondaryIndexesHandle,
                                 byte indexId, Object secondaryKey, ISerializer serializer) {
        return snapshot.executeInSnapshot(readOptions -> {
            return prefixScan(secondaryKey, secondaryIndexesHandle, indexId, serializer, readOptions);
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
    public void close() throws RocksDBException {
        txn.rollback();
    }
}
