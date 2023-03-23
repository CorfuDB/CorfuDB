package org.corfudb.runtime.object;

import io.netty.buffer.ByteBuf;
import lombok.NonNull;
import org.corfudb.runtime.collections.RocksDbEntryIterator;
import org.corfudb.util.serializer.ISerializer;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Transaction;

/**
 * Interface that is required to unify {@link RocksDB} and
 * {@link Transaction} interfaces.
 *
 * @param <S>
 */
public interface RocksDbApi<S extends SnapshotGenerator<S>> {

    byte[] get(@NonNull ByteBuf keyPayload) throws RocksDBException;

    void insert(@NonNull ByteBuf keyPayload, @NonNull ByteBuf valuePayload) throws RocksDBException;

    void delete(@NonNull ByteBuf keyPayload) throws RocksDBException;

    <K, V> RocksDbEntryIterator<K, V> getIterator(@NonNull ISerializer serializer);

    void clear() throws RocksDBException;

    long exactSize();

    SMRSnapshot<S> getSnapshot(@NonNull ViewGenerator<S> viewGenerator,
                               @NonNull VersionedObjectIdentifier version);

    void close() throws RocksDBException;
}
