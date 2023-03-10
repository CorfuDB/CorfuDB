package org.corfudb.runtime.object;

import io.netty.buffer.ByteBuf;
import lombok.NonNull;
import org.corfudb.runtime.collections.RocksDbEntryIterator;
import org.corfudb.util.serializer.ISerializer;
import org.rocksdb.RocksDBException;

public interface RocksDbApi<T extends SnapshotGenerator<T>> {

    byte[] get(@NonNull ByteBuf keyPayload) throws RocksDBException;

    void insert(@NonNull ByteBuf keyPayload, @NonNull ByteBuf valuePayload) throws RocksDBException;

    void delete(@NonNull ByteBuf keyPayload) throws RocksDBException;

    <K, V> RocksDbEntryIterator<K, V> getIterator(@NonNull ISerializer serializer);

    ISMRSnapshot<T> getSnapshot(@NonNull ViewGenerator<T> viewGenerator, VersionedObjectIdentifier version);

    void close() throws RocksDBException;
}
