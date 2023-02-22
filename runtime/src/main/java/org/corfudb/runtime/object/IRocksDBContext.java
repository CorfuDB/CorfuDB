package org.corfudb.runtime.object;

import io.netty.buffer.ByteBuf;
import lombok.NonNull;
import org.corfudb.runtime.collections.RocksDbEntryIterator;
import org.corfudb.util.serializer.ISerializer;
import org.rocksdb.RocksDBException;

import java.util.function.Function;

public interface IRocksDBContext<T extends ICorfuSMR<T>> {

    byte[] get(@NonNull ByteBuf keyPayload) throws RocksDBException;

    void insert(@NonNull ByteBuf keyPayload, @NonNull ByteBuf valuePayload) throws RocksDBException;

    void delete(@NonNull ByteBuf keyPayload) throws RocksDBException;

    <K, V> RocksDbEntryIterator<K,V> getIterator(@NonNull ISerializer serializer);

    void close() throws RocksDBException;

    ISMRSnapshot<T> getSnapshot(@NonNull Function<IRocksDBContext<T>, T> instanceProducer);
}
