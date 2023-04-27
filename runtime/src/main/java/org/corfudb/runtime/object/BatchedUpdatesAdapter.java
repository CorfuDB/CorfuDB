package org.corfudb.runtime.object;

import io.netty.buffer.ByteBuf;
import lombok.NonNull;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

public interface BatchedUpdatesAdapter extends AutoCloseable {

    void insert(@NonNull ColumnFamilyHandle cfh,
                @NonNull ByteBuf keyPayload,
                @NonNull ByteBuf valuePayload) throws RocksDBException;

    void delete(@NonNull ColumnFamilyHandle cfh,
                @NonNull ByteBuf keyPayload) throws RocksDBException;

    void process() throws RocksDBException;

    @Override
    void close();

}
