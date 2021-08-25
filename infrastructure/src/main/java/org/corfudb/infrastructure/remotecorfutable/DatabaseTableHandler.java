package org.corfudb.infrastructure.remotecorfutable;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.Longs;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import static org.corfudb.infrastructure.remotecorfutable.DatabaseHandler.CAUSE_OF_ERROR;
import org.rocksdb.AbstractImmutableNativeReference;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.ComparatorOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class provides utility for adding and removing tables from the database, as well as
 * providing a consistent view of column families present in the database.
 *
 * Created by nvaishampayan517 on 08/25/21
 */
@Slf4j
@AllArgsConstructor
public class DatabaseTableHandler {
    //Loads the C++ library backing RocksDB
    static {
        RocksDB.loadLibrary();
    }

    @NonNull
    private final RocksDB database;
    @NonNull
    private final ConcurrentHashMap<UUID, ReferenceCountingHandle> columnFamilies;

    @Data
    private static class ReferenceCountingHandle {
        private final ColumnFamilyHandle handle;
        private final AtomicInteger referenceCount;
    }
    public boolean addTable(@NonNull UUID streamId) {
        ReferenceCountingHandle result = columnFamilies.computeIfAbsent(streamId, id -> {
            ColumnFamilyOptions tableOptions = new ColumnFamilyOptions();
            tableOptions.optimizeUniversalStyleCompaction();
            ComparatorOptions comparatorOptions = new ComparatorOptions();
            ReversedVersionedKeyComparator comparator = new ReversedVersionedKeyComparator(comparatorOptions);
            tableOptions.setComparator(comparator);
            ColumnFamilyDescriptor tableDescriptor = new ColumnFamilyDescriptor(getBytes(id), tableOptions);
            ColumnFamilyHandle tableHandle;
            try {
                tableHandle = database.createColumnFamily(tableDescriptor);
            } catch (RocksDBException e) {
                log.error("Error in creating column family for table {}.", id);
                log.error(CAUSE_OF_ERROR, e);
                tableOptions.close();
                comparatorOptions.close();
                comparator.close();
                return null;
            }
            return new ReferenceCountingHandle(tableHandle, new AtomicInteger(0));
        });
        return result != null;
    }

    public boolean removeTable(@NonNull UUID streamId) {
        ReferenceCountingHandle result = columnFamilies.computeIfPresent(streamId, (id, handle) -> {
            //check to see if the ref count was 0 before entering remove attempt
            if (handle.getReferenceCount().getAndDecrement() == 0) {
                try {
                    database.dropColumnFamily(handle.getHandle());
                } catch (RocksDBException e) {
                    log.error("Error in dropping column families for table {}.", id);
                    log.error(CAUSE_OF_ERROR, e);
                    //undo modification to reference handle
                    handle.getReferenceCount().getAndIncrement();
                    return handle;
                }
                handle.getHandle().close();
                return null;
            } else {
                log.error("Cannot remove table during usage of handle");
                //undo modification to reference handle
                handle.getReferenceCount().getAndIncrement();
                return handle;
            }
        });
        return result == null;
    }

    private byte[] getBytes(UUID id) {
        return Bytes.concat(Longs.toByteArray(id.getMostSignificantBits()),
                Longs.toByteArray(id.getLeastSignificantBits()));
    }

    public <R> R executeOnTable(@NonNull UUID streamId, ExceptionFunction<ColumnFamilyHandle, R> operation) throws Exception {
        ReferenceCountingHandle refHandle = columnFamilies.get(streamId);
        if (refHandle == null) {
            throw new IllegalArgumentException("Reference handle does not exist");
        }
        int initCount = refHandle.getReferenceCount().getAndIncrement();
        try {
            if (initCount == -1) {
                //value of -1 indicates that a remove operation is in progress/completed
                //thus we should fail the execution
                throw new IllegalArgumentException("Requested handle is removed");
            } else {
                //we have incremented the reference counter, so this handle cannot be removed until we are finished
                return operation.run(refHandle.getHandle());
            }
        } finally {
            //decrement the counter to release the reference
            refHandle.getReferenceCount().getAndDecrement();
        }
    }

    public List<UUID> getCurrentStreamsRegistered() {
        //no check performed for reference counts here since we will fail transaction on write attempt
        return columnFamilies.keySet().stream().collect(ImmutableList.toImmutableList());
    }

    public void shutdown() {
        columnFamilies.values().stream()
                .map(ReferenceCountingHandle::getHandle).forEach(AbstractImmutableNativeReference::close);
    }

    @FunctionalInterface
    public interface ExceptionFunction<T, R> {
        R run(T arg) throws Exception;
    }
}
