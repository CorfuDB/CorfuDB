package org.corfudb.infrastructure.remotecorfutable;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.Longs;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import static org.corfudb.infrastructure.remotecorfutable.DatabaseHandler.CAUSE_OF_ERROR;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.ComparatorOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

/**
 * This class provides utility for adding and removing column families from the database, as well as
 * providing a consistent view of column families present in the database.
 *
 * Created by nvaishampayan517 on 08/25/21
 */
@Slf4j
@AllArgsConstructor
public class ColumnFamiliesHandler {
    //Loads the C++ library backing RocksDB
    static {
        RocksDB.loadLibrary();
    }

    @NonNull
    private final RocksDB database;
    @NonNull
    private final ConcurrentMap<UUID, ColumnFamilyHandle> columnFamilies;

    public void addTable(@NonNull UUID streamId) {
        columnFamilies.computeIfAbsent(streamId, id -> {
            log.trace("Allocating new column family for stream {}", streamId);
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
            log.trace("Successfully created new column family for stream {}", id);
            return tableHandle;
        });
    }

    private byte[] getBytes(UUID id) {
        return Bytes.concat(Longs.toByteArray(id.getMostSignificantBits()),
                Longs.toByteArray(id.getLeastSignificantBits()));
    }

    public <R> R executeOnTable(@NonNull UUID streamId, ExceptionFunction<ColumnFamilyHandle, R> operation) throws Exception {
        log.trace("Getting correct column family for requested table");
        ColumnFamilyHandle streamHandle = columnFamilies.get(streamId);
        if (streamHandle == null) {
            log.trace("Column Family did not exist - creating new column family");
            addTable(streamId);
            streamHandle = columnFamilies.get(streamId);
        }
        return operation.run(streamHandle);
    }

    public List<UUID> getCurrentStreamsRegistered() {
        //no check performed for reference counts here since we will fail transaction on write attempt
        return columnFamilies.keySet().stream().collect(ImmutableList.toImmutableList());
    }

    public void shutdown() {
        columnFamilies.forEach((id, handle) -> handle.close());
    }

    @FunctionalInterface
    public interface ExceptionFunction<T, R> {
        R run(T arg) throws Exception;
    }
}
