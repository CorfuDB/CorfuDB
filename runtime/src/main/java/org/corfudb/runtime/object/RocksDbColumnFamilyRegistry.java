package org.corfudb.runtime.object;

import com.google.common.collect.ImmutableMap;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import org.rocksdb.ColumnFamilyHandle;

@AllArgsConstructor
public class RocksDbColumnFamilyRegistry {

    private final ColumnFamilyHandle defaultColumnFamily;
    private final ImmutableMap<String, ColumnFamilyHandle> secondaryIndexHandles;

    public static final String DEFAULT_COLUMN_FAMILY = "default-column-family";

    public ColumnFamilyHandle get(@NonNull String name) {
        if (name.equals(DEFAULT_COLUMN_FAMILY)) {
            return defaultColumnFamily;
        }

        // TODO(Zach): error check?
        return secondaryIndexHandles.get(name);
    }

    /**
     * Close all ColumnFamilyHandle references.
     */
    public void close() {
        defaultColumnFamily.close();
        secondaryIndexHandles.forEach((name, cfh) -> cfh.close());
    }
}
