package org.corfudb.runtime.object;

import org.rocksdb.ColumnFamilyHandle;

public interface ColumnFamilyRegistry {

    ColumnFamilyHandle getDefaultColumnFamily();

    ColumnFamilyHandle getSecondaryIndexColumnFamily();
}
