package org.corfudb.runtime.object;

import org.rocksdb.ColumnFamilyHandle;

/**
 * Provides ColumnFamilyHandles.
 */
public interface ColumnFamilyRegistry {

    ColumnFamilyHandle getDefaultColumnFamily();

    ColumnFamilyHandle getSecondaryIndexColumnFamily();
}
