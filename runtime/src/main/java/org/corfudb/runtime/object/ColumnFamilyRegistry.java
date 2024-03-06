package org.corfudb.runtime.object;

import org.rocksdb.ColumnFamilyHandle;

import java.util.Optional;

/**
 * Provides ColumnFamilyHandles.
 */
public interface ColumnFamilyRegistry {

    ColumnFamilyHandle getDefaultColumnFamily();

    ColumnFamilyHandle getSecondaryIndexColumnFamily();
}
