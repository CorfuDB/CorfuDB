package org.corfudb.runtime.collections;

import lombok.Getter;

public class DataLocator {
    /**
     * Fully qualified corfu table name.
     *
     */
    @Getter
    private final String tableName;

    /**
     * Serialized key of the modified record.
     *
     */
    @Getter
    private final byte[] key;

    public DataLocator(String tableName, byte[] key) {
        this.tableName = tableName;
        this.key = key;
    }
}
