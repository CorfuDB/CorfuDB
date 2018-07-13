package org.corfudb.runtime.view;

/**
 *
 * Cache options for write operations.
 *
 * Created by Maithem on 6/19/18.
 */
public enum CacheOption {
    /**
     * Issue a write without caching the result
     */
    WRITE_AROUND,
    /**
     * Issue a write and cache the result if the write
     * succeeds
     */
    WRITE_THROUGH
}
