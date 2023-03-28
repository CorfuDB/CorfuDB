package org.corfudb.runtime;

/**
 * LogReplication code resides in the infrastructure package.  Adding a dependency from this package(runtime) to
 * infrastructure introduces a circular dependency.  This class defines LR-specific constants and utility methods
 * required in runtime.  Note that the constants are unique and not duplicated from infrastructure.
 */
public final class LogReplicationUtils {
    public static final String LR_STATUS_STREAM_TAG = "lr_status";

    public static final String REPLICATION_STATUS_TABLE_NAME = "LogReplicationStatusSource";
}
