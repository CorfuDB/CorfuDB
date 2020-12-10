package org.corfudb.infrastructure.logreplication.infrastructure.plugins;

import java.util.Set;

/**
 * This Interface must be implemented by any external
 * provider of Log Replication Configuration.
 *
 * Log Replication Configuration encompasses:
 * (1) Streams to replicate
 * (2) System's version
 */
public interface ILogReplicationConfigAdapter {

    /*
     * Returns a set of fully qualified stream names to replicate
     */
    Set<String> fetchStreamsToReplicate();

    String getVersion();
}
