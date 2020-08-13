package org.corfudb.infrastructure.logreplication.infrastructure.plugins;

import java.util.Map;

/**
 * This Interface must be implemented by any external
 * provider of Log Replication Configuration.
 *
 * Log Replication Configuration encompasses:
 * (1) Streams to replicate
 * (2) System's version
 */
public interface ILogReplicationConfigAdapter {

    Map<String, String> fetchStreamsToReplicate();

    String getVersion();
}
