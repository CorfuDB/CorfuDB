package org.corfudb.infrastructure.logreplication.infrastructure.plugins;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * This Interface must be implemented by any external
 * provider of Log Replication Configuration.
 *
 * Log Replication Configuration encompasses:
 * (1) Streams to replicate
 * (2) System's version
 */
public interface ILogReplicationConfigAdapter {
    /**
     * Get the version of the product that Log Replicator works for, which should be provided
     * by an external static file.
     */
    String getVersion();

    /**
     * Returns configuration for streaming on sink (standby)
     *
     * This configuration consists of a map containing data stream IDs to stream tags
     * Note that: since data is not deserialized we have no access to stream tags corresponding
     * to the replicated data, therefore, this data must be provided by the plugin externally.
     */
    Map<UUID, List<UUID>> getStreamingConfigOnSink();
}
