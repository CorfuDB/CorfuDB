package org.corfudb.logreplication.fsm;

import lombok.Data;

import java.util.Set;
import java.util.UUID;

/**
 * This class represents all configuration parameters required for Log Replication.
 */
@Data
public class LogReplicationConfig {

    /*
     * Unique identifiers for all streams to be replicated across sites.
     */
    private Set<String> streamsToReplicate;

    /*
     * Unique identifier of the remote/destination site ID.
     */
    private UUID remoteSiteID;

    /*
     * Number of worker threads used for log replication state machine tasks.
     */
    private int logReplicationFSMNumWorkers = 1;

    /**
     * Constructor
     *
     * @param streamsToReplicate Unique identifiers for all streams to be replicated across sites.
     * @param remoteSiteID Unique identifier of the remote/destination site.
     */
    public LogReplicationConfig(Set<String> streamsToReplicate, UUID remoteSiteID) {
        this.streamsToReplicate = streamsToReplicate;
        this.remoteSiteID = remoteSiteID;
    }

    /**
     * Constructor
     *
     * @param streamsToReplicate Unique identifiers for all streams to be replicated across sites.
     * @param remoteSiteID Unique identifier of the remote/destination site.
     * @param logReplicationFSMNumWorkers Number of worker threads used for log replication state machine tasks (default = 1)
     */
    public LogReplicationConfig(Set<String> streamsToReplicate, UUID remoteSiteID, int logReplicationFSMNumWorkers) {
        this.streamsToReplicate = streamsToReplicate;
        this.remoteSiteID = remoteSiteID;
        this.logReplicationFSMNumWorkers = logReplicationFSMNumWorkers;
    }
}
