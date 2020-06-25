package org.corfudb.infrastructure.logreplication;

import lombok.Data;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.Set;

/**
 * This class represents any Log Replication Configuration,
 * i.e., set of parameters common across all Clusters.
 */
@Slf4j
@Data
public class LogReplicationConfig {

    // TODO: It's cleaner to make LogReplicationConfig agnostic of cluster information (common across all clusters) .

    /*
     * Unique identifiers for all streams to be replicated across sites.
     */
    private Set<String> streamsToReplicate;

    /*
     * Unique identifier of the current cluster ID.
     */
    private String localClusterId;

    /*
     * Unique identifier of the remote/destination cluster ID.
     */
    private String remoteClusterId;

    /**
     * Constructor
     *
     * @param streamsToReplicate Unique identifiers for all streams to be replicated across sites.
     */
    public LogReplicationConfig(Set<String> streamsToReplicate) {
        this.streamsToReplicate = streamsToReplicate;
    }

    /**
     * Constructor
     *
     * @param streamsToReplicate Unique identifiers for all streams to be replicated across sites.
     */
    public LogReplicationConfig(Set<String> streamsToReplicate, @NonNull String localClusterId, @NonNull String remoteClusterId) {
        this(streamsToReplicate);
        this.localClusterId = localClusterId;
        this.remoteClusterId = remoteClusterId;
    }
}