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
     * Unique identifier of the current site ID.
     */
    private UUID siteID;

    /*
     * Unique identifier of the remote/destination site ID.
     */
    // TODO: Tuple? <APP_ID, DEST_CLUSTER_ID> ?
    private UUID remoteSiteID;

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
}
