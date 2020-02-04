package org.corfudb.logreplication.fsm;

import lombok.Builder;
import lombok.Data;

import java.util.Set;

/**
 * A class that contains Log Replication Configuration parameters.
 */
@Builder
@Data
public class LogReplicationConfig {

    // Set of all streams to be replicated
    private Set<String> streamsToReplicate;

    private String siteID;

    private int logReplicationFSMNumWorkers = 1;
}
