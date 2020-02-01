package org.corfudb.logreplication.fsm;

import java.util.List;

/**
 * A class that contains Log Replication Configuration parameters.
 */
public class LogReplicationConfig {

    // List of all streams to be replicated
    private List<String> streamsToReplicate;

    public LogReplicationConfig(List<String> streamsToReplicate) {
        this.streamsToReplicate = streamsToReplicate;
    }
}
