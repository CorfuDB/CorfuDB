package org.corfudb.logreplication.fsm;

import lombok.Getter;

import java.util.Set;

/**
 * A class that contains Log Replication Configuration parameters.
 */
public class LogReplicationConfig {

    // Set of all streams to be replicated
    @Getter
    private Set<String> streamsToReplicate;

    public LogReplicationConfig(Set<String> streamsToReplicate) {
        this.streamsToReplicate = streamsToReplicate;
    }
}
