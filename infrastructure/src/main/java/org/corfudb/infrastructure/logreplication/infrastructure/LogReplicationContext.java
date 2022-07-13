package org.corfudb.infrastructure.logreplication.infrastructure;

import lombok.Getter;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;

/**
 * This class represents the Log Replication Context.
 *
 * It contains all abstractions required to initiate log replication either as a source cluster or as sink cluster.
 *
 * @author amartinezman
 */
public class LogReplicationContext {

    @Getter
    private LogReplicationConfig config;

    @Getter
    private TopologyDescriptor topology;

    @Getter
    private String localCorfuEndpoint;

    /**
     * Constructor
     **/
    public LogReplicationContext(LogReplicationConfig config, TopologyDescriptor topology, String localCorfuEndpoint) {
        this.config = config;
        this.topology = topology;
        this.localCorfuEndpoint = localCorfuEndpoint;
    }
}
