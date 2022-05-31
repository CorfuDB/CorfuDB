package org.corfudb.infrastructure.logreplication.infrastructure;

import lombok.Getter;
import lombok.Setter;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;

/**
 * This class represents the Log Replication Context.
 *
 * It contains all abstractions required to initiate log replication either as
 * an active cluster (source) or as standby cluster (standby).
 *
 * @author amartinezman
 */
public class LogReplicationContext {

    @Getter
    private LogReplicationConfig config;

    @Getter
    @Setter
    private TopologyDescriptor topology;

    @Getter
    private String localCorfuEndpoint;

    /**
     * Constructor
     **/
    public LogReplicationContext(LogReplicationConfig config,  TopologyDescriptor topology, String localCorfuEndpoint) {
        this.config = config;
        this.topology = topology;
        this.localCorfuEndpoint = localCorfuEndpoint;
    }
}
