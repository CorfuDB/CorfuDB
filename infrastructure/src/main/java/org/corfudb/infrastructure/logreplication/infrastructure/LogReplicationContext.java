package org.corfudb.infrastructure.logreplication.infrastructure;

import lombok.Getter;
import org.corfudb.infrastructure.LogReplicationServer;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;

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
    private TopologyDescriptor topology;
    
    @Getter
    private LogReplicationServer server;

    @Getter
    private String localCorfuEndpoint;

    /**
     * Constructor
     *
     * @param config log replication configuration
     * @param topology topology descriptor (multi-cluster view)
     */
    public LogReplicationContext(LogReplicationConfig config, TopologyDescriptor topology,
                                 LogReplicationServer server, String localCorfuEndpoint) {
        this.config = config;
        this.topology = topology;
        this.server = server;
        this.localCorfuEndpoint = localCorfuEndpoint;
    }
}
