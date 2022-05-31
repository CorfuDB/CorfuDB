package org.corfudb.infrastructure.logreplication.infrastructure;

import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import lombok.Setter;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;

/**
 * This class represents the Log Replication Context.
 *
 * It contains all abstractions required to initiate log replication either as
 * a source or as sink cluster.
 *
 * @author amartinezman
 */
public class LogReplicationContext {

    @Getter
    private final LogReplicationConfigManager configManager;

    @Getter
    @Setter
    private TopologyDescriptor topology;

    @Getter
    @Setter
    private long topologyConfigId;

    /**
     * Constructor
     **/
    public LogReplicationContext(LogReplicationConfig config,  TopologyDescriptor topology, String localCorfuEndpoint) {
        this.config = config;
        this.topology = topology;
        this.localCorfuEndpoint = localCorfuEndpoint;
    }
}
