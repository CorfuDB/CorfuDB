package org.corfudb.infrastructure.logreplication.infrastructure;

import lombok.Getter;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.infrastructure.logreplication.transport.IChannelContext;

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
    private final LogReplicationConfig config;

    @Getter
    private final TopologyDescriptor topology;

    @Getter
    private final String localCorfuEndpoint;

    @Getter
    private final IChannelContext channelContext;

    /**
     * Constructor
     **/
    public LogReplicationContext(LogReplicationConfig config,  TopologyDescriptor topology, String localCorfuEndpoint,
                                 IChannelContext channelContext) {
        this.config = config;
        this.topology = topology;
        this.localCorfuEndpoint = localCorfuEndpoint;
        this.channelContext = channelContext;
    }
}
