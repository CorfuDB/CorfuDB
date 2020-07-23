package org.corfudb.infrastructure.logreplication.infrastructure;

import lombok.Getter;
import lombok.Setter;

import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.TopologyConfigurationMsg;

public class DiscoveryServiceEvent {
    @Getter
    private DiscoveryServiceEventType type;

    @Getter
    private TopologyConfigurationMsg topologyConfig = null;

    @Getter
    @Setter
    private ClusterDescriptor remoteClusterInfo;

    public DiscoveryServiceEvent(DiscoveryServiceEventType type) {
       this.type = type;
    }

    public DiscoveryServiceEvent(DiscoveryServiceEventType type, TopologyConfigurationMsg topologyConfigMsg) {
        this.type = type;
        this.topologyConfig = topologyConfigMsg;
    }

    public enum DiscoveryServiceEventType {
        DISCOVERED_TOPOLOGY,
        DISCOVER_INIT_TOPOLOGY,
        ACQUIRE_LOCK,
        RELEASE_LOCK,
        UPGRADE
    }
}
