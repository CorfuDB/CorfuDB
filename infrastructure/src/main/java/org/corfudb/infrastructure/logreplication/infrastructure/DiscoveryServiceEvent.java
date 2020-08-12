package org.corfudb.infrastructure.logreplication.infrastructure;

import lombok.Getter;
import lombok.Setter;

import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.TopologyConfigurationMsg;

public class DiscoveryServiceEvent {
    DiscoveryServiceEventType type;

    @Getter
    TopologyConfigurationMsg topologyConfig = null;

    @Getter
    @Setter
    ClusterDescriptor remoteSiteInfo;

    public DiscoveryServiceEvent(DiscoveryServiceEventType type) {
       this.type = type;
    }

    public DiscoveryServiceEvent(DiscoveryServiceEventType type, TopologyConfigurationMsg topologyConfigMsg) {
        this.type = type;
        this.topologyConfig = topologyConfigMsg;
    }

    public DiscoveryServiceEvent(DiscoveryServiceEventType type, String clusterId) {
        this.type = type;
        this.remoteSiteInfo = new ClusterDescriptor(clusterId, LogReplicationClusterInfo.ClusterRole.STANDBY, 9000);
    }

    public enum DiscoveryServiceEventType {
        DISCOVERED_TOPOLOGY,
        ACQUIRE_LOCK,
        RELEASE_LOCK,
        UPGRADE,
        ENFORCE_SNAPSHOT_FULL_SYNC
    }
}
