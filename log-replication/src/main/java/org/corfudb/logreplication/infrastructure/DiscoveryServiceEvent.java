package org.corfudb.logreplication.infrastructure;

import lombok.Getter;
import lombok.Setter;
import org.corfudb.infrastructure.logreplication.cluster.ClusterDescriptor;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.TopologyConfigurationMsg;

public class DiscoveryServiceEvent {
    DiscoveryServiceEventType type = null;

    @Getter
    TopologyConfigurationMsg topologyConfig = null;

    @Getter
    @Setter
    ClusterDescriptor remoteSiteInfo;

    public DiscoveryServiceEvent(DiscoveryServiceEventType type) {
       this.type = type;
    }

    public DiscoveryServiceEvent(DiscoveryServiceEventType type, ClusterDescriptor siteInfo) {
        new DiscoveryServiceEvent(type);
        this.remoteSiteInfo = siteInfo;
    }

    public DiscoveryServiceEvent(DiscoveryServiceEventType type, TopologyConfigurationMsg topologyConfigMsg) {
        this.type = type;
        this.topologyConfig = topologyConfigMsg;
    }

    public enum DiscoveryServiceEventType {
        DISCOVERY_SITE,
        ACQUIRE_LOCK,
        RELEASE_LOCK,
        UPGRADE
    }
}
