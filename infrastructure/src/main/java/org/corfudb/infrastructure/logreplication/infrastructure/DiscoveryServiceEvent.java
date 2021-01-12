package org.corfudb.infrastructure.logreplication.infrastructure;

import lombok.Getter;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.TopologyConfigurationMsg;

import java.util.UUID;

@Getter
public class DiscoveryServiceEvent {

    private final DiscoveryServiceEventType type;

    private TopologyConfigurationMsg topologyConfig = null;

    private ClusterDescriptor remoteClusterInfo;

    private UUID eventId = null;

    public DiscoveryServiceEvent(DiscoveryServiceEventType type) {
       this.type = type;
    }

    public DiscoveryServiceEvent(DiscoveryServiceEventType type, String clusterId) {
        this.type = type;
        this.remoteClusterInfo = new ClusterDescriptor(clusterId);
    }

    public DiscoveryServiceEvent(DiscoveryServiceEventType type, TopologyConfigurationMsg topologyConfigMsg) {
        this.type = type;
        this.topologyConfig = topologyConfigMsg;
    }

    public DiscoveryServiceEvent(DiscoveryServiceEventType type, String clusterId, String eventId) {
        this(type, clusterId);
        this.eventId = UUID.fromString(eventId);
    }

    public enum DiscoveryServiceEventType {
        DISCOVERED_TOPOLOGY,
        ACQUIRE_LOCK,
        RELEASE_LOCK,
        UPGRADE,
        ENFORCE_SNAPSHOT_SYNC
    }
}
