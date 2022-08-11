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

    private ReplicationSubscriber replicationSubscriber;

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

    public DiscoveryServiceEvent(DiscoveryServiceEventType type, ReplicationSubscriber subscriber) {
        this.type = type;
        this.replicationSubscriber = subscriber;
    }

    public enum DiscoveryServiceEventType {
        DISCOVERED_TOPOLOGY,
        ACQUIRE_LOCK,
        RELEASE_LOCK,
        ENFORCE_SNAPSHOT_SYNC,
        NEW_REPLICATION_SUBSCRIBER
    }
}
