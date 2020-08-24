package org.corfudb.infrastructure.logreplication.infrastructure;

import lombok.Getter;
import lombok.Setter;

import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.TopologyConfigurationMsg;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class DiscoveryServiceEvent {
    DiscoveryServiceEventType type;

    // Used by enforcing snapshot sync event
    @Getter
    private UUID eventUUID;

    // Used by enforcing snapshot sync event
    @Getter
    CompletableFuture<UUID> cf;

    @Getter
    TopologyConfigurationMsg topologyConfig = null;

    @Getter
    @Setter
    ClusterDescriptor remoteClusterInfo;

    public DiscoveryServiceEvent(DiscoveryServiceEventType type) {
       this.type = type;
    }

    public DiscoveryServiceEvent(DiscoveryServiceEventType type, UUID eventUUID) {
        this.type = type;
        this.eventUUID = eventUUID;
        this.cf = new CompletableFuture<>();
    }

    public DiscoveryServiceEvent(DiscoveryServiceEventType type, TopologyConfigurationMsg topologyConfigMsg) {
        this.type = type;
        this.topologyConfig = topologyConfigMsg;
    }

    public enum DiscoveryServiceEventType {
        DISCOVERED_TOPOLOGY,
        ACQUIRE_LOCK,
        RELEASE_LOCK,
        UPGRADE,
        ENFORCE_SNAPSHOT_SYNC
    }
}
