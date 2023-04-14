package org.corfudb.infrastructure.logreplication.infrastructure;

import lombok.Getter;
import org.corfudb.runtime.LogReplication.LogReplicationSession;

import java.util.UUID;

@Getter
public class DiscoveryServiceEvent {

    private final DiscoveryServiceEventType type;

    private TopologyDescriptor topologyConfig = null;

    private LogReplicationSession session;

    private UUID eventId = null;

    public DiscoveryServiceEvent(DiscoveryServiceEventType type) {
       this.type = type;
    }

    public DiscoveryServiceEvent(DiscoveryServiceEventType type, TopologyDescriptor topologyConfig) {
        this.type = type;
        this.topologyConfig = topologyConfig;
    }

    public DiscoveryServiceEvent(DiscoveryServiceEventType type, LogReplicationSession session, String eventId) {
        this.type = type;
        this.session = session;
        this.eventId = UUID.fromString(eventId);
    }

    public enum DiscoveryServiceEventType {
        DISCOVERED_TOPOLOGY,
        ACQUIRE_LOCK,
        RELEASE_LOCK,
        ENFORCE_SNAPSHOT_SYNC
    }
}
