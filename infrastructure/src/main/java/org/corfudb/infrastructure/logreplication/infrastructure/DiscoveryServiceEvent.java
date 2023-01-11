package org.corfudb.infrastructure.logreplication.infrastructure;

import lombok.Getter;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo.TopologyConfigurationMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.LogReplicationSession;

import java.util.UUID;

@Getter
public class DiscoveryServiceEvent {

    private final DiscoveryServiceEventType type;

    private TopologyConfigurationMsg topologyConfig = null;

    private LogReplicationSession session;

    private UUID eventId = null;

    public DiscoveryServiceEvent(DiscoveryServiceEventType type) {
       this.type = type;
    }

    public DiscoveryServiceEvent(DiscoveryServiceEventType type, String sourceClusterId, String sinkClusterId) {
        this.type = type;
        this.session = LogReplicationSession.newBuilder()
                .setSourceClusterId(sourceClusterId)
                .setSinkClusterId(sinkClusterId)
                .setSubscriber(SessionManager.getDefaultSubscriber())
                .build();
    }

    public DiscoveryServiceEvent(DiscoveryServiceEventType type, TopologyConfigurationMsg topologyConfigMsg) {
        this.type = type;
        this.topologyConfig = topologyConfigMsg;
    }

    public DiscoveryServiceEvent(DiscoveryServiceEventType type, String sourceClusterId,
                                 String sinkClusterId, String eventId) {
        this(type, sourceClusterId, sinkClusterId);
        this.eventId = UUID.fromString(eventId);
    }

    public enum DiscoveryServiceEventType {
        DISCOVERED_TOPOLOGY,
        ACQUIRE_LOCK,
        RELEASE_LOCK,
        ENFORCE_SNAPSHOT_SYNC
    }
}
