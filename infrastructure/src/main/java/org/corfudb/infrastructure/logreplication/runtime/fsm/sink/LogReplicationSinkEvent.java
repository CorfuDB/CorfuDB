package org.corfudb.infrastructure.logreplication.runtime.fsm.sink;

import lombok.Getter;

/**
 * This class represents events for SINK to establish a replication channel with the SOURCE, when SINK is the connection
 * initiator
 */
public class LogReplicationSinkEvent {

    /* Enum listing the various type of LogReplicationSinkEvent.
     */
    public enum LogReplicationSinkEventType {
        ON_CONNECTION_UP, // a channel was successfully established
        ON_CONNECTION_DOWN, // something when wrong with the connection
        REMOTE_LEADER_NOT_FOUND, // Remote leader not found during verifyRemoteLeader
        REMOTE_LEADER_FOUND, // Remote leader found on verifyRemoteLeader
        REMOTE_LEADER_LOSS // Leadership changed on the remote
    }

    // Communication Event Type
    @Getter
    private LogReplicationSinkEvent.LogReplicationSinkEventType type;

    // NodeId associated to this communication event.
    @Getter
    private String nodeId;

    public LogReplicationSinkEvent(LogReplicationSinkEvent.LogReplicationSinkEventType type, String nodeId) {
        this.type = type;
        this.nodeId = nodeId;
    }

    public LogReplicationSinkEvent(LogReplicationSinkEvent.LogReplicationSinkEventType type) {
        this.type = type;
    }

}
