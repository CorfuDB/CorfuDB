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
        ON_CONNECTION_UP,
        ON_CONNECTION_DOWN,
        REMOTE_LEADER_NOT_FOUND,
        REMOTE_LEADER_FOUND,
        REMOTE_LEADER_LOSS
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
