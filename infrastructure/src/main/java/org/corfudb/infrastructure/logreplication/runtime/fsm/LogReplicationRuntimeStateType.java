package org.corfudb.infrastructure.logreplication.runtime.fsm;

public enum LogReplicationRuntimeStateType {
    WAITING_FOR_CONNECTIVITY,
    VERIFYING_REMOTE_LEADER,
    NEGOTIATING,
    REPLICATING,
    STOPPED,
    UNRECOVERABLE
}
