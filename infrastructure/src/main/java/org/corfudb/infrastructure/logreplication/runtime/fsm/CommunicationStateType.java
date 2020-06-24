package org.corfudb.infrastructure.logreplication.runtime.fsm;

public enum CommunicationStateType {
    INIT,
    VERIFY_LEADER,
    NEGOTIATE,
    REPLICATE,
    STOP
}
