package org.corfudb.transport.logreplication;

public enum CommunicationStateType {
    INIT,
    VERIFY_LEADER,
    NEGOTIATE,
    REPLICATE,
    STOP
}
