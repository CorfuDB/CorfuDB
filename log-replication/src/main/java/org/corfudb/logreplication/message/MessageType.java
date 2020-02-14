package org.corfudb.logreplication.message;

public enum MessageType {
    LOG_ENTRY_MESSAGE,
    SNAPSHOT_MESSAGE,
    LOG_ENTRY_REPLICATED,
    SNAPSHOT_REPLICATED
}
