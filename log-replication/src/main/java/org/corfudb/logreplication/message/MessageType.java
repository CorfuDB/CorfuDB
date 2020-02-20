package org.corfudb.logreplication.message;

import lombok.Getter;

public enum MessageType {
    LOG_ENTRY_MESSAGE(1),
    SNAPSHOT_MESSAGE(2),
    SNAPSHOT_START(3),
    LOG_ENTRY_REPLICATED(4),
    SNAPSHOT_REPLICATED(5);

    @Getter
    int val;
    MessageType(int newVal) {
        val  = newVal;
    }

    public static MessageType fromValue(int newVal) {
        for (MessageType messageType: values()) {
            if (messageType.getVal() == newVal) {
                return messageType;
            }
        }
        throw new IllegalArgumentException("wrong value " + newVal);
    }

}
