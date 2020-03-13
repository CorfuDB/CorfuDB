package org.corfudb.protocols.wireprotocol.logreplication;

import lombok.Getter;
import org.corfudb.protocols.logprotocol.CheckpointEntry;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public enum MessageType {
    LOG_ENTRY_MESSAGE(1),
    SNAPSHOT_MESSAGE(2),
    SNAPSHOT_START(3),
    LOG_ENTRY_REPLICATED(4),
    SNAPSHOT_REPLICATED(5),
    SNAPSHOT_END(6);

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
