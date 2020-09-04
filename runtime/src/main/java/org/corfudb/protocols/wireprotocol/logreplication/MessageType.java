package org.corfudb.protocols.wireprotocol.logreplication;

import lombok.Getter;
import org.corfudb.runtime.Messages.LogReplicationEntryType;


public enum MessageType {
    LOG_ENTRY_MESSAGE(1, LogReplicationEntryType.LOG_ENTRY_MESSAGE),
    SNAPSHOT_MESSAGE(2, LogReplicationEntryType.SNAPSHOT_MESSAGE),
    SNAPSHOT_START(3, LogReplicationEntryType.SNAPSHOT_START),
    LOG_ENTRY_REPLICATED(4, LogReplicationEntryType.LOG_ENTRY_REPLICATED),
    SNAPSHOT_REPLICATED(5, LogReplicationEntryType.SNAPSHOT_REPLICATED),
    SNAPSHOT_END(6, LogReplicationEntryType.SNAPSHOT_END),
    SNAPSHOT_TRANSFER_COMPLETE(7, LogReplicationEntryType.SNAPSHOT_TRANSFER_COMPLETE);

    @Getter
    private int val;

    @Getter
    private LogReplicationEntryType protoType;

    MessageType(int newVal, LogReplicationEntryType type) {
        val  = newVal;
        protoType = type;
    }

    public static MessageType fromValue(int newVal) {
        for (MessageType messageType: values()) {
            if (messageType.getVal() == newVal) {
                return messageType;
            }
        }
        throw new IllegalArgumentException("wrong value " + newVal);
    }

    public static MessageType fromProtoType(LogReplicationEntryType type) {
        for (MessageType messageType: values()) {
            if (messageType.getProtoType() == type) {
                return messageType;
            }
        }
        throw new IllegalArgumentException("Wrong value " + type);
    }


}
