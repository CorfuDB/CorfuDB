package org.corfudb.logreplication.message;

import lombok.Data;
import lombok.Getter;


/**
 * This class represents the data message, it contains the data to be replicated and metadata required for sequencing.
 */
@Data
public class DataMessage {

    @Getter
    private byte[] data;

    public DataMessage(byte[] data) {
        this.data = data;
    }

    public static DataMessage generateAck(LogReplicationEntryMetadata metadata) {
        LogReplicationEntry entry = new LogReplicationEntry(metadata, new byte[0]);
        return new DataMessage(entry.serialize());
    }
}
