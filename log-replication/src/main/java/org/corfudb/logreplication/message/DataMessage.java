package org.corfudb.logreplication.message;

import com.google.common.annotations.VisibleForTesting;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

/**
 * This class represents the data message, it contains the data to be replicated and metadata required for sequencing.
 */
@Data
public class DataMessage {

    @Getter
    public MessageMetadata metadata;

    @Setter
    private byte[] data;

    public DataMessage() {}

    @VisibleForTesting
    public DataMessage(byte[] data) {
        this(data, new MessageMetadata());
    }

    public DataMessage(byte[] data, MessageMetadata metadata) {
       this.data = data;
       this.metadata = metadata;
    }

    public DataMessage(MessageMetadata metadata) {
        this.metadata = metadata;
    }

    public DataMessage(MessageType type, long entryTS, long preTS, long snapshot, long sequence, byte[] data) {
        metadata = new MessageMetadata(type, entryTS, preTS, snapshot, sequence);
        this.data = data;
    }
}
