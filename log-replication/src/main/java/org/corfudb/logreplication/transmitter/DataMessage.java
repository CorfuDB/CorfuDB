package org.corfudb.logreplication.transmitter;

import com.google.common.annotations.VisibleForTesting;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.corfudb.logreplication.MessageMetadata;
import org.corfudb.logreplication.MessageType;

@Data
public class DataMessage {

    @Getter
    public MessageMetadata metadata;

    @Setter
    private byte[] data;

    public DataMessage() {}

    @VisibleForTesting
    public DataMessage(byte[] data) {
        this.data = data;
    }

    public DataMessage(MessageType type, long entryTS, long preTS, long snapshot, long sequence, byte[] data) {
        metadata = new MessageMetadata(type, entryTS, preTS, sequence, snapshot);
        this.data = data;
    }
}
