package org.corfudb.logreplication.transmitter;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.corfudb.logreplication.MessageMetadata;
import org.corfudb.logreplication.MessageType;

@Data
public class TxMessage {

    @Getter
    public MessageMetadata metadata;

    @Setter
    private byte[] data;

    public TxMessage() {}

    public TxMessage(MessageType type, long entryTS, long preTS, long snapshot) {
        metadata = new MessageMetadata(type, entryTS, preTS, snapshot);
    }
}
