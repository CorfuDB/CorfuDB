package org.corfudb.logreplication.transmitter;

import lombok.Data;

import java.util.List;

@Data
public class SnapshotReadMessage {

    private boolean endRead;

    private List<TxMessage> messages;

    public SnapshotReadMessage(List<TxMessage> messages, boolean endRead) {
        this.messages = messages;
        this.endRead = endRead;
    }
}
