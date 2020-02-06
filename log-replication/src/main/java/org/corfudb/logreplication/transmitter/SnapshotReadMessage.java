package org.corfudb.logreplication.transmitter;

import lombok.Data;

import java.util.List;

/**
 * This class represents the read message of a snapshot sync.
 */
@Data
public class SnapshotReadMessage {

    /*
     * This flag indicates reads have completed for snapshot sync.
     */
    private boolean endRead;

    /*
     * List of messages to transmit
     */
    private List<TxMessage> messages;

    /**
     * Constructor
     *
     * @param messages list of messages to transmit
     * @param endRead True, last read of snapshot sync. False, otherwise.
     */
    public SnapshotReadMessage(List<TxMessage> messages, boolean endRead) {
        this.messages = messages;
        this.endRead = endRead;
    }
}
