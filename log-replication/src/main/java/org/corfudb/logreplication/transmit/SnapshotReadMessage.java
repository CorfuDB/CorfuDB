package org.corfudb.logreplication.transmit;

import lombok.Data;
import org.corfudb.logreplication.message.DataMessage;

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
    private List<DataMessage> messages;

    /**
     * Constructor
     *
     * @param messages list of messages to transmit
     * @param endRead True, last read of snapshot sync. False, otherwise.
     */
    public SnapshotReadMessage(List<DataMessage> messages, boolean endRead) {
        this.messages = messages;
        this.endRead = endRead;

        // Enforce end of read if there is no data
        if(messages.isEmpty() && !endRead) {
            throw new IllegalArgumentException("List of messages is empty and no end read marker found.");
        }
    }
}
