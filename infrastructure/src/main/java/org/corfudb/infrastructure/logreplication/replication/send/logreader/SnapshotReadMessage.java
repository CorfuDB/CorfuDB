package org.corfudb.infrastructure.logreplication.replication.send.logreader;

import lombok.Data;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;

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
     * List of messages to send
     */
    private List<LogReplicationEntry> messages;

    /**
     * Constructor
     *
     * @param messages the message to send
     * @param endRead True, last read of snapshot sync. False, otherwise.
     */
    public SnapshotReadMessage(List<LogReplicationEntry> messages, boolean endRead) {
        this.messages = messages;
        this.endRead = endRead;

        // Enforce end of read if there is no data
        if(messages.isEmpty() && !endRead) {
            throw new IllegalArgumentException("List of messages is empty and no end read marker found.");
        }
    }
}
