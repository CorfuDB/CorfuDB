package org.corfudb.logreplication.fsm;

import lombok.Getter;
import org.corfudb.infrastructure.logreplication.DataSender;
import org.corfudb.infrastructure.logreplication.LogReplicationError;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;
import org.corfudb.protocols.wireprotocol.logreplication.MessageType;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;


/**
 * Test Implementation of Snapshot Data Sender
 */
public class TestDataSender implements DataSender {

    @Getter
    private Queue<LogReplicationEntry> entryQueue = new LinkedList<>();

    public TestDataSender() {
    }

    @Override
    public CompletableFuture<LogReplicationEntry> send(LogReplicationEntry message) {
        if (message != null && message.getPayload() != null) {
            if (message.getMetadata().getMessageMetadataType().equals(MessageType.SNAPSHOT_MESSAGE) ||
                    message.getMetadata().getMessageMetadataType().equals(MessageType.LOG_ENTRY_MESSAGE)) {
                // Ignore, do not account Start and End Markers as messages
                entryQueue.add(message);
            }
        }

        return new CompletableFuture<>();
    }

    @Override
    public boolean send(List<LogReplicationEntry> messages) {
        if (messages != null && !messages.isEmpty()) {
            // Add all received messages to the queue
            messages.forEach(msg -> {
                if (msg.getMetadata().getMessageMetadataType().equals(MessageType.SNAPSHOT_MESSAGE) ||
                        msg.getMetadata().getMessageMetadataType().equals(MessageType.LOG_ENTRY_MESSAGE)) {
                    // Ignore, do not account Start and End Markers as messages
                    entryQueue.add(msg);
                }
            });
            return true;
        }

        return false;
    }

    public void reset() {
        entryQueue.clear();
    }

    @Override
    public void onError(LogReplicationError error) {}
}
