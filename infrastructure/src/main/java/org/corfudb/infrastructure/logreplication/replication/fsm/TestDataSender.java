package org.corfudb.infrastructure.logreplication.replication.fsm;

import lombok.Getter;
import org.corfudb.infrastructure.logreplication.DataSender;
import org.corfudb.infrastructure.logreplication.replication.send.LogReplicationError;
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

        CompletableFuture<LogReplicationEntry> cf = new CompletableFuture<>();
        LogReplicationEntry ack = LogReplicationEntry.generateAck(message.getMetadata());
        cf.complete(ack);

        return cf;
    }

    @Override
    public CompletableFuture<LogReplicationEntry> send(List<LogReplicationEntry> messages) {

        CompletableFuture<LogReplicationEntry> lastSentMessage = new CompletableFuture<>();

        if (messages != null && !messages.isEmpty()) {
            CompletableFuture<LogReplicationEntry> tmp;

            for (LogReplicationEntry message : messages) {
                tmp = send(message);
                if (message.getMetadata().getMessageMetadataType().equals(MessageType.SNAPSHOT_END) ||
                        message.getMetadata().getMessageMetadataType().equals(MessageType.LOG_ENTRY_MESSAGE)) {
                    lastSentMessage = tmp;
                }
            }
        }

        return lastSentMessage;
    }
    
    public void reset() {
        entryQueue.clear();
    }

    @Override
    public void onError(LogReplicationError error) {}
}
