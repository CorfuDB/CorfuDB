package org.corfudb.logreplication.fsm;

import lombok.Getter;
import org.corfudb.infrastructure.logreplication.DataSender;
import org.corfudb.infrastructure.logreplication.LogReplicationError;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;


/**
 * Test Implementation of Snapshot Data Sender
 */
public class TestDataSender implements DataSender {

    @Getter
    private Queue<org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry> snapshotQueue = new LinkedList<>();

    @Getter
    private Queue<LogReplicationEntry> logEntryQueue = new LinkedList<>();

    public TestDataSender() {
    }

    @Override
    public CompletableFuture<LogReplicationEntry> send(LogReplicationEntry message) {
        if (message != null && message.getPayload() != null) {
            logEntryQueue.add(message);
        }

        return new CompletableFuture<>();
    }

    @Override
    public boolean send(List<LogReplicationEntry> messages) {
        if (messages != null && !messages.isEmpty()) {
            // Add all received messages to the queue
            messages.forEach(msg -> logEntryQueue.add(msg));
            return true;
        }

        return false;
    }

    @Override
    public void onError(LogReplicationError error) {}
}
