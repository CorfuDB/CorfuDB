package org.corfudb.logreplication.fsm;

import lombok.Data;
import lombok.Getter;
import org.corfudb.logreplication.DataSender;
import org.corfudb.logreplication.message.LogReplicationEntry;
import org.corfudb.logreplication.send.LogReplicationError;
import org.corfudb.logreplication.message.DataMessage;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.UUID;


/**
 * Test Implementation of Snapshot Data Sender
 */
public class TestDataSender implements DataSender {

    @Getter
    private Queue<DataMessage> snapshotQueue = new LinkedList<>();

    @Getter
    private Queue<DataMessage> logEntryQueue = new LinkedList<>();

    public TestDataSender() {
    }

    @Override
    public boolean send(DataMessage message, UUID snapshotSyncId, boolean completed) {
        // Hack to bypass and write the snapshotSyncId
        LogReplicationEntry entry = LogReplicationEntry.deserialize(message.getData());
        entry.getMetadata().setSnapshotRequestId(snapshotSyncId);
        message = new DataMessage(entry.serialize());

        if (message != null && message.getData().length != 0) {
            snapshotQueue.add(message);
            return true;
        }

        return false;
    }

    @Override
    public boolean send(List<DataMessage> messages, UUID snapshotSyncId, boolean completed) {
        if (messages != null && !messages.isEmpty()) {
            // Add all received messages to the queue
            messages.forEach(msg -> send(msg, snapshotSyncId, completed));
            return true;
        }

        return false;
    }

    @Override
    public boolean send(DataMessage message) {
        if (message != null && message.getData() != null) {
            logEntryQueue.add(message);
            return true;
        }

        return false;
    }

    @Override
    public boolean send(List<DataMessage> messages) {
        if (messages != null && !messages.isEmpty()) {
            // Add all received messages to the queue
            messages.forEach(msg -> logEntryQueue.add(msg));
            return true;
        }

        return false;
    }

    @Override
    public void onError(LogReplicationError error, UUID snapshotSyncId) {}

    @Override
    public void onError(LogReplicationError error) {}
}
