package org.corfudb.logreplication.fsm;

import lombok.Getter;
import org.corfudb.logreplication.DataSender;
import org.corfudb.logreplication.transmit.LogReplicationError;
import org.corfudb.logreplication.message.DataMessage;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.UUID;


/**
 * Test Implementation of Snapshot Listener
 */
public class TestDataSender implements DataSender {

    @Getter
    private Queue<DataMessage> snapshotQueue = new LinkedList<>();

    @Getter
    private Queue<DataMessage> logEntryQueue = new LinkedList<>();

    public TestDataSender() {
    }

    @Override
    public boolean onNext(DataMessage message, UUID snapshotSyncId) {
        if (message != null && message.getData().length != 0) {
            snapshotQueue.add(message);
            return true;
        }

        return false;
    }

    @Override
    public boolean onNext(List<DataMessage> messages, UUID snapshotSyncId) {
        if (messages != null && !messages.isEmpty()) {
            // Add all received messages to the queue
            messages.forEach(msg -> snapshotQueue.add(msg));
            return true;
        }

        return false;
    }

    @Override
    public boolean onNext(DataMessage message) {
        if (message != null && message.getData() != null) {
            logEntryQueue.add(message);
            return true;
        }

        return false;
    }

    @Override
    public boolean onNext(List<DataMessage> messages) {
        if (messages != null && !messages.isEmpty()) {
            // Add all received messages to the queue
            messages.forEach(msg -> logEntryQueue.add(msg));
            return true;
        }

        return false;
    }

    @Override
    public boolean complete(UUID snapshotSyncId) { return true; }

    @Override
    public void onError(LogReplicationError error, UUID snapshotSyncId) {

    }
}
