package org.corfudb.logreplication.fsm;

import lombok.Getter;
import org.corfudb.logreplication.LogReplicationError;
import org.corfudb.logreplication.message.DataMessage;
import org.corfudb.logreplication.transmit.SnapshotListener;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.UUID;


/**
 * Test Implementation of Snapshot Listener
 */
public class TestSnapshotListener implements SnapshotListener {

    @Getter
    private Queue<DataMessage> txQueue = new LinkedList<>();

    private TestTransmitterConfig config;

    public TestSnapshotListener(TestTransmitterConfig config) {
        this.config = config;
    }

    @Override
    public boolean onNext(DataMessage message, UUID snapshotSyncId) {
        txQueue.add(message);
        return true;
    }

    @Override
    public boolean onNext(List<DataMessage> messages, UUID snapshotSyncId) {
        // Add all received messages to the queue
        messages.forEach(msg -> txQueue.add(msg));

        return true;
    }

    @Override
    public boolean complete(UUID snapshotSyncId) { return true; }

    @Override
    public void onError(LogReplicationError error, UUID snapshotSyncId) {

    }

    public void clearQueue() {
        txQueue.clear();
    }
}
