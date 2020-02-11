package org.corfudb.logreplication.fsm;

import lombok.Getter;
import org.corfudb.logreplication.transmit.LogReplicationError;
import org.corfudb.logreplication.message.DataMessage;
import org.corfudb.logreplication.transmit.LogEntryListener;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * Test Implementation of Log Entry Listener
 */
public class TestLogEntryListener implements LogEntryListener {

    @Getter
    Queue<DataMessage> dataMessageQueue = new LinkedList<>();

    @Override
    public boolean onNext(DataMessage message) {
        return true;
    }

    @Override
    public boolean onNext(List<DataMessage> messages) {
        messages.forEach(msg -> dataMessageQueue.add(msg));
        return true;
    }

    @Override
    public void onError(LogReplicationError error) {

    }
}
