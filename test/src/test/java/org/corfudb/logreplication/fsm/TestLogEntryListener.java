package org.corfudb.logreplication.fsm;

import org.corfudb.logreplication.LogReplicationError;
import org.corfudb.logreplication.message.DataMessage;
import org.corfudb.logreplication.transmit.LogEntryListener;

import java.util.List;

/**
 * Test Implementation of Log Entry Listener
 */
public class TestLogEntryListener implements LogEntryListener {
    @Override
    public boolean onNext(DataMessage message) {
        return true;
    }

    @Override
    public boolean onNext(List<DataMessage> messages) {
        return true;
    }

    @Override
    public void onError(LogReplicationError error) {

    }
}
