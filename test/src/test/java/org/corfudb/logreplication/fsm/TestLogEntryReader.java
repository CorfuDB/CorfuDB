package org.corfudb.logreplication.fsm;

import org.corfudb.logreplication.send.LogEntryReader;
import org.corfudb.logreplication.message.DataMessage;

/**
 * Test Implementation of Log Entry Reader
 */
public class TestLogEntryReader implements LogEntryReader {

    public TestLogEntryReader() {

    }

    @Override
    public DataMessage read() {
        return new DataMessage();
    }

    @Override
    public void reset(long lastSentBaseSnapshotTimestamp, long lastAckedTimestamp) {
        // Read everything from start
    }
}
