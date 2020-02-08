package org.corfudb.logreplication.fsm;

import org.corfudb.logreplication.transmitter.LogEntryReader;
import org.corfudb.logreplication.transmitter.DataMessage;

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
}
