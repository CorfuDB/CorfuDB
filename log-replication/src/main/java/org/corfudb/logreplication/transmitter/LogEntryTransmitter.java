package org.corfudb.logreplication.transmitter;

import org.corfudb.runtime.CorfuRuntime;

public class LogEntryTransmitter {

    private CorfuRuntime runtime;
    private LogEntryReader logEntryReader;
    private LogEntryListener logEntryListener;

    public LogEntryTransmitter(CorfuRuntime runtime, LogEntryReader logEntryReader, LogEntryListener logEntryListener) {
        this.runtime = runtime;
        this.logEntryReader = logEntryReader;
        this.logEntryListener = logEntryListener;
    }

    public void transmit() {

    }
}
