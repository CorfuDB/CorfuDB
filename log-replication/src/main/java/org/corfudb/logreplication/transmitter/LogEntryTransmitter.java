package org.corfudb.logreplication.transmitter;

import org.corfudb.runtime.CorfuRuntime;

/**
 * This class is responsible of managing the transmission of log entries,
 * i.e, reading and sending incremental updates to a remote site.
 *
 * It reads log entries from the datastore through the LogEntryReader, and sends them
 * through the LogEntryListener (the application specific callback).
 */
public class LogEntryTransmitter {

    /*
     * Corfu Runtime
     */
    private CorfuRuntime runtime;

    /*
     * Implementation of Log Entry Reader. Default implementation reads at the stream layer.
     */
    private LogEntryReader logEntryReader;

    /*
     * Log Entry Listener, application callback to send out reads.
     */
    private LogEntryListener logEntryListener;

    /**
     * Constructor
     *
     * @param runtime corfu runtime
     * @param logEntryReader log entry reader implementation
     * @param logEntryListener log entry listener implementation (application callback)
     */
    public LogEntryTransmitter(CorfuRuntime runtime, LogEntryReader logEntryReader, LogEntryListener logEntryListener) {
        this.runtime = runtime;
        this.logEntryReader = logEntryReader;
        this.logEntryListener = logEntryListener;
    }

    /**
     * Read and send incremental updates (log entries)
     */
    public void transmit() {
            // TODO
    }
}
