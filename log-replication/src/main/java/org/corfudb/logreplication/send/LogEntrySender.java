package org.corfudb.logreplication.send;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.logreplication.DataSender;
import org.corfudb.logreplication.fsm.LogReplicationEvent;
import org.corfudb.logreplication.fsm.LogReplicationFSM;
import org.corfudb.logreplication.message.DataMessage;
import org.corfudb.runtime.CorfuRuntime;

/**
 * This class is responsible of managing the transmission of log entries,
 * i.e, reading and sending incremental updates to a remote site.
 *
 * It reads log entries from the datastore through the LogEntryReader, and sends them
 * through the LogEntryListener (the application specific callback).
 */
@Slf4j
public class LogEntrySender {

    private static final int READ_BATCH_SIZE = 5;
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
    private DataSender dataSender;

    /*
     * Log Replication FSM (to insert internal events)
     */
    private LogReplicationFSM logReplicationFSM;

    private ReadProcessor readProcessor;

    private volatile boolean taskActive = false;

    /**
     * Stop the send for Log Entry Sync
     */
    public void stop() {
        taskActive = false;
    }

    /**
     * Constructor
     *
     * @param runtime corfu runtime
     * @param logEntryReader log entry reader implementation
     * @param dataSender implementation of a data sender, both snapshot and log entry, this represents
     *                   the application callback for data transmission
     */
    public LogEntrySender(CorfuRuntime runtime, LogEntryReader logEntryReader, DataSender dataSender,
                          ReadProcessor readProcessor, LogReplicationFSM logReplicationFSM) {
        this.runtime = runtime;
        this.logEntryReader = logEntryReader;
        this.dataSender = dataSender;
        this.readProcessor = readProcessor;
        this.logReplicationFSM = logReplicationFSM;
    }

    /**
     * Read and send incremental updates (log entries)
     */
    public void transmit() {
        taskActive = true;
        int reads = 0;

        while (taskActive && reads < READ_BATCH_SIZE) {
            DataMessage message;

            // Read and Send Log Entries
            try {
                message = logEntryReader.read();
                // readProcessor.process(message);
                if (dataSender.send(message)) {
                    // Write meta-data
                    reads++;
                } else {
                    // ??
                    // Request full sync (something is wrong I cant deliver)
                    // (Optimization):
                    // Back-off for couple of seconds and retry n times if not require full sync
                }
            } catch (Exception e) {
                log.error("Caught exception at LogEntrySender", e);
                // Unrecoverable error, noisy streams found in transaction stream (streams of interest and others not
                // intended for replication). Shutdown.
                logReplicationFSM.input(new LogReplicationEvent(LogReplicationEvent.LogReplicationEventType.REPLICATION_SHUTDOWN));
            }
        }
    }
}
