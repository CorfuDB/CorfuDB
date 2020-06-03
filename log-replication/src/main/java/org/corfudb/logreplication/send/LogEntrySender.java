package org.corfudb.logreplication.send;

import lombok.extern.slf4j.Slf4j;

import org.corfudb.infrastructure.logreplication.DataSender;
import org.corfudb.infrastructure.logreplication.LogReplicationError;
import org.corfudb.logreplication.fsm.LogReplicationEvent;
import org.corfudb.logreplication.fsm.LogReplicationFSM;
import org.corfudb.logreplication.fsm.LogReplicationEvent.LogReplicationEventType;
import org.corfudb.logreplication.send.logreader.LogEntryReader;
import org.corfudb.logreplication.send.logreader.ReadProcessor;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.TrimmedException;

import java.util.UUID;

/**
 * This class is responsible of managing the transmission of log entries,
 * i.e, reading and sending incremental updates to a remote site.
 *
 * It reads log entries from the datastore through the LogEntryReader, and sends them
 * through LogReplicationSenderBuffer.
 */
@Slf4j
public class LogEntrySender {

    /*
     * Corfu Runtime
     */
    private CorfuRuntime runtime;

    /*
     * Implementation of Log Entry Reader. Default implementation reads at the stream layer.
     */
    private LogEntryReader logEntryReader;

   /*
    * Implementation of buffering messages and sending/resending messages
    */
    private SenderBufferManager dataSenderBufferManager;

    /*
     * Log Replication FSM (to insert internal events)
     */
    private LogReplicationFSM logReplicationFSM;


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
     * @param logEntryReader log entry logreader implementation
     * @param dataSender implementation of a data sender, both snapshot and log entry, this represents
     *                   the application callback for data transmission
     */
    public LogEntrySender(CorfuRuntime runtime, LogEntryReader logEntryReader, DataSender dataSender,
                          ReadProcessor readProcessor, LogReplicationFSM logReplicationFSM) {

        this.runtime = runtime;
        this.logEntryReader = logEntryReader;
        this.dataSenderBufferManager = new LogEntrySenderBufferManager(dataSender);
        this.logReplicationFSM = logReplicationFSM;
    }

    /**
     * Read and send incremental updates (log entries)
     */
    public void send(UUID logEntrySyncEventId) {
        taskActive = true;

        try {
            LogReplicationEntry ack = dataSenderBufferManager.resend();
            if (ack != null) {
                logReplicationFSM.input(new LogReplicationEvent(LogReplicationEventType.LOG_ENTRY_SYNC_REPLICATED,
                        new LogReplicationEventMetadata(ack.getMetadata().getSyncRequestId(), ack.getMetadata().getTimestamp())));
            }
        } catch (LogEntrySyncTimeoutException te) {
            log.error("LogEntrySyncTimeoutException after several retries.", te);
            cancelLogEntrySync(LogReplicationError.LOG_ENTRY_ACK_TIMEOUT, LogReplicationEventType.SYNC_CANCEL, logEntrySyncEventId);
            return;
        }

        while (taskActive && !dataSenderBufferManager.getPendingEntries().isFull()) {
            LogReplicationEntry message;

            // Read and Send Log Entries
            try {
                message = logEntryReader.read(logEntrySyncEventId);

                if (message != null) {
                    dataSenderBufferManager.sendWithBuffering(message);
                } else {
                    // If no message is returned we can break out and enqueue a CONTINUE, so other processes can
                    // take over the shared thread pool of the state machine
                    taskActive = false;
                    break;

                    // Request full sync (something is wrong I cant deliver)
                    // (Optimization):
                    // Back-off for couple of seconds and retry n times if not require full sync
                }

            } catch (TrimmedException te) {
                log.error("Caught Trimmed Exception while reading for {}", logEntrySyncEventId);
                cancelLogEntrySync(LogReplicationError.TRIM_LOG_ENTRY_SYNC, LogReplicationEvent.LogReplicationEventType.SYNC_CANCEL, logEntrySyncEventId);
                return;
            } catch (IllegalTransactionStreamsException se) {
                // Unrecoverable error, noisy streams found in transaction stream (streams of interest and others not
                // intended for replication). Shutdown.
                log.error("IllegalTransactionStreamsException, log replication will be TERMINATED.", se);
                cancelLogEntrySync(LogReplicationError.ILLEGAL_TRANSACTION, LogReplicationEventType.REPLICATION_SHUTDOWN, logEntrySyncEventId);
                return;
            } catch (Exception e) {
                log.error("Caught exception at LogEntrySender", e);
                cancelLogEntrySync(LogReplicationError.UNKNOWN, LogReplicationEventType.SYNC_CANCEL, logEntrySyncEventId);
                return;
            }
        }

        logReplicationFSM.input(new LogReplicationEvent(LogReplicationEvent.LogReplicationEventType.LOG_ENTRY_SYNC_CONTINUE,
                new LogReplicationEventMetadata(logEntrySyncEventId)));
    }

    private void cancelLogEntrySync(LogReplicationError error, LogReplicationEventType transition, UUID logEntrySyncEventId) {
        dataSenderBufferManager.onError(error);
        logReplicationFSM.input(new LogReplicationEvent(transition, new LogReplicationEventMetadata(logEntrySyncEventId)));
    }

    public void updateAckTs(long ts) {
        dataSenderBufferManager.updateAckTs(ts);
    }

    /**
     * Reset the log entry sender to initial state
     */
    public void reset(long lastSentBaseSnapshotTimestamp, long lastAckedTimestamp) {
        taskActive = true;
        log.info("Reset baseSnapshot %s ackTs %s", lastSentBaseSnapshotTimestamp, lastAckedTimestamp);
        logEntryReader.reset(lastSentBaseSnapshotTimestamp, lastAckedTimestamp);
        dataSenderBufferManager.reset(lastAckedTimestamp);
    }
}
