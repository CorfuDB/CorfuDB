package org.corfudb.infrastructure.logreplication.replication.send;

import io.micrometer.core.instrument.Tag;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MeterRegistryProvider;
import org.corfudb.infrastructure.logreplication.DataSender;
import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationEvent;
import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationEvent.LogReplicationEventType;
import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationFSM;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.LogEntryReader;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.ReadProcessor;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;
import org.corfudb.runtime.exceptions.TrimmedException;

import java.util.UUID;

import static org.corfudb.protocols.CorfuProtocolCommon.getUUID;

/**
 * This class is responsible of managing the transmission of log entries,
 * i.e, reading and sending incremental updates to a remote cluster.
 * <p>
 * It reads log entries from the datastore through the LogEntryReader, and sends them
 * through LogReplicationSenderBuffer.
 */
@Slf4j
public class LogEntrySender {

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
     * @param logEntryReader    log entry logreader implementation
     * @param dataSender        implementation of a data sender, both snapshot and log entry, this represents
     *                          the application callback for data transmission
     * @param readProcessor     post read processing logic
     * @param logReplicationFSM log replication FSM to insert events upon message acknowledgement
     */
    public LogEntrySender(LogEntryReader logEntryReader, DataSender dataSender,
                          ReadProcessor readProcessor, LogReplicationFSM logReplicationFSM) {

        this.logEntryReader = logEntryReader;
        this.logReplicationFSM = logReplicationFSM;
        this.dataSenderBufferManager = new LogEntrySenderBufferManager(dataSender, logReplicationFSM.getAckReader());
    }

    /**
     * Read and send incremental updates (log entries)
     *
     * @param logEntrySyncEventId
     */
    public void send(UUID logEntrySyncEventId) {

        log.trace("Send Log Entry Sync, id={}", logEntrySyncEventId);

        taskActive = true;

        try {
            /*
             * It will first resend entries in the buffer that hasn't ACKed
             */
            LogReplicationEntryMsg ack = dataSenderBufferManager.resend();
            if (ack != null) {
                logReplicationFSM.input(new LogReplicationEvent(LogReplicationEventType.LOG_ENTRY_SYNC_REPLICATED,
                        new LogReplicationEventMetadata(getUUID(ack.getMetadata().getSyncRequestId()), ack.getMetadata().getTimestamp())));
            }
        } catch (LogEntrySyncTimeoutException te) {
            log.error("LogEntrySyncTimeoutException after several retries.", te);
            cancelLogEntrySync(LogReplicationError.LOG_ENTRY_ACK_TIMEOUT, LogReplicationEventType.SYNC_CANCEL, logEntrySyncEventId);
            return;
        }

        while (taskActive && !dataSenderBufferManager.getPendingMessages().isFull()) {
            LogReplicationEntryMsg message;

            /*
             * Read and Send Log Entries
             */
            try {
                message = logEntryReader.read(logEntrySyncEventId);
                if (message != null) {
                    if (MeterRegistryProvider.getInstance().isPresent()) {
                        dataSenderBufferManager.sendWithBuffering(message, "logreplication.sender.duration.seconds",
                                Tag.of("replication.type", "logentry"));
                    }
                    else {
                        dataSenderBufferManager.sendWithBuffering(message);
                    }

                } else {
                    /*
                     * If no message is returned we can break out and enqueue a CONTINUE, so other processes can
                     * take over the shared thread pool of the state machine.
                     */
                    taskActive = false;
                    break;
                    // Request full sync (something is wrong I cant deliver)
                    // (Optimization):
                    // Back-off for couple of seconds and retry n times if not require full sync
                }

                if (logEntryReader.hasMessageExceededSize()) {
                    cancelLogEntrySync(LogReplicationError.LOG_ENTRY_MESSAGE_SIZE_EXCEEDED, LogReplicationEventType.REPLICATION_SHUTDOWN, logEntrySyncEventId);
                    return;
                }

            } catch (TrimmedException te) {
                log.error("Caught Trimmed Exception while reading for {}", logEntrySyncEventId);
                cancelLogEntrySync(LogReplicationError.TRIM_LOG_ENTRY_SYNC, LogReplicationEvent.LogReplicationEventType.SYNC_CANCEL, logEntrySyncEventId);
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

    /**
     * Generate a CancelLogEntrySync Event due to error.
     *
     * @param error
     * @param transition
     * @param logEntrySyncEventId
     */
    private void cancelLogEntrySync(LogReplicationError error, LogReplicationEventType transition, UUID logEntrySyncEventId) {
        dataSenderBufferManager.onError(error);
        logReplicationFSM.input(new LogReplicationEvent(transition, new LogReplicationEventMetadata(logEntrySyncEventId)));
    }

    /**
     * Reset the log entry sender to initial state
     */
    public void reset(long lastSentBaseSnapshotTimestamp, long lastAckedTimestamp) {
        taskActive = true;
        log.info("Reset baseSnapshot {} maxAckTimestamp {}", lastSentBaseSnapshotTimestamp, lastAckedTimestamp);
        logEntryReader.reset(lastSentBaseSnapshotTimestamp, lastAckedTimestamp);
        dataSenderBufferManager.reset(lastAckedTimestamp);
    }

    public void updateTopologyConfigId(long topologyConfigId) {
        dataSenderBufferManager.updateTopologyConfigId(topologyConfigId);
    }
}
