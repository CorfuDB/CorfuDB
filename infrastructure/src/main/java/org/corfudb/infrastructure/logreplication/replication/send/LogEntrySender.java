package org.corfudb.infrastructure.logreplication.replication.send;

import io.micrometer.core.instrument.Tag;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MeterRegistryProvider;
import org.corfudb.infrastructure.logreplication.DataSender;
import org.corfudb.infrastructure.logreplication.exceptions.GroupDestinationChangeException;
import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationEvent;
import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationEvent.LogReplicationEventType;
import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationFSM;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.LogEntryReader;
import org.corfudb.infrastructure.logreplication.exceptions.MessageSizeExceededException;
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
    private final LogEntryReader logEntryReader;

    /*
     * Implementation of buffering messages and sending/resending messages
     */
    @Getter
    private final SenderBufferManager dataSenderBufferManager;

    /*
     * Log Replication FSM (to insert internal events)
     */
    private final LogReplicationFSM logReplicationFSM;

    private volatile boolean taskActive = false;

    // TODO V2: These values need to be tuned as to not add any unnecessary increase in latency
    // Duration in milliseconds to delay the execution of the next read + send operation.
    private final long WAIT_RETRY_READ_DEFAULT_MS = 100;
    private final long WAIT_RETRY_READ_INCREMENT_MS = 50;
    private final long WAIT_RETRY_READ_MAX_MS = 200;
    private long waitRetryRead = WAIT_RETRY_READ_DEFAULT_MS;

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
     * @param logReplicationFSM log replication FSM to insert events upon message acknowledgement
     */
    public LogEntrySender(LogEntryReader logEntryReader, DataSender dataSender, LogReplicationFSM logReplicationFSM) {

        this.logEntryReader = logEntryReader;
        this.logReplicationFSM = logReplicationFSM;
        this.dataSenderBufferManager = new LogEntrySenderBufferManager(dataSender, logReplicationFSM.getAckReader());
    }

    /**
     * Read and send incremental updates (log entries)
     *
     * @param logEntrySyncEventId Transition event id that cased FSM transits into IN_LOG_ENTRY_SYNC state.
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
                        new LogReplicationEventMetadata(getUUID(ack.getMetadata().getSyncRequestId()),
                                ack.getMetadata().getTimestamp()),
                        logReplicationFSM));
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
                        dataSenderBufferManager.sendWithBuffering(message, "logreplication.sender.duration.nanoseconds",
                                Tag.of("replication.type", "logentry"));
                    } else {
                        dataSenderBufferManager.sendWithBuffering(message);
                    }

                } else {
                    /*
                     * If no message is returned we can break out and enqueue a CONTINUE, so other processes can
                     * take over the shared thread pool of the state machine.
                     */
                    taskActive = false;
                    break;
                }
            } catch (TrimmedException te) {
                log.error("Caught Trimmed Exception while reading for {}", logEntrySyncEventId);
                cancelLogEntrySync(LogReplicationError.TRIM_LOG_ENTRY_SYNC,
                        LogReplicationEventType.SYNC_CANCEL, logEntrySyncEventId);
                return;
            } catch (MessageSizeExceededException me) {
                log.error("Caught Message Size Exceeded Exception while reading for {}", logEntrySyncEventId);
                cancelLogEntrySync(LogReplicationError.LOG_ENTRY_MESSAGE_SIZE_EXCEEDED,
                        LogReplicationEventType.SYNC_CANCEL, logEntrySyncEventId);
                return;
            } catch (GroupDestinationChangeException gce) {
                cancelLogEntrySync(LogReplicationError.GROUP_DESTINATION_CHANGE,
                        LogReplicationEventType.SYNC_CANCEL, logEntrySyncEventId);
                return;
            } catch (Exception e) {
                log.error("Caught exception at log entry sender", e);
                cancelLogEntrySync(LogReplicationError.UNKNOWN, LogReplicationEventType.SYNC_CANCEL, logEntrySyncEventId);
                return;
            }
        }

        if (taskActive) {
            logReplicationFSM.input(new LogReplicationEvent(LogReplicationEvent.LogReplicationEventType.LOG_ENTRY_SYNC_CONTINUE,
                    new LogReplicationEventMetadata(logEntrySyncEventId), logReplicationFSM));
            waitRetryRead = WAIT_RETRY_READ_DEFAULT_MS;
        } else {
            logReplicationFSM.inputWithDelay(new LogReplicationEvent(LogReplicationEvent.LogReplicationEventType.LOG_ENTRY_SYNC_CONTINUE,
                    new LogReplicationEventMetadata(logEntrySyncEventId), logReplicationFSM), waitRetryRead);
            waitRetryRead = Math.min(waitRetryRead + WAIT_RETRY_READ_INCREMENT_MS, WAIT_RETRY_READ_MAX_MS);
        }
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
        logReplicationFSM.input(new LogReplicationEvent(transition, new LogReplicationEventMetadata(logEntrySyncEventId),
                logReplicationFSM));
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

    public int getPendingACKQueueSize() {
        return dataSenderBufferManager.getPendingMessages().getSize();
    }
}
