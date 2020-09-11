package org.corfudb.infrastructure.logreplication.replication.fsm;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata;
import org.corfudb.infrastructure.logreplication.replication.send.LogEntrySender;

import java.util.UUID;
import java.util.concurrent.Future;

/**
 * This class represents the InLogEntrySync state of the Log Replication State Machine.
 *
 * In this state, incremental (delta) updates are being synced to the remote cluster.
 */
@Slf4j
public class InLogEntrySyncState implements LogReplicationState {

    /**
     * Log Replication State Machine, used to insert internal events into the queue.
     */
    private final LogReplicationFSM fsm;

    /**
     * Log Entry Sender, used to read and send incremental updates.
     */
    private LogEntrySender logEntrySender;

    /**
     * A future on the log entry send, send call.
     */
    private Future<?> logEntrySyncFuture;

    /**
     * Unique Identifier of the event that caused the transition to this state,
     * i.e., current event/request being processed.
     */
    private UUID transitionEventId;

    /**
     * Constructor
     *
     * @param logReplicationFSM log replication finite state machine
     * @param logEntrySender instance that will manage reading and sending log data between sites.
     */
    public InLogEntrySyncState(LogReplicationFSM logReplicationFSM, LogEntrySender logEntrySender) {
        this.fsm = logReplicationFSM;
        this.logEntrySender = logEntrySender;
    }

    @Override
    public LogReplicationState processEvent(LogReplicationEvent event) throws IllegalTransitionException {
        switch (event.getType()) {
            case SNAPSHOT_SYNC_REQUEST:
                cancelLogEntrySync("snapshot sync request.");
                LogReplicationState snapshotSyncState = fsm.getStates().get(LogReplicationStateType.IN_SNAPSHOT_SYNC);
                snapshotSyncState.setTransitionEventId(event.getEventID());
                return snapshotSyncState;
            case SYNC_CANCEL:
                // If cancel was intended for current log entry sync task, cancel and transition to new state
                // In the case of log entry sync, cancel is caused by an encountered trimmed exception or any
                // other exception while reading/sending.
                if (transitionEventId == event.getMetadata().getRequestId()) {
                    cancelLogEntrySync("sync cancel.");
                    LogReplicationState inSnapshotSyncState = fsm.getStates().get(LogReplicationStateType.IN_SNAPSHOT_SYNC);
                    inSnapshotSyncState.setTransitionEventId(UUID.randomUUID());
                    return inSnapshotSyncState;
                }

                log.warn("Log Entry Sync cancel for eventId {}, but running log entry sync for {}",
                        event.getEventID(), transitionEventId);
                return this;
            case REPLICATION_STOP:
                cancelLogEntrySync("replication being stopped.");
                return fsm.getStates().get(LogReplicationStateType.INITIALIZED);
            case REPLICATION_SHUTDOWN:
                cancelLogEntrySync("replication terminated.");
                return fsm.getStates().get(LogReplicationStateType.STOPPED);
            case LOG_ENTRY_SYNC_REPLICATED:
                // Verify the replicated entry corresponds to the current log entry sync cycle (and not a previous/old one)
                if (transitionEventId.equals(event.getMetadata().getRequestId())) {
                    log.debug("Log Entry Sync ACK, update last ack timestamp to {}", event.getMetadata().getLastLogEntrySyncedTimestamp());
                    fsm.setAckedTimestamp(event.getMetadata().getLastLogEntrySyncedTimestamp());
                }
                // Do not return a new state as there is no actual transition, the IllegalTransitionException
                // will allow us to avoid any transition from this state given the event.
                break;
            case LOG_ENTRY_SYNC_CONTINUE:
                // Snapshot sync is broken into multiple tasks, where each task sends a batch of messages
                // corresponding to this snapshot sync. This is done to accommodate the case
                // of multi-cluster replication sharing a common thread pool, continuation allows to send another
                // batch of updates for the current snapshot sync.
                if (event.getMetadata().getRequestId() == transitionEventId) {
                    log.trace("Continuation of log entry sync for {}", event.getEventID());
                    return this;
                } else {
                    log.warn("Unexpected log entry sync continue event {} when in log entry sync state {}.",
                            event.getEventID(), transitionEventId);
                }
            default: {
                log.warn("Unexpected log replication event {} when in log entry sync state.", event.getType());
                break;
            }
        }

        throw new IllegalTransitionException(event.getType(), getType());
    }

    /**
     * Cancel log entry sync task.
     *
     * @param cancelCause cancel cause specific to the caller for debug.
     */
    private void cancelLogEntrySync(String cancelCause) {
        // Stop log entry sync.
        // We can tolerate the last cycle of the sync being executed before stopped,
        // as snapshot sync is triggered by the app which handles separate listeners
        // for log entry sync and snapshot sync (app can handle this)
        logEntrySender.stop();
        if (!logEntrySyncFuture.isDone()) {
            try {
                logEntrySyncFuture.get();
            } catch (Exception e) {
                log.warn("Exception while waiting on log entry sync to complete.", e);
            }
        }
        log.info("Log Entry sync has been canceled due to {}", cancelCause);
    }

    @Override
    public void onEntry(LogReplicationState from) {
        // Execute snapshot transaction for every table to be replicated
        try {
            // Reset before start sending log entry data, only when we're coming
            // from snapshot sync or initialized state, this way we will seek the stream up to the base snapshot
            // address and send incremental updates from this point onwards.
            if (from.getType() == LogReplicationStateType.WAIT_SNAPSHOT_APPLY
                    || from.getType() == LogReplicationStateType.INITIALIZED) {
                // Set LogEntryAckReader to Log Entry Sync state, to compute remaining entries based
                // on the tx stream, regardless of ACKs or updates being processed for the tx stream
                fsm.getAckReader().setSyncType(LogReplicationMetadata.ReplicationStatusVal.SyncType.LOG_ENTRY);
                logEntrySender.reset(fsm.getBaseSnapshot(), fsm.getAckedTimestamp());
            }

            logEntrySyncFuture = fsm.getLogReplicationFSMWorkers().submit(() -> logEntrySender.send(transitionEventId));

        } catch (Throwable t) {
            log.error("Error on entry of InLogEntrySyncState", t);
        }
    }

    @Override
    public void setTransitionEventId(UUID eventId) {
        transitionEventId = eventId;
    }

    @Override
    public UUID getTransitionEventId() {
        return transitionEventId;
    }

    @Override
    public LogReplicationStateType getType() {
        return LogReplicationStateType.IN_LOG_ENTRY_SYNC;
    }
}
