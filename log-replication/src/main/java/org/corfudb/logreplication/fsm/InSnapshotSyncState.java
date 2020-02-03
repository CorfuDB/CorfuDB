package org.corfudb.logreplication.fsm;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.logreplication.transmitter.SnapshotReader;

import java.util.concurrent.Future;

/**
 * A class that represents the in snapshot sync state of the Log Replication FSM.
 *
 * In this state full logs are being synced to the remote site, based on a snapshot timestamp.
 */
@Slf4j
public class InSnapshotSyncState implements LogReplicationState {

    LogReplicationContext context;

    SnapshotReader snapshotReader;

    Future<?> syncFuture;

    public InSnapshotSyncState(LogReplicationContext context) {
        this.context = context;
        this.snapshotReader = new SnapshotReader(context);
    }

    @Override
    public LogReplicationState processEvent(LogReplicationEvent event) {
        switch (event.getType()) {
            // Case where another snapshot (full) sync is requested.
            case SNAPSHOT_SYNC_REQUEST:
                /*
                  Cancel snapshot sync if still in progress, if sync cannot be canceled
                  we cannot transition to the new state.
                 */
                return cancelSnapshotSync("another snapshot sync request.") ?
                        new InSnapshotSyncState(context) : this;
            case SNAPSHOT_SYNC_CANCEL:
                 /*
                  Cancel snapshot sync if still in progress, if sync cannot be canceled
                  we cannot transition to the new state.
                 */
                return cancelSnapshotSync("a explicit cancel by app.") ?
                        new InRequireSnapshotSyncState(context) : this;
            case TRIMMED_EXCEPTION:
                return new InRequireSnapshotSyncState(context);
            case SNAPSHOT_SYNC_COMPLETE:
                return new InLogEntrySyncState(context);
            case REPLICATION_STOP:
                /*
                  Cancel snapshot sync if still in progress, if sync cannot be canceled
                  we cannot transition to the new state.
                 */
                return cancelSnapshotSync("request to stop replication.") ?
                        new InitializedState(context) : this;
            default: {
                log.warn("Unexpected log replication event {} when in snapshot sync state.", event.getType());
            }
        }
        return this;
    }

    private boolean cancelSnapshotSync(String cancelCause) {
        // Cancel snapshot sync if still in progress
        if (syncFuture != null && !syncFuture.isDone()) {
            boolean cancel = syncFuture.cancel(true);
            // Verify if task could not be canceled due to normal completion.
            if (!cancel && !syncFuture.isDone()) {
                log.error("Snapshot sync in progress could not be canceled.");
                return false;
            }
        }
        log.info("Snapshot sync has been canceled due to {}", cancelCause);
        return true;
    }

    @Override
    public void onEntry(LogReplicationState from) {
        // Execute snapshot transaction for every table to be replicated
        try {
            syncFuture = context.getBlockingOpsScheduler().submit(snapshotReader::sync);
        } catch (Throwable t) {
            log.error("Error on entry of InSnapshotSyncState.", t);
        }
    }

    @Override
    public void onExit(LogReplicationState to) {
    }

    @Override
    public LogReplicationStateType getType() {
        return LogReplicationStateType.IN_SNAPSHOT_SYNC;
    }
}
