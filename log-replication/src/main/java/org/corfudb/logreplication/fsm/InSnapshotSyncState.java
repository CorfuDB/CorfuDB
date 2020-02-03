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
                // Add logic to cancel previous snapshot sync
                return new InSnapshotSyncState(context);
            case SNAPSHOT_SYNC_CANCEL:
                return new InRequireSnapshotSyncState(context);
            case TRIMMED_EXCEPTION:
                return new InRequireSnapshotSyncState(context);
            case SNAPSHOT_SYNC_COMPLETE:
                return new InLogEntrySyncState(context);
            case REPLICATION_STOP:
                return new InitializedState(context);
            default: {
                log.warn("Unexpected log replication event {} when in snapshot sync state.", event.getType());
            }
        }
        return this;
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
        switch (to.getType()) {
            case INITIALIZED:
                // Cancel snapshot sync if still in progress
                if (syncFuture != null && !syncFuture.isDone()) {
                    syncFuture.cancel(true);
                    log.info("Snapshot sync has been canceled due to request to stop replication.");
                }
            case IN_REQUIRE_SNAPSHOT_SYNC:
                // Cancel snapshot sync if still in progress
                if (syncFuture != null && !syncFuture.isDone()) {
                    syncFuture.cancel(true);
                    log.info("Snapshot sync has been canceled due to a explicit cancel by app.");
                }
            case IN_SNAPSHOT_SYNC:
                // Cancel snapshot sync if still in progress
                if (syncFuture != null && !syncFuture.isDone()) {
                    syncFuture.cancel(true);
                    log.info("Snapshot sync has been canceled due to another snapshot sync request.");
                }
        }
    }

    @Override
    public LogReplicationStateType getType() {
        return LogReplicationStateType.IN_SNAPSHOT_SYNC;
    }
}
