package org.corfudb.logreplication.fsm;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.logreplication.DataControl;

import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * This class represents the InRequireSnapshotSync state of the Log Replication State Machine.
 *
 * This state is entered after a cancel or error (trim), and awaits for a signal to start snapshot sync again.
 */
@Slf4j
public class InRequireSnapshotSyncState implements LogReplicationState {

    /*
     * Log Replication Finite State Machine Instance
     */
    private final LogReplicationFSM fsm;

    /*
     * Used to schedule verification of the sent request, if not, resend snapshot sync request
     */
    private ScheduledExecutorService resendSnapshotSyncRequest;

    /*
     * Resend Snapshot Sync Request Timeout
     */
    private final int RESEND_REQUEST_TIMEOUT = 600;

    /*
     * Event that caused the transition to this state
     */
    private UUID transitionEventId;

    /*
     * Count reschedules, used for testing purposes.
     */
    private int rescheduleCounter = 0;

    @Getter
    private ObservableValue rescheduleCount = new ObservableValue(rescheduleCounter);

    /*
     * Data Control Implementation
     */
    private final DataControl dataControl;

    public InRequireSnapshotSyncState(@NonNull LogReplicationFSM logReplicationFSM, @NonNull DataControl dataControl) {
        this.fsm = logReplicationFSM;
        this.dataControl = dataControl;
        this.resendSnapshotSyncRequest = Executors.newSingleThreadScheduledExecutor(new
                ThreadFactoryBuilder().setNameFormat("scheduled-snapshot-request").build());
    }

    @Override
    public LogReplicationState processEvent(LogReplicationEvent event) throws IllegalTransitionException {
        switch (event.getType()) {
            case SNAPSHOT_SYNC_REQUEST:
                LogReplicationState snapshotSyncState = fsm.getStates().get(LogReplicationStateType.IN_SNAPSHOT_SYNC);
                snapshotSyncState.setTransitionEventId(event.getEventID());
                return snapshotSyncState;
            case REPLICATION_STOP:
                return fsm.getStates().get(LogReplicationStateType.INITIALIZED);
            case REPLICATION_SHUTDOWN:
                return fsm.getStates().get(LogReplicationStateType.STOPPED);
            default: {
                log.warn("Unexpected log replication event {} when in require snapshot send state.", event.getType());
                throw new IllegalTransitionException(event.getType(), getType());
            }
        }
    }

    @Override
    public void onEntry(LogReplicationState from) {
        requestSnapshotSync();
    }

    private void requestSnapshotSync() {
        log.info("Request Snapshot Sync to the application");
        rescheduleCounter++;
        rescheduleCount.setValue(rescheduleCounter);
        dataControl.requestSnapshotSync();
        // We will remain in this state until the application triggers a snapshot sync as we have determined
        // it is required due to an error in the current log replication.
        // For this reason, we schedule a verification of this request, as the message might have been dropped.
        // If we have not transitioned to a state of log replication, resend the request.
        resendSnapshotSyncRequest.schedule(() -> verifyPendingRequest(transitionEventId),
                RESEND_REQUEST_TIMEOUT, TimeUnit.MILLISECONDS);
    }

    @Override
    public LogReplicationStateType getType() {
        return LogReplicationStateType.IN_REQUIRE_SNAPSHOT_SYNC;
    }

    @Override
    public void setTransitionEventId(UUID transitionEventId) {
        this.transitionEventId = transitionEventId;
    }

    @Override
    public UUID getTransitionEventId() {
        return this.transitionEventId;
    }

    private void verifyPendingRequest(UUID pendingEventId) {
        if(fsm.getState().getType() == this.getType() && pendingEventId == transitionEventId) {
            log.info("Snapshot Sync Request {} is pending. Application has not reacted to this request.", transitionEventId);
            requestSnapshotSync();
            return;
        }

        // Refresh the scheduler even though it doesn't count as a re-schedule so observers can notice this
        rescheduleCount.setValue(rescheduleCounter);
        log.trace("Snapshot Sync Request {} has already been triggered by application. Re-scheduling not required.",
                pendingEventId);
    }
}
