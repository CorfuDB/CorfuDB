package org.corfudb.infrastructure.logreplication.runtime.fsm;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.replication.LogReplicationSourceManager;
import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationEvent;
import org.corfudb.infrastructure.logreplication.runtime.CorfuLogReplicationRuntime;

import java.util.UUID;

/**
 * Log Replication Runtime Replicating State.
 *
 * During this state, data logs are being replicated across clusters,
 *
 * @author amartinezman
 */
@Slf4j
public class ReplicatingState implements LogReplicationRuntimeState {

    private CorfuLogReplicationRuntime fsm;
    private LogReplicationSourceManager replicationSourceManager;
    private LogReplicationEvent replicationEvent;

    public ReplicatingState(CorfuLogReplicationRuntime fsm, LogReplicationSourceManager sourceManager) {
        this.fsm = fsm;
        this.replicationSourceManager = sourceManager;
    }

    @Override
    public LogReplicationRuntimeStateType getType() {
        return LogReplicationRuntimeStateType.REPLICATING;
    }

    @Override
    public LogReplicationRuntimeState processEvent(LogReplicationRuntimeEvent event) throws IllegalTransitionException {
        switch (event.getType()) {
            case ON_CONNECTION_DOWN:
                String endpointDown = event.getEndpoint();
                // Update list of valid connections.
                fsm.updateDisconnectedEndpoints(endpointDown);

                // If the leader is the node that become unavailable, verify new leader and attempt to reconnect.
                if (fsm.getRemoteLeader().isPresent() && fsm.getRemoteLeader().get().equals(endpointDown)) {
                    log.warn("Connection to remote leader endpoint={} is down. Attempt to reconnect.", endpointDown);
                    fsm.resetRemoteLeaderEndpoint();
                    // If remaining connections verify leadership on connected endpoints, otherwise, return to init
                    // state, until a connection is available.
                    return fsm.getConnectedEndpoints().size() == 0 ? fsm.getStates().get(LogReplicationRuntimeStateType.WAITING_FOR_CONNECTIVITY) :
                            fsm.getStates().get(LogReplicationRuntimeStateType.VERIFYING_REMOTE_LEADER);
                }

                log.debug("Connection lost to non-leader node {}", endpointDown);
                // If a non-leader node loses connectivity, reconnect async and continue.
                return null;
            case ON_CONNECTION_UP:
                // Some node got connected, update connected endpoints
                fsm.updateConnectedEndpoints(event.getEndpoint());
                return null;
            case LOCAL_LEADER_LOSS:
                return fsm.getStates().get(LogReplicationRuntimeStateType.STOPPED);
            default: {
                log.warn("Unexpected communication event {} when in init state.", event.getType());
                throw new IllegalTransitionException(event.getType(), getType());
            }
        }
    }

    @Override
    public void onEntry(LogReplicationRuntimeState from) {
        switch (replicationEvent.getType()) {
            case SNAPSHOT_SYNC_REQUEST:
                UUID snapshotSyncRequestId = replicationSourceManager.startSnapshotSync();
                log.trace("Start Snapshot Sync[{}]", snapshotSyncRequestId);
                break;
            case SNAPSHOT_TRANSFER_COMPLETE:
                replicationSourceManager.resumeSnapshotSync(replicationEvent.getMetadata());
                log.trace("Wait Snapshot Sync to complete, request={}", replicationEvent.getMetadata().getRequestId());
                break;
            case LOG_ENTRY_SYNC_REQUEST:
                replicationSourceManager.startReplication(replicationEvent);
                log.trace("Start Log Entry Sync Replication");
                break;
            default:
                log.info("Invalid Negotiation result. Re-trigger discovery.");
                break;
        }
    }

    @Override
    public void onExit(LogReplicationRuntimeState to) {
        log.debug("Transition to {} from replicating state.", to.getType());
        switch (to.getType()) {
            case STOPPED:
                log.debug("onExit :: transition to stopped state");
                replicationSourceManager.shutdown();
                break;
            case VERIFYING_REMOTE_LEADER:
            case WAITING_FOR_CONNECTIVITY:
                log.debug("onExit :: transition to {} state", to.getType());
                replicationSourceManager.stopLogReplication();
                break;
            default:
                break;
        }
    }

    /**
     * Set Negotiation Result, obtained during negotiation phase. This will
     * trigger the Log Replication FSM into the correct starting state: snapshot or log entry sync.
     *
     * @param replicationEvent
     */
    public void setReplicationEvent(LogReplicationEvent replicationEvent) {
        this.replicationEvent = replicationEvent;
    }

}
