package org.corfudb.infrastructure.logreplication.runtime.fsm;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.replication.LogReplicationSourceManager;
import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationEvent;
import org.corfudb.infrastructure.logreplication.runtime.CorfuLogReplicationRuntime;

import java.util.UUID;


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
                if (fsm.getLeader().isPresent() && fsm.getLeader().get().equals(endpointDown)) {
                    // If remaining connections verify leadership on connected endpoints, otherwise, return to init
                    // state, until a connection is available.
                    return fsm.getConnectedEndpoints().size() == 0 ? fsm.getStates().get(LogReplicationRuntimeStateType.WAITING_FOR_CONNECTIVITY) :
                            fsm.getStates().get(LogReplicationRuntimeStateType.VERIFYING_REMOTE_LEADER);
                }

                // If a non-leader node loses connectivity, reconnect async and continue.
                return this;
            case ON_CONNECTION_UP:
                // Some node got connected, update connected endpoints
                fsm.updateConnectedEndpoints(event.getEndpoint());
                return this;
            case NEGOTIATION_COMPLETE:
                return fsm.getStates().get(LogReplicationRuntimeStateType.REPLICATING);
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
                log.info("Start Snapshot Sync[{}]", snapshotSyncRequestId);
                break;
            case SNAPSHOT_WAIT_COMPLETE:
                // TODO :: Phase II
                log.warn("Should Start Snapshot Sync Phase II,but for now just restart full snapshot sync");
                UUID snapshotSyncId = replicationSourceManager.startSnapshotSync();
                log.info("Start Snapshot Sync[{}]", snapshotSyncId);
                break;
            case REPLICATION_START:
                log.info("Start Log Entry Sync Replication");
                replicationSourceManager.startReplication(replicationEvent);
                break;
            default:
                log.info("Invalid Negotiation result. Re-trigger discovery.");
                break;
        }
    }

    @Override
    public void onExit(LogReplicationRuntimeState to) {
        if (to.getType().equals(LogReplicationRuntimeStateType.STOPPED)) {
            replicationSourceManager.shutdown();
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
