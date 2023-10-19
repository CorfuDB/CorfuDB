package org.corfudb.infrastructure.logreplication.runtime.fsm;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.runtime.CorfuLogReplicationRuntime;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationClientServerRouter;

import java.util.concurrent.ThreadPoolExecutor;

import static org.corfudb.infrastructure.logreplication.runtime.fsm.LogReplicationFsmUtil.canEnqueueStopRuntimeFsmEvent;
import static org.corfudb.infrastructure.logreplication.runtime.fsm.LogReplicationFsmUtil.verifyRemoteLeader;

/**
 * Log Replication Runtime Verifying Remote Leader State.
 *
 * In this state the leader node in the remote cluster is identified.
 *
 * @author amartinezman
 */
@Slf4j
public class VerifyingRemoteSinkLeaderState implements LogReplicationRuntimeState {

    private CorfuLogReplicationRuntime fsm;

    private LogReplicationClientServerRouter router;

    public VerifyingRemoteSinkLeaderState(CorfuLogReplicationRuntime fsm, LogReplicationClientServerRouter router) {
        this.fsm = fsm;
        this.router = router;
    }

    @Override
    public LogReplicationRuntimeStateType getType() {
        return LogReplicationRuntimeStateType.VERIFYING_REMOTE_LEADER;
    }

    @Override
    public LogReplicationRuntimeState processEvent(LogReplicationRuntimeEvent event) throws IllegalTransitionException {
        switch (event.getType()) {
            case REMOTE_LEADER_FOUND:
                ((NegotiatingState)fsm.getStates().get(LogReplicationRuntimeStateType.NEGOTIATING)).setLeaderNodeId(event.getNodeId());
                return fsm.getStates().get(LogReplicationRuntimeStateType.NEGOTIATING);
            case ON_CONNECTION_DOWN:
                String nodeIdDown = event.getNodeId();
                log.debug("Detected connection down from node={}", nodeIdDown);
                fsm.updateDisconnectedNodes(nodeIdDown);

                // If no connection exists, return to init state, until a connection is established.
                if (fsm.getConnectedNodes().size() == 0) {
                    return fsm.getStates().get(LogReplicationRuntimeStateType.WAITING_FOR_CONNECTIVITY);
                }
                return this;
            case REMOTE_LEADER_NOT_FOUND:
                return this;
            case ON_CONNECTION_UP:
                log.debug("Detected connection up from endpoint={}", event.getNodeId());
                // Add new connected node, for leadership verification
                fsm.updateConnectedNodes(event.getNodeId());
                return this;
            case LOCAL_LEADER_LOSS:
                if (canEnqueueStopRuntimeFsmEvent(router, fsm, event.isConnectionStarter())) {
                    return fsm.getStates().get(LogReplicationRuntimeStateType.STOPPED);
                }
                return null;
            default: {
                log.warn("Unexpected communication event {} when in init state.", event.getType());
                throw new IllegalTransitionException(event.getType(), getType());
            }
        }
    }

    @Override
    public void onEntry(LogReplicationRuntimeState from) {
        log.debug("onEntry :: Verifying Remote Leader, transition from {}", from.getType());

        // Proceed if remoteLeader is known. Only if local is connection starter for the session and the remoteLeader is
        // not known, verify Leadership on connected nodes
        if (fsm.getRemoteLeaderNodeId().isPresent()) {
            fsm.input(new LogReplicationRuntimeEvent(
                    LogReplicationRuntimeEvent.LogReplicationRuntimeEventType.REMOTE_LEADER_FOUND,
                    fsm.getRemoteLeaderNodeId().get(), fsm)
            );
            log.debug("Exit :: leadership verification");
        } else if (router.isConnectionStarterForSession(fsm.session)){
            // Leadership verification is done only if connection starter.
            verifyRemoteLeader(fsm, fsm.getConnectedNodes(), fsm.getRemoteClusterId(), router,
                    CorfuLogReplicationRuntime.class);
        }
    }
}
