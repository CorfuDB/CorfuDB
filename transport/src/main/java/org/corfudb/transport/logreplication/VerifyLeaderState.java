package org.corfudb.transport.logreplication;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.cluster.NodeDescriptor;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationQueryLeaderShipResponse;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Communication FSM Verify Leader State.
 *
 * In this state the leader node in the remote cluster is identified.
 *
 * @author amartinezman
 */
@Slf4j
public class VerifyLeaderState implements CommunicationState {

    private static final int LEADERSHIP_RETRIES = 5;

    private LogReplicationCommunicationFSM fsm;

    private ExecutorService workers;

    private LogReplicationClientRouter router;

    public VerifyLeaderState(LogReplicationCommunicationFSM fsm, ExecutorService workers, LogReplicationClientRouter router) {
        this.fsm = fsm;
        this.workers = workers;
        this.router = router;
    }

    @Override
    public CommunicationStateType getType() {
        return CommunicationStateType.VERIFY_LEADER;
    }

    @Override
    public CommunicationState processEvent(CommunicationEvent event) throws IllegalTransitionException {
        switch (event.getType()) {
            case LEADER_FOUND:
                ((NegotiateState)fsm.getStates().get(CommunicationStateType.NEGOTIATE)).setLeaderEndpoint(event.getEndpoint());
                return fsm.getStates().get(CommunicationStateType.NEGOTIATE);
            case CONNECTION_DOWN:
                String endpointDown = event.getEndpoint();
                fsm.updateDisconnectedEndpoints(endpointDown);
                fsm.reconnectAsync(endpointDown);

                // If no connection exists, return to init state, until a connection is established.
                if (fsm.getConnectedEndpoints().size() == 0) {
                    return fsm.getStates().get(CommunicationStateType.INIT);
                }
                return this;
            case LEADER_NOT_FOUND:
                return this;
            case CONNECTION_UP:
                // Add new connected node, for leadership verification
                fsm.updateConnectedEndpoints(event.getEndpoint());
                return this;
            default: {
                log.warn("Unexpected communication event {} when in init state.", event.getType());
                throw new IllegalTransitionException(event.getType(), getType());
            }
        }
    }

    @Override
    public void onEntry(CommunicationState from) {
        // Verify Leadership on connected nodes (ignore those for which leadership is pending)
        workers.submit(this::verifyLeadership);

    }


    /**
     * Verify who is the leader node on the remote cluster by sending leadership request to all nodes.
     * <p>
     * If no leader is found, the verification will be attempted for LEADERSHIP_RETRIES times.
     */
    public synchronized void verifyLeadership() {
        boolean leadershipVerified = false;
        String leader = "";

        Map<String, CompletableFuture<LogReplicationQueryLeaderShipResponse>> pendingLeadershipQueries = new HashMap<>();

        // Verify leadership on remote cluster, only if no leader is currently selected.
        if (!fsm.getLeader().isPresent()) {

            for (int i = 0; i < LEADERSHIP_RETRIES; i++) {
                // TODO: Do not re-attempt pending...
                log.info("Verify leadership on remote cluster, attempt={}", i);

                try {
                    for (String node : fsm.getConnectedEndpoints()) {
                        log.debug("Verify leadership status for node {}", node);
                        // Check Leadership
                        CompletableFuture<LogReplicationQueryLeaderShipResponse> leadershipRequestCf =
                                router.sendMessageAndGetCompletable(new CorfuMsg(CorfuMsgType.LOG_REPLICATION_QUERY_LEADERSHIP).setEpoch(0), node);
                        pendingLeadershipQueries.put(node, leadershipRequestCf);
                    }

                    // TODO: Anny should we block or wait for this to complete async...
                    // Block until all leadership requests are completed, or a leader is discovered.
                    while (!leadershipVerified || pendingLeadershipQueries.size() != 0) {
                        LogReplicationQueryLeaderShipResponse leadershipResponse = (LogReplicationQueryLeaderShipResponse) CompletableFuture.anyOf(pendingLeadershipQueries.values()
                                .toArray(new CompletableFuture<?>[pendingLeadershipQueries.size()])).get(LogReplicationCommunicationFSM.DEFAULT_TIMEOUT, TimeUnit.MILLISECONDS);

                        if (leadershipResponse.isLeader()) {
                            log.info("Leader for remote cluster, node={}", leadershipResponse.getEndpoint());
                            leader = leadershipResponse.getEndpoint();
                            fsm.setLeaderEndpoint(leader);
                            leadershipVerified = true;
                            // Remove all CF, based on the assumption that one leader response is the expectation.
                            pendingLeadershipQueries.clear();
                            break;
                        } else {
                            // Remove CF for completed request
                            pendingLeadershipQueries.remove(leadershipResponse.getEndpoint());
                        }
                    }

                    if (leadershipVerified) {
                        // A new leader has been found, start negotiation, to determine log replication
                        // continuation or start point
                        fsm.input(new CommunicationEvent(CommunicationEvent.CommunicationEventType.LEADER_FOUND, leader));
                        break;
                    }
                } catch (Exception ex) {
                    log.warn("Exception caught while verifying remote leader. Retry={}", i, ex);
                }
            }
        }
    }
}
