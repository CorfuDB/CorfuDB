package org.corfudb.infrastructure.logreplication.runtime.fsm;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.runtime.CorfuLogReplicationRuntime;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationClientRouter;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationQueryLeaderShipResponse;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Log Replication Runtime Verifying Remote Leader State.
 *
 * In this state the leader node in the remote cluster is identified.
 *
 * @author amartinezman
 */
@Slf4j
public class VerifyingRemoteLeaderState implements LogReplicationRuntimeState {

    private CorfuLogReplicationRuntime fsm;

    private ThreadPoolExecutor worker;

    private LogReplicationClientRouter router;

    public VerifyingRemoteLeaderState(CorfuLogReplicationRuntime fsm, ThreadPoolExecutor worker, LogReplicationClientRouter router) {
        this.fsm = fsm;
        this.worker = worker;
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
                ((NegotiatingState)fsm.getStates().get(LogReplicationRuntimeStateType.NEGOTIATING)).setLeaderEndpoint(event.getEndpoint());
                return fsm.getStates().get(LogReplicationRuntimeStateType.NEGOTIATING);
            case ON_CONNECTION_DOWN:
                String endpointDown = event.getEndpoint();
                log.debug("Detected connection down from endpoint={}", endpointDown);
                fsm.updateDisconnectedEndpoints(endpointDown);

                // If no connection exists, return to init state, until a connection is established.
                if (fsm.getConnectedEndpoints().size() == 0) {
                    return fsm.getStates().get(LogReplicationRuntimeStateType.WAITING_FOR_CONNECTIVITY);
                }
                return this;
            case REMOTE_LEADER_NOT_FOUND:
                return this;
            case ON_CONNECTION_UP:
                log.debug("Detected connection up from endpoint={}", event.getEndpoint());
                // Add new connected node, for leadership verification
                fsm.updateConnectedEndpoints(event.getEndpoint());
                return this;
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
        log.debug("onEntry :: Verifying Remote Leader, transition from {}", from.getType());
        log.trace("Submitted tasks to worker :: size={} activeCount={} taskCount={}", worker.getQueue().size(),
                worker.getActiveCount(), worker.getTaskCount());
        // Verify Leadership on connected nodes (ignore those for which leadership is pending)
        this.worker.submit(this::verifyLeadership);
    }


    /**
     * Verify who is the leader node on the remote cluster by sending leadership request to all nodes.
     *
     * If no leader is found, the verification will be attempted for LEADERSHIP_RETRIES times.
     */
    public synchronized void verifyLeadership() {

        log.debug("Enter :: leadership verification");

        String leader = "";

        Map<String, CompletableFuture<LogReplicationQueryLeaderShipResponse>> pendingLeadershipQueries = new HashMap<>();

        // Verify leadership on remote cluster, only if no leader is currently selected.
        if (!fsm.getRemoteLeader().isPresent()) {

                log.debug("Verify leader on remote cluster {}", fsm.getRemoteClusterId());

                try {
                    for (String node : fsm.getConnectedEndpoints()) {
                        log.debug("Verify leadership status for node {}", node);
                        // Check Leadership
                        CompletableFuture<LogReplicationQueryLeaderShipResponse> leadershipRequestCf =
                                router.sendMessageAndGetCompletable(new CorfuMsg(CorfuMsgType.LOG_REPLICATION_QUERY_LEADERSHIP).setEpoch(0), node);
                        pendingLeadershipQueries.put(node, leadershipRequestCf);
                    }

                    // Block until all leadership requests are completed, or a leader is discovered.
                    while (pendingLeadershipQueries.size() != 0) {
                        LogReplicationQueryLeaderShipResponse leadershipResponse = (LogReplicationQueryLeaderShipResponse) CompletableFuture.anyOf(pendingLeadershipQueries.values()
                                .toArray(new CompletableFuture<?>[pendingLeadershipQueries.size()])).get(CorfuLogReplicationRuntime.DEFAULT_TIMEOUT, TimeUnit.MILLISECONDS);

                        if (leadershipResponse.isLeader()) {
                            log.info("Received Leadership Response :: leader for remote cluster, node={}", leadershipResponse.getEndpoint());
                            leader = leadershipResponse.getEndpoint();
                            fsm.setRemoteLeaderEndpoint(leader);

                            // Remove all CF, based on the assumption that one leader response is the expectation.
                            pendingLeadershipQueries.clear();

                            // A new leader has been found, start negotiation, to determine log replication
                            // continuation or start point
                            fsm.input(new LogReplicationRuntimeEvent(LogReplicationRuntimeEvent.LogReplicationRuntimeEventType.REMOTE_LEADER_FOUND, leader));
                            log.debug("Exit :: leadership verification");
                            return;
                        } else {
                            log.debug("Received Leadership Response :: node {} is not the leader", leadershipResponse.getEndpoint());

                            // Remove CF for completed request
                            pendingLeadershipQueries.remove(leadershipResponse.getEndpoint());
                        }
                    }

                    // No remote leader was found, retry leadership
                    fsm.input(new LogReplicationRuntimeEvent(LogReplicationRuntimeEvent.LogReplicationRuntimeEventType.REMOTE_LEADER_NOT_FOUND, leader));

                } catch (Exception ex) {
                    log.warn("Exception caught while verifying remote leader.", ex);
                    fsm.input(new LogReplicationRuntimeEvent(LogReplicationRuntimeEvent.LogReplicationRuntimeEventType.REMOTE_LEADER_NOT_FOUND, leader));
                }
        } else {
            log.info("Remote Leader already present {}. Skip leader verification.", fsm.getRemoteLeader().get());
            fsm.input(new LogReplicationRuntimeEvent(LogReplicationRuntimeEvent.LogReplicationRuntimeEventType.REMOTE_LEADER_FOUND, leader));
        }

        log.debug("Exit :: leadership verification");
    }
}
