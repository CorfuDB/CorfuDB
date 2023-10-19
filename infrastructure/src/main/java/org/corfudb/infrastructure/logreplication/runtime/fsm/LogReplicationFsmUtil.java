package org.corfudb.infrastructure.logreplication.runtime.fsm;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.runtime.CorfuLogReplicationRuntime;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationClientServerRouter;
import org.corfudb.infrastructure.logreplication.runtime.fsm.sink.LogReplicationSinkEvent;
import org.corfudb.runtime.LogReplication;
import org.corfudb.runtime.proto.service.CorfuMessage;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.corfudb.protocols.service.CorfuProtocolLogReplication.getLeadershipLossRequestPayloadMsg;

@Slf4j
public class LogReplicationFsmUtil {

    /**
     * Verify who is the leader node on the remote cluster by sending leadership request to all nodes.
     *
     * If no leader is found, the verification will be attempted for LEADERSHIP_RETRIES times.
     */
    public static synchronized <T> void verifyRemoteLeader(Object fsm, Set<String> connectedNodes, String remoteClusterId,
                                     LogReplicationClientServerRouter router, Class<T> clazz) {
        log.debug("Enter :: leadership verification");

        String leader = "";

        Map<String, CompletableFuture<LogReplication.LogReplicationLeadershipResponseMsg>> pendingLeadershipQueries = new HashMap<>();

        // Verify leadership on remote cluster, only if no leader is currently selected.
        log.debug("Verify leader on remote cluster {}", remoteClusterId);

        try {
            for (String nodeId : connectedNodes) {
                log.debug("Verify leadership status for node {}", nodeId);
                // Check Leadership
                CorfuMessage.RequestPayloadMsg payload =
                        CorfuMessage.RequestPayloadMsg.newBuilder().setLrLeadershipQuery(
                                LogReplication.LogReplicationLeadershipRequestMsg.newBuilder().build()
                        ).build();
                CompletableFuture<LogReplication.LogReplicationLeadershipResponseMsg> leadershipRequestCf =
                        router.sendRequestAndGetCompletable(
                                (LogReplication.LogReplicationSession) clazz.getMethod("getSession").invoke(fsm),
                                payload, nodeId);
                pendingLeadershipQueries.put(nodeId, leadershipRequestCf);
            }

            // Block until all leadership requests are completed, or a leader is discovered.
            while (pendingLeadershipQueries.size() != 0) {
                LogReplication.LogReplicationLeadershipResponseMsg leadershipResponse =
                        (LogReplication.LogReplicationLeadershipResponseMsg) CompletableFuture
                                .anyOf(pendingLeadershipQueries.values()
                                        .toArray(new CompletableFuture<?>[pendingLeadershipQueries.size()]))
                                .get(CorfuLogReplicationRuntime.DEFAULT_TIMEOUT, TimeUnit.MILLISECONDS);

                if (leadershipResponse.getIsLeader()) {
                    log.info("Received Leadership Response :: leader for remote cluster, node={}", leadershipResponse.getNodeId());
                    leader = leadershipResponse.getNodeId();
                    clazz.getMethod("setRemoteLeaderNodeId",String.class).invoke(fsm, leader);

                    // Remove all CF, based on the assumption that one leader response is the expectation.
                    pendingLeadershipQueries.clear();

                    // A new leader has been found, start negotiation, to determine log replication
                    // continuation or start point
                    enqueueLeaderFound(fsm, clazz, leader);
                    log.debug("Exit :: leadership verification");
                    return;
                } else {
                    log.debug("Received Leadership Response :: node {} is not the leader", leadershipResponse.getNodeId());

                    // Remove CF for completed request
                    pendingLeadershipQueries.remove(leadershipResponse.getNodeId());
                }
            }

            // No remote leader was found, retry leadership
            enqueueLeaderNotFound(fsm, clazz, leader);

        } catch (Exception ex) {
            try {
                enqueueLeaderNotFound(fsm, clazz, leader);
                log.warn("Exception caught while verifying remote leader.", ex);
            } catch (Exception e) {
                // The FSM will not move ahead if enqueuing events were unsuccessful.
                log.warn("Exception caught while attempting to enqueue REMOTE_LEADER_NOT_FOUND event.", ex);
            }
        }

        log.debug("Exit :: leadership verification");
    }

    private static <T> void enqueueLeaderFound(Object fsm, Class<T> clazz, String leader) throws NoSuchMethodException,
            InvocationTargetException, IllegalAccessException {

        if (clazz.getName().equals(CorfuLogReplicationRuntime.class.getName())) {
            clazz.getMethod("input", LogReplicationRuntimeEvent.class).invoke(fsm,
                    new LogReplicationRuntimeEvent(
                            LogReplicationRuntimeEvent.LogReplicationRuntimeEventType.REMOTE_LEADER_FOUND,
                            leader, (CorfuLogReplicationRuntime) fsm));
        } else {
            clazz.getMethod("input", LogReplicationSinkEvent.class).invoke(fsm,
                    new LogReplicationSinkEvent(
                            LogReplicationSinkEvent.LogReplicationSinkEventType.REMOTE_LEADER_FOUND,
                            leader));
        }
    }

    private static <T> void enqueueLeaderNotFound(Object fsm, Class<T> clazz, String leader) throws NoSuchMethodException,
            InvocationTargetException, IllegalAccessException {

        if (clazz.getName().equals(CorfuLogReplicationRuntime.class.getName())) {
            clazz.getMethod("input", LogReplicationRuntimeEvent.class).invoke(fsm,
                    new LogReplicationRuntimeEvent(
                            LogReplicationRuntimeEvent.LogReplicationRuntimeEventType.REMOTE_LEADER_NOT_FOUND,
                            leader, (CorfuLogReplicationRuntime) fsm));
        } else {
            clazz.getMethod("input", LogReplicationSinkEvent.class).invoke(fsm,
                    new LogReplicationSinkEvent(
                            LogReplicationSinkEvent.LogReplicationSinkEventType.REMOTE_LEADER_NOT_FOUND,
                            leader));
        }
    }

    /**
     * Check if stop event can be enqueued. STOPPED event can be enqueued for a connection endpoint, only after sending
     * a leadership loss msg to remote.
     *
     * @param router
     * @param fsm
     * @return
     */
    public static boolean canEnqueueStopRuntimeFsmEvent(LogReplicationClientServerRouter router, CorfuLogReplicationRuntime fsm,
                                                        boolean isConnectionStarter) {
        // send a leadershipLoss msg to remote when the local cluster is NOT a connection starter
        if (!isConnectionStarter) {
            return sendLeadershipLossRequestMsg(router, fsm);
        }
        return true;
    }

    /**
     * SOURCE to send leadership loss msg when it is the connection endpoint for a session and the local node looses the
     * leadership.
     *
     * @param router send msg to remote via the router
     * @param fsm  runtime fsm
     */
    private static boolean sendLeadershipLossRequestMsg(LogReplicationClientServerRouter router, CorfuLogReplicationRuntime fsm) {
        if(fsm.getReplicationContext().getIsLeader().get()) {
            log.debug("the local node acquired the leadership. Stop sending Leadership_loss msg");
            // do not transition to another state
            return false;
        }
        try {
            log.debug("Sending LEADERSHIP_LOSS msg for session {}", fsm.getSession());
            CompletableFuture<LogReplication.LogReplicationMetadataResponseMsg> cf = router
                    .sendRequestAndGetCompletable(fsm.session,
                            getLeadershipLossRequestPayloadMsg(router.getLocalNodeId()),
                            fsm.getRemoteLeaderNodeId().get());
            cf.get(CorfuLogReplicationRuntime.DEFAULT_TIMEOUT, TimeUnit.MILLISECONDS);
            return true;
        } catch(TimeoutException | ExecutionException | InterruptedException ex) {
            log.error("Retry sending leadership loss msg until an ACK is received ", ex);
            fsm.input(new LogReplicationRuntimeEvent(LogReplicationRuntimeEvent.LogReplicationRuntimeEventType.LOCAL_LEADER_LOSS,
                    false,fsm));
        } catch (Exception ex) {
            // error occurring due to transport/network layer failure can be ignored as the remote will be notified. The
            // remote will then initiate a new connection. In this case, its safe to transition FSM to STOPPED state
            log.error("Sending leadership loss msg failed. Transitioning the FSM to STOPPED state. ", ex);
            return true;
        }
        return false;
    }

}
