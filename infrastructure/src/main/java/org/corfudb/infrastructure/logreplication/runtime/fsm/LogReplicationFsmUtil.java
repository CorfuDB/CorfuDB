package org.corfudb.infrastructure.logreplication.runtime.fsm;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.runtime.CorfuLogReplicationRuntime;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationClientServerRouter;
import org.corfudb.infrastructure.logreplication.runtime.fsm.LogReplicationRuntimeEvent;
import org.corfudb.infrastructure.logreplication.runtime.fsm.VerifyingRemoteSinkLeaderState;
import org.corfudb.infrastructure.logreplication.runtime.fsm.sink.LogReplicationSinkEvent;
import org.corfudb.runtime.LogReplication;
import org.corfudb.runtime.proto.service.CorfuMessage;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

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
                            leader));
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
                            leader));
        } else {
            clazz.getMethod("input", LogReplicationSinkEvent.class).invoke(fsm,
                    new LogReplicationSinkEvent(
                            LogReplicationSinkEvent.LogReplicationSinkEventType.REMOTE_LEADER_NOT_FOUND,
                            leader));
        }
    }

}
