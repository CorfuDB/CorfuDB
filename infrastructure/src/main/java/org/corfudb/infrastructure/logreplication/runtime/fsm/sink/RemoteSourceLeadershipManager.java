package org.corfudb.infrastructure.logreplication.runtime.fsm.sink;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.runtime.CorfuLogReplicationRuntime;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationClientServerRouter;
import org.corfudb.runtime.LogReplication;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.corfudb.runtime.proto.service.CorfuMessage;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.corfudb.protocols.CorfuProtocolCommon.getUuidMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getDefaultProtocolVersionMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getResponseMsg;

/**
 * This class is used only by SINK when connection initiator, to query leadership from the remote SOURCE cluster.
 *
 * Upon receiving the leadership response, a bidirectional stream is setup so SOURCE can drive the replication as LR
 * follows a push model.
 */
@Slf4j
public class RemoteSourceLeadershipManager {

    /**
     * Executor service for FSM event queue consume
     */
    private final ExecutorService communicationFSMConsumer;

    /**
     * Remote nodes to which connection has been established.
     */
    private final Set<String> connectedNodes;

    /**
     * A queue of events.
     */
    private final LinkedBlockingQueue<LogReplicationSinkEvent> eventQueue = new LinkedBlockingQueue<>();

    /**
     * Session information
     */
    private final LogReplicationSession session;

    /**
     * A router that forwards a message to the transport layer
     */
    private final LogReplicationClientServerRouter router;

    /**
     * NodeId of the remote leader.
     */
    private volatile Optional<String> leaderNodeId = Optional.empty();

    private final String localNodeId;

    public RemoteSourceLeadershipManager(LogReplicationSession session, LogReplicationClientServerRouter router,
                                         String localNodeId) {
        this.session = session;
        this.router = router;
        this.localNodeId = localNodeId;
        this.connectedNodes = new HashSet<>();

        this.communicationFSMConsumer = Executors.newSingleThreadExecutor(new
                ThreadFactoryBuilder().setNameFormat(
                "sink-consumer-"+session.hashCode()).build());


        communicationFSMConsumer.submit(this::consume);
    }

    public synchronized void input(LogReplicationSinkEvent event) {
        try {
            log.info("adding to the queue {}", event);
            eventQueue.put(event);
        } catch (InterruptedException ex) {
            log.error("Log Replication interrupted Exception: ", ex);
        }
    }

    /**
     * Consumer of the eventQueue.
     * <p>
     * This method consumes the log replication events and does the state transition.
     */
    private void consume() {
        try {
            //  Block until an event shows up in the queue.
            LogReplicationSinkEvent event = eventQueue.take();
            processEvent(event);

            communicationFSMConsumer.submit(this::consume);

        } catch (Throwable t) {
            log.error("Error on event consumer: ", t);
        }
    }

    private void processEvent(LogReplicationSinkEvent event) {
        log.info("processing event {}", event);
        switch (event.getType()) {
            case ON_CONNECTION_DOWN:
                String nodeIdDown = event.getNodeId();
                log.info("Detected connection down from node={}", nodeIdDown);
                updateDisconnectedNodes(nodeIdDown);
                resetRemoteLeader(nodeIdDown);
                break;
            case REMOTE_LEADER_NOT_FOUND:
                log.info("Remote Leader not found. Retrying...");
                verifyLeadership();
                break;
            case ON_CONNECTION_UP:
                log.info("Detected connection up from endpoint={}", event.getNodeId());
                // Add new connected node, for leadership verification
                updateConnectedNodes(event.getNodeId());
                verifyLeadership();
                break;
            case REMOTE_LEADER_FOUND:
                log.debug("Remote Leader is found: {}", event.getNodeId());
                invokeReverseReplication();
                break;
            case REMOTE_LEADER_LOSS:
                String oldLeader = event.getNodeId();
                log.debug("Remote leader has changed. old leader {}", oldLeader);
                resetRemoteLeader(oldLeader);
                verifyLeadership();
                break;
            default: {
                log.warn("Unexpected communication event {}", event.getType());
            }
        }
    }

    private void resetRemoteLeader(String nodeId) {
        if (leaderNodeId.isPresent() && leaderNodeId.get().equals(nodeId)) {
            log.debug("Reset remote leader");
            leaderNodeId = Optional.empty();
        }
    }

    private void updateConnectedNodes(String nodeId) {
        this.connectedNodes.add(nodeId);
    }

    private void updateDisconnectedNodes(String nodeId) {
        this.connectedNodes.remove(nodeId);
    }

    public synchronized void setRemoteLeaderNodeId(String leaderId) {
        log.debug("Set remote leader node id {}", leaderId);
        leaderNodeId = Optional.ofNullable(leaderId);
    }

    public synchronized Optional<String> getRemoteLeaderNodeId() {
        log.trace("Retrieve remote leader node id {}", leaderNodeId);
        return leaderNodeId;
    }

    private synchronized void verifyLeadership() {

        log.debug("Enter :: leadership verification");

        String leader = "";

        Map<String, CompletableFuture<LogReplication.LogReplicationLeadershipResponseMsg>> pendingLeadershipQueries = new HashMap<>();

        // Verify leadership on remote cluster, only if no leader is currently selected.
        if (!getRemoteLeaderNodeId().isPresent()) {

            log.debug("Verify leader on remote cluster {}", session.getSourceClusterId());

            try {
                for (String nodeId : connectedNodes) {
                    log.debug("Verify leadership status for node {}", nodeId);
                    CorfuMessage.RequestPayloadMsg payload =
                            CorfuMessage.RequestPayloadMsg.newBuilder().setLrLeadershipQuery(
                                    LogReplication.LogReplicationLeadershipRequestMsg.newBuilder().build()).build();

                    CompletableFuture<LogReplication.LogReplicationLeadershipResponseMsg> leadershipRequestCf =
                            router.sendRequestAndGetCompletable(session, payload, nodeId);
                    pendingLeadershipQueries.put(nodeId, leadershipRequestCf);
                }

                // Block until all leadership requests are completed, or a leader is discovered.
                while (pendingLeadershipQueries.size() != 0) {
                    LogReplication.LogReplicationLeadershipResponseMsg leadershipResponse = (LogReplication.LogReplicationLeadershipResponseMsg)
                            CompletableFuture.anyOf(pendingLeadershipQueries.values()
                                    .toArray(new CompletableFuture<?>[pendingLeadershipQueries.size()])).get(CorfuLogReplicationRuntime.DEFAULT_TIMEOUT, TimeUnit.MILLISECONDS);

                    if (leadershipResponse.getIsLeader()) {
                        log.info("Received Leadership Response :: leader for remote cluster, node={}", leadershipResponse.getNodeId());
                        leader = leadershipResponse.getNodeId();
                        setRemoteLeaderNodeId(leader);

                        // Remove all CF, based on the assumption that one leader response is the expectation.
                        pendingLeadershipQueries.clear();

                        // A new leader has been found,
                        input(new LogReplicationSinkEvent(LogReplicationSinkEvent.LogReplicationSinkEventType.REMOTE_LEADER_FOUND, leader));
                        router.getSessionToLeaderConnectionFuture().get(session).complete(null);
                        log.debug("Exit :: leadership verification");
                        return;
                    } else {
                        log.debug("Received Leadership Response :: node {} is not the leader", leadershipResponse.getNodeId());

                        // Remove CF for completed request
                        pendingLeadershipQueries.remove(leadershipResponse.getNodeId());
                    }
                }

                // No remote leader was found, retry leadership
                input(new LogReplicationSinkEvent(LogReplicationSinkEvent.LogReplicationSinkEventType.REMOTE_LEADER_NOT_FOUND, leader));

            } catch (Exception ex) {
                log.warn("Exception caught while verifying remote leader.", ex);
                input(new LogReplicationSinkEvent(LogReplicationSinkEvent.LogReplicationSinkEventType.REMOTE_LEADER_NOT_FOUND, leader));
            }
        } else {
            log.info("Remote Leader already present {}. Skip leader verification.", getRemoteLeaderNodeId().get());
        }

        log.debug("Exit :: leadership verification");
    }

    private void invokeReverseReplication() {
        CorfuMessage.ResponsePayloadMsg payload =
                CorfuMessage.ResponsePayloadMsg.newBuilder()
                        .setLrSubscribeMsg(LogReplication.SubscribeToReplicationMsg
                                .newBuilder()
                                .setSinkLeaderNodeId(localNodeId)
                                .build())
                        .build();

        CorfuMessage.HeaderMsg header = CorfuMessage.HeaderMsg.newBuilder()
                .setSession(session)
                .setRequestId(router.getSessionToRequestIdCounter().get(session).getAndIncrement())
                .setClusterId(getUuidMsg(UUID.fromString(session.getSourceClusterId())))
                .setVersion(getDefaultProtocolVersionMsg())
                .setIgnoreClusterId(true)
                .setIgnoreEpoch(true)
                .build();


        log.info("Trigger the reverseReplicate rpc {} for session {}", payload, session);
        router.sendResponse(getResponseMsg(header, payload));
    }

}
