package org.corfudb.infrastructure.logreplication.runtime.sinkFsm;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.ReplicationSession;
import org.corfudb.infrastructure.logreplication.runtime.CorfuLogReplicationRuntime;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationSinkClientRouter;
import org.corfudb.infrastructure.logreplication.runtime.fsm.LogReplicationRuntimeEvent;
import org.corfudb.runtime.LogReplication;
import org.corfudb.runtime.proto.service.CorfuMessage;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Slf4j
public class SinkVerifyRemoteLeader {

    /**
     * Executor service for FSM state tasks
     */
//    private final ThreadPoolExecutor communicationFSMWorkers;

    /**
     * Executor service for FSM event queue consume
     */
    private final ExecutorService communicationFSMConsumer;

    private final Set<String> connectedNodes;

    /**
     * A queue of events.
     */
    private final LinkedBlockingQueue<LogReplicationRuntimeEvent> eventQueue = new LinkedBlockingQueue<>();

    private final ReplicationSession session;

    private final LogReplicationSinkClientRouter router;

    private volatile Optional<String> leaderNodeId = Optional.empty();

    public SinkVerifyRemoteLeader(ReplicationSession session, LogReplicationSinkClientRouter router) {
        this.session = session;
        this.router = router;
        this.connectedNodes = new HashSet<>();

        String remoteClusterId = session.getRemoteClusterId();
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("sink-query-worker"+remoteClusterId)
                .build();

//        this.communicationFSMWorkers = new ThreadPoolExecutor(1, 1, 0L,
//                TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), threadFactory);

        this.communicationFSMConsumer = Executors.newSingleThreadExecutor(new
                ThreadFactoryBuilder().setNameFormat(
                "sink-query-consumer-"+remoteClusterId).build());


        communicationFSMConsumer.submit(this::consume);
    }

    public synchronized void input(LogReplicationRuntimeEvent event) {
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
            LogReplicationRuntimeEvent event = eventQueue.take();
            processEvent(event);

            communicationFSMConsumer.submit(this::consume);

        } catch (Throwable t) {
            log.error("Error on event consumer: ", t);
        }
    }

    private void processEvent(LogReplicationRuntimeEvent event) {
        log.info("processing event {}", event);
        switch (event.getType()) {
            case ON_CONNECTION_DOWN:
                String nodeIdDown = event.getNodeId();
                log.debug("Detected connection down from node={}", nodeIdDown);
                updateDisconnectedNodes(nodeIdDown);

                // If no connection exists, establish a connection again
                if (connectedNodes.size() == 0) {
                    router.getChannelAdapter().connectAsync();
                }
                break;
            case REMOTE_LEADER_NOT_FOUND:
                log.info("Remote Leader not found. Retrying...");
                verifyLeadership();
                break;
            case ON_CONNECTION_UP:
                log.debug("Detected connection up from endpoint={}", event.getNodeId());
                // Add new connected node, for leadership verification
                updateConnectedNodes(event.getNodeId());
                verifyLeadership();
                break;
            case REMOTE_LEADER_FOUND:
                log.debug("Remote Leader is found: {}", event.getNodeId());
                setupReplicationStream();
                break;
            default: {
                log.warn("Unexpected communication event {}", event.getType());
            }
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

//    public synchronized void resetRemoteLeaderNodeId() {
//        log.debug("Reset remote leader node id");
//        router.resetRemoteLeader();
//        leaderNodeId = Optional.empty();
//    }

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

            log.debug("Verify leader on remote cluster {}", session.getRemoteClusterId());

            try {
                for (String nodeId : connectedNodes) {
                    log.debug("Verify leadership status for node {}", nodeId);
                    // Check Leadership
                    LogReplication.ReplicationSessionMsg sessionMsg = LogReplication.ReplicationSessionMsg.newBuilder()
                            .setRemoteClusterId(session.getRemoteClusterId())
                            .setLocalClusterId(session.getLocalClusterId())
                            .setClient(session.getSubscriber().getClient())
                            .setReplicationModel(session.getSubscriber().getReplicationModel())
                            .build();
                    log.info("sessions in veryfingLeadership {}", sessionMsg);
                    CorfuMessage.RequestPayloadMsg payload =
                            CorfuMessage.RequestPayloadMsg.newBuilder().setLrLeadershipQuery(
                                    LogReplication.LogReplicationLeadershipRequestMsg
                                            .newBuilder()
                                            .setSessionInfo(sessionMsg)
                                            .build()
                            ).build();
                    log.info("payload in veryfingLeadership {}", payload);
                    CompletableFuture<LogReplication.LogReplicationLeadershipResponseMsg> leadershipRequestCf =
                            router.sendRequestAndGetCompletable(payload, nodeId);
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
                        input(new LogReplicationRuntimeEvent(LogReplicationRuntimeEvent.LogReplicationRuntimeEventType.REMOTE_LEADER_FOUND, leader));
                        log.debug("Exit :: leadership verification");
                        return;
                    } else {
                        log.debug("Received Leadership Response :: node {} is not the leader", leadershipResponse.getNodeId());

                        // Remove CF for completed request
                        pendingLeadershipQueries.remove(leadershipResponse.getNodeId());
                    }
                }

                // No remote leader was found, retry leadership
                input(new LogReplicationRuntimeEvent(LogReplicationRuntimeEvent.LogReplicationRuntimeEventType.REMOTE_LEADER_NOT_FOUND, leader));

            } catch (Exception ex) {
                log.warn("Exception caught while verifying remote leader.", ex);
                input(new LogReplicationRuntimeEvent(LogReplicationRuntimeEvent.LogReplicationRuntimeEventType.REMOTE_LEADER_NOT_FOUND, leader));
            }
        } else {
            log.info("Remote Leader already present {}. Skip leader verification.", getRemoteLeaderNodeId().get());
        }

        log.debug("Exit :: leadership verification");
    }

    private void setupReplicationStream() {
        LogReplication.ReplicationSessionMsg sessionMsg = LogReplication.ReplicationSessionMsg.newBuilder()
                .setRemoteClusterId(session.getRemoteClusterId())
                .setLocalClusterId(router.getLocalClusterId())
                .setClient(session.getSubscriber().getClient())
                .setReplicationModel(session.getSubscriber().getReplicationModel())
                .build();
        CorfuMessage.ResponsePayloadMsg payload =
                CorfuMessage.ResponsePayloadMsg.newBuilder().setLrSubscribeRequest(
                        LogReplication.LogReplicationSubscribeMsg
                                .newBuilder()
                                .setSessionInfo(sessionMsg)
                                .build()
                ).build();

        log.info("Sending the subscribe msg {}", payload);
        router.sendResponse(payload, getRemoteLeaderNodeId().get());
    }

}
