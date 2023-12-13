package org.corfudb.infrastructure.logreplication.runtime.fsm.sink;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.FsmTaskManager;
import org.corfudb.infrastructure.logreplication.infrastructure.LogReplicationContext;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationClientServerRouter;
import org.corfudb.runtime.LogReplication;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.corfudb.runtime.proto.service.CorfuMessage;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;

import static org.corfudb.infrastructure.logreplication.runtime.fsm.LogReplicationFsmUtil.verifyRemoteLeader;
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
    @Getter
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

    //TODO v2: tune thread count;
    private static final int SINK_TASK_WORKER_THREAD_COUNT = 2;

    private final FsmTaskManager taskManager;

    public RemoteSourceLeadershipManager(LogReplicationSession session, LogReplicationClientServerRouter router,
                                         String localNodeId, LogReplicationContext replicationContext) {
        this.session = session;
        this.router = router;
        this.localNodeId = localNodeId;
        this.connectedNodes = new HashSet<>();
        this.taskManager = replicationContext.getTaskManager();

        this.taskManager.createSinkTaskManager("sinkFSM", SINK_TASK_WORKER_THREAD_COUNT);
    }

    public void input(LogReplicationSinkEvent event) {
        log.info("adding to the queue {}", event);
        this.taskManager.addTask(event, FsmTaskManager.FsmEventType.LogReplicationSinkEvent, 0);
    }

    /**
     * Process the incoming events. These events will determine the next task to perform.
     *
     * Below are the valid events and the tasks performed:
     * (i)   ON_CONNECTION_DOWN -> reset remote leader.
     * (ii)  REMOTE_LEADER_NOT_FOUND -> retry verify remote leader
     * (iii) ON_CONNECTION_UP -> verify remote leader
     * (iv)  REMOTE_LEADER_FOUND -> trigger the long living reverseReplicate RPC. The first message containing the localNodeId
     * which will be cached by the remote source
     * (v)   REMOTE_LEADER_LOSS -> There was a leadership change on the remote. Verify remote leader again.
     *
     * @param event
     */
    public void processEvent(LogReplicationSinkEvent event) {
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
                verifyRemoteLeader(this, connectedNodes, session.getSourceClusterId(), router,
                        RemoteSourceLeadershipManager.class);
                break;
            case ON_CONNECTION_UP:
                log.info("Detected connection up from endpoint={}", event.getNodeId());
                // Add new connected node, for leadership verification
                updateConnectedNodes(event.getNodeId());
                verifyRemoteLeader(this, connectedNodes, session.getSourceClusterId(), router,
                        RemoteSourceLeadershipManager.class);
                break;
            case REMOTE_LEADER_FOUND:
                log.debug("Remote Leader is found: {}", event.getNodeId());
                invokeReverseReplication();
                break;
            case REMOTE_LEADER_LOSS:
                String oldLeader = event.getNodeId();
                log.debug("Remote leader has changed. old leader {}", oldLeader);
                resetRemoteLeader(oldLeader);
                verifyRemoteLeader(this, connectedNodes, session.getSourceClusterId(), router,
                        RemoteSourceLeadershipManager.class);
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

    private void invokeReverseReplication() {
        CorfuMessage.ResponsePayloadMsg payload =
                CorfuMessage.ResponsePayloadMsg.newBuilder()
                        .setLrReverseReplicateMsg(LogReplication.ReverseReplicateMsg
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


        log.info("Send the reverseReplicate rpc {} for session {}", payload, session);
        router.sendResponse(getResponseMsg(header, payload));
    }

}
