package org.corfudb.infrastructure.logreplication.runtime;


import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.ServerThreadFactory;
import org.corfudb.infrastructure.logreplication.infrastructure.ClusterDescriptor;
import org.corfudb.infrastructure.logreplication.infrastructure.CorfuReplicationManager;
import org.corfudb.infrastructure.logreplication.infrastructure.TopologyDescriptor;
import org.corfudb.infrastructure.logreplication.infrastructure.msghandlers.LogReplicationServer;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.LogReplicationPluginConfig;
import org.corfudb.infrastructure.logreplication.runtime.fsm.LogReplicationRuntimeEvent;
import org.corfudb.infrastructure.logreplication.runtime.fsm.sink.LogReplicationSinkEvent;
import org.corfudb.infrastructure.logreplication.runtime.fsm.sink.RemoteSourceLeadershipManager;
import org.corfudb.infrastructure.logreplication.transport.IClientServerRouter;
import org.corfudb.infrastructure.logreplication.transport.client.ChannelAdapterException;
import org.corfudb.infrastructure.logreplication.transport.client.IClientChannelAdapter;
import org.corfudb.infrastructure.logreplication.transport.server.IServerChannelAdapter;
import org.corfudb.runtime.LogReplication;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.runtime.proto.service.CorfuMessage;
import org.corfudb.util.CFUtils;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.IntervalRetry;
import org.corfudb.util.retry.RetryNeededException;

import javax.annotation.Nonnull;
import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import static org.corfudb.protocols.CorfuProtocolCommon.getUuidMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getDefaultProtocolVersionMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getHeaderMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getResponseMsg;

/**
 * Router, interfaces between the custom transport layer and LR core components.
 *
 * There is one router per node, and all the sessions (both incoming and outgoing) share the router.
 *
 * Multiple threads access the members of this class:
 * (1) The LR components (snapshotReader, LogEntryReader) access the router to send an outgoing msg
 * (2) The thread that receives the incoming message uses this class to route the message to the appropriate handler or
 * enqueues an event in the LR components.
 * All of this is done with the help of thread safe data structures.
 */
@Slf4j
public class LogReplicationClientServerRouter implements IClientServerRouter {

    public static final String REMOTE_LEADER = "REMOTE_LEADER";

    /**
     * A map of session to {@link CompletableFuture}, which is completed when a connection,
     * including a successful handshake, completes and messages can be sent
     * to the remote node.
     */
    @Getter
    private final Map<LogReplicationSession, CompletableFuture<Void>> sessionToLeaderConnectionFuture;

    /**
     * A map of session to the current requestID for the session.
     */
    @Getter
    @SuppressWarnings("checkstyle:abbreviation")
    private final Map<LogReplicationSession,AtomicLong> sessionToRequestIdCounter;

    /**
     * Sync call response timeout (milliseconds).
     */
    @Getter
    @Setter
    private long timeoutResponse;

    /**
     * The outstanding requests (for Source and for Sink when Sink is the connection starter)  on this router.
     */
    @Getter
    private final Map<LogReplicationSession, Map<Long, CompletableFuture>> sessionToOutstandingRequests;

    /**
     * Runtime FSM, to insert connectivity events
     */
    @Getter
    private final Map<LogReplicationSession, CorfuLogReplicationRuntime> sessionToRuntimeFSM;

    /**
     * Remote cluster's clusterDescriptor
     */
    @Getter
    private final Map<LogReplicationSession, ClusterDescriptor> sessionToRemoteClusterDescriptor;

    /**
     * local cluster ID
     */
    private final String localClusterId;

    /**
     * local node ID
     */
    @Getter
    private final String localNodeId;

    /**
     * Client Transport adapter
     */
    @Setter
    private IClientChannelAdapter clientChannelAdapter;

    /**
     * Server transport adapter
     */
    @Setter
    @Getter
    private IServerChannelAdapter serverChannelAdapter;

    /**
     * Log replication server which has the handlers for incoming messages.
     */
    private final LogReplicationServer msgHandler;

    /**
     * Map of session -> verifySinkLeadership. VerifySinkLeadership triggers leadership request and and upon receiving
     * the response, creates a bidirectional streams to remote endpoints
     */
    @Getter
    private final Map<LogReplicationSession, RemoteSourceLeadershipManager> sessionToRemoteSourceLeaderManager;

    /**
     * Set of sessions where the local cluster is SINK
     */
    private final Set<LogReplicationSession> incomingSession;

    /**
     * Set of sessions where the local cluster is SOURCE
     */
    private final Set<LogReplicationSession> outgoingSession;

    /**
     * Topology descriptor
     */
    @Setter
    private TopologyDescriptor topology;

    /**
     * This is used only when the local cluster is Source and is a connection receiver.
     * Stores the first incoming reverseReplicate message when the source FSMs are not yet initialized. When the FSMs are
     * ready, the buffered message is used to advance the state machine to the Negotiation state.
     */
    private Map<LogReplicationSession, CorfuMessage.ResponseMsg> reverseReplicateInitMsgBuffer;



    /**
     * Log Replication Router Constructor
     *
     * @param replicationManager replicationManager to start FSM
     * @param localClusterId local cluster ID
     * @param localNodeId  local node ID
     * @param incomingSession sessions where the local cluster is SINK
     * @param outgoingSession sessions where the local cluster is SOURCE
     * @param msgHandler LogReplicationServer instance, used to handle the incoming messages.
     */
    public LogReplicationClientServerRouter(CorfuReplicationManager replicationManager,
                                            String localClusterId, String localNodeId,
                                            Set<LogReplicationSession> incomingSession,
                                            Set<LogReplicationSession> outgoingSession,
                                            LogReplicationServer msgHandler,
                                            ServerContext serverContext,
                                            LogReplicationPluginConfig pluginConfig) {
        this.timeoutResponse = replicationManager.getCorfuRuntime().getParameters().getRequestTimeout().toMillis();
        this.localClusterId = localClusterId;
        this.localNodeId = localNodeId;
        this.sessionToRuntimeFSM = replicationManager.getSessionRuntimeMap();
        this.incomingSession = incomingSession;
        this.outgoingSession = outgoingSession;
        this.msgHandler = msgHandler;

        this.sessionToRemoteClusterDescriptor = new ConcurrentHashMap<>();
        this.sessionToRequestIdCounter = new ConcurrentHashMap<>();
        this.sessionToOutstandingRequests = new ConcurrentHashMap<>();
        this.sessionToLeaderConnectionFuture = new ConcurrentHashMap<>();
        this.sessionToRemoteSourceLeaderManager = new ConcurrentHashMap<>();
        this.reverseReplicateInitMsgBuffer = new ConcurrentHashMap<>();
        createTransportServerAdapter(serverContext, pluginConfig);
        createTransportClientAdapter(pluginConfig);
    }

    /**
     * Initiate and start the transport layer server. All the sessions use this server to receive messages.
     *
     * @param serverContext
     * @param pluginConfig
     * @return
     */
    public void createTransportServerAdapter(ServerContext serverContext, LogReplicationPluginConfig pluginConfig) {
        File jar = new File(pluginConfig.getTransportAdapterJARPath());

        try (URLClassLoader child = new URLClassLoader(new URL[]{jar.toURI().toURL()}, this.getClass().getClassLoader())) {
            Class adapter = Class.forName(pluginConfig.getTransportServerClassCanonicalName(), true, child);
            this.serverChannelAdapter = (IServerChannelAdapter) adapter
                    .getDeclaredConstructor(ServerContext.class, LogReplicationClientServerRouter.class)
                    .newInstance(serverContext, this);
        } catch (Exception e) {
            log.error("Fatal error: Failed to create serverAdapter", e);
            throw new UnrecoverableCorfuError(e);
        }
    }

    /**
     * Initiate the transport layer client. All the sessions use this client to send messages.
     * @param pluginConfig
     */
    public void createTransportClientAdapter(LogReplicationPluginConfig pluginConfig) {
        if(this.clientChannelAdapter != null) {
            log.trace("The client channel adapter is already initialized");
            return;
        }

        File jar = new File(pluginConfig.getTransportAdapterJARPath());

        try (URLClassLoader child = new URLClassLoader(new URL[]{jar.toURI().toURL()}, this.getClass().getClassLoader())) {
            // Instantiate Channel Adapter (external implementation of the channel / transport)
            Class adapterType = Class.forName(pluginConfig.getTransportClientClassCanonicalName(), true, child);
            this.clientChannelAdapter = (IClientChannelAdapter) adapterType
                    .getDeclaredConstructor(String.class, LogReplicationClientServerRouter.class)
                    .newInstance(this.localClusterId, this);
            this.clientChannelAdapter.setChannelContext(this.serverChannelAdapter.getChannelContext());
        } catch (Exception e) {
            log.error("Fatal error: Failed to initialize transport adapter {}",
                    pluginConfig.getTransportClientClassCanonicalName(), e);
            throw new UnrecoverableCorfuError(e);
        }
    }

    /**
     * Start server channel adapter with retry.
     *
     * @return Completable Future on connection start
     */
    public CompletableFuture<Boolean> startServerChannelAdapter() {
        try {
            return IRetry.build(IntervalRetry.class, () -> {
                try {
                    return this.serverChannelAdapter.start();
                } catch (Exception e) {
                    log.warn("Exception caught while starting server adapter, retrying", e);
                    throw new RetryNeededException();
                }
            }).run();
        } catch (InterruptedException e) {
            log.error("Unrecoverable exception when starting server adapter.", e);
            throw new UnrecoverableCorfuInterruptedError(e);
        }
    }


    /**
     * Add runtimeFSM of the router
     *
     * @param session session information
     * @param runtimeFSM runtime state machine, insert connection related events
     */
    public void addRuntimeFSM(LogReplicationSession session, CorfuLogReplicationRuntime runtimeFSM) {
        this.sessionToRuntimeFSM.put(session, runtimeFSM);

        // check if buffer has a reverse_replicate msg for the session.
        if (reverseReplicateInitMsgBuffer.containsKey(session)) {
            String remoteLeaderId = reverseReplicateInitMsgBuffer.get(session).getPayload().getLrReverseReplicateMsg().getSinkLeaderNodeId();
            runtimeFSM.setRemoteLeaderNodeId(remoteLeaderId);
            addConnectionUpEvent(session, remoteLeaderId);
        }
    }


    @Override
    public <T> CompletableFuture<T> sendRequestAndGetCompletable(
            @Nonnull LogReplicationSession session,
            @Nonnull CorfuMessage.RequestPayloadMsg payload,
            @Nonnull String nodeId) {

        if (!isValidMessage(payload)) {
            log.error("Invalid message type {}. Currently only log replication messages are processed.",
                    payload.getPayloadCase());
            CompletableFuture<T> f = new CompletableFuture<>();
            f.completeExceptionally(new Throwable("Invalid message type"));
            return f;
        }

        CorfuMessage.HeaderMsg.Builder header = CorfuMessage.HeaderMsg.newBuilder()
                .setSession(session)
                .setVersion(getDefaultProtocolVersionMsg())
                .setIgnoreClusterId(true)
                .setIgnoreEpoch(true);

        long requestId = -1;

        // Generate a future and put it in the completion table.
        final CompletableFuture<T> cf = new CompletableFuture<>();
        try {
            // Get the next request ID.
            requestId = sessionToRequestIdCounter.getOrDefault(session, new AtomicLong(0)).getAndIncrement();

            sessionToOutstandingRequests.putIfAbsent(session, new HashMap<>());
            sessionToOutstandingRequests.get(session).put(requestId, cf);

            header.setRequestId(requestId);
            header.setClusterId(getUuidMsg(UUID.fromString(this.localClusterId)));

            // If no endpoint is specified, the message is to be sent to the remote leader node.
            // We should block until a connection to the leader is established.
            if (nodeId.equals(REMOTE_LEADER)) {

                if (isConnectionStarterForSession(session)) {
                    // Check the connection future. If connected, continue with sending the message.
                    // If timed out, return a exceptionally completed with the timeout.
                    // Because in Log Replication, messages are sent to the leader node, the connection future
                    // represents a connection to the leader.
                    try {
                        sessionToLeaderConnectionFuture.get(session).get(timeoutResponse, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        throw new UnrecoverableCorfuInterruptedError(e);
                    } catch (TimeoutException | ExecutionException te) {
                        cf.completeExceptionally(te);
                        return cf;
                    }
                }

                if(outgoingSession.contains(session)) {
                    CorfuLogReplicationRuntime runtimeFSM = sessionToRuntimeFSM.get(session);
                    // Get Remote Leader
                    if (runtimeFSM.getRemoteLeaderNodeId().isPresent()) {
                        nodeId = runtimeFSM.getRemoteLeaderNodeId().get();
                    } else {
                        log.error("Leader not found to remote cluster {}",
                                sessionToRemoteClusterDescriptor.get(session).getClusterId());
                        runtimeFSM.input(new LogReplicationRuntimeEvent(LogReplicationRuntimeEvent
                                .LogReplicationRuntimeEventType.REMOTE_LEADER_LOSS));
                        throw new ChannelAdapterException(
                                String.format("Leader not found to remote cluster %s",
                                        sessionToRemoteClusterDescriptor.get(session).getClusterId()));
                    }
                } else {
                    nodeId = sessionToRemoteSourceLeaderManager.get(session).getRemoteLeaderNodeId().get();
                }
            }

            // In the case the message is intended for a specific endpoint, we do not
            // block on connection future, this is the case of leader verification.
            if(isConnectionStarterForSession(session)) {
                log.info("Send requestID/payload {}/{} via clientChannelAdapter for session {}", header.getRequestId(),
                        payload.getPayloadCase(), session);
                clientChannelAdapter.send(nodeId, getRequestMsg(header.build(), payload));
            } else {
                // connection endpoints do not send leadership_query msgs.
                Preconditions.checkArgument(!payload.getPayloadCase().equals(
                        CorfuMessage.RequestPayloadMsg.PayloadCase.LR_LEADERSHIP_QUERY));
                log.info("Send requestID/payload {}/{} via serverChannelAdapter for session {}", header.getRequestId(),
                        payload.getPayloadCase(), session);
                serverChannelAdapter.send(getRequestMsg(header.build(), payload));
            }

            // Generate a timeout future, which will complete exceptionally
            // if the main future is not completed.
            final CompletableFuture<T> cfTimeout =
                    CFUtils.within(cf, Duration.ofMillis(timeoutResponse));
            final long finalRequestId = requestId;
            String finalNodeId = nodeId;
            cfTimeout.exceptionally(e -> {
                if (e.getCause() instanceof TimeoutException) {
                    sessionToOutstandingRequests.get(session).remove(finalRequestId);
                    log.debug("sendMessageAndGetCompletable: Remove request {} to {}/{} due to timeout! Message:{}",
                            finalRequestId, sessionToRemoteClusterDescriptor.get(session).getClusterId(), finalNodeId, payload.getPayloadCase());
                }
                return null;
            });

            return cfTimeout;

        } catch (NetworkException ne) {
            log.error("Caught Network Exception while trying to send message to remote leader {}", nodeId);
            if(outgoingSession.contains(session)) {
                sessionToRuntimeFSM.get(session).input(new LogReplicationRuntimeEvent(LogReplicationRuntimeEvent
                        .LogReplicationRuntimeEventType.ON_CONNECTION_DOWN, nodeId));
            } else {
                sessionToRemoteSourceLeaderManager.get(session).input(new LogReplicationSinkEvent(
                        LogReplicationSinkEvent.LogReplicationSinkEventType.ON_CONNECTION_DOWN, nodeId));
            }
            throw ne;
        } catch(NullPointerException npe) {
            log.info("components are not found for session {}, Error::{}", session, npe.getMessage());
            // clear the potential stale data generated in this method for the potential stale session.
            // This is done so the session information can be consistent when another thread might be working on stopping
            // the replication for the session
            removeSessionInfo(session);
        } catch (Exception e) {
            sessionToOutstandingRequests.get(session).remove(requestId);
            log.error("sendMessageAndGetCompletable: Remove request {} to {}/{} due to exception! Message:{}",
                    requestId, sessionToRemoteClusterDescriptor.get(session).getClusterId(), nodeId, payload.getPayloadCase(), e);
            cf.completeExceptionally(e);
        }
        return cf;
    }

    @Override
    public synchronized <T> void completeRequest(LogReplicationSession session, long requestID, T completion) {
        try {
            log.trace("Complete request: {}...outstandingRequests {}", requestID, sessionToOutstandingRequests.get(session));
            CompletableFuture<T> cf = (CompletableFuture<T>) sessionToOutstandingRequests.get(session).remove(requestID);
            cf.complete(completion);
        } catch (NullPointerException npe) {
            log.warn("Attempted to complete request {}, but request not outstanding!", requestID);
        }
    }

    @Override
    public void completeExceptionally(LogReplicationSession session, long requestID, Throwable cause) {
        try {
            CompletableFuture cf = sessionToOutstandingRequests.get(session).remove(requestID);
            cf.completeExceptionally(cause);
            log.debug("completeExceptionally: Remove request {} to {} due to {}.", requestID,
                    sessionToRemoteClusterDescriptor.get(session).getClusterId(), cause.getClass().getSimpleName(), cause);
        } catch (NullPointerException npe) {
            log.warn("Attempted to exceptionally complete request {}, but request not outstanding!",
                    requestID);
        }
    }

    @Override
    public void stop(Set<LogReplicationSession> sessions) {
        sessions.forEach(session -> removeSessionInfo(session));

        // When all the connection starter sessions are stopped, shut down the transport layer
        if(sessionToOutstandingRequests.isEmpty() && this.clientChannelAdapter != null) {
            // stop the client Adapter. The Server Adapter is closed in the interClusterServerNode.
            this.clientChannelAdapter.stop();
        }
    }

    @Override
    public void setResponseTimeout(long timeoutResponse) {
        this.timeoutResponse = timeoutResponse;
    }

    @Override
    public void sendResponse(CorfuMessage.ResponseMsg response) {
        log.trace("Ready to send response {}", response.getPayload().getPayloadCase());
        LogReplicationSession session = response.getHeader().getSession();
        if (isConnectionStarterForSession(session)) {
            try {
                if (incomingSession.contains(session)) {
                    if (sessionToRemoteSourceLeaderManager.get(session).getRemoteLeaderNodeId().isPresent()) {
                        clientChannelAdapter.send(sessionToRemoteSourceLeaderManager.get(session).getRemoteLeaderNodeId().get(), response);
                    } else {
                        sessionToRemoteSourceLeaderManager.get(session).input(
                                new LogReplicationSinkEvent(LogReplicationSinkEvent.LogReplicationSinkEventType.REMOTE_LEADER_LOSS));
                    }
                }
            } catch(NullPointerException npe) {
                log.info("RemoteSourceLeadershipManager not found for session {}", session);
            }
        } else {
            try {
                this.serverChannelAdapter.send(response);
                log.trace("Sent response: {}", response);
            } catch (IllegalArgumentException e) {
                log.warn("Illegal response type. Ignoring message.", e);
            }
        }
    }

    public void inputRemoteSourceLeaderLoss(LogReplicationSession session) {
        sessionToRemoteSourceLeaderManager.get(session).input(
                new LogReplicationSinkEvent(LogReplicationSinkEvent.LogReplicationSinkEventType.REMOTE_LEADER_LOSS));
    }

    @Override
    public void receive(CorfuMessage.ResponseMsg msg) {
        try {
            LogReplicationSession session = msg.getHeader().getSession();

            // If it is a Leadership Loss Message re-trigger leadership discovery
            if (msg.getPayload().getPayloadCase() == CorfuMessage.ResponsePayloadMsg.PayloadCase.LR_LEADERSHIP_LOSS) {
                String nodeId = msg.getPayload().getLrLeadershipLoss().getNodeId();
                if (outgoingSession.contains(session)) {
                    this.sessionToRuntimeFSM.get(session)
                            .input(new LogReplicationRuntimeEvent(LogReplicationRuntimeEvent
                                    .LogReplicationRuntimeEventType.REMOTE_LEADER_LOSS, nodeId));
                } else {
                    sessionToRemoteSourceLeaderManager.get(session)
                            .input(new LogReplicationSinkEvent(LogReplicationSinkEvent
                                    .LogReplicationSinkEventType.REMOTE_LEADER_LOSS, nodeId));
                }
                return;
            }

            if (msg.getPayload().getPayloadCase().equals(CorfuMessage.ResponsePayloadMsg.PayloadCase.LR_REVERSE_REPLICATE_MSG)) {
                // As creation of runtimeFSM and receiving of LR_REVERSE_REPLICATE_MSG is async, buffer the msg until
                // the runtimeFSM is ready.
                if(!sessionToRuntimeFSM.containsKey(session)) {
                    log.info("RuntimeFSM not present. Buffering the msg for session {}", session);
                    reverseReplicateInitMsgBuffer.put(session, msg);
                    return;
                }
                String remoteLeaderId = msg.getPayload().getLrReverseReplicateMsg().getSinkLeaderNodeId();
                sessionToRuntimeFSM.get(session).setRemoteLeaderNodeId(remoteLeaderId);
                // Start runtimeFSM
                addConnectionUpEvent(session, remoteLeaderId);
            } else if (isAckForLocalLeaderLoss(msg)) {
                log.debug("Received an ACK for requestID/payload {}/{}", msg.getHeader().getRequestId(),
                        msg.getPayload().getPayloadCase());
                // the ACK for leadership loss msg from SOURCE is completed with null
                completeRequest(msg.getHeader().getSession(), msg.getHeader().getRequestId(), null);
                return;

            } else {
                // Route the message to the handler.
                if (log.isTraceEnabled()) {
                    log.trace("Message routed to {}}: {}", msgHandler.getClass().getSimpleName(), msg);
                }
                msgHandler.handleMessage(null, msg, this);
            }
        } catch (NullPointerException npe) {
            log.info("Either runtimeFSM or remoteSourceLeadershipManager is not found when msg of type {} is received " +
                    "for session {}", msg.getPayload().getPayloadCase(), msg.getHeader().getSession());
        } catch (Exception e) {
            log.error("Exception caught while receiving message of type {}",
                    msg.getPayload().getPayloadCase(), e);
        }
    }

    private boolean isAckForLocalLeaderLoss(CorfuMessage.ResponseMsg msg) {
        return msg.getPayload().getPayloadCase().equals(CorfuMessage.ResponsePayloadMsg.PayloadCase.LR_ENTRY_ACK) &&
                !msg.getPayload().getLrEntryAck().hasMetadata();
    }

    @Override
    public void receive(CorfuMessage.RequestMsg message) {
        log.debug("Received request message {}", message.getPayload().getPayloadCase());
        try {
            if (message.getPayload().getPayloadCase().equals(CorfuMessage.RequestPayloadMsg.PayloadCase.LR_LEADERSHIP_LOSS)) {

                // Leadership Loss is received as a request only if the local cluster is Sink and connection starter.
                LogReplicationSession session = message.getHeader().getSession();

                this.clientChannelAdapter.processLeadershipLoss(session);
                sessionToRemoteSourceLeaderManager.get(session).input(new LogReplicationSinkEvent(
                        LogReplicationSinkEvent.LogReplicationSinkEventType.REMOTE_LEADER_LOSS,
                        message.getPayload().getLrLeadershipLoss().getNodeId()));
                // ack the leadership loss msg with an empty payload
                CorfuMessage.ResponsePayloadMsg responsePayloadMsg = CorfuMessage.ResponsePayloadMsg.newBuilder()
                        .setLrEntryAck(LogReplication.LogReplicationEntryMsg.newBuilder().build()).build();

                CorfuMessage.ResponseMsg responseMsg = getResponseMsg(getHeaderMsg(message.getHeader()),
                        responsePayloadMsg);

                // If there is a leadership loss on the remote Source cluster, send an ACK responding to it because
                // the old leader is waiting(blocked) on this ACK
                clientChannelAdapter.send(message.getPayload().getLrLeadershipLoss().getNodeId(), responseMsg);
                return;
            }
        } catch (NullPointerException npe) {
            log.error("Ignoring the leadership_loss msg. The replication components has already been stopped for " +
                    "session {} ", message.getHeader().getSession());
            return;
        }

        // Route the message to the handler.
        if (log.isTraceEnabled()) {
            log.trace("Message routed to {}: {}", msgHandler.getClass().getSimpleName(), message);
        }

        try {
            msgHandler.handleMessage(message, null, this);
        } catch (Throwable t) {
            log.error("channelRead: Handling {} failed due to {}:{}",
                    message.getPayload().getPayloadCase(),
                    t.getClass().getSimpleName(),
                    t.getMessage(),
                    t);
        }
    }

    /**
     * Check if this node is a connection starter for the given session.
     *
     * @param session
     * @return true if the session is a connection starter, false otherwise
     */
    public boolean isConnectionStarterForSession(LogReplicationSession session) {
        return topology.getRemoteClusterEndpoints().containsKey(session.getSinkClusterId()) ||
                topology.getRemoteClusterEndpoints().containsKey(session.getSourceClusterId());
    }

    private void removeSessionInfo(LogReplicationSession session) {
        sessionToRemoteSourceLeaderManager.remove(session);
        sessionToOutstandingRequests.remove(session);
        sessionToRuntimeFSM.remove(session);
        sessionToRemoteClusterDescriptor.remove(session);
        sessionToRequestIdCounter.remove(session);
        sessionToLeaderConnectionFuture.remove(session);
    }

    public void shutDownMsgHandlerServer() {
        // A executor service to create the shutdown threads plus name the threads correctly.
        final ExecutorService shutdownService = Executors.newSingleThreadExecutor(
                new ServerThreadFactory("ReplicationCorfuServer-shutdown-",
                        new ServerThreadFactory.ExceptionHandler()));

        // Turn into a list of futures on the shutdown, returning
        // generating a log message to inform of the result.
        CompletableFuture shutdownFuture = CompletableFuture.runAsync(() -> {
            try {
                log.info("Shutting down {}", msgHandler.getClass().getSimpleName());
                msgHandler.shutdown();
                log.info("Cleanly shutdown {}", msgHandler.getClass().getSimpleName());
            } catch (Exception e) {
                log.error("Failed to cleanly shutdown {}", msgHandler.getClass().getSimpleName(), e);
            }
        }, shutdownService);

        CompletableFuture.allOf(shutdownFuture).join();
        shutdownService.shutdown();
    }

    /**
     * Channel Adapter On Error Callback
     */
    public synchronized void onError(Throwable t, LogReplicationSession session) {
        try {
            sessionToRuntimeFSM.get(session).input(new LogReplicationRuntimeEvent(LogReplicationRuntimeEvent
                    .LogReplicationRuntimeEventType.ERROR, t));
        } catch (NullPointerException npe) {
            log.info("RuntimeFSM not found for session {}", session);
        }
    }


    /**
     * Verify Message is of valid Log Replication type.
     */
    private boolean isValidMessage(CorfuMessage.RequestPayloadMsg message) {
        return message.getPayloadCase().equals(CorfuMessage.RequestPayloadMsg.PayloadCase.LR_ENTRY) ||
                message.getPayloadCase().equals(CorfuMessage.RequestPayloadMsg.PayloadCase.LR_METADATA_REQUEST) ||
                message.getPayloadCase().equals(CorfuMessage.RequestPayloadMsg.PayloadCase.LR_LEADERSHIP_QUERY) ||
                message.getPayloadCase().equals(CorfuMessage.RequestPayloadMsg.PayloadCase.LR_LEADERSHIP_LOSS);
    }

    /**
     * Start log replication for a session.
     * For the connection initiator cluster, this is called when the connection is established.
     * For the connection receiving cluster, this is called when the cluster receives a subscribeMsg from remote.
     */
    private void addConnectionUpEvent(LogReplicationSession session, String nodeId) {
        try {
            log.debug("Input Connection Up event for session {}", session);
            sessionToRuntimeFSM.get(session)
                    .input(new LogReplicationRuntimeEvent(LogReplicationRuntimeEvent
                            .LogReplicationRuntimeEventType.ON_CONNECTION_UP, nodeId));
        } catch (NullPointerException npe) {
            log.info("runtimeFsm is not present for session {}", session);
        }
    }

    /**
     * A callback for Cluster Change, like a node added/removed from the remote cluster.
     *
     * @param clusterDescriptor remote cluster descriptor
     */
    public void onClusterChange(ClusterDescriptor clusterDescriptor) {
        if(this.clientChannelAdapter != null) {
            this.clientChannelAdapter.clusterChangeNotification(clusterDescriptor);
        }
    }

    public Optional<String> getRemoteLeaderNodeId() {
        return Optional.empty();
    }

    /**
     * Connect to remote cluster for a given session.
     * @param remoteClusterDescriptor remoteCLuster to connect
     * @param session session information
     */
    public void connect(ClusterDescriptor remoteClusterDescriptor, LogReplicationSession session) {
        log.info("Connect asynchronously to remote cluster {} and session {} ", remoteClusterDescriptor.getClusterId(),
                session);

        try {
            IRetry.build(IntervalRetry.class, () -> {
                try {
                    this.clientChannelAdapter.connectAsync(remoteClusterDescriptor, session);
                } catch (Exception e) {
                    log.error("Failed to connect to remote cluster for session {}. Retry after 1 second. Exception {}.",
                            session, e);
                    throw new RetryNeededException();
                }
                return null;
            }).run();
        } catch (InterruptedException e) {
            log.error("Unrecoverable exception when attempting to connect to remote session.", e);
        }
    }

    /**
     * Called as soon as the connection to the remote node has been established
     *
     * @param nodeId remote node to which connection has been established successfully
     * @param session session which will use the established connection
     */
    public void onConnectionUp(String nodeId, LogReplicationSession session) {
        log.info("Connection established to remote node {} for session {}.", nodeId, session);
        if (outgoingSession.contains(session)) {
            addConnectionUpEvent(session, nodeId);
        } else {
            sessionToRemoteSourceLeaderManager.get(session).input(new LogReplicationSinkEvent(
                    LogReplicationSinkEvent.LogReplicationSinkEventType.ON_CONNECTION_UP, nodeId));
        }
    }

    /**
     * Called when the connection to remote is lost.
     *
     * @param nodeId remote node to which the connection is lost
     * @param session session which is interrupted
     */
    public void onConnectionDown(String nodeId, LogReplicationSession session) {
        try {
            log.info("Connection lost to remote node {} for session {}", nodeId, session);
            if (outgoingSession.contains(session)) {
                sessionToRuntimeFSM.get(session).input(new LogReplicationRuntimeEvent(
                        LogReplicationRuntimeEvent.LogReplicationRuntimeEventType.ON_CONNECTION_DOWN, nodeId));
            } else if (incomingSession.contains(session)){
                sessionToRemoteSourceLeaderManager.get(session).input(new LogReplicationSinkEvent(
                        LogReplicationSinkEvent.LogReplicationSinkEventType.ON_CONNECTION_DOWN, nodeId));
            }
        } catch (NullPointerException npe) {
            log.info("The replication components are already stopped for session {}", session);
            return;
        }

        try {
            IRetry.build(IntervalRetry.class, () -> {
                try {
                    this.clientChannelAdapter.connectAsync(sessionToRemoteClusterDescriptor.get(session), nodeId, session);
                } catch(NullPointerException npe) {
                    log.info("The session is already stopped. Abort trying to reconnect {}", session);
                    return null;
                } catch (Exception e) {
                    log.error("Failed to connect to remote node {} for session {}. Retry after 1 second. Exception {}.",
                            nodeId, session, e);
                    throw new RetryNeededException();
                }
                return null;
            }).run();
        } catch (InterruptedException e) {
            log.error("Unrecoverable exception when attempting to connect to remote node {}.", nodeId, e);
        }
    }

    public void onConnectionDown(LogReplicationSession session) {
        Optional<String> remoteLeader;
        try {
            remoteLeader = sessionToRuntimeFSM.get(session).getRemoteLeaderNodeId();
        } catch (NullPointerException npe) {
            log.info("Runtime FSM is not present for session {}", session);
            remoteLeader = Optional.empty();
        }

        if(remoteLeader.isPresent()) {
            onConnectionDown(remoteLeader.get(), session);
        }
    }

    /**
     * Update topology config ID
     *
     * @param topologyId new topology config ID
     */
    public void updateTopologyConfigId(long topologyId) {
        msgHandler.updateTopologyConfigId(topologyId);
    }

    public void resetRemoteLeader(LogReplicationSession session) {
        if (isConnectionStarterForSession(session)) {
            this.clientChannelAdapter.resetRemoteLeader();
        }
    }
}
