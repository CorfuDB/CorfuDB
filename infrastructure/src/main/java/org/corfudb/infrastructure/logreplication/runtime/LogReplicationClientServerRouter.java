package org.corfudb.infrastructure.logreplication.runtime;

import com.sun.istack.internal.NotNull;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.ServerThreadFactory;
import org.corfudb.infrastructure.logreplication.infrastructure.ClusterDescriptor;
import org.corfudb.infrastructure.logreplication.infrastructure.CorfuReplicationManager;
import org.corfudb.infrastructure.logreplication.infrastructure.SessionManager;
import org.corfudb.infrastructure.logreplication.infrastructure.msgHandlers.LogReplicationServer;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.LogReplicationPluginConfig;
import org.corfudb.infrastructure.logreplication.runtime.fsm.LogReplicationRuntimeEvent;
import org.corfudb.infrastructure.logreplication.runtime.fsm.sink.LogReplicationSinkEvent;
import org.corfudb.infrastructure.logreplication.runtime.fsm.sink.VerifyRemoteSourceLeader;
import org.corfudb.infrastructure.logreplication.transport.IClientServerRouter;
import org.corfudb.infrastructure.logreplication.transport.client.ChannelAdapterException;
import org.corfudb.infrastructure.logreplication.transport.client.IClientChannelAdapter;
import org.corfudb.infrastructure.logreplication.transport.server.IServerChannelAdapter;
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
import static org.corfudb.protocols.service.CorfuProtocolMessage.getRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getResponseMsg;

/**
 * Router, interfaces between the custom transport layer and LR core components.
 *
 * There is one router per node, and all the sessions (both incoming and outgoing) share the router.
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
    private volatile Map<LogReplicationSession, CompletableFuture<Void>> sessionToLeaderConnectionFuture;

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
     * The outstanding requests on this router.
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
     * The replicationManager
     */
    private final CorfuReplicationManager replicationManager;

    /**
     * local cluster ID
     */
    private String localClusterId;

    /**
     * local node ID
     */
    private String localNodeId;

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
    @Getter
    private final LogReplicationServer msgHandler;

    /**
     * Map of session -> verifySinkLeadership. VerifySinkLeadership triggers leadership request and and upon receiving
     * the response, creates a bidirectional streams to remote endpoints
     */
    private final Map<LogReplicationSession, VerifyRemoteSourceLeader> sessionToSinkVerifyLeadership;

    /**
     * Session manager to check session details
     */
    private final SessionManager sessionManager;


    /**
     * Log Replication Router Constructor
     *
     * @param responseTimeout timeout for requests
     * @param replicationManager replicationManager to start FSM
     * @param localClusterId local cluster ID
     * @param serverContext server context
     * @param localCorfuEndpoint local endpoint
     * @param sessionManager session manager
     */
    public LogReplicationClientServerRouter(long responseTimeout,
                                            CorfuReplicationManager replicationManager, String localClusterId, String localNodeId,
                                            ServerContext serverContext, String localCorfuEndpoint,
                                            SessionManager sessionManager) {
        this.timeoutResponse = responseTimeout;
        this.localClusterId = localClusterId;
        this.localNodeId = localNodeId;
        this.replicationManager = replicationManager;
        this.sessionToRuntimeFSM = replicationManager.getSessionRuntimeMap();
        this.sessionManager = sessionManager;

        this.sessionToRemoteClusterDescriptor = new ConcurrentHashMap<>();
        this.sessionToRequestIdCounter = new ConcurrentHashMap<>();
        this.sessionToOutstandingRequests = new ConcurrentHashMap<>();
        this.sessionToLeaderConnectionFuture = new ConcurrentHashMap<>();
        this.sessionToSinkVerifyLeadership = new ConcurrentHashMap<>();
        this.clientChannelAdapter = null;
        this.serverChannelAdapter = null;

        msgHandler = new LogReplicationServer(serverContext, sessionManager, localCorfuEndpoint);
    }

    public CompletableFuture<Boolean> createTransportServerAdapter(ServerContext serverContext) {

        LogReplicationPluginConfig config = new LogReplicationPluginConfig(serverContext.getPluginConfigFilePath());
        File jar = new File(config.getTransportAdapterJARPath());

        try (URLClassLoader child = new URLClassLoader(new URL[]{jar.toURI().toURL()}, this.getClass().getClassLoader())) {
            Class adapter = Class.forName(config.getTransportServerClassCanonicalName(), true, child);
            this.serverChannelAdapter =  (IServerChannelAdapter) adapter
                    .getDeclaredConstructor(ServerContext.class, LogReplicationClientServerRouter.class)
                    .newInstance(serverContext, this);
            return this.serverChannelAdapter.start();
        } catch (Exception e) {
            log.error("Fatal error: Failed to create serverAdapter", e);
            throw new UnrecoverableCorfuError(e);
        }
    }

    public void createTransportClientAdapter(String pluginFilePath) {
        if(this.clientChannelAdapter != null) {
            log.trace("The client channel adapter is already initialized");
            return;
        }

        LogReplicationPluginConfig config = new LogReplicationPluginConfig(pluginFilePath);
        File jar = new File(config.getTransportAdapterJARPath());

        try (URLClassLoader child = new URLClassLoader(new URL[]{jar.toURI().toURL()}, this.getClass().getClassLoader())) {
            // Instantiate Channel Adapter (external implementation of the channel / transport)
            Class adapterType = Class.forName(config.getTransportClientClassCanonicalName(), true, child);
            this.clientChannelAdapter = (IClientChannelAdapter) adapterType
                    .getDeclaredConstructor(String.class, LogReplicationClientServerRouter.class)
                    .newInstance(this.localClusterId, this);
        } catch (Exception e) {
            log.error("Fatal error: Failed to initialize transport adapter {}",
                    config.getTransportClientClassCanonicalName(), e);
            throw new UnrecoverableCorfuError(e);
        }
    }


    /**
     * Add runtimeFSM of the router
     * @param runtimeFSM runtime state machine, insert connection related events
     */
    public void addRuntimeFSM(LogReplicationSession session, CorfuLogReplicationRuntime runtimeFSM) {
        this.sessionToRuntimeFSM.put(session, runtimeFSM);
    }

    /**
     * Send a request message and get a completable future to be fulfilled by the reply.
     *
     * @param payload
     * @param <T> The type of completable to return.
     * @return A completable future which will be fulfilled by the reply,
     * or a timeout in the case there is no response.
     */
    @Override
    public <T> CompletableFuture<T> sendRequestAndGetCompletable(
            @NotNull LogReplicationSession session,
            @Nonnull CorfuMessage.RequestPayloadMsg payload,
            @Nonnull String nodeId) {

        CorfuMessage.HeaderMsg.Builder header = CorfuMessage.HeaderMsg.newBuilder()
                .setSession(session)
                .setVersion(getDefaultProtocolVersionMsg())
                .setIgnoreClusterId(true)
                .setIgnoreEpoch(true);

        if (isValidMessage(payload)) {
            // Get the next request ID.
            final long requestId = sessionToRequestIdCounter.get(session).getAndIncrement();

            // Generate a future and put it in the completion table.
            final CompletableFuture<T> cf = new CompletableFuture<>();
            sessionToOutstandingRequests.get(session).put(requestId, cf);

            try {
                header.setRequestId(requestId);
                header.setClusterId(getUuidMsg(UUID.fromString(this.localClusterId)));

                // If no endpoint is specified, the message is to be sent to the remote leader node.
                // We should block until a connection to the leader is established.
                if (nodeId.equals(REMOTE_LEADER)) {

                    if (isConnectionInitiator(session)) {
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

                    if(sessionManager.getOutgoingSessions().contains(session)) {
                        CorfuLogReplicationRuntime runtimeFSM = sessionToRuntimeFSM.get(session);
                        // Get Remote Leader
                        if (runtimeFSM.getRemoteLeaderNodeId().isPresent()) {
                            nodeId = runtimeFSM.getRemoteLeaderNodeId().get();
                        } else {
                            log.error("Leader not found to remote cluster {}", sessionToRemoteClusterDescriptor.get(session).getClusterId());
                            runtimeFSM.input(new LogReplicationRuntimeEvent(LogReplicationRuntimeEvent.LogReplicationRuntimeEventType.REMOTE_LEADER_LOSS));
                            throw new ChannelAdapterException(
                                    String.format("Leader not found to remote cluster %s", sessionToRemoteClusterDescriptor.get(session).getClusterId()));
                        }
                    }
                }

                // In the case the message is intended for a specific endpoint, we do not
                // block on connection future, this is the case of leader verification.
                if(isConnectionInitiator(session)) {
                    clientChannelAdapter.send(nodeId, getRequestMsg(header.build(), payload));
                } else {
                    serverChannelAdapter.send(getRequestMsg(header.build(), payload));
                }
            } catch (NetworkException ne) {
                log.error("Caught Network Exception while trying to send message to remote leader {}", nodeId);
                if(sessionManager.getOutgoingSessions().contains(session)) {
                    sessionToRuntimeFSM.get(session).input(new LogReplicationRuntimeEvent(LogReplicationRuntimeEvent.LogReplicationRuntimeEventType.ON_CONNECTION_DOWN,
                            nodeId));
                } else {
                    sessionToSinkVerifyLeadership.get(session).input(new LogReplicationSinkEvent(
                            LogReplicationSinkEvent.LogReplicationSinkEventType.ON_CONNECTION_DOWN, nodeId));
                }
                throw ne;
            } catch (Exception e) {
                sessionToOutstandingRequests.get(session).remove(requestId);
                log.error("sendMessageAndGetCompletable: Remove request {} to {} due to exception! Message:{}",
                        requestId, sessionToRemoteClusterDescriptor.get(session).getClusterId(), payload.getPayloadCase(), e);
                cf.completeExceptionally(e);
                return cf;
            }

            // Generate a timeout future, which will complete exceptionally
            // if the main future is not completed.
            final CompletableFuture<T> cfTimeout =
                    CFUtils.within(cf, Duration.ofMillis(timeoutResponse));
            cfTimeout.exceptionally(e -> {
                if (e.getCause() instanceof TimeoutException) {
                    sessionToOutstandingRequests.get(session).remove(requestId);
                    log.debug("sendMessageAndGetCompletable: Remove request {} to {} due to timeout! Message:{}",
                            requestId, sessionToRemoteClusterDescriptor.get(session).getClusterId(), payload.getPayloadCase());
                }
                return null;
            });

            return cfTimeout;
        }

        log.error("Invalid message type {}. Currently only log replication messages are processed.",
                payload.getPayloadCase());
        CompletableFuture<T> f = new CompletableFuture<>();
        f.completeExceptionally(new Throwable("Invalid message type"));
        return f;
    }

    @Override
    public <T> void completeRequest(LogReplicationSession session, long requestID, T completion) {
        log.trace("Complete request: {}...outstandingRequests {}", requestID, sessionToOutstandingRequests.get(session));
        CompletableFuture<T> cf;
        if ((cf = (CompletableFuture<T>) sessionToOutstandingRequests.get(session).remove(requestID)) != null) {
            cf.complete(completion);
        } else {
            log.warn("Attempted to complete request {}, but request not outstanding!", requestID);
        }
    }

    @Override
    public void completeExceptionally(LogReplicationSession session, long requestID, Throwable cause) {
        CompletableFuture cf;
        if ((cf = sessionToOutstandingRequests.get(session).remove(requestID)) != null) {
            cf.completeExceptionally(cause);
            log.debug("completeExceptionally: Remove request {} to {} due to {}.", requestID,
                    sessionToRemoteClusterDescriptor.get(session).getClusterId(), cause.getClass().getSimpleName(), cause);
        } else {
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
    public void setTimeoutResponse(long timeoutResponse) {
        this.timeoutResponse = timeoutResponse;
    }

    /**
     * Interfaces between the LogReplicationServer (msg handler), and the transport layer
     * @param response Log replication response message
     */
    @Override
    public void sendResponse(CorfuMessage.ResponseMsg response) {
        log.trace("Ready to send response {}", response.getPayload().getPayloadCase());
        LogReplicationSession session = response.getHeader().getSession();
        if (isConnectionInitiator(session)) {
            if(sessionManager.getIncomingSessions().contains(session)) {
                if (sessionToSinkVerifyLeadership.get(session).getRemoteLeaderNodeId().isPresent()) {
                    clientChannelAdapter.send(sessionToSinkVerifyLeadership.get(session).getRemoteLeaderNodeId().get(), response);
                } else {
                    sessionToSinkVerifyLeadership.get(session).input(
                            new LogReplicationSinkEvent(LogReplicationSinkEvent.LogReplicationSinkEventType.REMOTE_LEADER_LOSS));
                }
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

    /**
     * Receive Corfu Message from the Channel Adapter for further processing
     *
     * @param msg received corfu message
     */
    @Override
    public void receive(CorfuMessage.ResponseMsg msg) {
        try {
            LogReplicationSession session = msg.getHeader().getSession();

            // If it is a Leadership Loss Message re-trigger leadership discovery
            if (msg.getPayload().getPayloadCase() == CorfuMessage.ResponsePayloadMsg.PayloadCase.LR_LEADERSHIP_LOSS) {
                String nodeId = msg.getPayload().getLrLeadershipLoss().getNodeId();
                if (isOutgoingSession(session)) {
                    this.sessionToRuntimeFSM.get(session)
                            .input(new LogReplicationRuntimeEvent(LogReplicationRuntimeEvent.LogReplicationRuntimeEventType.REMOTE_LEADER_LOSS, nodeId));
                } else {
                    sessionToSinkVerifyLeadership.get(session)
                            .input(new LogReplicationSinkEvent(LogReplicationSinkEvent.LogReplicationSinkEventType.REMOTE_LEADER_LOSS, nodeId));
                }
                return;
            }

            if (msg.getPayload().getPayloadCase().equals(CorfuMessage.ResponsePayloadMsg.PayloadCase.LR_SUBSCRIBE_MSG)) {
                String RemoteLeaderId = msg.getPayload().getLrSubscribeMsg().getSinkLeaderNodeId();
                // Start runtimeFSM
                sessionToRuntimeFSM.get(session).setRemoteLeaderNodeId(RemoteLeaderId);
                sessionToRuntimeFSM.get(session).start();
                log.debug("runtimeFSM started for session {}", session);
                sessionToRuntimeFSM.get(session)
                        .input(new LogReplicationRuntimeEvent(LogReplicationRuntimeEvent.LogReplicationRuntimeEventType.ON_CONNECTION_UP, RemoteLeaderId));
            } else {
                // Route the message to the handler.
                if (log.isTraceEnabled()) {
                    log.trace("Message routed to {}}: {}", msgHandler.getClass().getSimpleName(), msg);
                }
                msgHandler.handleMessage(null, msg, this);
            }
        } catch (Exception e) {
            log.error("Exception caught while receiving message of type {}",
                    msg.getPayload().getPayloadCase(), e);
        }
    }

    /**
     * Receive messages from the 'custom' serverAdapter implementation. This message will be forwarded
     * for processing.
     *
     * @param message
     */
    @Override
    public void receive(CorfuMessage.RequestMsg message) {
        log.debug("Received request message {}", message.getPayload().getPayloadCase());

        // Route the message to the handler.
        if (log.isTraceEnabled()) {
            log.trace("Message routed to {}: {}", msgHandler.getClass().getSimpleName(), message);
        }

        try {
            msgHandler.handleMessage(message, null,  this);
        } catch (Throwable t) {
            log.error("channelRead: Handling {} failed due to {}:{}",
                    message.getPayload().getPayloadCase(),
                    t.getClass().getSimpleName(),
                    t.getMessage(),
                    t);
        }
    }

    public boolean isConnectionInitiator(LogReplicationSession session) {
        return sessionManager.isConnectionStarterForSession(session);
    }

    private void removeSessionInfo(LogReplicationSession session) {
        sessionToSinkVerifyLeadership.remove(session);
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

    private boolean isOutgoingSession(LogReplicationSession session) {
        return sessionManager.getOutgoingSessions().contains(session);
    }

    /**
     * Channel Adapter On Error Callback
     */
    public synchronized void onError(Throwable t, LogReplicationSession session) {
        sessionToRuntimeFSM.get(session).input(new LogReplicationRuntimeEvent(LogReplicationRuntimeEvent.LogReplicationRuntimeEventType.ERROR, t));
    }


    /**
     * Verify Message is of valid Log Replication type.
     */
    private boolean isValidMessage(CorfuMessage.RequestPayloadMsg message) {
        return message.getPayloadCase().equals(CorfuMessage.RequestPayloadMsg.PayloadCase.LR_ENTRY) ||
                message.getPayloadCase().equals(CorfuMessage.RequestPayloadMsg.PayloadCase.LR_METADATA_REQUEST) ||
                message.getPayloadCase().equals(CorfuMessage.RequestPayloadMsg.PayloadCase.LR_LEADERSHIP_QUERY);
    }

    private void startReplication(LogReplicationSession session, String nodeId) {
        replicationManager.startLogReplicationRuntime(session);
        log.debug("runtimeFSM started for session {}", session);
        sessionToRuntimeFSM.get(session)
                .input(new LogReplicationRuntimeEvent(LogReplicationRuntimeEvent.LogReplicationRuntimeEventType.ON_CONNECTION_UP, nodeId));
    }

    /**
     * A callback for Cluster Change, like a node added/removed from the remote cluster.
     *
     * @param clusterDescriptor remote cluster descriptor
     */
    public synchronized void onClusterChange(ClusterDescriptor clusterDescriptor) {
        if(this.clientChannelAdapter != null) {
            this.clientChannelAdapter.clusterChangeNotification(clusterDescriptor);
        }
    }

    public Optional<String> getRemoteLeaderNodeId() {
        return Optional.empty();
    }

    public void resetRemoteLeader() {
    }

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

        if (sessionManager.getIncomingSessions().contains(session)) {
            sessionToSinkVerifyLeadership.put(session, new VerifyRemoteSourceLeader(session, this, localNodeId));
        }
    }

    public void onConnectionUp(String nodeId, LogReplicationSession session) {
        log.info("Connection established to remote node {} for session {}.", nodeId, session);
        if (sessionManager.getOutgoingSessions().contains(session)) {
            startReplication(session, nodeId);
        } else {
            sessionToSinkVerifyLeadership.get(session).input(new LogReplicationSinkEvent(
                    LogReplicationSinkEvent.LogReplicationSinkEventType.ON_CONNECTION_UP, nodeId));
        }
    }

    public void onConnectionDown(String nodeId, LogReplicationSession session) {
        log.info("Connection lost to remote node {} for session {}", nodeId, session);
        if(sessionManager.getOutgoingSessions().contains(session)) {
            sessionToRuntimeFSM.get(session).input(new LogReplicationRuntimeEvent(
                    LogReplicationRuntimeEvent.LogReplicationRuntimeEventType.ON_CONNECTION_DOWN, nodeId));
        } else {
            sessionToSinkVerifyLeadership.get(session).input(new LogReplicationSinkEvent(
                    LogReplicationSinkEvent.LogReplicationSinkEventType.ON_CONNECTION_DOWN, nodeId));
        }

        if (isConnectionInitiator(session)) {
            this.clientChannelAdapter.connectAsync(sessionToRemoteClusterDescriptor.get(session), nodeId, session);
        }
    }

    public void updateTopologyConfigId(long topologyId) {
        msgHandler.updateTopologyConfigId(topologyId);
    }
}
