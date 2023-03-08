package org.corfudb.infrastructure.logreplication.runtime;

import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.LogReplicationRuntimeParameters;
import org.corfudb.infrastructure.logreplication.infrastructure.ClusterDescriptor;
import org.corfudb.infrastructure.logreplication.infrastructure.CorfuReplicationManager;
import org.corfudb.infrastructure.logreplication.infrastructure.msgHandlers.LogReplicationAbstractServer;
import org.corfudb.infrastructure.logreplication.runtime.fsm.LogReplicationRuntimeEvent;
import org.corfudb.infrastructure.logreplication.transport.IClientServerRouter;
import org.corfudb.infrastructure.logreplication.transport.client.ChannelAdapterException;
import org.corfudb.infrastructure.logreplication.transport.client.IClientChannelAdapter;
import org.corfudb.infrastructure.logreplication.transport.server.IServerChannelAdapter;
import org.corfudb.protocols.service.CorfuProtocolMessage;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.corfudb.runtime.clients.IClient;
import org.corfudb.runtime.clients.IClientRouter;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.runtime.proto.RpcCommon;
import org.corfudb.runtime.proto.service.CorfuMessage;
import org.corfudb.util.CFUtils;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import static org.corfudb.protocols.CorfuProtocolCommon.getUuidMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getDefaultProtocolVersionMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getRequestMsg;

/**
 * A Base Source Router, which interfaces between the Log replication Source components (FSMs, AckReader, etc) and the
 * custom transport adapter
 *
 * This class is inherited by both LogReplicationSourceClientRouter and LogReplicationSourceServerRouter.
 */
@Slf4j
public class LogReplicationSourceBaseRouter implements IClientServerRouter {

    public static String REMOTE_LEADER = "REMOTE_LEADER";

    /**
     * The handlers registered to this router.
     */
    protected final Map<String, LogReplicationAbstractServer> handlerMap;

    /**
     * Whether or not this router is shutdown.
     */
    private volatile boolean shutdown;

    /**
     * A map of remoteCluster to {@link CompletableFuture} which is completed when a connection,
     * including a successful handshake completes and messages can be sent
     * to the remote node.
     */
    @Getter
    protected volatile Map<String, CompletableFuture<Void>> remoteClusterIdToLeaderConnectionFuture;

    /**
     * A map of session to the current requestID for the session.
     */
    @Getter
    @SuppressWarnings("checkstyle:abbreviation")
    private Map<LogReplicationSession,AtomicLong> sessionToRequestIdCounter;

    /**
     * Sync call response timeout (milliseconds).
     */
    @Getter
    @Setter
    private long timeoutResponse;

    /**
     * The outstanding requests on this router.
     */
    protected final Map<LogReplicationSession, Map<Long, CompletableFuture>> sessionToOutstandingRequests;

    /**
     * Runtime FSM, to insert connectivity events
     */
    @Getter
    protected Map<LogReplicationSession, CorfuLogReplicationRuntime> sessionToRuntimeFSM;

    /**
     * Remote cluster's clusterDescriptor
     */
    protected Map<LogReplicationSession, ClusterDescriptor> sessionToRemoteClusterDescriptor;

    /**
     * The replicationManager
     */
    protected final CorfuReplicationManager replicationManager;

    /**
     * local cluster ID
     */
    protected String localClusterId;

    /**
     * If the current cluster is the connection starter
     */
    protected Map<LogReplicationSession, Boolean> sessionToIsConnectionInitiator;

    /**
     * Client Transport adapter
     */
    @Setter
    protected IClientChannelAdapter clientChannelAdapter;

    /**
     * Server transport adapter
     */
    @Setter
    @Getter
    protected IServerChannelAdapter serverChannelAdapter;

    /**
     * session to which the router belongs.
     */
    @Getter
    protected LogReplicationSession session;
    // Shama: session should be removed

    /**
     * Log Replication Client Constructor
     *
     * @param remoteCluster the remote source cluster
     * @param parameters runtime parameters (including connection settings)
     * @param replicationManager replicationManager to start FSM
     * @param session replication session between current and remote cluster
     */
    public LogReplicationSourceBaseRouter(ClusterDescriptor remoteCluster,
                                          LogReplicationRuntimeParameters parameters, CorfuReplicationManager replicationManager,
                                          LogReplicationSession session, boolean isConnectionInitiator) {
        this.timeoutResponse = parameters.getRequestTimeout().toMillis();
        this.localClusterId = session.getSourceClusterId();
        this.replicationManager = replicationManager;

        this.sessionToRemoteClusterDescriptor = new ConcurrentHashMap<>();
        this.handlerMap = new ConcurrentHashMap<>();
        this.sessionToRequestIdCounter = new ConcurrentHashMap<>();
        this.sessionToOutstandingRequests = new ConcurrentHashMap<>();
        this.sessionToRuntimeFSM = new ConcurrentHashMap<>();
        this.remoteClusterIdToLeaderConnectionFuture = new ConcurrentHashMap<>();
        this.sessionToIsConnectionInitiator = new ConcurrentHashMap<>();
        this.clientChannelAdapter = null;
        this.serverChannelAdapter = null;
    }

    public viof


    // ------------------- IClientRouter Interface ----------------------

    @Override
    private IClientRouter addClient(IClient client) {
        // Set the client's router to this instance.
        client.setRouter(this);

        // Iterate through all types of CorfuMsgType, registering the handler
        try {
            client.getHandledCases().forEach(x -> {
                handlerMap.put(x, client);
                log.info("Registered client to handle messages of type {}", x);
            });
        } catch (UnsupportedOperationException ex) {
            log.trace("No registered CorfuMsg handler for client {}", client, ex);
        }

        // Register this type
        clientList.add(client);
        return this;
    }

    /**
     * set runtimeFSM of the router
     * @param runtimeFSM runtime state machine, insert connection related events
     */
    public void setRuntimeFSM(CorfuLogReplicationRuntime runtimeFSM) {
        this.runtimeFSM = runtimeFSM;
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
            @Nonnull CorfuMessage.RequestPayloadMsg payload,
            @Nonnull String nodeId) {

        CorfuMessage.HeaderMsg.Builder header = CorfuMessage.HeaderMsg.newBuilder()
                .setSession(session)
                .setVersion(getDefaultProtocolVersionMsg())
                .setIgnoreClusterId(true)
                .setIgnoreEpoch(true);

        if (isValidMessage(payload)) {
            // Get the next request ID.
            final long requestId = requestID.getAndIncrement();

            // Generate a future and put it in the completion table.
            final CompletableFuture<T> cf = new CompletableFuture<>();
            outstandingRequests.put(requestId, cf);

            try {
                LogReplicationRuntimeParameters parameters = this.runtimeFSM.getSourceManager().getParameters();
                header.setClientId(getUuidMsg(parameters.getClientId()));
                header.setRequestId(requestId);
                header.setClusterId(getUuidMsg(UUID.fromString(this.localClusterId)));

                // If no endpoint is specified, the message is to be sent to the remote leader node.
                // We should block until a connection to the leader is established.
                if (nodeId.equals(REMOTE_LEADER)) {
                    // Check the connection future. If connected, continue with sending the message.
                    // If timed out, return a exceptionally completed with the timeout.
                    // Because in Log Replication, messages are sent to the leader node, the connection future
                    // represents a connection to the leader.
                    try {
                        remoteLeaderConnectionFuture
                                .get(parameters.getConnectionTimeout().toMillis(), TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        throw new UnrecoverableCorfuInterruptedError(e);
                    } catch (TimeoutException | ExecutionException te) {
                        cf.completeExceptionally(te);
                        return cf;
                    }

                    // Get Remote Leader
                    if (runtimeFSM.getRemoteLeaderNodeId().isPresent()) {
                        nodeId = runtimeFSM.getRemoteLeaderNodeId().get();
                    } else {
                        log.error("Leader not found to remote cluster {}", remoteClusterDescriptor.getClusterId());
                        runtimeFSM.input(new LogReplicationRuntimeEvent(LogReplicationRuntimeEvent.LogReplicationRuntimeEventType.REMOTE_LEADER_LOSS));
                        throw new ChannelAdapterException(
                                String.format("Leader not found to remote cluster %s", remoteClusterDescriptor.getClusterId()));
                    }
                }

                // In the case the message is intended for a specific endpoint, we do not
                // block on connection future, this is the case of leader verification.
                if(isConnectionInitiator) {
                    clientChannelAdapter.send(nodeId, getRequestMsg(header.build(), payload));
                } else {
                    serverChannelAdapter.send(nodeId, getRequestMsg(header.build(), payload));
                }
            } catch (NetworkException ne) {
                log.error("Caught Network Exception while trying to send message to remote leader {}", nodeId);
                runtimeFSM.input(new LogReplicationRuntimeEvent(LogReplicationRuntimeEvent.LogReplicationRuntimeEventType.ON_CONNECTION_DOWN,
                        nodeId));
                throw ne;
            } catch (Exception e) {
                outstandingRequests.remove(requestId);
                log.error("sendMessageAndGetCompletable: Remove request {} to {} due to exception! Message:{}",
                        requestId, remoteClusterDescriptor.getClusterId(), payload.getPayloadCase(), e);
                cf.completeExceptionally(e);
                return cf;
            }

            // Generate a timeout future, which will complete exceptionally
            // if the main future is not completed.
            final CompletableFuture<T> cfTimeout =
                    CFUtils.within(cf, Duration.ofMillis(timeoutResponse));
            cfTimeout.exceptionally(e -> {
                if (e.getCause() instanceof TimeoutException) {
                    outstandingRequests.remove(requestId);
                    log.debug("sendMessageAndGetCompletable: Remove request {} to {} due to timeout! Message:{}",
                            requestId, remoteClusterDescriptor.getClusterId(), payload.getPayloadCase());
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
    public <T> void completeRequest(long requestID, T completion) {
        log.trace("Complete request: {}...outstandingRequests {}", requestID, outstandingRequests);
        CompletableFuture<T> cf;
        if ((cf = (CompletableFuture<T>) outstandingRequests.remove(requestID)) != null) {
            cf.complete(completion);
        } else {
            log.warn("Attempted to complete request {}, but request not outstanding!", requestID);
        }
    }

    @Override
    public void completeExceptionally(long requestID, Throwable cause) {
        CompletableFuture cf;
        if ((cf = outstandingRequests.remove(requestID)) != null) {
            cf.completeExceptionally(cause);
            log.debug("completeExceptionally: Remove request {} to {} due to {}.", requestID,
                    remoteClusterDescriptor.getClusterId(), cause.getClass().getSimpleName(), cause);
        } else {
            log.warn("Attempted to exceptionally complete request {}, but request not outstanding!",
                    requestID);
        }

        if(cause.getCause().getMessage().contains("Network closed")) {

        }
    }

    @Override
    public void stop() {
        log.debug("stop: Shutting down router for {}", remoteClusterDescriptor.getClusterId());
        shutdown = true;
        // the source is GRPC-client, close all channels
        if (clientChannelAdapter != null) {
            clientChannelAdapter.stop();
        }
        remoteLeaderConnectionFuture = new CompletableFuture<>();
        remoteLeaderConnectionFuture.completeExceptionally(new NetworkException("Router stopped", remoteClusterDescriptor.getClusterId()));
    }

    @Override
    public Integer getPort() {
        // For logging purposes return one port (as this abstraction does not make sense for a Log Replication
        // Client Router) as it is a router to an entire cluster/site.
        return Integer.valueOf(remoteClusterDescriptor.getNodeDescriptors().iterator().next().getPort());
    }

    @Override
    public String getHost() {
        String host = "";
        // For logging purposes return all remote cluster nodes host in a concatenated form
        remoteClusterDescriptor.getNodeDescriptors().forEach(node -> host.concat(node.getHost() + ":"));
        return host;
    }

    @Override
    public void setTimeoutResponse(long timeoutResponse) {
        this.timeoutResponse = timeoutResponse;
    }

    @Override
    public void sendResponse(CorfuMessage.ResponseMsg response) {
        log.info("Ready to send response {}", response.getPayload().getPayloadCase());
        try {
            this.serverChannelAdapter.send(response);
            log.info("Sent response: {}", response);
        } catch (IllegalArgumentException e) {
            log.warn("Illegal response type. Ignoring message.", e);
        }
    }


    // ---------------------------------------------------------------------------

    /**
     * Receive Corfu Message from the Channel Adapter for further processing
     *
     * @param msg received corfu message
     */
    protected void receive(CorfuMessage.ResponseMsg msg) {
        try {
            // If it is a Leadership Loss Message re-trigger leadership discovery
            if (msg.getPayload().getPayloadCase() == CorfuMessage.ResponsePayloadMsg.PayloadCase.LR_LEADERSHIP_LOSS) {
                String nodeId = msg.getPayload().getLrLeadershipLoss().getNodeId();
                this.runtimeFSM.input(new LogReplicationRuntimeEvent(LogReplicationRuntimeEvent.LogReplicationRuntimeEventType.REMOTE_LEADER_LOSS, nodeId));
                return;
            }

            // We get the handler for this message from the map
            IClient handler = handlerMap.get(msg.getPayload().getPayloadCase());

            if (handler == null) {
                // The message was unregistered, we are dropping it.
                log.warn("Received unregistered message {}, dropping", msg);
            } else {
                // Route the message to the handler.
                if (log.isTraceEnabled()) {
                    log.trace("Message routed to {}: {}",
                            handler.getClass().getSimpleName(), msg);
                }
                handler.handleMessage(msg, null);
            }
        } catch (Exception e) {
            log.error("Exception caught while receiving message of type {}",
                    msg.getPayload().getPayloadCase(), e);
        }
    }


    /**
     * Verify Message is of valid Log Replication type.
     */
    private boolean isValidMessage(CorfuMessage.RequestPayloadMsg message) {
        return message.getPayloadCase().equals(CorfuMessage.RequestPayloadMsg.PayloadCase.LR_ENTRY) ||
                message.getPayloadCase().equals(CorfuMessage.RequestPayloadMsg.PayloadCase.LR_METADATA_REQUEST) ||
                message.getPayloadCase().equals(CorfuMessage.RequestPayloadMsg.PayloadCase.LR_LEADERSHIP_QUERY);
    }

    protected void startReplication(String nodeId) {
        replicationManager.startLogReplicationRuntime(session);
        log.debug("runtimeFSM started for session {}", session);
        runtimeFSM.input(new LogReplicationRuntimeEvent(LogReplicationRuntimeEvent.LogReplicationRuntimeEventType.ON_CONNECTION_UP, nodeId));
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
}
