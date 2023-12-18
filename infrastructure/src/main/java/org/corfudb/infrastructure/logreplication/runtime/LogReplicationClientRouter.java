package org.corfudb.infrastructure.logreplication.runtime;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.LogReplicationRuntimeParameters;
import org.corfudb.infrastructure.logreplication.infrastructure.ClusterDescriptor;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.LogReplicationPluginConfig;
import org.corfudb.infrastructure.logreplication.runtime.fsm.LogReplicationRuntimeEvent;
import org.corfudb.infrastructure.logreplication.runtime.fsm.LogReplicationRuntimeEvent.LogReplicationRuntimeEventType;
import org.corfudb.infrastructure.logreplication.transport.client.ChannelAdapterException;
import org.corfudb.infrastructure.logreplication.transport.client.IClientChannelAdapter;
import org.corfudb.protocols.service.CorfuProtocolMessage.ClusterIdCheck;
import org.corfudb.protocols.service.CorfuProtocolMessage.EpochCheck;
import org.corfudb.runtime.clients.IClient;
import org.corfudb.runtime.clients.IClientRouter;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.runtime.proto.RpcCommon;
import org.corfudb.runtime.proto.service.CorfuMessage;
import org.corfudb.runtime.proto.service.CorfuMessage.HeaderMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestPayloadMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponsePayloadMsg.PayloadCase;
import org.corfudb.util.CFUtils;

import javax.annotation.Nonnull;
import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
 * This Client Router is used when a custom (client-defined) transport layer is specified for
 * Log Replication Server communication.
 */
@Slf4j
public class LogReplicationClientRouter implements IClientRouter {

    public static String REMOTE_LEADER = "REMOTE_LEADER";

    @Getter
    private final LogReplicationRuntimeParameters parameters;

    /**
     * The handlers registered to this router.
     */
    private final Map<PayloadCase, IClient> handlerMap;

    /**
     * The clients registered to this router.
     */
    public final List<IClient> clientList;

    /**
     * Whether or not this router is shutdown.
     */
    public volatile boolean shutdown;

    /**
     * A {@link CompletableFuture} which is completed when a connection,
     * including a successful handshake completes and messages can be sent
     * to the remote node.
     */
    @Getter
    private volatile CompletableFuture<Void> remoteLeaderConnectionFuture;

    /**
     * The current request ID.
     */
    @Getter
    @SuppressWarnings("checkstyle:abbreviation")
    public AtomicLong requestID;

    /**
     * Sync call response timeout (milliseconds).
     */
    @Getter
    @Setter
    public long timeoutResponse;

    /**
     * The outstanding requests on this router.
     */
    public final Map<Long, CompletableFuture> outstandingRequests;

    /**
     * Adapter to the channel implementation
     */
    private IClientChannelAdapter channelAdapter;

    /**
     * Remote Cluster/Site Full Descriptor
     */
    private final ClusterDescriptor remoteClusterDescriptor;

    /**
     * Remote Cluster/Site unique identifier
     */
    private final String remoteClusterId;

    /**
     * Runtime FSM, to insert connectivity events
     */
    private final CorfuLogReplicationRuntime runtimeFSM;

    /**
     * Log Replication Client Constructor
     *
     * @param parameters runtime parameters (including connection settings)
     * @param runtimeFSM runtime state machine, insert connection related events
     */
    public LogReplicationClientRouter(LogReplicationRuntimeParameters parameters,
                                      CorfuLogReplicationRuntime runtimeFSM) {
        this.remoteClusterDescriptor = parameters.getRemoteClusterDescriptor();
        this.remoteClusterId = remoteClusterDescriptor.getClusterId();
        this.parameters = parameters;
        this.timeoutResponse = parameters.getRequestTimeout().toMillis();
        this.runtimeFSM = runtimeFSM;

        this.handlerMap = new ConcurrentHashMap<>();
        this.clientList = new ArrayList<>();
        this.requestID = new AtomicLong();
        this.outstandingRequests = new ConcurrentHashMap<>();
        this.remoteLeaderConnectionFuture = new CompletableFuture<>();
    }

    // ------------------- IClientRouter Interface ----------------------

    @Override
    public IClientRouter addClient(IClient client) {
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
     * Send a request message and get a completable future to be fulfilled by the reply.
     *
     * @param payload
     * @param <T> The type of completable to return.
     * @return A completable future which will be fulfilled by the reply,
     * or a timeout in the case there is no response.
     */
    @Override
    public <T> CompletableFuture<T> sendRequestAndGetCompletable(
            @Nonnull RequestPayloadMsg payload,
            @Nonnull String nodeId) {

        HeaderMsg.Builder header = HeaderMsg.newBuilder()
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
                header.setClientId(getUuidMsg(parameters.getClientId()));
                header.setRequestId(requestId);

                // If no endpoint is specified, the message is to be sent to the remote leader node.
                // We should block until a connection to the leader is established.
                if (nodeId.equals(REMOTE_LEADER)) {
                    // Check the connection future. If connected, continue with sending the message.
                    // If timed out, return a exceptionally completed with the timeout.
                    // Because in Log Replication, messages are sent to the leader node, the connection future
                    // represents a connection to the leader.
                    try {
                        remoteLeaderConnectionFuture
                                .get(getParameters().getConnectionTimeout().toMillis(), TimeUnit.MILLISECONDS);
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
                        log.error("Leader not found to remote cluster {}", remoteClusterId);
                        runtimeFSM.input(new LogReplicationRuntimeEvent(LogReplicationRuntimeEventType.REMOTE_LEADER_LOSS));
                        throw new ChannelAdapterException(
                                String.format("Leader not found to remote cluster %s", remoteClusterDescriptor.getClusterId()));
                    }
                }

                // In the case the message is intended for a specific endpoint, we do not
                // block on connection future, this is the case of leader verification.
                log.info("Send message to {}, type={}", nodeId, payload.getPayloadCase());
                channelAdapter.send(nodeId, getRequestMsg(header.build(), payload));
            } catch (NetworkException ne) {
                log.error("Caught Network Exception while trying to send message to remote leader {}", nodeId);
                runtimeFSM.input(new LogReplicationRuntimeEvent(LogReplicationRuntimeEventType.ON_CONNECTION_DOWN,
                        nodeId));
                throw ne;
            } catch (Exception e) {
                outstandingRequests.remove(requestId);
                log.error("sendMessageAndGetCompletable: Remove request {} to {} due to exception! Message:{}",
                        requestId, remoteClusterId, payload.getPayloadCase(), e);
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
                            requestId, remoteClusterId, payload.getPayloadCase());
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

    /**
     * Send a request message and get a completable future to be fulfilled by the reply.
     *
     * @param payload
     * @param epoch
     * @param clusterId
     * @param priority
     * @param ignoreClusterId
     * @param ignoreEpoch
     * @param <T> The type of completable to return.
     * @return A completable future which will be fulfilled by the reply,
     * or a timeout in the case there is no response.
     */
    @Override
    public <T> CompletableFuture<T> sendRequestAndGetCompletable(RequestPayloadMsg payload, long epoch, RpcCommon.UuidMsg clusterId,
                                                                 org.corfudb.runtime.proto.service.CorfuMessage.PriorityLevel priority,
                                                                 ClusterIdCheck ignoreClusterId, EpochCheck ignoreEpoch) {
        // This is an empty stub. This method is not being used anywhere in the LR framework.
        return null;
    }

    /**
     * Send a one way message, without adding a completable future.
     *
     * @param payload
     * @param epoch
     * @param clusterId
     * @param priority
     * @param ignoreClusterId
     * @param ignoreEpoch
     */
    @Override
    public void sendRequest(RequestPayloadMsg payload, long epoch, RpcCommon.UuidMsg clusterId,
                            org.corfudb.runtime.proto.service.CorfuMessage.PriorityLevel priority,
                            ClusterIdCheck ignoreClusterId, EpochCheck ignoreEpoch) {
        // This is an empty stub. This method is not being used anywhere in the LR framework.
    }

    @Override
    public <T> void completeRequest(long requestID, T completion) {
        log.trace("Complete request: {}", requestID);
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
            log.debug("completeExceptionally: Remove request {} to {} due to {}.", requestID, remoteClusterId,
                    cause.getClass().getSimpleName(), cause);
        } else {
            log.warn("Attempted to exceptionally complete request {}, but request not outstanding!",
                    requestID);
        }
    }

    @Override
    public void stop() {
        log.debug("stop: Shutting down router for {}", remoteClusterId);
        shutdown = true;
        channelAdapter.stop();
        remoteLeaderConnectionFuture = new CompletableFuture<>();
        remoteLeaderConnectionFuture.completeExceptionally(new NetworkException("Router stopped", remoteClusterId));
    }

    @Override
    public void reconnect() {
        // no-op
    }

    @Override
    public Integer getPort() {
        // For logging purposes return one port (as this abstraction does not make sense for a Log Replication
        // Client Router) as it is a router to an entire cluster/site.
        return Integer.valueOf(remoteClusterDescriptor.getNodesDescriptors().iterator().next().getPort());
    }

    @Override
    public String getHost() {
        String host = "";
        // For logging purposes return all remote cluster nodes host in a concatenated form
        remoteClusterDescriptor.getNodesDescriptors().forEach(node -> host.concat(node.getHost() + ","));
        return host;
    }

    @Override
    public void setTimeoutConnect(long timeoutConnect) {
    }

    @Override
    public void setTimeoutRetry(long timeoutRetry) {
    }

    @Override
    public void setTimeoutResponse(long timeoutResponse) {
        this.timeoutResponse = timeoutResponse;
    }


    // ---------------------------------------------------------------------------

    /**
     * Receive Corfu Message from the Channel Adapter for further processing
     *
     * @param msg received corfu message
     */
    public void receive(CorfuMessage.ResponseMsg msg) {
        try {
            // If it is a Leadership Loss Message re-trigger leadership discovery
            if (msg.getPayload().getPayloadCase() == PayloadCase.LR_LEADERSHIP_LOSS) {
                String nodeId = msg.getPayload().getLrLeadershipLoss().getNodeId();
                runtimeFSM.input(new LogReplicationRuntimeEvent(LogReplicationRuntimeEventType.REMOTE_LEADER_LOSS, nodeId));
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
     * When an error occurs in the Channel Adapter, trigger
     * exceptional completeness of all pending requests.
     *
     * @param e exception
     */
    public void completeAllExceptionally(Exception e) {
        // Exceptionally complete all requests that were waiting for a completion.
        outstandingRequests.forEach((reqId, reqCompletableFuture) -> {
            reqCompletableFuture.completeExceptionally(e);
            // And also remove them.
            outstandingRequests.remove(reqId);
        });
    }

    /**
     * Connect to remote cluster through the specified channel adapter
     */
    public void connect() {
        LogReplicationPluginConfig config = new LogReplicationPluginConfig(parameters.getPluginFilePath());
        File jar = new File(config.getTransportAdapterJARPath());

        try (URLClassLoader child = new URLClassLoader(new URL[]{jar.toURI().toURL()}, this.getClass().getClassLoader())) {
            // Instantiate Channel Adapter (external implementation of the channel / transport)
            Class adapterType = Class.forName(config.getTransportClientClassCanonicalName(), true, child);
            channelAdapter = (IClientChannelAdapter) adapterType.getDeclaredConstructor(String.class, ClusterDescriptor.class, LogReplicationClientRouter.class)
                    .newInstance(parameters.getLocalClusterId(), remoteClusterDescriptor, this);
            channelAdapter.setChannelContext(parameters.getChannelContext());
            log.info("Connect asynchronously to remote cluster... ");
            // When connection is established to the remote leader node, the remoteLeaderConnectionFuture will be completed.
            channelAdapter.connectAsync();
        } catch (Exception e) {
            log.error("Fatal error: Failed to initialize transport adapter {}", config.getTransportClientClassCanonicalName(), e);
            throw new UnrecoverableCorfuError(e);
        }
    }

    /**
     * Verify Message is of valid Log Replication type.
     */
    private boolean isValidMessage(RequestPayloadMsg message) {
        return message.getPayloadCase().equals(RequestPayloadMsg.PayloadCase.LR_ENTRY) ||
                message.getPayloadCase().equals(RequestPayloadMsg.PayloadCase.LR_METADATA_REQUEST) ||
                message.getPayloadCase().equals(RequestPayloadMsg.PayloadCase.LR_LEADERSHIP_QUERY);
    }

    /**
     * Connection Up Callback.
     *
     * @param nodeId id of the remote node to which connection was established.
     */
    public synchronized void onConnectionUp(String nodeId) {
        log.info("Connection established to remote node {}", nodeId);
        runtimeFSM.input(new LogReplicationRuntimeEvent(LogReplicationRuntimeEventType.ON_CONNECTION_UP, nodeId));
    }

    /**
     * Connection Down Callback.
     *
     * @param nodeId id of the remote node to which connection came down.
     */
    public synchronized void onConnectionDown(String nodeId) {
        log.info("Connection lost to remote node {} on cluster {}", nodeId, remoteClusterId);
        runtimeFSM.input(new LogReplicationRuntimeEvent(LogReplicationRuntimeEventType.ON_CONNECTION_DOWN,
                nodeId));
        // Attempt to reconnect to this endpoint
        channelAdapter.connectAsync(nodeId);
    }

    /**
     * Channel Adapter On Error Callback
     */
    public synchronized void onError(Throwable t) {
        runtimeFSM.input(new LogReplicationRuntimeEvent(LogReplicationRuntimeEventType.ERROR, t));
    }

    /**
     * Cluster Change Callback.
     *
     * @param clusterDescriptor remote cluster descriptor
     */
    public synchronized void onClusterChange(ClusterDescriptor clusterDescriptor) {
        channelAdapter.clusterChangeNotification(clusterDescriptor);
    }

    public Optional<String> getRemoteLeaderNodeId() {
        return runtimeFSM.getRemoteLeaderNodeId();
    }

    public void resetRemoteLeader() {
        if (channelAdapter != null) {
            log.debug("Reset remote leader from channel adapter.");
            channelAdapter.resetRemoteLeader();
        }
    }

}
