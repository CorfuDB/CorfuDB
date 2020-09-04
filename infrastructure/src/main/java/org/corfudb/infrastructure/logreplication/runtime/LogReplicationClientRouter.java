package org.corfudb.infrastructure.logreplication.runtime;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.infrastructure.LogReplicationRuntimeParameters;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.LogReplicationPluginConfig;
import org.corfudb.infrastructure.logreplication.infrastructure.ClusterDescriptor;
import org.corfudb.infrastructure.logreplication.runtime.fsm.LogReplicationRuntimeEvent;
import org.corfudb.infrastructure.logreplication.runtime.fsm.LogReplicationRuntimeEvent.LogReplicationRuntimeEventType;
import org.corfudb.infrastructure.logreplication.utils.CorfuMessageConverterUtils;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.runtime.Messages.CorfuMessage;
import org.corfudb.runtime.clients.IClient;
import org.corfudb.runtime.clients.IClientRouter;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.infrastructure.logreplication.transport.client.ChannelAdapterException;
import org.corfudb.infrastructure.logreplication.transport.client.IClientChannelAdapter;
import org.corfudb.util.CFUtils;
import org.corfudb.utils.common.CorfuMessageProtoBufException;

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

/**
 * This Client Router is used when a custom (client-defined) transport layer is specified for
 * Log Replication Server communication.
 *
 */
@Slf4j
public class LogReplicationClientRouter implements IClientRouter {

    @Getter
    private LogReplicationRuntimeParameters parameters;

    /**
     * The handlers registered to this router.
     */
    private final Map<CorfuMsgType, IClient> handlerMap;

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
    private ClusterDescriptor remoteClusterDescriptor;

    /**
     * Remote Cluster/Site unique identifier
     */
    private String remoteClusterId;

    /**
     * Runtime FSM, to insert connectivity events
     */
    private CorfuLogReplicationRuntime runtimeFSM;

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
        client.getHandledTypes().stream()
                .forEach(x -> {
                    handlerMap.put(x, client);
                    log.info("Registered client to handle messages of type {}", x);
                });

        // Register this type
        clientList.add(client);
        return this;
    }

    @Override
    public <T> CompletableFuture<T> sendMessageAndGetCompletable(CorfuMsg message) {
        return sendMessageAndGetCompletable(message, null);
    }

    public <T> CompletableFuture<T> sendMessageAndGetCompletable(CorfuMsg message, String endpoint) {
        if (isValidMessage(message)) {
            // Get the next request ID.
            final long requestId = requestID.getAndIncrement();

            // Generate a future and put it in the completion table.
            final CompletableFuture<T> cf = new CompletableFuture<>();
            outstandingRequests.put(requestId, cf);

            try {
                message.setClientID(parameters.getClientId());
                message.setRequestID(requestId);

                // If no endpoint is specified, the message is to be sent to the remote leader node.
                // We should block until a connection to the leader is established.
                if (endpoint == null || endpoint.length() == 0) {
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
                    if(runtimeFSM.getRemoteLeader().isPresent()) {
                        endpoint = runtimeFSM.getRemoteLeader().get();
                    } else {
                        log.error("Leader not found to remote cluster {}", remoteClusterId);
                        runtimeFSM.input(new LogReplicationRuntimeEvent(LogReplicationRuntimeEventType.REMOTE_LEADER_LOSS));
                        throw new ChannelAdapterException(
                                String.format("Leader not found to remote cluster %s", remoteClusterDescriptor.getClusterId()));
                    }
                }

                // In the case the message is intended for a specific endpoint, we do not
                // block on connection future, this is the case of leader verification.
                log.info("Send message to {}, type={}", endpoint, message.getMsgType());
                channelAdapter.send(endpoint, CorfuMessageConverterUtils.toProtoBuf(message));

            } catch (NetworkException ne) {
                log.error("Caught Network Exception while trying to send message to remote leader {}", endpoint);
                runtimeFSM.input(new LogReplicationRuntimeEvent(LogReplicationRuntimeEventType.ON_CONNECTION_DOWN,
                        endpoint));
                throw ne;
            } catch (Exception e) {
                outstandingRequests.remove(requestId);
                log.error("sendMessageAndGetCompletable: Remove request {} to {} due to exception! Message:{}",
                        requestId, remoteClusterId, message, e);
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
                            requestId, remoteClusterId, message);
                }
                return null;
            });

            return cfTimeout;
        }

        log.error("Invalid message type {}. Currently only log replication messages are processed.");
        CompletableFuture<T> f = new CompletableFuture<>();
        f.completeExceptionally(new Throwable("Invalid message type"));
        return f;
    }

    /**
     * Send a one way message, without adding a completable future.
     *
     * @param message The message to send.
     */
    @Override
    public void sendMessage(CorfuMsg message) {
        // Get the next request ID.
        message.setRequestID(requestID.getAndIncrement());
        // Get Remote Leader
        if(runtimeFSM.getRemoteLeader().isPresent()) {
            String remoteLeader = runtimeFSM.getRemoteLeader().get();
            channelAdapter.send(remoteLeader, CorfuMessageConverterUtils.toProtoBuf(message));
            log.trace("Sent one-way message: {}", message);
        } else {
            log.error("Leader not found to remote cluster {}, dropping {}", remoteClusterId, message.getMsgType());
            runtimeFSM.input(new LogReplicationRuntimeEvent(LogReplicationRuntimeEventType.REMOTE_LEADER_LOSS));
        }
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
    public Integer getPort() {
        // For logging purposes return one port (as this abstraction does not make sense for a Log Replication
        // Client Router) as it is a router to an entire cluster/site.
        return Integer.valueOf(remoteClusterDescriptor.getNodesDescriptors().iterator().next().getPort());
    }

    @Override
    public String getHost() {
        String host = "";
        // For logging purposes return all remote cluster nodes host in a concatenated form
        remoteClusterDescriptor.getNodesDescriptors().forEach(node -> host.concat(node.getHost() + ":"));
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
    public void receive(CorfuMessage msg) {
        try {
            CorfuMsg corfuMsg = CorfuMessageConverterUtils.fromProtoBuf(msg);

            // If it is a Leadership Loss Message re-trigger leadership discovery
            if (corfuMsg.getMsgType() == CorfuMsgType.LOG_REPLICATION_LEADERSHIP_LOSS) {
                runtimeFSM.input(new LogReplicationRuntimeEvent(LogReplicationRuntimeEventType.REMOTE_LEADER_LOSS));
                return;
            }

            // We get the handler for this message from the map
            IClient handler = handlerMap.get(corfuMsg.getMsgType());

            if (handler == null) {
                // The message was unregistered, we are dropping it.
                log.warn("Received unregistered message {}, dropping", corfuMsg);
            } else {
                // Route the message to the handler.
                if (log.isTraceEnabled()) {
                    log.trace("Message routed to {}: {}",
                            handler.getClass().getSimpleName(), corfuMsg);
                }
                handler.handleMessage(corfuMsg, null);
            }
        } catch (CorfuMessageProtoBufException | Exception e) {
            log.error("Exception caught while receiving message of type {}", msg.getType(), e);
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
    private boolean isValidMessage(CorfuMsg message) {
        return message.getMsgType().equals(CorfuMsgType.LOG_REPLICATION_ENTRY) ||
                message.getMsgType().equals(CorfuMsgType.LOG_REPLICATION_METADATA_REQUEST) ||
                message.getMsgType().equals(CorfuMsgType.LOG_REPLICATION_QUERY_LEADERSHIP);
    }

    /**
     * Connection Up Callback.
     *
     * @param endpoint endpoint of the remote node to which connection was established.
     */
    public synchronized void onConnectionUp(String endpoint) {
        log.info("Connection established to remote endpoint {}", endpoint);
        runtimeFSM.input(new LogReplicationRuntimeEvent(LogReplicationRuntimeEventType.ON_CONNECTION_UP, endpoint));
    }

    /**
     * Connection Down Callback.
     *
     * @param endpoint endpoint of the remote node to which connection came down.
     */
    public synchronized void onConnectionDown(String endpoint) {
        log.info("Connection lost to remote endpoint {} on cluster {}", endpoint, remoteClusterId);
        runtimeFSM.input(new LogReplicationRuntimeEvent(LogReplicationRuntimeEventType.ON_CONNECTION_DOWN,
                endpoint));
        // Attempt to reconnect to this endpoint
        channelAdapter.connectAsync(endpoint);
    }

    /**
     * Channel Adapter On Error Callback
     */
    public synchronized void onError(Throwable t) {
        runtimeFSM.input(new LogReplicationRuntimeEvent(LogReplicationRuntimeEventType.ERROR, t));
    }

    public Optional<String> getRemoteLeaderEndpoint() {
        return runtimeFSM.getRemoteLeader();
    }
}
