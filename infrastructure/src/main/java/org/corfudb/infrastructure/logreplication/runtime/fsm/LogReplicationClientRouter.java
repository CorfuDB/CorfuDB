package org.corfudb.infrastructure.logreplication.runtime.fsm;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.infrastructure.LogReplicationRuntimeParameters;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.LogReplicationPluginConfig;
import org.corfudb.infrastructure.logreplication.infrastructure.ClusterDescriptor;
import org.corfudb.infrastructure.logreplication.infrastructure.NodeDescriptor;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationNegotiationResponse;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationQueryLeaderShipResponse;
import org.corfudb.runtime.Messages.CorfuMessage;
import org.corfudb.runtime.clients.IClient;
import org.corfudb.runtime.clients.IClientRouter;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.infrastructure.logreplication.transport.client.ChannelAdapterException;
import org.corfudb.infrastructure.logreplication.transport.client.IClientChannelAdapter;
import org.corfudb.util.CFUtils;
import org.corfudb.util.Utils;
import org.corfudb.utils.common.CorfuMessageConverter;
import org.corfudb.utils.common.CorfuMessageProtoBufException;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * This Client Router is used when a custom (client-defined) transport layer is specified for
 * Log Replication Server communication.
 *
 */
@Slf4j
public class LogReplicationClientRouter implements IClientRouter {

    private static final int LEADERSHIP_RETRIES = 5;

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
    volatile CompletableFuture<Void> connectionFuture;

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

    @Getter
    private Optional<String> remoteLeaderEndpoint = Optional.empty();

    private Map<String, NodeDescriptor> connectedNodes = new ConcurrentHashMap<>();

    private Map<String, NodeDescriptor> disconnectedNodes = new ConcurrentHashMap<>();

    private static final int DEFAULT_TIMEOUT = 5000;

    private ExecutorService executorService;

    private Consumer<LogReplicationNegotiationResponse> negotiationCallback;

    public LogReplicationClientRouter(ClusterDescriptor remoteClusterDescriptor,
                                      LogReplicationRuntimeParameters parameters,
                                      Consumer<LogReplicationNegotiationResponse> negotiationCallback) {
        this.remoteClusterDescriptor = remoteClusterDescriptor;
        this.remoteClusterId = remoteClusterDescriptor.getClusterId();
        this.parameters = parameters;
        this.timeoutResponse = parameters.getRequestTimeout().toMillis();
        this.negotiationCallback = negotiationCallback;

        this.handlerMap = new ConcurrentHashMap<>();
        this.clientList = new ArrayList<>();
        this.requestID = new AtomicLong();
        this.outstandingRequests = new ConcurrentHashMap<>();
        this.connectionFuture = new CompletableFuture<>();
        // Thread pool for this router to handle business logic from channel callbacks
        this.executorService = Executors.newSingleThreadExecutor();
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
                    log.info("Registered {} to handle messages of type {}", client, x);
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
                        connectionFuture
                                .get(getParameters().getConnectionTimeout().toMillis(), TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        throw new UnrecoverableCorfuInterruptedError(e);
                    } catch (TimeoutException te) {
                        cf.completeExceptionally(te);
                        return cf;
                    } catch (ExecutionException ee) {
                        cf.completeExceptionally(Utils.extractCauseWithCompleteStacktrace(ee));
                        return cf;
                    }

                    String remoteLeader = remoteLeaderEndpoint.orElseThrow(() -> new ChannelAdapterException(
                            String.format("Leader not found to remote cluster %s", remoteClusterDescriptor.getClusterId())));
                    endpoint = remoteLeader;
                }

                // In the case the message is intended for a specific endpoint, we do not
                // block on connection future, this is the case of leader verification.
                log.info("Send message to {}, type={}", endpoint, message.getMsgType());
                channelAdapter.send(endpoint, CorfuMessageConverter.toProtoBuf(message));

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
        if(remoteLeaderEndpoint.isPresent()) {
            String remoteLeader = remoteLeaderEndpoint.get();
            // Write this message out on the serverAdapter.
            channelAdapter.send(remoteLeader, CorfuMessageConverter.toProtoBuf(message));
            log.trace("Sent one-way message: {}", message);
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
        connectionFuture = new CompletableFuture<>();
        connectionFuture.completeExceptionally(new NetworkException("Router stopped", remoteClusterId));
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
        remoteClusterDescriptor.getNodesDescriptors().forEach(node -> host.concat(node.getIpAddress() + ":"));
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
            CorfuMsg corfuMsg = CorfuMessageConverter.fromProtoBuf(msg);

            // If it is a Leadership Loss Message re-trigger leadership discovery
            if (corfuMsg.getMsgType() == CorfuMsgType.LOG_REPLICATION_LEADERSHIP_LOSS) {
                log.warn("Leadership loss reported by remote node {}", remoteLeaderEndpoint);
                remoteLeaderEndpoint = Optional.empty();
                verifyLeadership();
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
            // When connection is established to the remote leader node, the connectionFuture will be completed.
            channelAdapter.connectAsync();
        } catch (Exception e) {
            log.error("Fatal error: Failed to initialize transport adapter {}", config.getTransportClientClassCanonicalName(), e);
            throw new UnrecoverableCorfuError(e);
        }
    }

    /**
     * Verify who is the leader node on the remote cluster by sending leadership request to all nodes.
     * <p>
     * If no leader is found, the verification will be attempted for LEADERSHIP_RETRIES times.
     */
    public synchronized void verifyLeadership() {
        executorService.submit(() -> {
            boolean leadershipVerified = false;

            Map<String, CompletableFuture<LogReplicationQueryLeaderShipResponse>> pendingLeadershipQueries = new HashMap<>();

            // Verify leadership on remote cluster, only if no leader is currently selected.
            if (!remoteLeaderEndpoint.isPresent()  || remoteLeaderEndpoint.get().length() == 0) {

                for (int i = 0; i < LEADERSHIP_RETRIES; i++) {

                    log.info("Verify leadership on remote cluster {}, retry={}", remoteClusterDescriptor.getClusterId(), i);

                    try {

                        for (NodeDescriptor node : connectedNodes.values()) {
                            log.debug("Verify leadership status for node {}", node.getEndpoint());
                            // Check Leadership
                            CompletableFuture<LogReplicationQueryLeaderShipResponse> leadershipRequestCf =
                                    sendMessageAndGetCompletable(new CorfuMsg(CorfuMsgType.LOG_REPLICATION_QUERY_LEADERSHIP).setEpoch(0), node.getEndpoint());
                            pendingLeadershipQueries.put(node.getEndpoint(), leadershipRequestCf);
                        }

                        // TODO: Anny should we block or wait for this to complete async...
                        // Block until all leadership requests are completed, or a leader is discovered.
                        while (!leadershipVerified || pendingLeadershipQueries.size() != 0) {
                            LogReplicationQueryLeaderShipResponse leadershipResponse = (LogReplicationQueryLeaderShipResponse) CompletableFuture.anyOf(pendingLeadershipQueries.values()
                                    .toArray(new CompletableFuture<?>[pendingLeadershipQueries.size()])).get(DEFAULT_TIMEOUT, TimeUnit.MILLISECONDS);

                            if (leadershipResponse.isLeader()) {
                                log.info("Leader for remote cluster id={}, node={}", remoteClusterDescriptor.getClusterId(),
                                        leadershipResponse.getEndpoint());
                                remoteLeaderEndpoint = Optional.of(leadershipResponse.getEndpoint());
                                updateDescriptor(leadershipResponse.getEndpoint());
                                leadershipVerified = true;
                                // Remove all CF, based on the assumption that one leader response is the expectation.
                                pendingLeadershipQueries.clear();
                                break;
                            } else {
                                // Remove CF for completed request
                                pendingLeadershipQueries.remove(leadershipResponse.getEndpoint());
                            }
                        }

                        if (leadershipVerified) {
                            // A new leader has been found, start negotiation, to determine log replication
                            // continuation or start point
                            startNegotiation();
                            break;
                        }
                    } catch (Exception ex) {
                        log.warn("Exception caught while verifying remote leader on cluster {}, retry={}", remoteClusterDescriptor.getClusterId(), i, ex);
                    }
                }
            }
        });
    }

    /**
     * Start Log Replication Negotiation.
     *
     * In this stage the active cluster negotiates with the standby cluster to determine
     * the log replication start point. This can be to continue log entry sync (delta) from an earlier point
     * (as data is still available) or to start from a snapshot sync (full).
     *
     */
    private void startNegotiation() {
        executorService.submit( () -> {
            try {
                String remoteLeader = remoteLeaderEndpoint.get();
                CompletableFuture<LogReplicationNegotiationResponse> cf = sendMessageAndGetCompletable(
                        new CorfuMsg(CorfuMsgType.LOG_REPLICATION_NEGOTIATION_REQUEST).setEpoch(0), remoteLeader);
                LogReplicationNegotiationResponse response = cf.get(DEFAULT_TIMEOUT, TimeUnit.MILLISECONDS);
                
                // Notify of negotiation result
                negotiationCallback.accept(response);

                // Negotiation to leader node completed, unblock channel.
                connectionFuture.complete(null);
            } catch (Exception e) {
               log.error("Exception during negotiation", e);
               // If the cause of exception is leadership loss.
               startNegotiation();
            }
        });
    }


    /**
     * Update leader endpoint
     **/
    private void updateDescriptor(String leaderEndpoint) {
        remoteClusterDescriptor.getNodesDescriptors().forEach(node -> {
            if (node.getEndpoint().equals(leaderEndpoint)) {
                node.setLeader(true);
            } else {
                node.setLeader(false);
            }
        });
    }

    /**
     * Verify Message is of valid Log Replication type.
     */
    private boolean isValidMessage(CorfuMsg message) {
        return message.getMsgType().equals(CorfuMsgType.LOG_REPLICATION_ENTRY) ||
                message.getMsgType().equals(CorfuMsgType.LOG_REPLICATION_NEGOTIATION_REQUEST) ||
                message.getMsgType().equals(CorfuMsgType.LOG_REPLICATION_QUERY_LEADERSHIP);
    }

    /**
     * Connection Up Callback.
     *
     * @param endpoint endpoint of the remote node to which connection was established.
     */
    public synchronized void onConnectionUp(String endpoint) {
        log.info("Connection established to remote endpoint {}", endpoint);
        remoteClusterDescriptor.getNodesDescriptors().forEach(node -> {
            if (node.getEndpoint().equals(endpoint)) {
                log.debug("Add {} as a connected endpoint", endpoint);
                connectedNodes.put(endpoint, node);
            }
        });

        verifyLeadership();
    }

    /**
     * Connection Down Callback.
     *
     * @param endpoint endpoint of the remote node to which connection was established.
     */
    public synchronized void onConnectionDown(String endpoint) {
        log.info("Connection lost to remote endpoint {} on cluster {}", endpoint, remoteClusterId);
        remoteClusterDescriptor.getNodesDescriptors().forEach(node -> {
            if (node.getEndpoint().equals(endpoint)) {
                log.debug("Remove {} as a connected endpoint", endpoint);
                connectedNodes.remove(endpoint);
                disconnectedNodes.put(endpoint, node);
            }
        });

        // If the node for which connection went down is the current leader, reset remote leader
        if (remoteLeaderEndpoint.isPresent() && remoteLeaderEndpoint.get().equals(endpoint)) {
            resetLeader();
        }

        // Attempt to reconnect to this endpoint
        channelAdapter.connectAsync(endpoint);
    }

    private void resetLeader() {
        // To reset current leadership
        // (1) Clear current leader endpoint
        // (2) Remove the current completion future, forcing clients to wait for reconnection to new leader.
        // (3) Re-trigger leadership verification

        remoteLeaderEndpoint = Optional.empty();
        connectionFuture = new CompletableFuture<>();
        verifyLeadership();
    }
}
