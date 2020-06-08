package org.corfudb.transport.logreplication;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.infrastructure.LogReplicationRuntimeParameters;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.runtime.Messages.CorfuMessage;
import org.corfudb.runtime.clients.IClient;
import org.corfudb.runtime.clients.IClientRouter;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.transport.client.IClientChannelAdapter;
import org.corfudb.util.CFUtils;
import org.corfudb.util.NodeLocator;
import org.corfudb.util.InterClusterConnectionDescriptor;
import org.corfudb.utils.common.CorfuMessageConverter;
import org.corfudb.utils.common.CorfuMessageProtoBufException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;



/*

                            Corfu Consumer
+-------------------------+                    +-----------------------------+
|                         |                    |                             |
|                         |                    |                             |
|                         |                    |                             |
|                         |                    |                             |
|                         |                    |                             |
+-------------------------+     Send           +-----------------------------+
| IClientChannelAdapter   | <--------------->  | IServerChannelAdapter       |
+-------------------------+                    +-----------------------------+




                               Corfu
+------------------------+                    +-----------------------------+
| IClientRouter          |                    | IServerRouter               |
| Netty(default)         |                    | Netty(default)              |
| Custom(protobuf)       |                    | Custom(protobuf)            |
+------------------------|                    +-----------------------------+
|                        |                    |                             |
|                        |                    |                             |
|                        |                    |                             |
|                        |                    |                             |
|                        |                    |                             |
|                        |                    |                             |
|                        |                    |                             |
+------------------------+                    +-----------------------------+
LogReplicationServerNode                        LogReplicationServerNode


 * This Client Router is used when a custom (client-defined) transport layer is specified for
 * Log Replication Server communication.
 *
 * If default communication channel is used (Netty) instead a NettyClientRouter will be instantiated.
 */

@Slf4j
public class LogReplicationClientRouter implements IClientRouter {

    private NodeLocator node;

    private LogReplicationRuntimeParameters parameters;

    /**
     * The handlers registered to this router.
     */
    public final Map<CorfuMsgType, IClient> handlerMap;

    /**
     * The clients registered to this router.
     */
    public final List<IClient> clientList;

    /**
     * Whether or not this router is shutdown.
     */
    public volatile boolean shutdown;

    /** A {@link CompletableFuture} which is completed when a connection,
     *  including a successful handshake completes and messages can be sent
     *  to the remote node.
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
     * Remote Site/Cluster unique identifier
     */
    private String remoteSiteId;

    public LogReplicationClientRouter(NodeLocator remoteEndpoint, String remoteSideId,
                                      LogReplicationRuntimeParameters parameters, Class adapterType, String localSiteId) {
        this.node = remoteEndpoint;
        this.remoteSiteId = remoteSideId;
        this.parameters = parameters;
        this.timeoutResponse = parameters.getRequestTimeout().toMillis();
        this.handlerMap = new ConcurrentHashMap<>();
        this.clientList = new ArrayList<>();
        this.requestID = new AtomicLong();
        this.outstandingRequests = new ConcurrentHashMap<>();
        this.connectionFuture = new CompletableFuture<>();

        initialize(adapterType, localSiteId);
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
                    log.trace("Registered {} to handle messages of type {}", client, x);
                });

        // Register this type
        clientList.add(client);
        return this;
    }

    @Override
    public <T> CompletableFuture<T> sendMessageAndGetCompletable(CorfuMsg message) {
        if (isValidMessage(message)) {
            // Get the next request ID.
            final long requestId = requestID.getAndIncrement();

            // Generate a future and put it in the completion table.
            final CompletableFuture<T> cf = new CompletableFuture<>();
            outstandingRequests.put(requestId, cf);

            try {
                message.setClientID(parameters.getClientId());
                message.setRequestID(requestId);
                channelAdapter.send(remoteSiteId, CorfuMessageConverter.toProtoBuf(message));
            } catch (Exception e) {
                outstandingRequests.remove(requestId);
                log.error("sendMessageAndGetCompletable: Remove request {} to {} due to exception! Message:{}",
                        requestId, node, message, e);
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
                            requestId, node, message);
                }
                return null;
            });

            log.info("Return Completable on message {}", message.getRequestID());
            return cfTimeout;
        }

        log.error("Invalid message type {}. Currently only log replication messages are processed.");
        CompletableFuture<T> f = new CompletableFuture<>();
        f.completeExceptionally(new Throwable("Invalid message type"));
        return f;
    }

    private boolean isValidMessage(CorfuMsg message) {
        return message.getMsgType().equals(CorfuMsgType.LOG_REPLICATION_ENTRY) ||
                message.getMsgType().equals(CorfuMsgType.LOG_REPLICATION_NEGOTIATION_REQUEST) ||
                message.getMsgType().equals(CorfuMsgType.LOG_REPLICATION_NEGOTIATION_RESPONSE) ||
                message.getMsgType().equals(CorfuMsgType.LOG_REPLICATION_QUERY_LEADERSHIP) ||
                message.getMsgType().equals(CorfuMsgType.LOG_REPLICATION_QUERY_LEADERSHIP_RESPONSE);
    }

    /**
     * Send a one way message, without adding a completable future.
     *
     * @param message The message to send.
     */
    public void sendMessage(CorfuMsg message) {
        // Get the next request ID.
        message.setRequestID(requestID.getAndIncrement());
        // Write this message out on the serverAdapter.
        channelAdapter.send(remoteSiteId, CorfuMessageConverter.toProtoBuf(message));
        log.trace("Sent one-way message: {}", message);
    }

    public void receive(CorfuMessage msg) {
        try {
            CorfuMsg corfuMsg = CorfuMessageConverter.fromProtoBuf(msg);

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
            log.debug("completeExceptionally: Remove request {} to {} due to {}.", requestID, node,
                    cause.getClass().getSimpleName(), cause);
        } else {
            log.warn("Attempted to exceptionally complete request {}, but request not outstanding!",
                    requestID);
        }
    }

    @Override
    public void stop() {
        log.debug("stop: Shutting down router for {}", node);
        shutdown = true;
        connectionFuture.completeExceptionally(new NetworkException("Router stopped", node));

        // TODO: notify serverAdapter / adapter to close the serverAdapte
    }

    @Override
    public Integer getPort() {
        return node.getPort();
    }

    @Override
    public String getHost() {
        return node.getHost();
    }

    @Override
    public void setTimeoutConnect(long timeoutConnect) {

    }

    @Override
    public void setTimeoutRetry(long timeoutRetry) {

    }

    @Override
    public void setTimeoutResponse(long timeoutResponse) {

    }

    public void initialize(Class adapterType, String localSiteId) {
        try {
            channelAdapter = (IClientChannelAdapter) adapterType.getDeclaredConstructor(Integer.class, String.class, String.class, LogReplicationClientRouter.class)
                    .newInstance(node.getPort(), node.getHost(), localSiteId,  this);
            channelAdapter.start();
        } catch (Exception e) {
            log.error("Unhandled exception caught while initializing custom adapter {}", adapterType.getSimpleName(), e);
            throw new UnrecoverableCorfuError(e);
        }
    }
}
