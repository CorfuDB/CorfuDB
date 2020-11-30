package org.corfudb.runtime.clients;

import com.google.protobuf.TextFormat;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.infrastructure.TestServerRouter;
import org.corfudb.protocols.CorfuProtocolCommon;
import org.corfudb.protocols.service.CorfuProtocolMessage;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.proto.RpcCommon;
import org.corfudb.runtime.proto.ServerErrors.ServerErrorMsg.ErrorCase;
import org.corfudb.runtime.proto.service.CorfuMessage.HeaderMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestPayloadMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponsePayloadMsg.PayloadCase;
import org.corfudb.runtime.proto.service.CorfuMessage.PriorityLevel;
import org.corfudb.util.CFUtils;
import org.corfudb.util.NodeLocator;

import static org.corfudb.AbstractCorfuTest.PARAMETERS;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getHeaderMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getRequestMsg;

/**
 * Created by mwei on 12/13/15.
 */
@Slf4j
// this class does not have access to parameters and is
// scheduled to be deprecated anyway.
@SuppressWarnings("checkstyle:magicnumber")
public class TestClientRouter implements IClientRouter {

    /**
     * The clients registered to this router.
     */
    public List<IClient> clientList;

    /**
     * @deprecated
     * The handlers registered to this router.
     */
    @Deprecated
    public Map<CorfuMsgType, IClient> handlerMap;

    /**
     * The handlers registered to this router.
     */
    public Map<PayloadCase, IClient> responseHandlerMap;

    /**
     * The handlers registered to this router for server errors.
     */
    public Map<ErrorCase, IClient> errorHandlerMap;

    /**
     * The outstanding requests on this router.
     */
    public Map<Long, CompletableFuture> outstandingRequests;

    public volatile AtomicLong requestID;

    @Getter
    @Setter
    public long serverEpoch;

    @Getter
    @Setter
    public UUID clientID;

    private volatile boolean connected = true;

    public void simulateDisconnectedEndpoint() {
        connected = false;
    }

    /**
     * New connection timeout (milliseconds)
     */
    @Getter
    @Setter
    public long timeoutConnect = PARAMETERS.TIMEOUT_NORMAL.toMillis();
    /**
     * Sync call response timeout (milliseconds)
     */
    @Getter
    @Setter
    public long timeoutResponse = PARAMETERS.TIMEOUT_NORMAL.toMillis();
    /**
     * Retry interval after timeout (milliseconds)
     */
    @Getter
    @Setter
    public long timeoutRetry = PARAMETERS.TIMEOUT_SHORT.toMillis();

    public List<TestRule> rules;

    /** The server router endpoint this client should route to. */
    TestServerRouter serverRouter;

    /** A mock channel context for this connection. */
    TestChannelContext channelContext;

    /**
     * The test host that this router is routing requests for.
     */
    @Getter
    String host = "testServer";

    /**
     * The test port that this router is routing requests for.
     */
    @Getter
    Integer port;

    public TestClientRouter(TestServerRouter serverRouter) {
        clientList = new ArrayList<>();
        handlerMap = new ConcurrentHashMap<>();
        responseHandlerMap = new EnumMap<>(PayloadCase.class);
        errorHandlerMap = new EnumMap<>(ErrorCase.class);
        outstandingRequests = new ConcurrentHashMap<>();
        requestID = new AtomicLong();
        clientID = CorfuRuntime.getStreamID("testClient");
        rules = new ArrayList<>();
        this.serverRouter = serverRouter;
        channelContext = new TestChannelContext(this::handleMessage);
        port = serverRouter.getPort();
    }

    private void handleMessage(Object o) {
        if (o instanceof CorfuMsg) {
            CorfuMsg m = (CorfuMsg) o;
            if (validateClientId(m)) {
                IClient handler = handlerMap.get(m.getMsgType());
                if (handler == null){
                    throw new IllegalStateException("Client handler doesn't exists for message: " + m.getMsgType());
                }

                handler.handleMessage(m, null);
            }
        } else if (o instanceof ResponseMsg) {
            ResponseMsg m = (ResponseMsg) o;
            if (validateClientId(m.getHeader().getClientId())) {
                PayloadCase payloadCase = m.getPayload().getPayloadCase();
                IClient handler = responseHandlerMap.get(m.getPayload().getPayloadCase());

                if (handler == null && payloadCase.equals(PayloadCase.SERVER_ERROR)) {
                    handler = errorHandlerMap.get(m.getPayload().getServerError().getErrorCase());
                }

                if (handler == null){
                    throw new IllegalStateException("Client handler doesn't exists for message: "
                            + m.getPayload().getPayloadCase());
                } else {
                    handler.handleMessage(m, null);
                }
            }
        }
    }

    @Deprecated
    private void routeMessage(CorfuMsg message) {
        CorfuMsg m = simulateSerialization(message);
        serverRouter.sendServerMessage(m, channelContext);
    }

    /**
     * Add a new client to the router.
     *
     * @param client The client to add to the router.
     * @return This IClientRouter, to support chaining and the builder pattern.
     */
    @Override
    public IClientRouter addClient(IClient client) {
        // Set the client's router to this instance.
        client.setRouter(this);

        // Iterate through all types of CorfuMsgType, registering the handler
        try {
            client.getHandledTypes().stream()
                    .forEach(x -> {
                        handlerMap.put(x, client);
                        log.trace("Registered {} to handle messages of type {}", client, x);
                    });
        } catch (UnsupportedOperationException ex) {
            log.trace("No registered CorfuMsg handler for client {}", client, ex);
        }

        if (!client.getHandledCases().isEmpty()) {
            client.getHandledCases()
                    .forEach(x -> {
                        responseHandlerMap.put(x, client);
                        log.trace("Registered {} to handle protobuf messages of type {}", client, x);
                    });
        }

        if (!client.getHandledErrors().isEmpty()) {
            client.getHandledErrors()
                    .forEach(x -> {
                        errorHandlerMap.put(x, client);
                        log.trace("Registered {} to handle server error of type {}", client, x);
                    });
        }

        // Register this type
        clientList.add(client);
        return this;
    }

    /**
     * Send a message and get a completable future to be fulfilled by the reply.
     *
     * @param message The message to send.
     * @return A completable future which will be fulfilled by the reply,
     * or a timeout in the case there is no response.
     */
    @Override
    @Deprecated
    public <T> CompletableFuture<T> sendMessageAndGetCompletable(@NonNull CorfuMsg message) {
        // Simulate a "disconnected endpoint"
        if (!connected) {
            log.trace("Disconnected endpoint " + host + ":" + port);
            throw new NetworkException("Disconnected endpoint", NodeLocator.builder()
                    .host(host)
                    .port(port).build());
        }

        // Get the next request ID.
        final long thisRequest = requestID.getAndIncrement();
        // Set the message fields.
        message.setClientID(clientID);
        message.setRequestID(thisRequest);

        // Generate a future and put it in the completion table.
        final CompletableFuture<T> cf = new CompletableFuture<>();
        outstandingRequests.put(thisRequest, cf);

        // Evaluate rules.
        if (rules.stream().allMatch(x -> x.evaluate(message, this))) {
            // Write the message out to the channel
            log.trace(Thread.currentThread().getId() + ":Sent message: {}", message);
            routeMessage(message);
        }

        // Generate a timeout future, which will complete exceptionally if the main future is not completed.
        final CompletableFuture<T> cfTimeout = CFUtils.within(cf, Duration.ofMillis(timeoutResponse));
        cfTimeout.exceptionally(e -> {
            outstandingRequests.remove(thisRequest);
            log.debug("Remove request {} due to timeout!", thisRequest);
            return null;
        });
        return cfTimeout;
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
    public  <T> CompletableFuture<T> sendRequestAndGetCompletable(RequestPayloadMsg payload,
                                                                  long epoch, RpcCommon.UuidMsg clusterId,
                                                                  PriorityLevel priority,
                                                                  boolean ignoreClusterId, boolean ignoreEpoch) {
        // Simulate a "disconnected endpoint"
        if (!connected) {
            log.trace("Disconnected endpoint " + host + ":" + port);
            throw new NetworkException("Disconnected endpoint", NodeLocator.builder()
                    .host(host)
                    .port(port).build());
        }

        // Get the next request ID.
        final long thisRequestId = requestID.getAndIncrement();
        RpcCommon.UuidMsg protoClientId = CorfuProtocolCommon.getUuidMsg(clientID);

        // Set the header for this request message.
        HeaderMsg header = getHeaderMsg(thisRequestId, priority,
                epoch, clusterId, protoClientId, ignoreClusterId, ignoreEpoch);
        RequestMsg request = getRequestMsg(header, payload);

        // Generate a future and put it in the completion table.
        final CompletableFuture<T> cf = new CompletableFuture<>();
        outstandingRequests.put(thisRequestId, cf);

        // Evaluate rules.
        if (rules.stream().allMatch(x -> x.evaluate(request, this))) {
            log.trace(Thread.currentThread().getId() + ":Sent request: {}", TextFormat.shortDebugString(request));
            // Write the message out to the channel
            try {
                RequestMsg requestMsg = simulateSerialization(request);
                serverRouter.sendServerMessage(requestMsg, channelContext);
            } catch (IOException e) {
                log.error("encode: Error during serialization or deserialization!", e);
            }
        }

        // Generate a timeout future, which will complete exceptionally if the main future is not completed.
        final CompletableFuture<T> cfTimeout = CFUtils.within(cf, Duration.ofMillis(timeoutResponse));
        cfTimeout.exceptionally(e -> {
            outstandingRequests.remove(thisRequestId);
            log.debug("Remove request {} due to timeout!", thisRequestId);
            return null;
        });
        return cfTimeout;
    }

    /**
     * @deprecated
     * Send a one way message, without adding a completable future.
     *
     * @param message The message to send.
     */
    @Override
    @Deprecated
    public void sendMessage(CorfuMsg message) {
        // Get the next request ID.
        final long thisRequest = requestID.getAndIncrement();
        message.setClientID(clientID);
        message.setRequestID(thisRequest);
        // Evaluate rules.
        if (rules.stream()
                .map(x -> x.evaluate(message, this))
                .allMatch(x -> x)) {
            // Write the message out to the channel.
            routeMessage(message);
        }
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
                            PriorityLevel priority, boolean ignoreClusterId, boolean ignoreEpoch) {
        final long thisRequestId = requestID.getAndIncrement();
        RpcCommon.UuidMsg protoClientId = CorfuProtocolCommon.getUuidMsg(clientID);

        // Set the header for this message request.
        HeaderMsg header = CorfuProtocolMessage.getHeaderMsg(thisRequestId, priority,
                epoch, clusterId, protoClientId, ignoreClusterId, ignoreEpoch);
        RequestMsg request = getRequestMsg(header, payload);

        // Evaluate rules.
        if (rules.stream().allMatch(x -> x.evaluate(request, this))) {
            log.trace(Thread.currentThread().getId() + ":Sent request: {}", TextFormat.shortDebugString(request));
            // Write the message out to the channel
            try {
                RequestMsg requestMsg = simulateSerialization(request);
                serverRouter.sendServerMessage(requestMsg, channelContext);
            } catch (IOException e) {
                log.error("encode: Error during serialization or deserialization!", e);
            }
        }
    }

    /**
     * @deprecated
     * Validate the client ID of a CorfuMsg.
     *
     * @param msg The incoming message to validate.
     * @return True, if the clientID is correct, but false otherwise.
     */
    @Deprecated
    private boolean validateClientId(CorfuMsg msg) {
        // Check if the message is intended for us. If not, drop the message.
        if (!msg.getClientID().equals(clientID)) {
            log.warn("Incoming message intended for client {}, our id is {}, dropping!",
                    msg.getClientID(), clientID);
            return false;
        }
        return true;
    }

    /**
     * Validate the clientID of a CorfuMsg.
     *
     * @param protoClientId The clientID of the incoming message used for validation.
     * @return True, if the clientID is correct, but false otherwise.
     */
    private boolean validateClientId(RpcCommon.UuidMsg protoClientId) {
        // Check if the message is intended for us. If not, drop the message.
        if (!protoClientId.equals(CorfuProtocolCommon.getUuidMsg(clientID))) {
            log.warn("Incoming message intended for client {}, our id is {}, dropping!",
                    TextFormat.shortDebugString(protoClientId), clientID);
            return false;
        }
        return true;
    }

    /**
     * Complete a given outstanding request with a completion value.
     *
     * @param requestID  The request to complete.
     * @param completion The value to complete the request with
     * @param <T>        The type of the completion.
     */
    @SuppressWarnings("unchecked")
    public <T> void completeRequest(long requestID, T completion) {
        CompletableFuture<T> cf;
        if ((cf = (CompletableFuture<T>) outstandingRequests.get(requestID)) != null) {
            cf.complete(completion);
            outstandingRequests.remove(requestID);
        } else {
            log.warn("Attempted to complete request {}, but request not outstanding!", requestID);
        }
    }

    /**
     * Exceptionally complete a request with a given cause.
     *
     * @param requestID The request to complete.
     * @param cause     The cause to give for the exceptional completion.
     */
    public void completeExceptionally(long requestID, Throwable cause) {
        CompletableFuture cf;
        if ((cf = outstandingRequests.get(requestID)) != null) {
            cf.completeExceptionally(cause);
            outstandingRequests.remove(requestID);
        } else {
            log.warn("Attempted to exceptionally complete request {}, but request not outstanding!", requestID);
        }
    }

    /**
     * Stops routing requests.
     */
    @Override
    public void stop() {
        //TODO - pause pipeline
    }

    @Deprecated
    public CorfuMsg simulateSerialization(CorfuMsg message) {
        /* simulate serialization/deserialization */
        ByteBuf oBuf = Unpooled.buffer();
        message.serialize(oBuf);
        oBuf.resetReaderIndex();
        CorfuMsg msg = CorfuMsg.deserialize(oBuf);
        oBuf.release();
        return msg;
    }

    public RequestMsg simulateSerialization(RequestMsg message) throws IOException {
        // Simulate serialization/deserialization
        ByteBuf oBuf = Unpooled.buffer();
        RequestMsg msg;

        try (ByteBufOutputStream requestOutputStream = new ByteBufOutputStream(oBuf)) {
            message.writeTo(requestOutputStream);
            oBuf.resetReaderIndex();
            try (ByteBufInputStream msgInputStream = new ByteBufInputStream(oBuf)) {
                msg = RequestMsg.parseFrom(msgInputStream);
            }
        } finally {
            oBuf.release();
        }

        return msg;
    }
}
