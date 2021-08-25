package org.corfudb.runtime.clients;

import com.codahale.metrics.Timer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.TestServerRouter;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.util.CFUtils;
import org.corfudb.util.CorfuComponent;
import org.corfudb.util.MetricsUtils;
import org.corfudb.util.NodeLocator;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.corfudb.AbstractCorfuTest.PARAMETERS;

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
     * The handlers registered to this router.
     */
    public Map<CorfuMsgType, IClient> handlerMap;

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
    private Map<CorfuMsgType, String> timerNameCache = new HashMap<>();

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
        }
    }

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
        client.getHandledTypes().stream()
                .forEach(x -> {
                    handlerMap.put(x, client);
                    log.trace("Registered {} to handle messages of type {}", client, x);
                });

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

        // Generate a benchmarked future to measure the underlying request
        final CompletableFuture<T> cfBenchmarked = cf.thenApply(x -> {
            return x;
        });

        // Generate a timeout future, which will complete exceptionally if the main future is not completed.
        final CompletableFuture<T> cfTimeout = CFUtils.within(cfBenchmarked, Duration.ofMillis(timeoutResponse));
        cfTimeout.exceptionally(e -> {
            outstandingRequests.remove(thisRequest);
            log.debug("Remove request {} due to timeout!", thisRequest);
            return null;
        });
        return cfTimeout;
    }

    /**
     * Send a one way message, without adding a completable future.
     *
     * @param message The message to send.
     */
    @Override
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
     * Validate the client ID of a CorfuMsg.
     *
     * @param msg The incoming message to validate.
     * @return True, if the clientID is correct, but false otherwise.
     */
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


    public CorfuMsg simulateSerialization(CorfuMsg message) {
        /* simulate serialization/deserialization */
        ByteBuf oBuf = Unpooled.buffer();
        message.serialize(oBuf);
        oBuf.resetReaderIndex();
        CorfuMsg msg = CorfuMsg.deserialize(oBuf);
        oBuf.release();
        return msg;
    }

}
