package org.corfudb.runtime.clients;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.IServer;
import org.corfudb.infrastructure.IServerRouter;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.util.CFUtils;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by mwei on 12/13/15.
 */
@Slf4j
public class TestClientRouter implements IClientRouter, IServerRouter {

    /** The clients registered to this router. */
    public List<IClient> clientList;

    /** The handlers registered to this router. */
    public Map<CorfuMsg.CorfuMsgType, IClient> handlerMap;

    /** The outstanding requests on this router. */
    public Map<Long, CompletableFuture> outstandingRequests;

    /** The list of test server handlers attached to this router. */
    public Map<CorfuMsg.CorfuMsgType, IServer> serverMap;

    public AtomicLong requestID;

    /** The optional address for this router, if set. */
    @Getter
    @Setter
    public String address;

    public TestClientRouter()
    {
        clientList = new ArrayList<>();
        handlerMap = new HashMap<>();
        outstandingRequests = new HashMap<>();
        serverMap = new HashMap<>();
        requestID = new AtomicLong();
    }

    public void addServer(IServer server)
    {
        // Iterate through all types of CorfuMsgType, registering the handler
        Arrays.<CorfuMsg.CorfuMsgType>stream(CorfuMsg.CorfuMsgType.values())
                .forEach(x -> {
                    if (x.handler.isInstance(server))
                    {
                        serverMap.put(x, server);
                        log.trace("Registered {} to handle messages of type {}", server, x);
                    }
                });
    }

    void routeMessage(CorfuMsg message)
    {
        CorfuMsg m = simulateSerialization(message);
        serverMap.get(message.getMsgType()).handleMessage(m, null, this);
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
     * Gets a client that matches a particular type.
     *
     * @param clientType The class of the client to match.
     * @return The first client that matches that type.
     * @throws NoSuchElementException If there are no clients matching that type.
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T extends IClient> T getClient(Class<T> clientType) {
        return (T) clientList.stream()
                .filter(clientType::isInstance)
                .findFirst().get();
    }

    /**
     * Send a message and get a completable future to be fulfilled by the reply.
     *
     * @param ctx     The channel handler context to send the message under.
     * @param message The message to send.
     * @return A completable future which will be fulfilled by the reply,
     * or a timeout in the case there is no response.
     */
    @Override
    public <T> CompletableFuture<T> sendMessageAndGetCompletable(ChannelHandlerContext ctx, CorfuMsg message) {
        // Get the next request ID.
        final long thisRequest = requestID.getAndIncrement();
        // Set the message fields.
        message.setClientID(0);
        message.setRequestID(thisRequest);
        // Generate a future and put it in the completion table.
        final CompletableFuture<T> cf = new CompletableFuture<>();
        outstandingRequests.put(thisRequest, cf);
        // Write the message out to the channel.
        routeMessage(message);
        log.trace("Sent message: {}", message);
        // Generate a timeout future, which will complete exceptionally if the main future is not completed.
        final CompletableFuture<T> cfTimeout = CFUtils.within(cf, Duration.ofMillis(100));
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
     * @param ctx     The context to send the message under.
     * @param message The message to send.
     */
    @Override
    public void sendMessage(ChannelHandlerContext ctx, CorfuMsg message) {
        routeMessage(message);
    }

    /**
     * Send a netty message through this router, setting the fields in the outgoing message.
     *
     * @param ctx    Channel handler context to use.
     * @param inMsg  Incoming message to respond to.
     * @param outMsg Outgoing message.
     */
    @Override
    public void sendResponseToServer(ChannelHandlerContext ctx, CorfuMsg inMsg, CorfuMsg outMsg) {
        outMsg.copyBaseFields(inMsg);
        ctx.writeAndFlush(outMsg);
        log.trace("Sent response: {}", outMsg);
    }


    /** Complete a given outstanding request with a completion value.
     *
     * @param requestID     The request to complete.
     * @param completion    The value to complete the request with
     * @param <T>           The type of the completion.
     */
    @SuppressWarnings("unchecked")
    public <T> void completeRequest(long requestID, T completion)
    {
        CompletableFuture<T> cf;
        if ((cf = (CompletableFuture<T>) outstandingRequests.get(requestID)) != null)
        {
            cf.complete(completion);
        }
        else
        {
            log.warn("Attempted to complete request {}, but request not outstanding!", requestID);
        }
    }

    /** Exceptionally complete a request with a given cause.
     *
     * @param requestID     The request to complete.
     * @param cause         The cause to give for the exceptional completion.
     */
    public void completeExceptionally(long requestID, Throwable cause)
    {
        CompletableFuture cf;
        if ((cf = outstandingRequests.get(requestID)) != null)
        {
            cf.completeExceptionally(cause);
        }
        else
        {
            log.warn("Attempted to exceptionally complete request {}, but request not outstanding!", requestID);
        }
    }

    /**
     * Starts routing requests.
     */
    @Override
    public void start() {

    }

    public CorfuMsg simulateSerialization(CorfuMsg message)
    {
        /* simulate serialization/deserialization */
        ByteBuf oBuf = ByteBufAllocator.DEFAULT.buffer();
        message.serialize(oBuf);
        oBuf.resetReaderIndex();
        return CorfuMsg.deserialize(oBuf);
    }
    @Override
    public void sendResponse(ChannelHandlerContext ctx, CorfuMsg inMsg, CorfuMsg outMsg) {
        outMsg.copyBaseFields(inMsg);
        log.info("(server) send Response: {}", outMsg);
        CorfuMsg m = simulateSerialization(outMsg);
        IClient handler = handlerMap.get(m.getMsgType());
        handler.handleMessage(m, null);
    }
}
