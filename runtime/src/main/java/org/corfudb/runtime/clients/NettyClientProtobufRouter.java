package org.corfudb.runtime.clients;

import com.codahale.metrics.Timer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.io.IOUtils;
import org.corfudb.protocols.API;
import org.corfudb.protocols.wireprotocol.orchestrator.Request;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.RuntimeParameters;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.runtime.protocol.proto.CorfuProtocol.Request;
import org.corfudb.runtime.protocol.proto.CorfuProtocol.MessageType;
import org.corfudb.runtime.protocol.proto.CorfuProtocol.Response;
import org.corfudb.runtime.protocol.proto.CorfuProtocol.Header;
import org.corfudb.util.CFUtils;
import org.corfudb.util.MetricsUtils;
import org.corfudb.util.NodeLocator;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;


@Slf4j
@ChannelHandler.Sharable
public class NettyClientProtobufRouter extends ChannelInboundHandlerAdapter
        implements IClientProtobufRouter {

    /**
     * New connection timeout (milliseconds).
     */
    @Getter
    @Setter
    public long timeoutConnect;

    /**
     * Sync call response timeout (milliseconds).
     */
    @Getter
    @Setter
    public long timeoutResponse;

    /**
     * Retry interval after timeout (milliseconds).
     */
    @Getter
    @Setter
    public long timeoutRetry;

    /**
     * The current request ID.
     */
    @Getter
    @SuppressWarnings("checkstyle:abbreviation")
    public AtomicLong requestID;

    /**
     * The handlers registered to this router.
     */
    public final Map<MessageType, IClient> handlerMap;

    /** The {@link NodeLocator} which represents the remote node this
     *  {@link NettyClientProtobufRouter} connects to.
     */
    @Getter
    private final NodeLocator node;

    /** The {@link CorfuRuntime.CorfuRuntimeParameters} used to configure the
     *  router.
     */
    private final RuntimeParameters parameters;

    /**
     * The outstanding requests on this router.
     */
    public final Map<Long, CompletableFuture> outstandingRequests;

    /** A {@link CompletableFuture} which is completed when a connection,
     *  including a successful handshake completes and messages can be sent
     *  to the remote node.
     */
    @Getter
    volatile CompletableFuture<Void> connectionFuture;

    /**
     * Timer map for measuring request
     */
    private final Map<MessageType, String> timerNameCache;

    /**
     * Add a new client to the router
     *
     * @param client The client to add to the router
     * @return This IClientProtobufRouter
     */
    public IClientProtobufRouter addClient(IClient client) {
        // TODO: Rewrite IClient and corresponding methods,

        return this;
    }

    /**
     * @param requestID
     * @param completion
     */
    @Override
    public <T> void completeRequest(long requestID, T completion) {
        // TODO: Implementation
    }

    /**
     * @param requestID
     * @param cause
     */
    @Override
    public void completeExceptionally(long requestID, Throwable cause) {
        // TODO: Implementation
    }

    /**
     *
     */
    @Override
    public void stop() {
        // TODO: Implementation
    }

    /**
     * @return
     */
    @Override
    public String getHost() {
        // TODO: Implementation

        return null;
    }

    /**
     * @return
     */
    @Override
    public Integer getPort() {
        // TODO: Implementation

        return null;
    }

    /**
     *
     * @param request
     * @param ctx
     * @param <T>
     * @return
     */
    @Override
    public  <T> CompletableFuture<T> sendRequestAndGetCompletable(Request request, ChannelHandlerContext ctx) {

        // Check the connection future. If connected, continue with sending the message.
        // If timed out, return a exceptionally completed with the timeout.
        try {
            connectionFuture
                    .get(parameters.getConnectionTimeout().toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new UnrecoverableCorfuInterruptedError(e);
        } catch (TimeoutException te) {
            CompletableFuture<T> f = new CompletableFuture<>();
            f.completeExceptionally(te);
            return f;
        } catch (ExecutionException ee) {
            CompletableFuture<T> f = new CompletableFuture<>();
            f.completeExceptionally(ee.getCause());
            return f;
        }

        // Set up the timer and context to measure request
        final Timer roundTripMsgTimer = CorfuRuntime.getDefaultMetrics()
                .timer(timerNameCache.get(message.getMsgType()));

        final Timer.Context roundTripMsgContext = MetricsUtils
                .getConditionalContext(roundTripMsgTimer);

        // Get the next request ID
        final long thisRequest = requestID.getAndIncrement();

        // Set the base fields for this message.
        Request.Builder reqBuilder = request.toBuilder();
        reqBuilder.getHeaderBuilder().setClientId(parameters.getClientId()).setRequestId(thisRequest);
        request = reqBuilder.build();

        // Generate a future and put it in the completion table.
        final CompletableFuture<T> cf = new CompletableFuture<>();
        outstandingRequests.put(thisRequest, cf);

        // Write this message out on the channel
        ByteBuf outBuf = PooledByteBufAllocator.DEFAULT.buffer();
        ByteBufOutputStream requestOutputStream = new ByteBufOutputStream(outBuf);

        try {
            // Mark this message as Protobuf message (temporarily)
            requestOutputStream.writeByte(API.PROTO_CORFU_MSG_MARK);
            request.writeTo(requestOutputStream);
            ctx.writeAndFlush(outBuf, ctx.voidPromise());
            log.trace("Sent one-way request message: {}", request.getHeader());
        } catch (IOException e) {
            log.warn("sendRequest[{}]: Exception occurred when sending request {}, caused by {}",
                    request.getHeader().getRequestId(), request.getHeader(), e.getCause(), e);
        } finally {
            IOUtils.closeQuietly(requestOutputStream);
        }

        // Generate a benchmarked future to measure the underlying request
        final CompletableFuture<T> cfBenchmarked = cf.thenApply(x -> {
            MetricsUtils.stopConditionalContext(roundTripMsgContext);
            return x;
        });

        // Generate a timeout future, which will complete exceptionally
        // if the main future is not completed.
        final CompletableFuture<T> cfTimeout =
                CFUtils.within(cfBenchmarked, Duration.ofMillis(timeoutResponse));
        Request finalRequest = request;
        cfTimeout.exceptionally(e -> {
            // CFUtils.within() can wrap different kinds of exceptions in
            // CompletionException, just dealing with TimeoutException here since
            // the router is not aware of it and this::completeExceptionally()
            // takes care of others. This avoids handling same exception twice.
            if (e.getCause() instanceof TimeoutException) {
                outstandingRequests.remove(thisRequest);
                log.debug("sendMessageAndGetCompletable: Remove request {} to {} due to timeout! Request:{}",
                        thisRequest, node, finalRequest.getHeader());
            }
            return null;
        });

        return cfTimeout;
    }

    /**
     * Send a one way request, without adding a completable future.
     * @param request The request to send.
     */
    @Override
    public void sendRequest(Request request, ChannelHandlerContext ctx) {
        // Get the next request ID
        final long thisRequest = requestID.getAndIncrement();
        // Set the base fields for this message.
        Request.Builder reqBuilder = request.toBuilder();
        reqBuilder.getHeaderBuilder().setClientId(parameters.getClientId()).setRequestId(thisRequest);
        request = reqBuilder.build();
        // Write this message out on the channel
        ByteBuf outBuf = PooledByteBufAllocator.DEFAULT.buffer();
        ByteBufOutputStream requestOutputStream = new ByteBufOutputStream(outBuf);

        try {
            // Mark this message as Protobuf message (temporarily)
            requestOutputStream.writeByte(API.PROTO_CORFU_MSG_MARK);
            request.writeTo(requestOutputStream);
            ctx.writeAndFlush(outBuf, ctx.voidPromise());
            log.trace("Sent one-way request message: {}", request.getHeader());
        } catch (IOException e) {
            log.warn("sendRequest[{}]: Exception occurred when sending request {}, caused by {}",
                    request.getHeader().getRequestId(), request.getHeader(), e.getCause(), e);
        } finally {
            IOUtils.closeQuietly(requestOutputStream);
        }
    }

    /**
     * Validate the clientID of a CorfuMsg.
     *
     * @param header The header of incoming message used for validation.
     * @return True, if the clientID is correct, but false otherwise.
     */
    private boolean validateClientId(UUID clientId) {
        // Check if the message is intended for us. If not, drop the message.
        if (!clientId.equals(parameters.getClientId())) {
            log.warn("Incoming message intended for client {}, our id is {}, dropping!",
                    clientId, parameters.getClientId());
            return false;
        }
        return true;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf msgBuf = (ByteBuf) msg;

        // Temporary -- If message is a legacy message, forward the message.
        byte msgMark = msgBuf.getByte(msgBuf.readerIndex());
        if (msgMark == API.LEGACY_CORFU_MSG_MARK) {
            // TODO should we also use this method on the client side?
            ctx.fireChannelRead(msgBuf);
            return;
        } else if(msgMark != API.PROTO_CORFU_MSG_MARK) {
            throw new IllegalStateException("Received incorrectly marked message.");
        }

        msgBuf.readByte();
        ByteBufInputStream msgInputStream = new ByteBufInputStream(msgBuf);

        try {
            Response response = Response.parseFrom(msgInputStream);
            Header header = response.getHeader();

            // TODO: New Implementation of handlers
            IClient handler = handlerMap.get(header.getType());
            if (handler == null) {
                // The message was unregistered, we are dropping it.
                log.warn("Received unregistered message {}, dropping", header.getType());
            } else {
                if (validateClientId(header.getClientId())) {
                    // Route the message to the handler.
                    if (log.isTraceEnabled()) {
                        log.trace("Message routed to {}: {}",
                                handler.getClass().getSimpleName(), response);
                    }
                    handler.handleMessage(m, ctx);
                }
            }
        } catch (Exception e) {
            log.error("channelRead: Exception during read!", e);
        } finally {
            msgInputStream.close();
            msgBuf.release();
        }
    }
}
