package org.corfudb.runtime.clients;

import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.timeout.IdleStateHandler;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.io.IOUtils;
import org.corfudb.protocols.API;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.RuntimeParameters;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.runtime.protocol.proto.CorfuProtocol.UUID;
import org.corfudb.runtime.protocol.proto.CorfuProtocol.Request;
import org.corfudb.runtime.protocol.proto.CorfuProtocol.MessageType;
import org.corfudb.runtime.protocol.proto.CorfuProtocol.Response;
import org.corfudb.runtime.protocol.proto.CorfuProtocol.Header;
import org.corfudb.security.sasl.SaslUtils;
import org.corfudb.security.sasl.plaintext.PlainTextSaslNettyClient;
import org.corfudb.security.tls.SslContextConstructor;
import org.corfudb.util.CFUtils;
import org.corfudb.util.CorfuComponent;
import org.corfudb.util.MetricsUtils;
import org.corfudb.util.NodeLocator;
import org.corfudb.util.Sleep;

import javax.annotation.Nonnull;
import javax.net.ssl.SSLException;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;


@Slf4j
@ChannelHandler.Sharable
public class NettyClientProtobufRouter extends ChannelInboundHandlerAdapter
        implements IClientProtobufRouter, AutoCloseable {

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
     * The clients registered to this router.
     */
    public final List<IClient> clientList;

    /**
     * The outstanding requests on this router.
     */
    public final Map<Long, CompletableFuture> outstandingRequests;

    /**
     * The currently registered channel.
     */
    private volatile Channel channel = null;

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
     * A secure socket protocol implementation provided by netty.
     */
    private SslContext sslContext;

    /**
     * Timer map for measuring request
     */
    private final Map<MessageType, String> timerNameCache;

    /**
     * If true this instance will manage the life-cycle of the event loop.
     * For example, when the router is stopped the event loop will be released
     * for GC.
     */
    private final boolean manageEventLoop;

    /**
     * Thread pool for this channel to use
     */
    private final EventLoopGroup eventLoopGroup;

    /**
     * Overload constructor, provide default value to be false for manageEventLoop.
     */
    public NettyClientProtobufRouter(@Nonnull NodeLocator node,
                             @Nonnull EventLoopGroup eventLoopGroup,
                             @Nonnull RuntimeParameters parameters) {
        this(node, eventLoopGroup, parameters, false);
    }

    /**
     * Creates a new NettyClientRouter connected to the specified host and port with the
     * specified tls and sasl options. The new {@link this} will attempt connection to
     * the node until {@link this#stop()} is called.
     *
     * @param node              The node to connect to.
     * @param eventLoopGroup    The {@link EventLoopGroup} for servicing I/O.
     * @param parameters        A {@link CorfuRuntime.CorfuRuntimeParameters} with the desired configuration.
     * @param manageEventLoop   True if this instance will manage the life-cycle of the event loop.
     */
    public NettyClientProtobufRouter(@NonNull NodeLocator node,
                                     @NonNull EventLoopGroup eventLoopGroup,
                                     @NonNull RuntimeParameters parameters,
                                     boolean manageEventLoop) {
        this.node = node;
        this.eventLoopGroup = eventLoopGroup;
        this.parameters = parameters;
        this.manageEventLoop = manageEventLoop;
        this.connectionFuture = new CompletableFuture<>();
        this.handlerMap = new ConcurrentHashMap<>();

        // Set timer mapping
        ImmutableMap.Builder<MessageType, String> mapBuilder = ImmutableMap.builder();
        for (MessageType type : MessageType.values()) {
            mapBuilder.put(type,
                    CorfuComponent.CLIENT_ROUTER.toString() + type.name().toLowerCase());
        }

        timerNameCache = mapBuilder.build();

        timeoutConnect = parameters.getConnectionTimeout().toMillis();
        timeoutResponse = parameters.getRequestTimeout().toMillis();
        timeoutRetry = parameters.getConnectionRetryRate().toMillis();

        clientList = new ArrayList<>();
        requestID = new AtomicLong();
        outstandingRequests = new ConcurrentHashMap<>();
        shutdown = true;

        if (parameters.isTlsEnabled()) {
            try {
                sslContext = SslContextConstructor.constructSslContext(false,
                        parameters.getKeyStore(),
                        parameters.getKsPasswordFile(),
                        parameters.getTrustStore(),
                        parameters.getTsPasswordFile());
            } catch (SSLException e) {
                throw new UnrecoverableCorfuError(e);
            }
        }

        // TODO new implementation of IClient interface
        addClient(new BaseHandler());

        // Initialize the channel
        shutdown = false;
        Bootstrap b = new Bootstrap();
        b.group(eventLoopGroup);
        b.channel(parameters.getSocketType().getChannelClass());
        parameters.getNettyChannelOptions().forEach(b::option);
        b.handler(getChannelInitializer());
        b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) timeoutConnect);

        // Asynchronously connect, retrying until shut down.
        // Once connected, connectionFuture will be completed.
        connectAsync(b);
    }

    /** Get the {@link ChannelInitializer} used for initializing the Netty channel pipeline.
     *
     * @return A {@link ChannelInitializer} which initializes the pipeline.
     */
    private ChannelInitializer getChannelInitializer() {
        return new ChannelInitializer() {
            @Override
            protected void initChannel(@Nonnull Channel ch) throws Exception {
                ch.pipeline().addLast(new IdleStateHandler(parameters.getIdleConnectionTimeout(),
                        parameters.getKeepAlivePeriod(), 0));
                if (parameters.isTlsEnabled()) {
                    ch.pipeline().addLast("ssl", sslContext.newHandler(ch.alloc()));
                }
                ch.pipeline().addLast(new LengthFieldPrepender(4));
                ch.pipeline().addLast(new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE,
                        0, 4, 0,
                        4));
                if (parameters.isSaslPlainTextEnabled()) {
                    PlainTextSaslNettyClient saslNettyClient =
                            SaslUtils.enableSaslPlainText(parameters.getUsernameFile(),
                                    parameters.getPasswordFile());
                    ch.pipeline().addLast("sasl/plain-text", saslNettyClient);
                }
                ch.pipeline().addLast(NettyClientProtobufRouter.this);

                // TODO reorganize remaining handlers
//                ch.pipeline().addLast(new NettyCorfuMessageDecoder());
//                ch.pipeline().addLast(new NettyCorfuMessageEncoder());
//                ch.pipeline().addLast(new ClientHandshakeHandler(parameters.getClientId(),
//                        node.getNodeId(), parameters.getHandshakeTimeout()));
//
//                // If parameters include message filters, add corresponding filter handler
//                if (parameters.getNettyClientInboundMsgFilters() != null) {
//                    final InboundMsgFilterHandler inboundMsgFilterHandler =
//                            new InboundMsgFilterHandler(parameters.getNettyClientInboundMsgFilters());
//                    ch.pipeline().addLast(inboundMsgFilterHandler);
//                }
//                ch.pipeline().addLast(NettyClientProtobufRouter.this);
            }
        };
    }

    /**
     * Connect to a remote server asynchronously.
     *
     * @param bootstrap The channel bootstrap to use
     */
    private void connectAsync(@Nonnull Bootstrap bootstrap) {
        // If shutdown, return a ChannelFuture that is exceptionally completed.
        if (shutdown) {
            return;
        }
        log.info("Connect Async {}:{}", node.getHost(), node.getPort());
        // Use the bootstrap to create a new channel.
        ChannelFuture f = bootstrap.connect(node.getHost(), node.getPort());
        f.addListener((ChannelFuture cf) -> channelConnectionFutureHandler(cf, bootstrap));
    }

    /** Handle when a channel is connected.
     *
     * @param future        The future that is completed when the channel is connected/
     * @param bootstrap     The bootstrap to connect a new channel (used on reconnect).
     */
    private void channelConnectionFutureHandler(@Nonnull ChannelFuture future,
                                                @Nonnull Bootstrap bootstrap) {
        if (future.isSuccess()) {
            // Register a future to reconnect in case we get disconnected
            addReconnectionOnCloseFuture(future.channel(), bootstrap);
            log.debug("connectAsync[{}]: Channel connected.", node);
        } else {
            // Otherwise, the connection failed. If we're not shutdown, try reconnecting after
            // a sleep period.
            if (!shutdown) {
                Sleep.sleepUninterruptibly(parameters.getConnectionRetryRate());
                log.debug("connectAsync[{}]: Channel connection failed, reconnecting...", node);
                // Call connect, which will retry the call again.
                // Note that this is not recursive, because it is called in the
                // context of the handler future.
                connectAsync(bootstrap);
            }
        }
    }

    /** Add a future which reconnects the server.
     *
     * @param channel       The channel to use
     * @param bootstrap     The channel bootstrap to use
     */
    private void addReconnectionOnCloseFuture(@Nonnull Channel channel,
                                              @Nonnull Bootstrap bootstrap) {
        channel.closeFuture().addListener((r) -> {
            log.debug("addReconnectionOnCloseFuture[{}]: disconnected", node);
            // Remove the current completion future, forcing clients to wait for reconnection.
            connectionFuture = new CompletableFuture<>();
            // Exceptionally complete all requests that were waiting for a completion.
            outstandingRequests.forEach((reqId, reqCompletableFuture) -> {
                reqCompletableFuture.completeExceptionally(
                        new NetworkException("Disconnected", node));
                // And also remove them.
                outstandingRequests.remove(reqId);
            });
            // If we aren't shutdown, reconnect.
            if (!shutdown) {
                Sleep.sleepUninterruptibly(parameters.getConnectionRetryRate());
                log.debug("addReconnectionOnCloseFuture[{}]: reconnecting...", node);
                // Asynchronously connect again.
                connectAsync(bootstrap);
            }
        });
    }

    /**
     * Add a new client to the router
     *
     * @param client The client to add to the router
     * @return This IClientProtobufRouter
     */
    public IClientProtobufRouter addClient(IClient client) {
        // TODO: Rewrite IClient interface

        return this;
    }

    /**
     * Send a request message and get a completable future to be fulfilled by the reply.
     *
     * @param reqBuilder The message builder for the request to send.
     * @param <T> The type of completable to return.
     * @return A completable future which will be fulfilled by the reply,
     * or a timeout in the case there is no response.
     */
    @Override
    public  <T> CompletableFuture<T> sendRequestAndGetCompletable(Request.Builder reqBuilder) {

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

        // Get the next request ID
        final long thisRequest = requestID.getAndIncrement();

        // Set the base fields for this message.
        Header header = reqBuilder.getHeaderBuilder()
                .setClientId(API.getProtoUUID(parameters.getClientId()))
                .setRequestId(thisRequest)
                .build();
        Request request = reqBuilder.setHeader(header).build();

        // Set up the timer and context to measure request
        final Timer roundTripMsgTimer = CorfuRuntime.getDefaultMetrics()
                .timer(timerNameCache.get(request.getHeader().getType()));

        final Timer.Context roundTripMsgContext = MetricsUtils
                .getConditionalContext(roundTripMsgTimer);

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
            channel.writeAndFlush(outBuf, channel.voidPromise());
            log.trace("Sent request message: {}", request.getHeader());
        } catch (IOException e) {
            log.warn("sendRequestAndGetCompletable[{}]: Exception occurred when sending request {}, caused by {}",
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
                log.debug("sendRequestAndGetCompletable: Remove request {} to {} due to timeout! Request:{}",
                        thisRequest, node, finalRequest.getHeader());
            }
            return null;
        });

        return cfTimeout;
    }

    /**
     * Send a one way message, without adding a completable future.
     *
     * @param reqBuilder The message builder for the request to send.
     */
    @Override
    public void sendRequest(Request.Builder reqBuilder) {
        // Get the next request ID
        final long thisRequest = requestID.getAndIncrement();
        // Set the base fields for this message.
        Header header = reqBuilder.getHeaderBuilder()
                .setClientId(API.getProtoUUID(parameters.getClientId()))
                .setRequestId(thisRequest)
                .build();
        Request request = reqBuilder.setHeader(header).build();

        // Write this message out on the channel
        ByteBuf outBuf = PooledByteBufAllocator.DEFAULT.buffer();
        ByteBufOutputStream requestOutputStream = new ByteBufOutputStream(outBuf);

        try {
            // Mark this message as Protobuf message (temporarily)
            requestOutputStream.writeByte(API.PROTO_CORFU_MSG_MARK);
            request.writeTo(requestOutputStream);
            channel.writeAndFlush(outBuf, channel.voidPromise());
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
     * @param clientId The header of incoming message used for validation.
     * @return True, if the clientID is correct, but false otherwise.
     */
    private boolean validateClientId(UUID clientId) {
        // Check if the message is intended for us. If not, drop the message.
        if (!clientId.equals(API.getProtoUUID(parameters.getClientId()))) {
            log.warn("Incoming message intended for client {}, our id is {}, dropping!",
                    clientId, parameters.getClientId());
            return false;
        }
        return true;
    }

    /**
     * Read data from the Channel.
     *
     * @param ctx Channel handler context
     * @param msg Object received in inbound buffer
     * @throws Exception Exception thrown during channelRead
     */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf msgBuf = (ByteBuf) msg;

        // Temporary -- If message is not marked as a Protobuf message, throw an exception.
        byte msgMark = msgBuf.getByte(msgBuf.readerIndex());
        if(msgMark != API.PROTO_CORFU_MSG_MARK) {
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
                    // TODO: New implementation of methods for handling response
                    // handler.handleMessage(response, ctx);
                }
            }
        } catch (Exception e) {
            log.error("channelRead: Exception during read!", e);
        } finally {
            msgInputStream.close();
            msgBuf.release();
        }
    }

    /**
     * Complete a given outstanding request with a completion value.
     *
     * @param requestId  The request to complete.
     * @param completion The value to complete the request with.
     */
    @Override
    public <T> void completeRequest(long requestId, T completion) {
        CompletableFuture<T> cf;
        if ((cf = (CompletableFuture<T>) outstandingRequests.remove(requestId)) != null) {
            cf.complete(completion);
        } else {
            log.warn("Attempted to complete request {}, but request not outstanding!", requestId);
        }
    }

    /**
     * Exceptionally complete a request with a given cause.
     *
     * @param requestId The request to complete.
     * @param cause     The cause to give for the exceptional completion.
     */
    @Override
    public void completeExceptionally(long requestId, Throwable cause) {
        CompletableFuture cf;
        if ((cf = outstandingRequests.remove(requestId)) != null) {
            cf.completeExceptionally(cause);
            log.debug("completeExceptionally: Remove request {} to {} due to {}.", requestId, node,
                    cause.getClass().getSimpleName(), cause);
        } else {
            log.warn("Attempted to exceptionally complete request {}, but request not outstanding!",
                    requestId);
        }
    }

    /**
     * Stops routing requests.
     */
    @Override
    public void stop() {
        log.debug("stop: Shutting down router for {}", node);
        shutdown = true;
        connectionFuture.completeExceptionally(new NetworkException("Router stopped", node));
        if (channel != null && channel.isOpen()) {
            channel.close();
        }
    }

    /**
     * Get the host that this router is routing requests for.
     */
    @Override
    public String getHost() {
        return node.getHost();
    }

    /**
     * Get the port that this router is routing requests for.
     */
    @Override
    public Integer getPort() {
        return node.getPort();
    }

    /**
     * Closes this resource, relinquishing any underlying resources.
     */
    @Override
    public void close() {
        stop();
        if (manageEventLoop) {
            this.eventLoopGroup.shutdownGracefully();
        }
    }

    // TODO Override exceptionCaught and userEventTriggered

    @VisibleForTesting
    EventLoopGroup getEventLoopGroup() {
        return eventLoopGroup;
    }

    // region Deprecated Methods
    // The methods below are deprecated and may be removed in a future release.
    /**
     * Creates a new NettyClientRouter connected to the specified endpoint.
     *
     * @param endpoint Endpoint to connectAsync to.
     * @deprecated Use {@link this#NettyClientProtobufRouter(NodeLocator, EventLoopGroup, RuntimeParameters)}
     */
    @Deprecated
    public NettyClientProtobufRouter(String endpoint) {
        this(endpoint.split(":")[0], Integer.parseInt(endpoint.split(":")[1]));
    }

    /**
     * Creates a new NettyClientRouter connected to the specified host and port.
     *
     * @param host Host to connectAsync to.
     * @param port Port to connectAsync to.
     * @deprecated Use {@link this#NettyClientProtobufRouter(NodeLocator, EventLoopGroup, RuntimeParameters)}
     */
    @Deprecated
    public NettyClientProtobufRouter(String host, Integer port) {
        this(NodeLocator.builder().host(host).port(port).build(),
                RuntimeParameters.builder().build());
    }

    /**
     * Creates a new NettyClientRouter connected to the specified host and port.
     *
     * @param host Host to connectAsync to.
     * @param port Port to connectAsync to.
     * @deprecated Use {@link this#NettyClientProtobufRouter(NodeLocator, EventLoopGroup, RuntimeParameters)}
     */
    @Deprecated
    public NettyClientProtobufRouter(String host, Integer port, Boolean tls,
                             String keyStore, String ksPasswordFile, String trustStore,
                             String tsPasswordFile, Boolean saslPlainText, String usernameFile,
                             String passwordFile) {
        this(NodeLocator.builder().host(host).port(port).build(),
                RuntimeParameters.builder()
                        .tlsEnabled(tls)
                        .keyStore(keyStore)
                        .ksPasswordFile(ksPasswordFile)
                        .trustStore(trustStore)
                        .tsPasswordFile(tsPasswordFile)
                        .saslPlainTextEnabled(saslPlainText)
                        .usernameFile(usernameFile)
                        .passwordFile(passwordFile)
                        .build());
    }

    /**
     * Creates a new NettyClientRouter connected to the specified node with the desired configuration.
     *
     * @param node          The node to connect to.
     * @param parameters    A {@link CorfuRuntime.CorfuRuntimeParameters} with the desired configuration.
     * @deprecated          Use {@link this#NettyClientProtobufRouter(NodeLocator, EventLoopGroup, RuntimeParameters)}
     */
    @Deprecated
    public NettyClientProtobufRouter(@Nonnull NodeLocator node,
                             @Nonnull RuntimeParameters parameters) {
        this(node, parameters.getSocketType()
                .getGenerator().generate(parameters.getNettyEventLoopThreads(),
                        new ThreadFactoryBuilder()
                                .setDaemon(true)
                                .setNameFormat(parameters.getNettyEventLoopThreadFormat())
                                .build()), parameters, true);
    }
    // endregion
}
