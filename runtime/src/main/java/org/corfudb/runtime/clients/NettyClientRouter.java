package org.corfudb.runtime.clients;

import com.codahale.metrics.Timer;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nonnull;
import javax.net.ssl.SSLException;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.protocols.wireprotocol.ClientHandshakeHandler;
import org.corfudb.protocols.wireprotocol.ClientHandshakeHandler.ClientHandshakeEvent;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.InboundMsgFilterHandler;
import org.corfudb.protocols.wireprotocol.NettyCorfuMessageDecoder;
import org.corfudb.protocols.wireprotocol.NettyCorfuMessageEncoder;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.security.sasl.SaslUtils;
import org.corfudb.security.sasl.plaintext.PlainTextSaslNettyClient;
import org.corfudb.security.tls.SslContextConstructor;
import org.corfudb.util.CFUtils;
import org.corfudb.util.CorfuComponent;
import org.corfudb.util.MetricsUtils;
import org.corfudb.util.NodeLocator;
import org.corfudb.util.Sleep;


/**
 * A client router which multiplexes operations over the Netty transport.
 *
 * <p>Created by mwei on 12/8/15.
 */
@Slf4j
@ChannelHandler.Sharable
public class NettyClientRouter extends SimpleChannelInboundHandler<CorfuMsg>
        implements IClientRouter {

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
    public Map<CorfuMsgType, IClient> handlerMap;
    /**
     * The clients registered to this router.
     */
    public List<IClient> clientList;

    /**
     * The outstanding requests on this router.
     */
    public Map<Long, CompletableFuture> outstandingRequests;

    /**
     * The currently registered channel.
     */
    private volatile Channel channel = null;

    /** Whether to shutdown the {@code eventLoopGroup} or not. Only applies when
     *  a deprecated constructor (which generates its own {@link EventLoopGroup} is used.
     */
    private boolean shutdownEventLoop = false;

    /**
     * Whether or not this router is shutdown.
     */
    public volatile boolean shutdown;

    /** The {@link NodeLocator} which represents the remote node this
     *  {@link NettyClientRouter} connects to.
     */
    @Getter
    private final NodeLocator node;

    /** The {@link CorfuRuntimeParameters} used to configure the
     *  router.
     */
    private final CorfuRuntimeParameters parameters;

    /** A {@link CompletableFuture} which is completed when a connection,
     *  including a successful handshake completes and messages can be sent
     *  to the remote node.
     */
    @Getter
    volatile CompletableFuture<Void> connectionFuture;

    private SslContext sslContext;
    private final Map<CorfuMsgType, String> timerNameCache;

    /**
     * Creates a new NettyClientRouter connected to the specified host and port with the
     * specified tls and sasl options. The new {@link this} will attempt connection to
     * the node until {@link this#stop()} is called.
     *
     * @param node           The node to connect to.
     * @param eventLoopGroup The {@link EventLoopGroup} for servicing I/O.
     * @param parameters     A {@link CorfuRuntimeParameters} with the desired configuration.
     */
    public NettyClientRouter(@Nonnull NodeLocator node,
        @Nonnull EventLoopGroup eventLoopGroup,
        @Nonnull CorfuRuntimeParameters parameters) {
        this.node = node;
        this.parameters = parameters;

        // Set timer mapping
        ImmutableMap.Builder<CorfuMsgType, String> mapBuilder = ImmutableMap.builder();
        for (CorfuMsgType type : CorfuMsgType.values()) {
            mapBuilder.put(type,
                    CorfuComponent.CLIENT_ROUTER.toString() + type.name().toLowerCase());
        }

        timerNameCache = mapBuilder.build();

        timeoutConnect = parameters.getConnectionTimeout().toMillis();
        timeoutResponse = parameters.getRequestTimeout().toMillis();
        timeoutRetry = parameters.getConnectionRetryRate().toMillis();

        connectionFuture = new CompletableFuture<>();

        handlerMap = new ConcurrentHashMap<>();
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

    /**
     * Add a new client to the router.
     *
     * @param client The client to add to the router.
     * @return This NettyClientRouter, to support chaining and the builder pattern.
     */
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
     * @param <T>        The type of the client to match.
     * @return The first client that matches that type.
     * @throws NoSuchElementException If there are no clients matching that type.
     */
    @SuppressWarnings("unchecked")
    public <T extends IClient> T getClient(Class<T> clientType) {
        return (T) clientList.stream()
            .filter(clientType::isInstance)
            .findFirst().get();
    }

    /**
     * {@inheritDoc}.
     *
     * @deprecated The router automatically starts now, so this function call is no
     *             longer necessary
     */
    @Override
    @Deprecated
    public synchronized void start() {
        // Do nothing, legacy call
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
                ch.pipeline().addLast(new NettyCorfuMessageDecoder());
                ch.pipeline().addLast(new NettyCorfuMessageEncoder());
                ch.pipeline().addLast(new ClientHandshakeHandler(parameters.getClientId(),
                    node.getNodeId(), parameters.getHandshakeTimeout()));

                // If parameters include message filters, add corresponding filter handler
                if (parameters.getNettyClientInboundMsgFilters() != null) {
                    final InboundMsgFilterHandler inboundMsgFilterHandler =
                            new InboundMsgFilterHandler(parameters.getNettyClientInboundMsgFilters());
                    ch.pipeline().addLast(inboundMsgFilterHandler);
                }
                ch.pipeline().addLast(NettyClientRouter.this);
            }
        };
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
     * Connect to a remote server asynchronously.
     *
     * @param bootstrap The channel bootstrap to use
     */
    private void connectAsync(@Nonnull Bootstrap bootstrap) {
        // If shutdown, return a ChannelFuture that is exceptionally completed.
        if (shutdown) {
            return;
        }
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

    /** {@inheritDoc}
     *  @deprecated  Deprecated, stopping a router without shutting it down is no longer supported.
     *               Please use {@link this#stop()}.
     */
    @Override
    @Deprecated
    public void stop(boolean shutdown) {
        stop();
    }

    /**
     * Send a message and get a completable future to be fulfilled by the reply.
     *
     * @param ctx     The channel handler context to send the message under.
     * @param message The message to send.
     * @param <T>     The type of completable to return.
     * @return A completable future which will be fulfilled by the reply,
     *     or a timeout in the case there is no response.
     */
    public <T> CompletableFuture<T> sendMessageAndGetCompletable(ChannelHandlerContext ctx,
        @NonNull CorfuMsg message) {
        boolean isEnabled = MetricsUtils.isMetricsCollectionEnabled();

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
                .getConditionalContext(isEnabled, roundTripMsgTimer);

        // Get the next request ID.
        final long thisRequest = requestID.getAndIncrement();

        // Set the message fields.
        message.setClientID(parameters.getClientId());
        message.setRequestID(thisRequest);

        // Generate a future and put it in the completion table.
        final CompletableFuture<T> cf = new CompletableFuture<>();
        outstandingRequests.put(thisRequest, cf);

        // Write the message out to the channel.
        if (ctx == null) {
            channel.writeAndFlush(message, channel.voidPromise());
        } else {
            ctx.writeAndFlush(message, ctx.voidPromise());
        }
        log.trace("Sent message: {}", message);

        // Generate a benchmarked future to measure the underlying request
        final CompletableFuture<T> cfBenchmarked = cf.thenApply(x -> {
            MetricsUtils.stopConditionalContext(roundTripMsgContext);
            return x;
        });

        // Generate a timeout future, which will complete exceptionally
        // if the main future is not completed.
        final CompletableFuture<T> cfTimeout =
            CFUtils.within(cfBenchmarked, Duration.ofMillis(timeoutResponse));
        cfTimeout.exceptionally(e -> {
            // CFUtils.within() can wrap different kinds of exceptions in
            // CompletionException, just dealing with TimeoutException here since
            // the router is not aware of it and this::completeExceptionally()
            // takes care of others. This avoids handling same exception twice.
            if (e.getCause() instanceof TimeoutException) {
                outstandingRequests.remove(thisRequest);
                log.debug("sendMessageAndGetCompletable: Remove request {} to {} due to timeout! Message:{}",
                        thisRequest, node, message);
            }
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
    public void sendMessage(ChannelHandlerContext ctx, CorfuMsg message) {
        // Get the next request ID.
        final long thisRequest = requestID.getAndIncrement();
        // Set the base fields for this message.
        message.setClientID(parameters.getClientId());
        message.setRequestID(thisRequest);
        // Write this message out on the channel.
        channel.writeAndFlush(message, channel.voidPromise());
        log.trace("Sent one-way message: {}", message);
    }


    /**
     * Send a netty message through this router, setting the fields in the outgoing message.
     *
     * @param ctx    Channel handler context to use.
     * @param inMsg  Incoming message to respond to.
     * @param outMsg Outgoing message.
     */
    public void sendResponseToServer(ChannelHandlerContext ctx, CorfuMsg inMsg, CorfuMsg outMsg) {
        outMsg.copyBaseFields(inMsg);
        ctx.writeAndFlush(outMsg, ctx.voidPromise());
        log.trace("Sent response: {}", outMsg);
    }

    /**
     * Complete a given outstanding request with a completion value.
     *
     * @param requestId  The request to complete.
     * @param completion The value to complete the request with
     * @param <T>        The type of the completion.
     */
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
     * @param requestID The request to complete.
     * @param cause     The cause to give for the exceptional completion.
     */
    public void completeExceptionally(long requestID, @Nonnull Throwable cause) {
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

    /**
     * Validate the clientID of a CorfuMsg.
     *
     * @param msg The incoming message to validate.
     * @return True, if the clientID is correct, but false otherwise.
     */
    private boolean validateClientId(CorfuMsg msg) {
        // Check if the message is intended for us. If not, drop the message.
        if (!msg.getClientID().equals(parameters.getClientId())) {
            log.warn("Incoming message intended for client {}, our id is {}, dropping!",
                    msg.getClientID(), parameters.getClientId());
            return false;
        }
        return true;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, CorfuMsg m) throws Exception {
        try {
            // We get the handler for this message from the map
            IClient handler = handlerMap.get(m.getMsgType());
            if (handler == null) {
                // The message was unregistered, we are dropping it.
                log.warn("Received unregistered message {}, dropping", m);
            } else {
                if (validateClientId(m)) {
                    // Route the message to the handler.
                    if (log.isTraceEnabled()) {
                        log.trace("Message routed to {}: {}",
                                handler.getClass().getSimpleName(), m);
                    }
                    handler.handleMessage(m, ctx);
                }
            }
        } catch (Exception e) {
            log.error("Exception during read!", e);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Exception during channel handling.", cause);
        ctx.close();
    }

    /**
     * Sends a keep alive message to the server so that the response will keep
     * the channel active in order to avoid a ReadTimeout exception that will
     * close the channel.
     */
    private void keepAlive() {
        if (channel == null || !channel.isOpen()) {
            log.info("keepAlive: channel not established or not open, skipping sending keep alive.");
            return;
        }
        // Send a keep alive message to server which ignores epoch
        sendMessageAndGetCompletable(null, CorfuMsgType.KEEP_ALIVE.msg());
        log.trace("keepAlive: sent keep alive to {}", this.channel.remoteAddress());
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt.equals(ClientHandshakeEvent.CONNECTED)) {
            // Handshake successful. Complete the connection future to allow
            // clients to proceed.
            channel = ctx.channel();
            connectionFuture.complete(null);
        } else if (evt.equals(ClientHandshakeEvent.FAILED) && connectionFuture.isDone()) {
            // Handshake failed. If the current completion future is complete,
            // create a new one to unset it, causing future requests
            // to wait.
            connectionFuture = new CompletableFuture<>();
        } else if (evt instanceof IdleStateEvent) {
            IdleStateEvent e = (IdleStateEvent) evt;
            if (e.state() == IdleState.READER_IDLE) {
                ctx.close();
            } else if (e.state() == IdleState.WRITER_IDLE) {
                keepAlive();
            }
        } else {
            log.warn("userEventTriggered: unhandled event {}", evt);
        }
    }

    // region Deprecated Methods
    // The methods below are deprecated and may be removed in a future release.
    /**
     * Creates a new NettyClientRouter connected to the specified endpoint.
     *
     * @param endpoint Endpoint to connectAsync to.
     * @deprecated Use {@link this#NettyClientRouter(NodeLocator, CorfuRuntimeParameters)}
     */
    @Deprecated
    public NettyClientRouter(String endpoint) {
        this(endpoint.split(":")[0], Integer.parseInt(endpoint.split(":")[1]));
    }

    /**
     * Creates a new NettyClientRouter connected to the specified host and port.
     *
     * @param host Host to connectAsync to.
     * @param port Port to connectAsync to.
     * @deprecated Use {@link this#NettyClientRouter(NodeLocator, CorfuRuntimeParameters)}
     */
    @Deprecated
    public NettyClientRouter(String host, Integer port) {
        this(NodeLocator.builder().host(host).port(port).build(),
            CorfuRuntimeParameters.builder().build());
    }

    /**
     * Creates a new NettyClientRouter connected to the specified host and port.
     *
     * @param host Host to connectAsync to.
     * @param port Port to connectAsync to.
     * @deprecated Use {@link this#NettyClientRouter(NodeLocator, CorfuRuntimeParameters)}
     */
    @Deprecated
    public NettyClientRouter(String host, Integer port, Boolean tls,
        String keyStore, String ksPasswordFile, String trustStore,
        String tsPasswordFile, Boolean saslPlainText, String usernameFile,
        String passwordFile) {
        this(NodeLocator.builder().host(host).port(port).build(),
            CorfuRuntimeParameters.builder()
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

    public NettyClientRouter(@Nonnull NodeLocator node,
        @Nonnull CorfuRuntimeParameters parameters) {
        this(node, parameters.getSocketType()
            .getGenerator().generate(Runtime.getRuntime().availableProcessors() * 2,
                new ThreadFactoryBuilder()
                    .setDaemon(true)
                    .setNameFormat(parameters.getNettyEventLoopThreadFormat())
                    .build()), parameters);
        shutdownEventLoop = true;
    }

    @Deprecated
    @Override
    public Integer getPort() {
        return node.getPort();
    }

    @Deprecated
    public String getHost() {
        return node.getHost();
    }
    // endregion
}
