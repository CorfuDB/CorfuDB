package org.corfudb.runtime.clients;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.ssl.SslContext;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.NettyCorfuMessageDecoder;
import org.corfudb.protocols.wireprotocol.NettyCorfuMessageEncoder;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.security.sasl.SaslUtils;
import org.corfudb.security.sasl.plaintext.PlainTextSaslNettyClient;
import org.corfudb.security.tls.TlsUtils;
import org.corfudb.util.CFUtils;
import org.corfudb.util.MetricsUtils;


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
     * Metrics: meter (counter), histogram.
     */
    private Gauge<Integer> gaugeConnected;
    private Timer timerConnect;
    private Timer timerSyncOp;
    private Counter counterConnectFailed;
    private Counter counterSendDisconnected;
    private Counter counterSendTimeout;
    private Counter counterAsyncOpSent;

    /**
     * A random instance.
     */
    public static final Random random = new Random();
    /**
     * The epoch this router is in.
     */
    @Getter
    public long epoch;

    /**
     * We should never set epoch backwards
     *
     * @param epoch
     */
    public void setEpoch(long epoch) {
        if (epoch < this.epoch) {
            log.warn("setEpoch: Rejected attempt to set the router {}:{} to epoch {} smaller than current epoch {}",
                    host, port, epoch, this.epoch);
            return;
        }
        this.epoch = epoch;
    }

    /**
     * The cluster ID to be attached in every message.
     */
    @Getter
    private UUID clusterId;

    public void setClusterId (UUID clusterId) {
        this.clusterId = this.clusterId == null ? clusterId : this.clusterId;
    }

    /**
     * The id of this client.
     */
    @Getter
    @Setter
    @Deprecated // TODO: Add replacement method that conforms to style
    @SuppressWarnings("checkstyle:abbreviation") // Due to deprecation
    public UUID clientID;
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
    @Deprecated // TODO: Add replacement method that conforms to style
    @SuppressWarnings("checkstyle:abbreviation") // Due to deprecation
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
     * The currently registered channel context.
     */
    public ChannelHandlerContext context;
    /**
     * The currently registered channel.
     */
    public Channel channel = null;
    /**
     * The worker group for this router.
     */
    public EventLoopGroup workerGroup;
    /**
     * The event executor group for this router.
     */
    public EventExecutorGroup ee;
    /**
     * Whether or not this router is shutdown.
     */
    public volatile boolean shutdown;
    /**
     * The host that this router is routing requests for.
     */
    @Getter
    String host;
    /**
     * The port that this router is routing requests for.
     */
    @Getter
    Integer port;
    /**
     * Flag, if we are connected.
     */
    @Getter
    volatile Boolean connected;

    private Boolean tlsEnabled = false;

    private SslContext sslContext;

    private Boolean saslPlainTextEnabled = false;

    private String saslPlainTextUsernameFile;

    private String saslPlainTextPasswordFile;

    /**
     * Creates a new NettyClientRouter connected to the specified endpoint.
     *
     * @param endpoint Endpoint to connect to.
     */
    public NettyClientRouter(String endpoint) {
        this(endpoint.split(":")[0], Integer.parseInt(endpoint.split(":")[1]));
    }

    /**
     * Creates a new NettyClientRouter connected to the specified host and port.
     *
     * @param host Host to connect to.
     * @param port Port to connect to.
     */
    public NettyClientRouter(String host, Integer port) {
        this(host, port, false, null, null, null,
                null, false, null, null);
    }

    public NettyClientRouter(String host, Integer port, Boolean tls,
                             String keyStore, String ksPasswordFile, String trustStore,
                             String tsPasswordFile, Boolean saslPlainText, String usernameFile,
                             String passwordFile) {
        this(host, port, tls, keyStore, ksPasswordFile, trustStore, tsPasswordFile,
                saslPlainText, usernameFile, passwordFile, null);
    }

    /**
     * Creates a new NettyClientRouter connected to the specified host and port with the
     * specified tls and sasl options.
     *
     * @param host           Host to connect to.
     * @param port           Port to conect to.
     * @param tls            TLS enable flag.
     * @param keyStore       Key store to be used.
     * @param ksPasswordFile Key store password file path.
     * @param trustStore     Trust store to be used.
     * @param tsPasswordFile Trust store password file path.
     * @param saslPlainText  Sasl to be used.
     * @param usernameFile   username file path
     * @param passwordFile   password file path
     */
    public NettyClientRouter(String host, Integer port, Boolean tls,
                             String keyStore, String ksPasswordFile, String trustStore,
                             String tsPasswordFile, Boolean saslPlainText, String usernameFile,
                             String passwordFile, MetricRegistry metricRegistry) {
        this.host = host;
        this.port = port;

        clientID = UUID.randomUUID();
        connected = false;
        timeoutConnect = 500;
        timeoutResponse = 5000;
        timeoutRetry = 1000;

        handlerMap = new ConcurrentHashMap<>();
        clientList = new ArrayList<>();
        requestID = new AtomicLong();
        outstandingRequests = new ConcurrentHashMap<>();
        shutdown = true;

        MetricRegistry metrics = CorfuRuntime.getDefaultMetrics();

        if (metricRegistry != null) {
            metrics = metricRegistry;
        }
        String pfx = CorfuRuntime.getMpCR() + host + ":" + port.toString() + ".";
        synchronized (metrics) {
            if (!metrics.getNames().contains(pfx + "connected")) {
                gaugeConnected = metrics.register(pfx + "connected", () -> connected ? 1 : 0);
            }
        }
        timerConnect = metrics.timer(pfx + "connect");
        timerSyncOp = metrics.timer(pfx + "sync-op");
        counterConnectFailed = metrics.counter(pfx + "connect-failed");
        counterSendDisconnected = metrics.counter(pfx + "send-disconnected");
        counterSendTimeout = metrics.counter(pfx + "send-timeout");
        counterAsyncOpSent = metrics.counter(pfx + "async-op-sent");

        if (tls) {
            sslContext =
                    TlsUtils.enableTls(TlsUtils.SslContextType.CLIENT_CONTEXT,
                            keyStore, e -> {
                                throw new RuntimeException("Could not read the key store "
                                        + "password file: " + e.getClass().getSimpleName(), e);
                            },
                            ksPasswordFile, e -> {
                                throw new RuntimeException("Could not load keys from the key "
                                        + "store: " + e.getClass().getSimpleName(), e);
                            },
                            trustStore, e -> {
                                throw new RuntimeException("Could not read the trust store "
                                        + "password file: " + e.getClass().getSimpleName(), e);
                            },
                            tsPasswordFile, e -> {
                                throw new RuntimeException("Could not load keys from the trust "
                                        + "store: " + e.getClass().getSimpleName(), e);
                            });
            this.tlsEnabled = true;
        }

        if (saslPlainText) {
            saslPlainTextUsernameFile = usernameFile;
            saslPlainTextPasswordFile = passwordFile;
            saslPlainTextEnabled = true;
        }

        addClient(new BaseClient());
        start();
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

    public void start() {
        start(-1);
    }

    /**
     * Initiates the connection and starts the netty client router.
     *
     * @param c Server startup code.
     */
    public void start(long c) {
        shutdown = false;
        if (workerGroup == null
                || workerGroup.isShutdown()
                || !channel.isOpen()) {
            workerGroup = new NioEventLoopGroup(Runtime.getRuntime()
                    .availableProcessors() * 2, new ThreadFactory() {
                        final AtomicInteger threadNum = new AtomicInteger(0);

                        @Override
                        public Thread newThread(Runnable r) {
                            Thread t = new Thread(r);
                            t.setName("worker-" + threadNum.getAndIncrement());
                            t.setDaemon(true);
                            return t;
                        }
                    });

            ee = new DefaultEventExecutorGroup(Runtime.getRuntime()
                    .availableProcessors() * 2, new ThreadFactory() {

                        final AtomicInteger threadNum = new AtomicInteger(0);

                        @Override
                        public Thread newThread(Runnable r) {
                            Thread t = new Thread(r);
                            t.setName(this.getClass().getName() + "event-"
                                    + threadNum.getAndIncrement());
                            t.setDaemon(true);
                            return t;
                        }
                    });

            Bootstrap b = new Bootstrap();
            b.group(workerGroup);
            b.channel(NioSocketChannel.class);
            b.option(ChannelOption.SO_KEEPALIVE, true);
            b.option(ChannelOption.SO_REUSEADDR, true);
            b.option(ChannelOption.TCP_NODELAY, true);
            NettyClientRouter router = this;
            b.handler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) throws Exception {
                    if (tlsEnabled) {
                        ch.pipeline().addLast("ssl", sslContext.newHandler(ch.alloc()));
                    }
                    ch.pipeline().addLast(new LengthFieldPrepender(4));
                    ch.pipeline().addLast(new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE,
                            0, 4, 0,
                            4));
                    if (saslPlainTextEnabled) {
                        PlainTextSaslNettyClient saslNettyClient =
                                SaslUtils.enableSaslPlainText(saslPlainTextUsernameFile,
                                        saslPlainTextPasswordFile);
                        ch.pipeline().addLast("sasl/plain-text", saslNettyClient);
                    }
                    ch.pipeline().addLast(ee, new NettyCorfuMessageDecoder());
                    ch.pipeline().addLast(ee, new NettyCorfuMessageEncoder());
                    ch.pipeline().addLast(ee, router);
                }
            });

            try {
                connectChannel(b, c);
            } catch (Exception e) {

                try {
                    // shutdown EventLoopGroup
                    workerGroup.shutdownGracefully().sync();
                } catch (InterruptedException ie) {
                    log.warn("workerGroup shutdown interrupted : {}", ie);
                }
                throw new NetworkException(e.getClass().getSimpleName()
                        + " connecting to endpoint failed", host + ":" + port, e);
            }
        }
    }

    synchronized void connectChannel(Bootstrap b, long c) {
        boolean isEnabled = MetricsUtils.isMetricsCollectionEnabled();
        try (Timer.Context context = MetricsUtils.getConditionalContext(isEnabled, timerConnect)) {
            ChannelFuture cf = b.connect(host, port);
            cf.syncUninterruptibly();
            if (!cf.awaitUninterruptibly(timeoutConnect)) {
                cf.channel().close();   // close port
//                MetricsUtils.incConditionalCounter(isEnabled, counterConnectFailed, 1);
                throw new NetworkException(c + " Timeout connecting to endpoint",
                        host + ":" + port);
            }
            channel = cf.channel();
        }
        channel.closeFuture().addListener((r) -> {
            connected = false;
            outstandingRequests.forEach((reqId, reqCompletableFuture) -> {
//                MetricsUtils.incConditionalCounter(isEnabled, counterSendDisconnected, 1);
                reqCompletableFuture.completeExceptionally(new NetworkException("Disconnected",
                        host + ":" + port));
                outstandingRequests.remove(reqId);
            });
            if (!shutdown) {
                log.trace("Disconnected, reconnecting...");
                while (!shutdown) {
                    try {
                        connectChannel(b, c);
                        return;
                    } catch (Exception ex) {
//                        MetricsUtils.incConditionalCounter(isEnabled,
//                                counterConnectFailed, 1);
                        log.warn("Exception while reconnecting, retry in {} ms", timeoutRetry);
                        Thread.sleep(timeoutRetry);
                    }
                }
            }
        });
        connected = true;
    }

    /**
     * Stops routing requests.
     */
    @Override
    public void stop() {
        stop(false);
    }

    @Override
    public void stop(boolean shutdown) {
        // A very hasty check of Netty state-of-the-art is that shutting down
        // the worker threads is tricksy or impossible.
        this.shutdown = shutdown;
        connected = false;

        if (shutdown) {
            try {
                ChannelFuture cf = channel.close();
                cf.syncUninterruptibly();
                cf.awaitUninterruptibly(1000);
            } catch (Exception e) {
                log.error("Error in closing channel");
            }
            try {
                ee.shutdownGracefully().sync();
                workerGroup.shutdownGracefully().sync();
            } catch (InterruptedException e) {
                log.error("Interrupted exception in shutting event pool : {}", e);
            }
        } else {
            ChannelFuture cf = channel.disconnect();
            cf.syncUninterruptibly();
            boolean b1 = cf.awaitUninterruptibly(1000);
        }
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
                                                                 CorfuMsg message) {
        boolean isEnabled = MetricsUtils.isMetricsCollectionEnabled();
        if (!connected) {
            log.trace("Disconnected endpoint " + host + ":" + port);
            MetricsUtils.incConditionalCounter(isEnabled, counterSendDisconnected, 1);
            throw new NetworkException("Disconnected endpoint", host + ":" + port);
        } else {
            final Timer.Context context = MetricsUtils
                    .getConditionalContext(isEnabled, timerSyncOp);
            // Get the next request ID.
            final long thisRequest = requestID.getAndIncrement();
            // Set the message fields.
            message.setClientID(clientID);
            message.setRequestID(thisRequest);
            message.setEpoch(epoch);
            message.setClusterId(clusterId);

            // Generate a future and put it in the completion table.
            final CompletableFuture<T> cf = new CompletableFuture<>();
            outstandingRequests.put(thisRequest, cf);
            // Write the message out to the channel.
            if (ctx == null) {
                channel.writeAndFlush(message);
            } else {
                ctx.writeAndFlush(message);
            }
            log.trace("Sent message: {}", message);
            final CompletableFuture<T> cfElapsed = cf.thenApply(x -> {
                MetricsUtils.stopConditionalContext(context);
                return x;
            });
            // Generate a timeout future, which will complete exceptionally
            // if the main future is not completed.
            final CompletableFuture<T> cfTimeout =
                    CFUtils.within(cfElapsed, Duration.ofMillis(timeoutResponse));
            cfTimeout.exceptionally(e -> {
//                MetricsUtils.incConditionalCounter(isEnabled, counterSendTimeout, 1);
                outstandingRequests.remove(thisRequest);
                log.debug("Remove request {} due to timeout!", thisRequest);
                return null;
            });
            return cfTimeout;
        }
    }

    /**
     * Send a one way message, without adding a completable future.
     *
     * @param ctx     The context to send the message under.
     * @param message The message to send.
     */
    public void sendMessage(ChannelHandlerContext ctx, CorfuMsg message) {
        ChannelHandlerContext outContext = context;
        if (ctx == null) {
            if (context == null) {
                // if the router's context is not set, return a failure
                log.warn("Attempting to send on a channel that is not ready.");
                return;
            }
            outContext = context;
        }
        // Get the next request ID.
        final long thisRequest = requestID.getAndIncrement();
        // Set the base fields for this message.
        message.setClientID(clientID);
        message.setRequestID(thisRequest);
        message.setEpoch(epoch);
        message.setClusterId(clusterId);
        // Write this message out on the channel.
        outContext.writeAndFlush(message);
//        MetricsUtils.incConditionalCounter(MetricsUtils
//                .isMetricsCollectionEnabled(), counterAsyncOpSent, 1);
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
        outMsg.setEpoch(epoch);
        outMsg.setClusterId(clusterId);
        ctx.writeAndFlush(outMsg);
        log.trace("Sent response: {}", outMsg);
    }

    /**
     * Complete a given outstanding request with a completion value.
     *
     * @param requestID  The request to complete.
     * @param completion The value to complete the request with
     * @param <T>        The type of the completion.
     */
    @Deprecated // TODO: Add replacement method that conforms to style
    @SuppressWarnings("checkstyle:abbreviation") // Due to deprecation
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
            log.warn("Attempted to exceptionally complete request {}, but request not outstanding!",
                    requestID);
        }
    }

    /**
     * Validate the epoch of a CorfuMsg, and send a WRONG_EPOCH response if
     * the server is in the wrong epoch. Ignored if the message type is reset (which
     * is valid in any epoch).
     *
     * @param msg The incoming message to validate.
     * @param ctx The context of the channel handler.
     * @return True, if the epoch is correct, but false otherwise.
     */
    @Deprecated // TODO: Add replacement method that conforms to style
    @SuppressWarnings("checkstyle:abbreviation") // Due to deprecation
    private boolean validateEpochAndClientID(CorfuMsg msg, ChannelHandlerContext ctx) {
        // Check if the message is intended for us. If not, drop the message.
        if (!msg.getClientID().equals(clientID)) {
            log.warn("Incoming message intended for client {}, our id is {}, dropping!",
                    msg.getClientID(), clientID);
            return false;
        }
        // Check if the message is in the right epoch.
        if (!msg.getMsgType().ignoreEpoch && msg.getEpoch() != epoch) {
            log.trace("Incoming message with wrong epoch, got {}, expected {}, message was: {}",
                    msg.getEpoch(), epoch, msg);
            /* If this message was pending a completion, complete it with an error. */
            completeExceptionally(msg.getRequestID(), new WrongEpochException(msg.getEpoch()));
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
                if (validateEpochAndClientID(m, ctx)) {
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

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        context = ctx;
        log.debug("Registered new channel {}", ctx);
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        super.channelUnregistered(ctx);
        context = null;
        log.debug("Unregistered channel {}", ctx);
    }
}
