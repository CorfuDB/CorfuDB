package org.corfudb.infrastructure.logreplication.transport.sample;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.TextFormat;
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
import io.netty.handler.timeout.IdleStateHandler;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.LogReplicationRuntimeParameters;
import org.corfudb.infrastructure.logreplication.infrastructure.NodeDescriptor;
import org.corfudb.protocols.wireprotocol.NettyCorfuMessageDecoder;
import org.corfudb.protocols.wireprotocol.NettyCorfuMessageEncoder;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;
import org.corfudb.security.sasl.SaslUtils;
import org.corfudb.security.sasl.plaintext.PlainTextSaslNettyClient;
import org.corfudb.security.tls.SslContextConstructor;

import javax.annotation.Nonnull;
import javax.net.ssl.SSLException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

@Slf4j
@ChannelHandler.Sharable
public class CorfuNettyClientChannel extends SimpleChannelInboundHandler<ResponseMsg> {

    /**
     * The currently registered channel.
     */
    private volatile Channel channel = null;

    /**
     * Whether or not this channel is shutdown.
     */
    public volatile boolean shutdown;

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
     * Thread pool for this channel to use
     */
    private final EventLoopGroup eventLoopGroup;

    /**
     * A handle on the reconnection attempt that is currently scheduled, if any, so that it can be
     * cancelled when this channel is closed.
     */
    private volatile ScheduledFuture<?> pendingReconnect;

    private SslContext sslContext;

    private final LogReplicationRuntimeParameters parameters;

    private final NettyLogReplicationClientChannelAdapter adapter;

    private final NodeDescriptor node;

    public CorfuNettyClientChannel(@Nonnull NodeDescriptor node,
                                   @Nonnull EventLoopGroup eventLoopGroup,
                                   @Nonnull NettyLogReplicationClientChannelAdapter adapter) {
        this.node = node;
        this.parameters = adapter.getRouter().getParameters();
        this.adapter = adapter;
        this.eventLoopGroup = eventLoopGroup == null ? getNewEventLoopGroup()
                : eventLoopGroup;

        timeoutConnect = parameters.getConnectionTimeout().toMillis();
        timeoutResponse = parameters.getRequestTimeout().toMillis();
        timeoutRetry = parameters.getConnectionRetryRate().toMillis();

        if (parameters.isTlsEnabled()) {
            setSslContext();
        }

        // Initialize the channel
        Bootstrap b = initializeChannel();

        // Asynchronously connect, retrying until shut down.
        connectAsync(b);
    }

    private Bootstrap initializeChannel() {
        shutdown = false;
        Bootstrap b = new Bootstrap();
        b.group(eventLoopGroup);
        b.channel(parameters.getSocketType().getChannelClass());
        parameters.getNettyChannelOptions().forEach(b::option);
        b.handler(getChannelInitializer());
        b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) timeoutConnect);
        return b;
    }

    private void setSslContext() {
         try {
            sslContext = SslContextConstructor.constructSslContext(
                    false,
                    parameters.getKeyStoreConfig(),
                    parameters.getTrustStoreConfig()
            );
        } catch (SSLException e) {
            throw new UnrecoverableCorfuError(e);
        }
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ResponseMsg msg) {
        log.info("Received msg type={}", msg.getPayload().getPayloadCase());
        adapter.receive(msg);
    }

    /**
     * Channel event that is triggered when a new connected channel is created.
     *
     * @param ctx channel handler context
     * @throws Exception exception
     */
    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        log.info("channelActive: Outgoing connection established to: {} from id={}", ctx.channel().remoteAddress(), ctx.channel().localAddress());
        channel = ctx.channel();
        adapter.onConnectionUp(node.getNodeId());
    }

    /**
     * Channel event that is triggered when the channel is closed.
     *
     * @param ctx channel handler context
     * @throws Exception
     */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        log.info("channelActive: Outgoing connection lost to: {} from id={}", ctx.channel().remoteAddress(), ctx.channel().localAddress());
        adapter.onConnectionDown(node.getNodeId());
    }

    public void close() {
        log.debug("Close channel to {}", node.getNodeId());
        shutdown = true;
        ScheduledFuture<?> scheduledReconnect = pendingReconnect;
        if (scheduledReconnect != null) {
            scheduledReconnect.cancel(false);
        }
        adapter.onError(new NetworkException("Channel closed", node.getClusterId()));
        if (channel != null && channel.isOpen()) {
            channel.close();
        }
        if (sslContext instanceof io.netty.util.ReferenceCounted) {
            ((io.netty.util.ReferenceCounted) sslContext).release();
            log.debug("close: Released SslContext {} for node {}", sslContext, node);
        }
        this.eventLoopGroup.shutdownGracefully();
    }

    /**
     * Get the {@link ChannelInitializer} used for initializing the Netty channel pipeline.
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

                ch.pipeline().addLast(CorfuNettyClientChannel.this);
            }
        };
    }

    public void send(RequestMsg message) {
        // Write the message out to the channel.
        channel.writeAndFlush(message, channel.voidPromise());
        log.info("Sent message: {}", TextFormat.shortDebugString(message));
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
        log.info("Connect Async {}", node.getNodeId());
        // Use the bootstrap to create a new channel.
        ChannelFuture f = bootstrap.connect(node.getHost(), Integer.valueOf(node.getPort()));
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
            log.info("connectAsync[{}]: Channel connected.", node);
        } else {
            // Otherwise, the connection failed. If we're not shutdown, try reconnecting after
            // the retry interval.
            if (!shutdown) {
                log.info("connectAsync[{}]: Channel connection failed, reconnecting in {}ms...",
                        node, timeoutRetry);
                // Schedule connect, which will retry the call again.
                scheduleReconnect(bootstrap);
            }
        }
    }

    /**
     * Schedule a reconnection attempt once the retry interval has elapsed.
     *
     * <p>This is invoked from Netty channel future listeners, which run on the event loop thread
     * that owns the channel. Sleeping on that thread would stall every other channel registered
     * to it - including channels to healthy nodes - so the retry is scheduled instead of blocking.
     *
     * @param bootstrap The channel bootstrap to use
     */
    private void scheduleReconnect(@Nonnull Bootstrap bootstrap) {
        try {
            // connectAsync re-checks the shutdown flag, so a retry that is scheduled concurrently
            // with close() is a no-op rather than a resurrected connection.
            pendingReconnect = eventLoopGroup.schedule(() -> connectAsync(bootstrap),
                    timeoutRetry, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException ree) {
            log.debug("scheduleReconnect[{}]: event loop is shutting down, not reconnecting", node);
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
            adapter.completeExceptionally(new NetworkException("Disconnected", node.getClusterId()));

            // If we aren't shutdown, reconnect.
            if (!shutdown) {
                log.debug("addReconnectionOnCloseFuture[{}]: reconnecting in {}ms...",
                        node, timeoutRetry);
                // Asynchronously connect again, once the retry interval has elapsed.
                scheduleReconnect(bootstrap);
            }
        });
    }

    /**
     * Get a new {@link EventLoopGroup} for scheduling threads for Netty. The
     * {@link EventLoopGroup} is typically passed to a router.
     *
     * @return An {@link EventLoopGroup}.
     */
    private EventLoopGroup getNewEventLoopGroup() {
        // Calculate the number of threads which should be available in the thread pool.
        int numThreads = parameters.getNettyEventLoopThreads() == 0
                ? Runtime.getRuntime().availableProcessors() * 2 :
                parameters.getNettyEventLoopThreads();
        ThreadFactory factory = new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat(parameters.getNettyEventLoopThreadFormat())
                .setUncaughtExceptionHandler(this::handleUncaughtThread)
                .build();
        return parameters.getSocketType().getGenerator().generate(numThreads, factory);
    }

    /**
     * Function which is called whenever the runtime encounters an uncaught thread.
     *
     * @param thread    The thread which terminated.
     * @param throwable The throwable which caused the thread to terminate.
     */
    private void handleUncaughtThread(@Nonnull Thread thread, @Nonnull Throwable throwable) {
        if (parameters.getUncaughtExceptionHandler() != null) {
            parameters.getUncaughtExceptionHandler().uncaughtException(thread, throwable);
        } else {
            log.error("handleUncaughtThread: {} terminated with throwable of type {}",
                    thread.getName(),
                    throwable.getClass().getSimpleName(),
                    throwable);
        }
    }
}
