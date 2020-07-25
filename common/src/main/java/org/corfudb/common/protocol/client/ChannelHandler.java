package org.corfudb.common.protocol.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.timeout.IdleStateHandler;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.protocol.CorfuExceptions;
import org.corfudb.common.protocol.proto.CorfuProtocol.Header;
import org.corfudb.common.protocol.proto.CorfuProtocol.Request;
import org.corfudb.common.protocol.proto.CorfuProtocol.Response;
import org.corfudb.common.protocol.proto.CorfuProtocol.ServerError;
import org.corfudb.common.protocol.CorfuExceptions.PeerUnavailable;

import javax.annotation.Nonnull;
import java.net.InetSocketAddress;
import java.sql.Time;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Created by Maithem on 7/1/20.
 */

@Slf4j
@NoArgsConstructor
public abstract class ChannelHandler extends ResponseHandler {

    //TODO(Maithem): what if the consuming client is using a different protobuf lib version?

    protected InetSocketAddress remoteAddress;

    protected EventLoopGroup eventLoopGroup;

    private volatile Channel channel;

    private volatile CompletableFuture<Channel> channelCf = new CompletableFuture<>();

    enum ChannelHandlerState {
        DISCONNECTED, CONNECTING, CONNECTED, CLOSED
    }

    volatile ChannelHandlerState state = ChannelHandlerState.DISCONNECTED;

    private final ConcurrentLinkedQueue<RequestTime> requestTimeoutQueue = new ConcurrentLinkedQueue<>();

    protected final Map<Long, CompletableFuture> pendingRequests = new ConcurrentHashMap<>();

    private ScheduledFuture<?> timeoutTask;

    protected final AtomicLong idGenerator = new AtomicLong();

    @Setter
    protected ClientConfig config;

    final ReentrantReadWriteLock requestLock = new ReentrantReadWriteLock();

    public ChannelHandler(InetSocketAddress remoteAddress, EventLoopGroup eventLoopGroup, ClientConfig clientConfig) {
        this.remoteAddress = remoteAddress;
        this.eventLoopGroup = eventLoopGroup;
        this.config = clientConfig;

        //TODO(Maithem): Set pooled allocator
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(eventLoopGroup);
        bootstrap.channel(config.getSocketType().getChannelClass());
        bootstrap.option(ChannelOption.TCP_NODELAY, config.isTcpNoDelay());
        bootstrap.option(ChannelOption.SO_REUSEADDR, config.isSoReuseAddress());
        bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, config.getConnectTimeoutInMs());
        bootstrap.handler(getChannelInitializer());

        connect(bootstrap);
    }

    private synchronized void connect(Bootstrap bootstrap) {
        checkArgument(state == ChannelHandlerState.DISCONNECTED);
        state = ChannelHandlerState.CONNECTING;
        bootstrap.connect(remoteAddress).addListener((ChannelFuture res) -> {
            synchronized (this) {
                if (res.isSuccess()) {
                    // Can this leak? dont change without lock?
                    this.channel = res.channel();
                    this.channel.closeFuture().addListener(r -> disconnect());
                    state = ChannelHandlerState.CONNECTED;
                    // TODO(Maithem) Complete handshake here
                    log.info("peer client connected");
                    this.channelCf.complete(this.channel);
                } else {
                    disconnect();
                }
            }
        });
    }

    private void errorOutAllPendingRequests() {
        for (long requestId : pendingRequests.keySet()) {
            CompletableFuture requestFuture = pendingRequests.remove(requestId);
            if (requestFuture != null && !requestFuture.isDone()) {
                requestFuture.completeExceptionally(new PeerUnavailable(remoteAddress));
            } else {
                // request is already completed successfully.
            }
        }
    }

    private synchronized void disconnect() {
        checkArgument(this.state == ChannelHandlerState.CONNECTED ||
                this.state == ChannelHandlerState.CONNECTING);

        errorOutAllPendingRequests();
        this.state = ChannelHandlerState.DISCONNECTED;

        if (!channelCf.isDone() && !channelCf.isCompletedExceptionally()) {
            // throw shutdown exception instead ?
            channelCf.completeExceptionally(new PeerUnavailable(remoteAddress));
        }
        // TODO(Maithem): need to get rid of this future, because clients waiting on this future
        // need to block till at least 1 connection retry, just fail right away ? queue ops ?
        this.channelCf = new CompletableFuture<>();
        // Ideally this should be exponential time off on each retry
        this.eventLoopGroup.schedule(() -> {
            log.debug("Retrying to connect to {} in {} ms", remoteAddress, config.getConnectRetryInMs());
            connect(null);
        }, config.getConnectRetryInMs(), TimeUnit.MILLISECONDS);
    }

    public synchronized void close() {
        requestLock.writeLock().lock();
        try {
            if (state == ChannelHandlerState.CLOSED) {
                // nothing to do
                return;
            }

            state = ChannelHandlerState.CLOSED;
            errorOutAllPendingRequests();
            CompletableFuture cf = channelCf;
            if (!cf.isDone() && !cf.isCompletedExceptionally()) {
                // throw shutdown exception instead ?
                cf.completeExceptionally(new PeerUnavailable(remoteAddress));
            }

            Channel currentChannel = channel;
            if (currentChannel != null) {
                // TODO(Maithem): will this end up calling channelInactive? need to cancel timer task
                currentChannel.close().addListener(res -> {
                    if (!res.isSuccess()) {
                        log.warn("Failed to close channel for {}", res.cause());
                    }
                });
            }
        } finally {
            requestLock.writeLock().unlock();
        }
    }


    private ChannelInitializer getChannelInitializer() {
        return new ChannelInitializer() {
            @Override
            protected void initChannel(@Nonnull Channel ch) throws Exception {
                // ch.pipeline().addLast(new IdleStateHandler(config.getIdleConnectionTimeoutInMs(),
                //        config.getKeepAlivePeriodInMs(), 0));

                ch.pipeline().addLast(new LengthFieldPrepender(4));
                ch.pipeline().addLast(new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE,
                        0, 4, 0,
                        4));

                /**
                 ch.pipeline().addLast(new NettyCorfuMessageDecoder());
                 ch.pipeline().addLast(new NettyCorfuMessageEncoder());
                 **/

                /**
                 * //TODO(Maithem): need to implement new handshake logic without netty pipelines
                 ch.pipeline().addLast(new ClientHandshakeHandler(parameters.getClientId(),
                 node.getNodeId(), parameters.getHandshakeTimeout()));
                 **/

                ch.pipeline().addLast(ChannelHandler.this);
            }
        };
    }

    protected long generateRequestId() {
        return idGenerator.incrementAndGet();
    }

    //TODO(Maithem) Add keep-alive logic here

    private void checkRequestTimeout() {
        while (!requestTimeoutQueue.isEmpty()) {
            RequestTime request = requestTimeoutQueue.peek();
            if (request == null || (System.currentTimeMillis() - request.creationTimeMs) < config.getRequestTimeoutInMs()) {
                // if there is no request that is timed out then exit the loop
                break;
            }
            request = requestTimeoutQueue.poll();
            CompletableFuture requestFuture = pendingRequests.remove(request.requestId);
            if (requestFuture != null && !requestFuture.isDone()) {
                requestFuture.completeExceptionally(new PeerUnavailable(remoteAddress));
            } else {
                // request is already completed successfully.
            }
        }
    }

    protected <T> CompletableFuture<T> sendRequest(Request request) {
        requestLock.readLock().lock();

        try {
            checkArgument(request.hasHeader());
            Header header = request.getHeader();
            CompletableFuture<T> retVal = new CompletableFuture<>();
            pendingRequests.put(header.getRequestId(), retVal);
            ByteBuf outBuf = PooledByteBufAllocator.DEFAULT.buffer();
            outBuf.writeByte(0x2); // Temporary -- Add Corfu msg marker indicating new message type
            // TODO(Maithem): remove allocation
            outBuf.writeBytes(request.toByteArray());
            // TODO(Maithem): Handle pipeline errors
            channel.writeAndFlush(outBuf);
            requestTimeoutQueue.add(new RequestTime(System.currentTimeMillis(), header.getRequestId()));
            return retVal;
        } finally {
            requestLock.readLock().unlock();
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        this.timeoutTask = this.eventLoopGroup.scheduleAtFixedRate(() -> checkRequestTimeout(),
                config.getRequestTimeoutInMs(),
                config.getRequestTimeoutInMs(),
                TimeUnit.MILLISECONDS);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        log.info("[{}] Disconnected", remoteAddress);

        PeerUnavailable exception = new PeerUnavailable(remoteAddress);

        pendingRequests.forEach((requestId, resultCf) -> resultCf.completeExceptionally(exception));
        pendingRequests.clear();
        timeoutTask.cancel(true);
    }

    protected void completeRequest(long requestId, Object result) {
        CompletableFuture cf = pendingRequests.remove(requestId);
        if (cf == null || cf.isDone()) {
            log.debug("[{}] failed to complete request {}", remoteAddress, requestId);
        }
        cf.complete(result);
    }

    @Override
    protected void handleServerError(Response response) {
        Header header = response.getHeader();
        CompletableFuture cf = pendingRequests.remove(response.getHeader().getRequestId());
        if (cf == null || cf.isDone()) {
            log.debug("[{}] failed to complete request {}", remoteAddress, header.getRequestId());
        }

        ServerError serverError = response.getError();

        if (log.isDebugEnabled()) {
            log.debug("");
        }

        // TODO(Maithem): what happens if we complete if its already completed
        cf.completeExceptionally(getCorfuException(serverError));
    }

    CorfuExceptions getCorfuException(ServerError serverError) {
        switch (serverError.getCode()) {
            case OK:
                throw new IllegalStateException("No error code!");
            case TRIMMED:
            case NOT_READY:
            case OVERWRITE:
            case WRONG_EPOCH:
            case BOOTSTRAPPED:
            case WRONG_CLUSTER:
            case NOT_BOOTSTRAPPED:
            case IO:
            case UNRECOGNIZED:
            case UNKNOWN:
            default:
                return new CorfuExceptions(serverError.getMessage());

        }
    }

    @Data
    @AllArgsConstructor
    class RequestTime {
        long creationTimeMs;
        long requestId;
    }
}
