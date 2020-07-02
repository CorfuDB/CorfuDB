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
import io.netty.handler.ssl.SslContext;
import io.netty.handler.timeout.IdleStateHandler;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.protocol.CorfuExceptions;
import org.corfudb.common.protocol.proto.CorfuProtocol.Header;
import org.corfudb.common.protocol.proto.CorfuProtocol.Request;
import org.corfudb.common.protocol.proto.CorfuProtocol.Response;
import org.corfudb.common.protocol.proto.CorfuProtocol.ServerError;
import org.corfudb.common.protocol.CorfuExceptions.PeerUnavailable;
import org.corfudb.common.security.sasl.SaslUtils;
import org.corfudb.common.security.sasl.plaintext.PlainTextSaslNettyClient;
import org.corfudb.common.security.tls.SslContextConstructor;

import javax.annotation.Nonnull;
import javax.net.ssl.SSLException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Created by Maithem on 7/1/20.
 */

@Slf4j
public abstract class ChannelHandler extends ResponseHandler {

    protected final InetSocketAddress remoteAddress;

    protected final EventLoopGroup eventLoopGroup;

    protected volatile Channel channel;

    private final ConcurrentLinkedQueue<RequestTime> requestTimeoutQueue = new ConcurrentLinkedQueue<>();

    protected final Map<Long, CompletableFuture> pendingRequests = new ConcurrentHashMap<>();

    private ScheduledFuture<?> timeoutTask;

    protected final AtomicLong idGenerator = new AtomicLong();

    private final ClientConfig config;

    private SslContext sslContext;

    public ChannelHandler(InetSocketAddress remoteAddress, EventLoopGroup eventLoopGroup, ClientConfig clientConfig) {
        this.remoteAddress = remoteAddress;
        this.eventLoopGroup = eventLoopGroup;
        this.config = clientConfig;

        if (this.config.isEnableTls()) {
            // Use optional here
            try {
                sslContext = SslContextConstructor.constructSslContext(false,
                        config.getKeyStore(),
                        config.getKeyStorePasswordFile(),
                        config.getTrustStore(),
                        config.getTrustStorePasswordFile());
            } catch (SSLException e) {
                // TODO(Maithem) replace with an appropriate exception
                //throw new UnrecoverableCorfuError(e);
                throw new RuntimeException(e);
            }
        }

        // On rpcs? dont connect right away
        connect(eventLoopGroup);
    }

    private void connect(EventLoopGroup eventLoopGroup) {
        //TODO(Maithem): Set pooled allocator
        Bootstrap b = new Bootstrap();
        b.group(eventLoopGroup);
        b.channel(config.getSocketType().getChannelClass());
        b.option(ChannelOption.TCP_NODELAY, config.isTcpNoDelay());
        b.option(ChannelOption.SO_REUSEADDR, config.isSoReuseAddress());
        b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, config.getConnectTimeoutInMs());
        b.handler(getChannelInitializer());
    }


    private ChannelInitializer getChannelInitializer() {
        return new ChannelInitializer() {
            @Override
            protected void initChannel(@Nonnull Channel ch) throws Exception {
                ch.pipeline().addLast(new IdleStateHandler(config.getIdleConnectionTimeoutInMs(),
                        config.getKeepAlivePeriodInMs(), 0));
                if (config.isEnableTls()) {
                    ch.pipeline().addLast("ssl", sslContext.newHandler(ch.alloc()));
                }
                ch.pipeline().addLast(new LengthFieldPrepender(4));
                ch.pipeline().addLast(new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE,
                        0, 4, 0,
                        4));
                if (config.isEnableSasl()) {
                    PlainTextSaslNettyClient saslNettyClient =
                            SaslUtils.enableSaslPlainText(config.getSaslUernameFile(),
                                    config.getSaslPasswordFile());
                    ch.pipeline().addLast("sasl/plain-text", saslNettyClient);
                }

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
        checkArgument(request.hasHeader());
        Header header = request.getHeader();
        CompletableFuture<T> retVal = new CompletableFuture<>();
        pendingRequests.put(header.getRequestId(), retVal);
        ByteBuf outBuf = PooledByteBufAllocator.DEFAULT.buffer();
        // TODO(Maithem): remove allocation
        outBuf.writeBytes(request.toByteArray());
        // TODO(Maithem): Handle pipeline errors
        channel.writeAndFlush(outBuf);
        requestTimeoutQueue.add(new RequestTime(System.currentTimeMillis(), header.getRequestId()));
        return retVal;
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
