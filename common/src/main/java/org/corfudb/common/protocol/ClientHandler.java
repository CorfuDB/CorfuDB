package org.corfudb.common.protocol;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoopGroup;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.protocol.proto.CorfuProtocol.Response;
import org.corfudb.common.protocol.CorfuExceptions.PeerUnavailable;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by Maithem on 7/1/20.
 */

@Slf4j
public abstract class ClientHandler extends ResponseHandler {

    protected final InetSocketAddress remoteAddress;

    protected final EventLoopGroup eventLoopGroup;

    protected volatile Channel channel;

    private final ConcurrentLinkedQueue<RequestTime> requestTimeoutQueue = new ConcurrentLinkedQueue<>();

    protected final Map<Long, CompletableFuture<Response>> pendingRequests = new ConcurrentHashMap<>();

    private final long requestTimeoutInMs;

    private ScheduledFuture<?> timeoutTask;

    protected final AtomicLong idGenerator = new AtomicLong();

    public ClientHandler(InetSocketAddress remoteAddress, EventLoopGroup eventLoopGroup, long requestTimeoutInMs) {
        this.remoteAddress = remoteAddress;
        this.eventLoopGroup = eventLoopGroup;
        this.requestTimeoutInMs = requestTimeoutInMs;
    }

    protected long getRequestId() {
        return idGenerator.incrementAndGet();
    }

    //TODO(Maithem) Add keep-alive logic here

    private void checkRequestTimeout() {
        while (!requestTimeoutQueue.isEmpty()) {
            RequestTime request = requestTimeoutQueue.peek();
            if (request == null || (System.currentTimeMillis() - request.creationTimeMs) < requestTimeoutInMs) {
                // if there is no request that is timed out then exit the loop
                break;
            }
            request = requestTimeoutQueue.poll();
            CompletableFuture<Response> requestFuture = pendingRequests.remove(request.requestId);
            if (requestFuture != null && !requestFuture.isDone()) {
                requestFuture.completeExceptionally(new PeerUnavailable(remoteAddress));
            } else {
                // request is already completed successfully.
            }
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        this.timeoutTask = this.eventLoopGroup.scheduleAtFixedRate(() -> checkRequestTimeout(), requestTimeoutInMs,
                requestTimeoutInMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        log.info("[{}] PeerUnavailable", remoteAddress);

        PeerUnavailable exception = new PeerUnavailable(remoteAddress);

        pendingRequests.forEach((requestId, resultCf) -> resultCf.completeExceptionally(exception));
        pendingRequests.clear();
        timeoutTask.cancel(true);
    }

    protected void completeRequest(long requestId, Response result) {
        CompletableFuture<Response> cf = pendingRequests.remove(requestId);
        if (cf == null || cf.isDone()) {
            log.debug("[{}] failed to complete request {}", remoteAddress, requestId);
        }
        cf.complete(result);
    }

    @Data
    class RequestTime {
        long creationTimeMs;
        long requestId;
    }
}
