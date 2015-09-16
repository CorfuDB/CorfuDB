package org.corfudb.runtime.protocols;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.corfudb.infrastructure.wireprotocol.NettyCorfuMsg;
import org.corfudb.infrastructure.wireprotocol.NettyStreamingServerTokenRequestMsg;
import org.corfudb.util.CFUtils;
import org.corfudb.util.SizeBufferPool;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by mwei on 9/16/15.
 */
@Slf4j
public abstract class NettyRPCChannelInboundHandlerAdapter extends ChannelInboundHandlerAdapter {

    private volatile Channel channel;
    private volatile UUID clientID;
    private volatile AtomicLong requestID;
    private ConcurrentHashMap<Long, CompletableFuture<?>> rpcMap;

    public abstract void handleMessage(NettyCorfuMsg message);

    @SuppressWarnings("unchecked")
    public <T> void completeRequest(long requestID, T result)
    {
        CompletableFuture<T> cf = (CompletableFuture<T>) rpcMap.get(requestID);
        if (cf != null) {
            cf.complete(result);
        }
    }

    public <T> CompletableFuture<T> sendMessageAndGetCompletable(SizeBufferPool pool, long epoch, NettyCorfuMsg message)
    {
        final long thisRequest = requestID.getAndIncrement();
        message.setClientID(clientID);
        message.setRequestID(thisRequest);
        message.setEpoch(epoch);
        final CompletableFuture<T> cf = new CompletableFuture<>();
        rpcMap.put(thisRequest, cf);
        SizeBufferPool.PooledSizedBuffer p = pool.getSizedBuffer();
        message.serialize(p.getBuffer());
        channel.writeAndFlush(p.writeSize());
        final CompletableFuture<T> cfTimeout = CFUtils.within(cf, Duration.ofSeconds(5));
        cfTimeout.exceptionally(e -> {
            rpcMap.remove(thisRequest);
            return null;
        });
        return cfTimeout;
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        channel = ctx.channel();
        clientID = UUID.randomUUID();
        requestID = new AtomicLong();
        rpcMap = new ConcurrentHashMap<>();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
    }

    @Override
    @SuppressWarnings("unchecked")
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf m = (ByteBuf) msg;
        try {
            NettyCorfuMsg pm = NettyCorfuMsg.deserialize(m);
            handleMessage(pm);
        }
        finally {
            m.release();
        }
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Exception during channel handling.", cause);
        ctx.close();
    }
}
