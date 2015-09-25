package org.corfudb.runtime.protocols;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.corfudb.infrastructure.wireprotocol.NettyCorfuMsg;
import org.corfudb.infrastructure.wireprotocol.NettyStreamingServerTokenRequestMsg;
import org.corfudb.util.CFUtils;
import org.corfudb.util.SizeBufferPool;

import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by mwei on 9/16/15.
 */
@Slf4j
@ChannelHandler.Sharable
public abstract class NettyRPCChannelInboundHandlerAdapter extends ChannelInboundHandlerAdapter {

    private volatile UUID clientID;
    private volatile AtomicLong requestID;
    public List<Channel> channelList;
    private ConcurrentHashMap<Long, CompletableFuture<?>> rpcMap;
    private Random random;

    public abstract void handleMessage(NettyCorfuMsg message);


    public NettyRPCChannelInboundHandlerAdapter()
    {
        channelList = new CopyOnWriteArrayList<>();
        clientID = UUID.randomUUID();
        requestID = new AtomicLong();
        rpcMap = new ConcurrentHashMap<>();
        random = new Random();
    }

    public @NonNull
    Channel getChannel()
    {
        Channel c = null;
        while ((c = channelList.get(random.nextInt(channelList.size()))) == null) {
            // this will be null if someone just removed the channel.
            try {
                Thread.sleep(10);
            } catch (InterruptedException ie)
            {
                // retry on interruption
            }
        }
        return c;
    }

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
        Channel c = getChannel();
        c.write(p.writeSize());
        queueFlush(c);
        final CompletableFuture<T> cfTimeout = CFUtils.within(cf, Duration.ofSeconds(600));
        cfTimeout.exceptionally(e -> {
            rpcMap.remove(thisRequest);
            return null;
        });
        return cfTimeout;
    }

    public void sendMessage(SizeBufferPool pool, long epoch, NettyCorfuMsg message)
    {
        final long thisRequest = requestID.getAndIncrement();
        message.setClientID(clientID);
        message.setRequestID(thisRequest);
        message.setEpoch(epoch);
        SizeBufferPool.PooledSizedBuffer p = pool.getSizedBuffer();
        message.serialize(p.getBuffer());
        Channel c = getChannel();
        c.write(p.writeSize());
        queueFlush(c);
    }

    public void queueFlush(Channel channel)
    {
        channel.flush();
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        channelList.add(ctx.channel());
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        super.channelUnregistered(ctx);
        channelList.remove(ctx.channel());
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
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Exception during channel handling.", cause);
        ctx.close();
    }
}
