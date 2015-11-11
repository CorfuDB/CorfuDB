package org.corfudb.runtime.protocols;

import io.netty.channel.*;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.wireprotocol.NettyCorfuMsg;
import org.corfudb.infrastructure.wireprotocol.NettyCorfuResetMsg;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.util.CFUtils;

import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by mwei on 9/16/15.
 */
@Slf4j
@ChannelHandler.Sharable
public abstract class NettyRPCChannelInboundHandlerAdapter extends ChannelInboundHandlerAdapter {

    private volatile UUID clientID;
    private volatile AtomicLong requestID;
    public List<ChannelHandlerContext> channelList;
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
    ChannelHandlerContext getChannel()
    {
        ChannelHandlerContext c = null;
        while (channelList.size() == 0 || (c = channelList.get(random.nextInt(channelList.size()))) == null) {
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

    /**
     * Fail the completable future with the given exception.
     * @param requestID     The request ID which has failed.
     * @param e             The exception to pass to the completable future.
     * @param <T>           The type of the request to fail.
     */
    @SuppressWarnings("unchecked")
    public <T> void failRequest(long requestID, Exception e)
    {
        CompletableFuture<T> cf = (CompletableFuture<T>) rpcMap.get(requestID);
        if (cf != null) {
            cf.completeExceptionally(e);
        }
    }

    public <T> CompletableFuture<T> sendMessageAndGetCompletable(long epoch, NettyCorfuMsg message)
    {
        final long thisRequest = requestID.getAndIncrement();
        message.setClientID(clientID);
        message.setRequestID(thisRequest);
        message.setEpoch(epoch);
        final CompletableFuture<T> cf = new CompletableFuture<>();
        rpcMap.put(thisRequest, cf);
        getChannel().writeAndFlush(message);
        final CompletableFuture<T> cfTimeout = CFUtils.within(cf, Duration.ofSeconds(500));
        cfTimeout.exceptionally(e -> {
            rpcMap.remove(thisRequest);
            return null;
        });
        return cfTimeout;
    }

    public void sendMessage(long epoch, NettyCorfuMsg message)
    {
        final long thisRequest = requestID.getAndIncrement();
        message.setClientID(clientID);
        message.setRequestID(thisRequest);
        message.setEpoch(epoch);
        getChannel().writeAndFlush(message);
    }

    public CompletableFuture<Boolean> ping(long epoch) {
        NettyCorfuMsg r =
                new NettyCorfuMsg();
        r.setMsgType(NettyCorfuMsg.NettyCorfuMsgType.PING);
        return sendMessageAndGetCompletable(epoch, r);
    }

    public void reset(long newEpoch) {
        NettyCorfuMsg r =
                new NettyCorfuResetMsg(newEpoch);
        sendMessage(0L, r);
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        channelList.add(ctx);
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        super.channelUnregistered(ctx);
        channelList.remove(ctx);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
    }

    @Override
    @SuppressWarnings("unchecked")
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        NettyCorfuMsg nettyMsg = (NettyCorfuMsg) msg;
        // Handle the case where the epoch is wrong.
        if (nettyMsg.getMsgType() == NettyCorfuMsg.NettyCorfuMsgType.WRONG_EPOCH)
        {
            failRequest(nettyMsg.getRequestID(),
                    new WrongEpochException(nettyMsg.getEpoch()));
        }
        else {
            handleMessage((NettyCorfuMsg) msg);
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
