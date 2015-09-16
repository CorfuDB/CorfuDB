package org.corfudb.runtime.protocols.sequencers;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.corfudb.infrastructure.wireprotocol.NettyStreamingServerMsg;
import org.corfudb.runtime.NetworkException;
import org.corfudb.runtime.protocols.IServerProtocol;
import org.corfudb.util.CFUtils;
import org.corfudb.util.SizeBufferPool;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static io.netty.buffer.Unpooled.directBuffer;

/**
 * Created by mwei on 9/15/15.
 */
@Slf4j
public class NettyStreamingSequencerProtocol implements IServerProtocol, INewStreamSequencer {

    @Getter
    Map<String, String> options;

    @Getter
    String host;

    @Getter
    Integer port;

    @Getter
    @Setter
    long epoch;

    private NettyStreamingSequencerHandler handler;
    private EventLoopGroup workerGroup;
    private SizeBufferPool pool;

    public static String getProtocolString()
    {
        return "nsss";
    }

    public static IServerProtocol protocolFactory(String host, Integer port, Map<String,String> options, Long epoch)
    {
        return new NettyStreamingSequencerProtocol(host, port, options, epoch);
    }

    public NettyStreamingSequencerProtocol(String host, Integer port, Map<String,String> options, long epoch)
    {
        this.host = host;
        this.port = port;
        this.options = options;
        this.epoch = epoch;

        pool = new SizeBufferPool(64);
        workerGroup = new NioEventLoopGroup();

            Bootstrap b = new Bootstrap();
            handler = new NettyStreamingSequencerHandler();
            b.group(workerGroup);
            b.channel(NioSocketChannel.class);
            b.option(ChannelOption.SO_KEEPALIVE, true);
            b.option(ChannelOption.TCP_NODELAY, true);
            b.handler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) throws Exception {
                    ch.pipeline().addLast(new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4));
                    ch.pipeline().addLast(handler);
                }
            });


        if (!b.connect(host, port).awaitUninterruptibly(5000))
        {
            throw new RuntimeException("Couldn't connect to endpoint " + this.getFullString());
        }
    }

    /**
     * Get the next tokens for a particular stream.
     *
     * @param streams   The streams to acquire this token for.
     * @param numTokens The number of tokens to acquire.
     * @return The start of the first token returned.
     */
    @Override
    public CompletableFuture<Long> getNext(Set<UUID> streams, long numTokens) {
        return handler.getToken(streams, numTokens);
    }

    class NettyStreamingSequencerHandler extends ChannelInboundHandlerAdapter {

        private volatile Channel channel;
        private volatile UUID clientID;
        private volatile AtomicLong requestID;
        private ConcurrentHashMap<Long, CompletableFuture<?>> rpcMap;


        //region Handler Interface
        public CompletableFuture<Long> getToken(Set<UUID> streamIDs, long numTokens) {
            final long thisRequest = requestID.getAndIncrement();
            NettyStreamingServerMsg.NettyStreamingServerTokenRequest r =
                    new NettyStreamingServerMsg.NettyStreamingServerTokenRequest
                            (clientID, thisRequest , streamIDs, numTokens);
            final CompletableFuture<Long> cf = new CompletableFuture<>();
            rpcMap.put(thisRequest, cf);
            val p = pool.getSizedBuffer();
            r.serialize(p.getBuffer());
            channel.writeAndFlush(p.writeSize());
            final CompletableFuture<Long> cfTimeout = CFUtils.within(cf, Duration.ofSeconds(5));
            cfTimeout.exceptionally( e -> {
                rpcMap.remove(thisRequest);
                return null;
            });
            return cfTimeout;
        }

        public CompletableFuture<Boolean> ping() {
            final long thisRequest = requestID.getAndIncrement();
            NettyStreamingServerMsg r =
                    new NettyStreamingServerMsg(clientID, thisRequest,
                            NettyStreamingServerMsg.NMStreamingServerMsgType.PING);
            final CompletableFuture<Boolean> cf = new CompletableFuture<>();
            rpcMap.put(thisRequest, cf);
            val p = pool.getSizedBuffer();
            r.serialize(p.getBuffer());
            channel.writeAndFlush(p.writeSize());
            final CompletableFuture<Boolean> cfTimeout = CFUtils.within(cf, Duration.ofSeconds(5));
            cfTimeout.exceptionally( e -> {
                rpcMap.remove(thisRequest);
                return null;
            });
            return cfTimeout;
        }

        //endregion
        //region Netty Handlers
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
                NettyStreamingServerMsg pm = NettyStreamingServerMsg.deserialize(m);
                switch (pm.getMsgType()) {
                    case TOKEN_RES: {
                        Long token = ((NettyStreamingServerMsg.NettyStreamingServerTokenResponse) pm).getToken();
                            ((CompletableFuture<Long>) rpcMap.get(pm.getRequestID()))
                                    .complete(token);
                        rpcMap.remove(pm.getRequestID());
                    }
                        break;
                    case PONG:
                        ((CompletableFuture<Boolean>)rpcMap.get(pm.getRequestID()))
                                .complete(true);
                        rpcMap.remove(pm.getRequestID());
                        break;
                }
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
//endregion
    }

    /**
     * Returns a boolean indicating whether or not the server was reachable.
     *
     * @return True if the server was reachable, false otherwise.
     */
    @Override
    public boolean ping() {
        try {
            return handler.ping().get();
        } catch (Exception e)
        {
            return false;
        }
    }

    /**
     * Resets the server. Used by the configuration master to reset the state of the server.
     * Should eliminate ALL hard state!
     *
     * @param epoch
     */
    @Override
    public void reset(long epoch) throws NetworkException {

    }
}
