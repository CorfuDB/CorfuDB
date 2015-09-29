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
import org.corfudb.infrastructure.wireprotocol.NettyCorfuMsg;
import org.corfudb.infrastructure.wireprotocol.NettyStreamingServerTokenRequestMsg;
import org.corfudb.infrastructure.wireprotocol.NettyStreamingServerTokenResponseMsg;
import org.corfudb.runtime.NetworkException;
import org.corfudb.runtime.protocols.IServerProtocol;
import org.corfudb.runtime.protocols.NettyRPCChannelInboundHandlerAdapter;
import org.corfudb.util.CFUtils;
import org.corfudb.util.SizeBufferPool;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
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
        workerGroup = new NioEventLoopGroup(Runtime.getRuntime().availableProcessors() * 2, new ThreadFactory() {

            final AtomicInteger threadNum = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setName("NettyStreamingSequencerProtocol-worker-" + threadNum.getAndIncrement());
                return t;
            }
        });

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

           // for (int i = 0; i < 2; i++) {
                if (!b.connect(host, port).awaitUninterruptibly(5000)) {
                    throw new RuntimeException("Couldn't connect to endpoint " + this.getFullString());
                }
        //    }
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

    class NettyStreamingSequencerHandler extends NettyRPCChannelInboundHandlerAdapter {

        //region Handler Interface

        @Override
        public void handleMessage(NettyCorfuMsg message)
        {
            switch (message.getMsgType())
            {
                case PONG:
                    completeRequest(message.getRequestID(), true);
                    break;
                case TOKEN_RES:
                    completeRequest(message.getRequestID(), ((NettyStreamingServerTokenResponseMsg)message).getToken());
                    break;
            }
        }

        public CompletableFuture<Long> getToken(Set<UUID> streamIDs, long numTokens) {
            NettyStreamingServerTokenRequestMsg r =
                    new NettyStreamingServerTokenRequestMsg
                            (streamIDs, numTokens);
            return sendMessageAndGetCompletable(pool, epoch, r);
        }

        public CompletableFuture<Boolean> ping() {
            NettyCorfuMsg r =
                    new NettyCorfuMsg();
            r.setMsgType(NettyCorfuMsg.NettyCorfuMsgType.PING);
            return sendMessageAndGetCompletable(pool, epoch, r);
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
