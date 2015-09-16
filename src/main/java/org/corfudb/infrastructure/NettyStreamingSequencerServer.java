package org.corfudb.infrastructure;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.corfudb.infrastructure.wireprotocol.NettyStreamingServerMsg;
import org.corfudb.util.SizeBufferPool;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.IntervalAndSentinelRetry;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static io.netty.buffer.Unpooled.directBuffer;

/**
 * A streaming sequencer which uses the netty library.
 * Created by mwei on 9/10/15.
 */
@Slf4j
public class NettyStreamingSequencerServer implements ICorfuDBServer {

    /**
     * The main thread for this server.
     */
    @Getter
    Thread thread;

    /**
     * True, if the server is running, false otherwise.
     */
    final AtomicBoolean running = new AtomicBoolean(false);

    /**
     * The port this server is running on.
     */
    @Getter
    Integer port;

    /**
     * A simple map of the most recently issued token for any given stream.
     */
    ConcurrentHashMap<UUID, Long> lastIssuedMap;

    /**
     * The current global index.
     */
    AtomicLong globalIndex;

    EventLoopGroup bossGroup;
    EventLoopGroup workerGroup;
    SizeBufferPool pool;

    @Override
    public ICorfuDBServer getInstance(Map<String, Object> configuration) {
        parseConfiguration(configuration);
        thread = new Thread(this);
        return this;
    }

    @Override
    public void close() {
        running.set(false);
        thread.interrupt();
        try {thread.join();}
        catch (InterruptedException ie) {}
    }

    private void parseConfiguration(Map<String, Object> configuration)
    {
        if ((port = (Integer) configuration.get("port")) == null)
        {
            log.error("Required key port is missing from configuration!");
            throw new RuntimeException("Invalid configuration provided!");
        }

        pool = new SizeBufferPool(64);
        globalIndex = new AtomicLong(0);
        lastIssuedMap = new ConcurrentHashMap<>();
    }

    /** Process an incoming message
     *
     * @param msg   The message to process.
     * @param r     Where to send the response.
     */
    private ByteBuf processMessage(NettyStreamingServerMsg msg)
    {
        switch (msg.getMsgType())
        {
            case TOKEN_REQ: {
                NettyStreamingServerMsg.NettyStreamingServerTokenRequest req = (NettyStreamingServerMsg.NettyStreamingServerTokenRequest) msg;
                long thisIssue = globalIndex.getAndAdd(req.getNumTokens());
                for (UUID id : req.getStreamIDs()) {
                    lastIssuedMap.compute(id, (k, v) -> v == null ? thisIssue : Math.max(thisIssue, v));
                }
                NettyStreamingServerMsg.NettyStreamingServerTokenResponse resp = new NettyStreamingServerMsg.NettyStreamingServerTokenResponse(
                        msg.getClientID(),
                        msg.getRequestID(),
                        thisIssue
                );
                val p = pool.getSizedBuffer();
                resp.serialize(p.getBuffer());
                return p.writeSize();
            }
            case PING: {
                NettyStreamingServerMsg resp = new NettyStreamingServerMsg(msg.getClientID(), msg.getRequestID(),
                        NettyStreamingServerMsg.NMStreamingServerMsgType.PONG);
                val p = pool.getSizedBuffer();
                resp.serialize(p.getBuffer());
                return p.writeSize();
            }
            default:
                log.warn("Unknown message type {} passed to handler!", msg.getMsgType());
                throw new RuntimeException("Unsupported message passed to handler!");
        }
    }

    public class NettyStreamingSequencerServerHandler extends ChannelInboundHandlerAdapter {
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            try {
                ctx.writeAndFlush(processMessage(NettyStreamingServerMsg.deserialize((ByteBuf) msg)));
            }
            catch (Exception e)
            {
                log.error("Exception during read!" , e);
            }
            ((ByteBuf) msg).release();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            log.error("Error in handling inbound message, {}", cause);
            ctx.close();
        }
    }

    /**
     * Serve sequence numbers.
     *
     * @return always True.
     */
    private Boolean serve()
    {
        log.info("{} starting on TCP port {}", this.getClass().getName(), port);

        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();

        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .option(ChannelOption.SO_BACKLOG, 100)
                    .handler(new LoggingHandler(LogLevel.INFO))
                    .childHandler(new ChannelInitializer<io.netty.channel.socket.SocketChannel>() {
                        @Override
                        public void initChannel(io.netty.channel.socket.SocketChannel ch) throws Exception {
                            ch.pipeline().addLast(new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4));
                            ch.pipeline().addLast(new NettyStreamingSequencerServerHandler());
                        }
                    });
            ChannelFuture f = b.bind(port).sync();
            while (running.get())
            {
                try {
                    f.channel().closeFuture().sync();
                } catch (InterruptedException ie)
                {}
            }

        }
        catch (InterruptedException ie)
        {

        }
        finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
        return true;
    }

    /**
     * Starts the streaming sequencer server as an IRetry.
     */
    @Override
    public void run() {
        running.set(true);
        IRetry.build(IntervalAndSentinelRetry.class, this::serve)
                .setOptions(x -> x.setRetryInterval(1000))
                .setOptions(x -> x.setSentinelReference(running))
                .runForever();
    }
}
