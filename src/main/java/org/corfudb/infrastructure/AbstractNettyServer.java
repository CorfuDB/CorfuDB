package org.corfudb.infrastructure;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.corfudb.infrastructure.wireprotocol.NettyCorfuMsg;
import org.corfudb.infrastructure.wireprotocol.NettyStreamingServerTokenRequestMsg;
import org.corfudb.infrastructure.wireprotocol.NettyStreamingServerTokenResponseMsg;
import org.corfudb.util.SizeBufferPool;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.IntervalAndSentinelRetry;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by mwei on 9/16/15.
 */
@Slf4j
public abstract class AbstractNettyServer implements ICorfuDBServer {
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
     * The name of this server. Used for generating threads, responding to version info requests, etc.
     */
    @Getter @Setter
    String serverName = "AbstractNettyServer";

    /**
     * The current epoch.
     */
    Long epoch;

    EventLoopGroup bossGroup;
    EventLoopGroup workerGroup;
    SizeBufferPool pool;

    @Override
    public ICorfuDBServer getInstance(Map<String, Object> configuration) {
        baseParseConfiguration(configuration);
        thread = new Thread(this);
        return this;
    }

    @Override
    public void close() {
        running.set(false);
        thread.interrupt();
        try {thread.join();}
        catch (InterruptedException ie) {
            //maybe join uninterruptedly?
        }
    }


    abstract void parseConfiguration(Map<String, Object> configuration);

    void baseParseConfiguration(Map<String, Object> configuration)
    {
        if ((port = (Integer) configuration.get("port")) == null)
        {
            log.error("Required key port is missing from configuration!");
            throw new RuntimeException("Invalid configuration provided!");
        }

        pool = new SizeBufferPool(512);
        epoch = 0L;
        parseConfiguration(configuration);
    }

    /** Process an incoming message
     *
     * @param msg   The message to process.
     * @param ctx   The channel context from the handler adapter.
     */
    abstract void processMessage(NettyCorfuMsg msg, ChannelHandlerContext ctx);

    void sendResponse(NettyCorfuMsg outMsg, NettyCorfuMsg inMsg, ChannelHandlerContext ctx)
    {
        outMsg.copyBaseFields(inMsg);
        outMsg.setEpoch(epoch);
        val p = pool.getSizedBuffer();
        outMsg.serialize(p.getBuffer());
        ctx.writeAndFlush(p.writeSize());
    }

    @ChannelHandler.Sharable
    public class NettyServerHandler extends ChannelInboundHandlerAdapter {
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            try {
                NettyCorfuMsg m = NettyCorfuMsg.deserialize((ByteBuf) msg);
                if (validateEpoch(m, ctx)) {
                 processBaseMessage(m, ctx);
                }
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
        bossGroup = new NioEventLoopGroup(1, new ThreadFactory() {
            final AtomicInteger threadNum = new AtomicInteger(0);
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setName(serverName + "-accept-" +threadNum.getAndIncrement());
                return t;
            }
        });
        workerGroup = new NioEventLoopGroup(Runtime.getRuntime().availableProcessors() * 2, new ThreadFactory() {

            final AtomicInteger threadNum = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setName(serverName + "-io-" + threadNum.getAndIncrement());
                return t;
            }
        });

        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .option(ChannelOption.SO_BACKLOG, 100)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .handler(new LoggingHandler(LogLevel.INFO))
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(io.netty.channel.socket.SocketChannel ch) throws Exception {
                            ch.pipeline().addLast(new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4));
                            ch.pipeline().addLast(new NettyServerHandler());
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

    /** Process a base message, and call a specialized handler if this message is not a base message.
     * @param msg   The message to process.
     * @param ctx   The channel handler context to return a message with.
     */
    public void processBaseMessage(NettyCorfuMsg msg, ChannelHandlerContext ctx)
    {
        switch (msg.getMsgType())
        {
            case PING: {
                NettyCorfuMsg resp = new NettyCorfuMsg(msg.getClientID(), msg.getRequestID(),
                        epoch, NettyCorfuMsg.NettyCorfuMsgType.PONG);
                sendResponse(resp, msg, ctx);
            }
            break;
            case RESET: {
                log.info("Request requested by client ", msg.getClientID());
                reset();
            }
            break;
            default:
                processMessage(msg, ctx);
        }
    }

    /** Reset the state of the server. */
    public abstract void reset();

    /** Validate the epoch of a NettyCorfuMsg, and send a WRONG_EPOCH response if
     * the server is in the wrong epoch. Ignored if the message type is reset (which
     * is valid in any epoch).
     * @param msg   The incoming message to validate.
     * @param ctx   The context of the channel handler.
     * @return      True, if the epoch is correct, but false otherwise.
     */
    public boolean validateEpoch(NettyCorfuMsg msg, ChannelHandlerContext ctx)
    {
        if (msg.getMsgType() != NettyCorfuMsg.NettyCorfuMsgType.RESET && msg.getEpoch() != epoch)
        {
            NettyCorfuMsg m = new NettyCorfuMsg();
            m.setMsgType(NettyCorfuMsg.NettyCorfuMsgType.WRONG_EPOCH);
            sendResponse(m, msg, ctx);
            return false;
        }
        return true;
    }

    /**
     * Starts the netty server as an IRetry.
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
