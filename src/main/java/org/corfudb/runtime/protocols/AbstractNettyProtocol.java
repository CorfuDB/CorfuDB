package org.corfudb.runtime.protocols;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.wireprotocol.NettyCorfuMessageDecoder;
import org.corfudb.infrastructure.wireprotocol.NettyCorfuMessageEncoder;
import org.corfudb.runtime.NetworkException;

import java.util.Map;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by mwei on 10/1/15.
 */
@Slf4j
public abstract class AbstractNettyProtocol<T extends NettyRPCChannelInboundHandlerAdapter> implements IServerProtocol {

    @Getter
    Map<String, String> options;

    @Getter
    String host;

    @Getter
    Integer port;

    @Getter
    @Setter
    public long epoch;

    public T handler;
    EventLoopGroup workerGroup;
    EventExecutorGroup ee;


    public AbstractNettyProtocol(String host, Integer port, Map<String,String> options, long epoch, T handler)
    {
        this.host = host;
        this.port = port;
        this.options = options;
        this.epoch = epoch;
        handler.setProtocol(this);
        this.handler = handler;

        workerGroup = new NioEventLoopGroup(Runtime.getRuntime().availableProcessors() * 2, new ThreadFactory() {
            final AtomicInteger threadNum = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setName(this.getClass().getName() + "-worker-" + threadNum.getAndIncrement());
                return t;
            }
        });

        ee = new DefaultEventExecutorGroup(Runtime.getRuntime().availableProcessors() * 2, new ThreadFactory() {

            final AtomicInteger threadNum = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setName(this.getClass().getName() + "-event-" + threadNum.getAndIncrement());
                return t;
            }
        });

        Bootstrap b = new Bootstrap();
        b.group(workerGroup);
        b.channel(NioSocketChannel.class);
        b.option(ChannelOption.SO_KEEPALIVE, true);
        b.option(ChannelOption.TCP_NODELAY, true);
        b.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) throws Exception {
                ch.pipeline().addLast(new LengthFieldPrepender(4));
                ch.pipeline().addLast(new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4));
                ch.pipeline().addLast(ee, new NettyCorfuMessageDecoder());
                ch.pipeline().addLast(ee, new NettyCorfuMessageEncoder());
                ch.pipeline().addLast(ee, handler);
            }
        });

        if (!b.connect(host, port).awaitUninterruptibly(5000)) {
            throw new RuntimeException("Couldn't connect to endpoint " + this.getFullString());
        }
    }

    /**
     * Returns a boolean indicating whether or not the server was reachable.
     *
     * @return True if the server was reachable, false otherwise.
     */
    @Override
    public boolean ping() {
        try {
            return handler.ping(epoch).get();
        } catch (Exception e)
        {
            log.error("Exception occurred during ping", e);
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
        handler.reset(epoch);
    }
}
