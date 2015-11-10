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
import org.corfudb.runtime.exceptions.NetworkException;

import java.util.Map;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by mwei on 10/1/15.
 *
 * The abstract AbstractNettyProtocol provides a framework for client to interact with {@link org.corfudb.infrastructure.AbstractNettyServer} servers.
 *
 * The framework sets up a threa pool to handle outgoing requests and incoming messages using NioEvenLooopGroup.
 *
 * Each specific client protocol must instantiate this class with a response-handler class that extends NettyRPCChannelInboundHandlerAdapter.
 * A repsonse handler of the appropriate type needs to be supplied as parameter to the constructor.
 * The handler class needs to override the method handleMessage(), which gets an incoming response as inputs.
 *
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

    static final EventLoopGroup workerGroup = new NioEventLoopGroup(Runtime.getRuntime().availableProcessors() * 2, new ThreadFactory() {
        final AtomicInteger threadNum = new AtomicInteger(0);

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setName(this.getClass().getName() + "-worker-" + threadNum.getAndIncrement());
            t.setDaemon(true);
            return t;
        }
    });

    static final EventExecutorGroup ee = new DefaultEventExecutorGroup(Runtime.getRuntime().availableProcessors() * 2, new ThreadFactory() {

        final AtomicInteger threadNum = new AtomicInteger(0);

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setName(this.getClass().getName() + "-event-" + threadNum.getAndIncrement());
            t.setDaemon(true);
            return t;
        }
    });


    public AbstractNettyProtocol(String host, Integer port, Map<String,String> options, long epoch, T handler)
    {
        this.host = host;
        this.port = port;
        this.options = options;
        this.epoch = epoch;
        this.handler = handler;

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


    public static String getProtocolString()
    {
        return "anp";
    }

    public static IServerProtocol protocolFactory(String host, Integer port, Map<String,String> options, Long epoch)
    {
        throw new RuntimeException("This is an abstract class!");
    }

}
