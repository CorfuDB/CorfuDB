package org.corfudb.runtime.clients;

import com.google.common.collect.ImmutableMap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.AbstractCorfuTest;
import org.corfudb.infrastructure.BaseServer;
import org.corfudb.infrastructure.NettyServerRouter;
import org.corfudb.protocols.wireprotocol.NettyCorfuMessageDecoder;
import org.corfudb.protocols.wireprotocol.NettyCorfuMessageEncoder;
import org.junit.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 3/28/16.
 */
@Slf4j
public class NettyCommTest extends AbstractCorfuTest {


    private Integer findRandomOpenPort() throws IOException {
        try (
                ServerSocket socket = new ServerSocket(0);
        ) {
            return socket.getLocalPort();
        }
    }

    @Test
    public void nettyServerClientPingable() throws Exception {
        runWithBaseServer((r, d) -> {
            assertThat(r.getClient(BaseClient.class).pingSync())
                    .isTrue();
        });
    }

    @Test
    public void nettyServerClientPingableAfterFailure() throws Exception {
        runWithBaseServer((r, d) -> {
            assertThat(r.getClient(BaseClient.class).pingSync())
                    .isTrue();
            d.shutdownServer();
            d.bootstrapServer();
            // We are now racing with the server's startup.  Immediate attempts
            // to ping the server will fail immediately due to client-side
            // rejection because the TCP session isn't yet connected.  Retry
            // for up to 60 seconds before giving up: TravisCI can be truly
            // unpredictably slow.
            int sleep_incr = 10;
            for (int i = 0; i < 60000; i += sleep_incr) {
                if (r.getClient(BaseClient.class).pingSync() == true)
                    break;
                Thread.sleep(sleep_incr);
            }
            assertThat(r.getClient(BaseClient.class).pingSync())
                    .isTrue();
        });
    }

    void runWithBaseServer(NettyCommFunction actionFn)
            throws Exception {

        NettyServerRouter nsr = new NettyServerRouter(new ImmutableMap.Builder<String, Object>().build());
        nsr.addServer(new BaseServer());
        int port = findRandomOpenPort();

        NettyServerData d = new NettyServerData(port);
        NettyClientRouter ncr = new NettyClientRouter("localhost", port);
        try {
            d.bootstrapServer();
            ncr.addClient(new BaseClient());
            ncr.start();
            actionFn.runTest(ncr, d);
        } catch (Exception ex) {
            log.error("Exception ", ex);
            throw ex;
        } finally {
            try {
                ncr.stop();
            } catch (Exception ex) {
                log.warn("Error shutting down client...", ex);
            }
            d.shutdownServer();
        }

    }

    @FunctionalInterface
    public interface NettyCommFunction {
        void runTest(NettyClientRouter r, NettyServerData d) throws Exception;
    }

    @Data
    public class NettyServerData {
        ServerBootstrap b;
        ChannelFuture f;
        int port;
        EventLoopGroup bossGroup;
        EventLoopGroup workerGroup;
        EventExecutorGroup ee;
        public NettyServerData(int port) {
            this.port = port;
        }

        void bootstrapServer() throws Exception {
            NettyServerRouter nsr = new NettyServerRouter(new ImmutableMap.Builder<String, Object>().build());
            bossGroup = new NioEventLoopGroup(1, new ThreadFactory() {
                final AtomicInteger threadNum = new AtomicInteger(0);

                @Override
                public Thread newThread(Runnable r) {
                    Thread t = new Thread(r);
                    t.setName("accept-" + threadNum.getAndIncrement());
                    return t;
                }
            });

            workerGroup = new NioEventLoopGroup(Runtime.getRuntime().availableProcessors() * 2, new ThreadFactory() {
                final AtomicInteger threadNum = new AtomicInteger(0);

                @Override
                public Thread newThread(Runnable r) {
                    Thread t = new Thread(r);
                    t.setName("io-" + threadNum.getAndIncrement());
                    return t;
                }
            });

            ee = new DefaultEventExecutorGroup(Runtime.getRuntime().availableProcessors() * 2, new ThreadFactory() {

                final AtomicInteger threadNum = new AtomicInteger(0);

                @Override
                public Thread newThread(Runnable r) {
                    Thread t = new Thread(r);
                    t.setName("event-" + threadNum.getAndIncrement());
                    return t;
                }
            });

            b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .option(ChannelOption.SO_BACKLOG, 100)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childOption(ChannelOption.SO_REUSEADDR, true)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                    .handler(new LoggingHandler(LogLevel.INFO))
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(io.netty.channel.socket.SocketChannel ch) throws Exception {
                            ch.pipeline().addLast(new LengthFieldPrepender(4));
                            ch.pipeline().addLast(new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4));
                            ch.pipeline().addLast(ee, new NettyCorfuMessageDecoder());
                            ch.pipeline().addLast(ee, new NettyCorfuMessageEncoder());
                            ch.pipeline().addLast(ee, nsr);
                        }
                    });
            f = b.bind(port).sync();
        }

        public void shutdownServer() {
            f.channel().close().awaitUninterruptibly();
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }

    }
}
