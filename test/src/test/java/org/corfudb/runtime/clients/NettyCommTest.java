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
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.AbstractCorfuTest;
import org.corfudb.infrastructure.BaseServer;
import org.corfudb.infrastructure.NettyServerRouter;
import org.corfudb.protocols.wireprotocol.NettyCorfuMessageDecoder;
import org.corfudb.protocols.wireprotocol.NettyCorfuMessageEncoder;
import org.corfudb.security.sasl.plaintext.PlainTextSaslNettyServer;
import org.corfudb.security.tls.TlsUtils;
import org.junit.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.ssl.SSLEngine;

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
        runWithBaseServer(
            (port) -> {
                return new NettyServerData(port);
            },
            (port) -> {
                return new NettyClientRouter("localhost", port);
            },
            (r, d) -> {
                assertThat(r.getClient(BaseClient.class).pingSync())
                    .isTrue();
            });
    }

    @Test
    public void nettyServerClientPingableAfterFailure() throws Exception {
        runWithBaseServer(
            (port) -> {
                return new NettyServerData(port);
            },
            (port) -> {
                return new NettyClientRouter("localhost", port);
            },
            (r, d) -> {
                assertThat(r.getClient(BaseClient.class).pingSync())
                        .isTrue();
                d.shutdownServer();
                d.bootstrapServer();

                r.getClient(BaseClient.class).pingSync();
            });
    }

    @Test
    public void nettyTlsNoMutualAuth() throws Exception {
        runWithBaseServer(
            (port) -> {
                NettyServerData d = new NettyServerData(port);
                String[] ciphers = {"TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"};
                String[] protocols = {"TLSv1.2"};
                d.enableTls(
                    "src/test/resources/security/s1.jks",
                    "src/test/resources/security/storepass",
                    "src/test/resources/security/s1.jks",
                    "src/test/resources/security/storepass",
                    false,
                    ciphers,
                    protocols);
                return d;
            },
            (port) -> {
                return new NettyClientRouter("localhost", port,
                    true,
                    "src/test/resources/security/r1.jks",
                    "src/test/resources/security/storepass",
                    "src/test/resources/security/trust1.jks",
                    "src/test/resources/security/storepass",
                    false, null, null);
            },
            (r, d) -> {
                assertThat(r.getClient(BaseClient.class).pingSync())
                    .isTrue();
            });
    }

    @Test
    public void nettyTlsMutualAuth() throws Exception {
        runWithBaseServer(
            (port) -> {
                NettyServerData d = new NettyServerData(port);
                String[] ciphers = {"TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"};
                String[] protocols = {"TLSv1.2"};
                d.enableTls(
                    "src/test/resources/security/s1.jks",
                    "src/test/resources/security/storepass",
                    "src/test/resources/security/trust1.jks",
                    "src/test/resources/security/storepass",
                    true,
                    ciphers,
                    protocols);
                return d;
            },
            (port) -> {
                return new NettyClientRouter("localhost", port,
                    true,
                    "src/test/resources/security/r1.jks",
                    "src/test/resources/security/storepass",
                    "src/test/resources/security/trust1.jks",
                    "src/test/resources/security/storepass",
                    false, null, null);
            },
            (r, d) -> {
                assertThat(r.getClient(BaseClient.class).pingSync())
                    .isTrue();
            });
    }

    @Test
    public void nettyTlsUnknownServer() throws Exception {
        runWithBaseServer(
            (port) -> {
                NettyServerData d = new NettyServerData(port);
                String[] ciphers = {"TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"};
                String[] protocols = {"TLSv1.2"};
                d.enableTls(
                    "src/test/resources/security/s3.jks",
                    "src/test/resources/security/storepass",
                    "src/test/resources/security/trust1.jks",
                    "src/test/resources/security/storepass",
                    true,
                    ciphers,
                    protocols);
                return d;
            },
            (port) -> {
                return new NettyClientRouter("localhost", port,
                    true,
                    "src/test/resources/security/r1.jks",
                    "src/test/resources/security/storepass",
                    "src/test/resources/security/trust2.jks",
                    "src/test/resources/security/storepass",
                    false, null, null);
            },
            (r, d) -> {
                assertThat(r.getClient(BaseClient.class).pingSync())
                    .isFalse();
            });
    }

    @Test
    public void nettyTlsUnknownClient() throws Exception {
        runWithBaseServer(
            (port) -> {
                NettyServerData d = new NettyServerData(port);
                String[] ciphers = {"TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"};
                String[] protocols = {"TLSv1.2"};
                d.enableTls(
                    "src/test/resources/security/s1.jks",
                    "src/test/resources/security/storepass",
                    "src/test/resources/security/trust2.jks",
                    "src/test/resources/security/storepass",
                    true,
                    ciphers,
                    protocols);
                return d;
            },
            (port) -> {
                return new NettyClientRouter("localhost", port,
                    true,
                    "src/test/resources/security/r2.jks",
                    "src/test/resources/security/storepass",
                    "src/test/resources/security/trust1.jks",
                    "src/test/resources/security/storepass",
                    false, null, null);
            },
            (r, d) -> {
                assertThat(r.getClient(BaseClient.class).pingSync())
                    .isFalse();
            });
    }

    @Test
    public void nettyTlsUnknownClientNoMutualAuth() throws Exception {
        runWithBaseServer(
            (port) -> {
                NettyServerData d = new NettyServerData(port);
                String[] ciphers = {"TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"};
                String[] protocols = {"TLSv1.2"};
                d.enableTls(
                    "src/test/resources/security/s1.jks",
                    "src/test/resources/security/storepass",
                    "src/test/resources/security/trust2.jks",
                    "src/test/resources/security/storepass",
                    false,
                    ciphers,
                    protocols);
                return d;
            },
            (port) -> {
                return new NettyClientRouter("localhost", port,
                    true,
                    "src/test/resources/security/r2.jks",
                    "src/test/resources/security/storepass",
                    "src/test/resources/security/trust1.jks",
                    "src/test/resources/security/storepass",
                    false, null, null);
            },
            (r, d) -> {
                assertThat(r.getClient(BaseClient.class).pingSync())
                    .isTrue();
            });
    }

    @Test
    public void nettySasl() throws Exception {
        runWithBaseServer(
            (port) -> {
                System.setProperty("java.security.auth.login.config",
                    "src/test/resources/security/corfudb_jaas.config");
                NettyServerData d = new NettyServerData(port);
                String[] ciphers = {"TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"};
                String[] protocols = {"TLSv1.2"};
                d.enableTls(
                    "src/test/resources/security/s1.jks",
                    "src/test/resources/security/storepass",
                    "src/test/resources/security/trust1.jks",
                    "src/test/resources/security/storepass",
                    true,
                    ciphers,
                    protocols);
                d.enableSaslPlainTextAuth();
                return d;
            },
            (port) -> {
                return new NettyClientRouter("localhost", port,
                    true,
                    "src/test/resources/security/r1.jks",
                    "src/test/resources/security/storepass",
                    "src/test/resources/security/trust1.jks",
                    "src/test/resources/security/storepass",
                    true,
                    "src/test/resources/security/username1",
                    "src/test/resources/security/userpass1");
            },
            (r, d) -> {
                assertThat(r.getClient(BaseClient.class).pingSync())
                    .isTrue();
            });
    }

    @Test
    public void nettySaslWrongPassword() throws Exception {
        runWithBaseServer(
            (port) -> {
                System.setProperty("java.security.auth.login.config",
                    "src/test/resources/security/corfudb_jaas.config");
                NettyServerData d = new NettyServerData(port);
                String[] ciphers = {"TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"};
                String[] protocols = {"TLSv1.2"};
                d.enableTls(
                    "src/test/resources/security/s1.jks",
                    "src/test/resources/security/storepass",
                    "src/test/resources/security/trust1.jks",
                    "src/test/resources/security/storepass",
                    true,
                    ciphers,
                    protocols);
                d.enableSaslPlainTextAuth();
                return d;
            },
            (port) -> {
                return new NettyClientRouter("localhost", port,
                    true,
                    "src/test/resources/security/r1.jks",
                    "src/test/resources/security/storepass",
                    "src/test/resources/security/trust1.jks",
                    "src/test/resources/security/storepass",
                    true,
                    "src/test/resources/security/username1",
                    "src/test/resources/security/userpass2");
            },
            (r, d) -> {
                assertThat(r.getClient(BaseClient.class).pingSync())
                    .isFalse();
            });
    }

    void runWithBaseServer(NettyServerDataConstructor nsdc,
            NettyClientRouterConstructor ncrc, NettyCommFunction actionFn)
            throws Exception {

        NettyServerRouter nsr = new NettyServerRouter(new ImmutableMap.Builder<String, Object>().build());
        nsr.addServer(new BaseServer());
        int port = findRandomOpenPort();

        NettyServerData d = nsdc.createNettyServerData(port);
        NettyClientRouter ncr = null;
        try {
            d.bootstrapServer();
            ncr = ncrc.createNettyClientRouter(port);
            ncr.addClient(new BaseClient());
            ncr.start();
            actionFn.runTest(ncr, d);
        } catch (Exception ex) {
            log.error("Exception ", ex);
            throw ex;
        } finally {
            try {
                if (ncr != null) {ncr.stop();}
            } catch (Exception ex) {
                log.warn("Error shutting down client...", ex);
            }
            d.shutdownServer();
        }

    }

    @FunctionalInterface
    public interface NettyServerDataConstructor {
        NettyServerData createNettyServerData(int port) throws Exception;
    }

    @FunctionalInterface
    public interface NettyClientRouterConstructor {
        NettyClientRouter createNettyClientRouter(int port) throws Exception;
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

        boolean tlsEnabled = false;
        SslContext sslContext;
        boolean tlsMutualAuthEnabled = false;
        String[] enabledTlsCipherSuites;
        String[] enabledTlsProtocols;

        boolean saslPlainTextAuthEnabled = false;

        public NettyServerData(int port) {
            this.port = port;
        }

        public void enableTls(String ksFile, String ksPasswordFile, String tsFile, String tsPasswordFile,
            boolean mutualAuth, String[] ciphers, String[] protocols) throws Exception {
            this.sslContext =
                    TlsUtils.enableTls(TlsUtils.SslContextType.SERVER_CONTEXT,
                            ksFile, e -> {
                                throw new RuntimeException("Could not load keys from the key " +
                                        "store: " + e.getClass().getSimpleName(), e);
                            },
                            ksPasswordFile, e -> {
                                throw new RuntimeException("Could not read the key store " +
                                        "password file: " + e.getClass().getSimpleName(), e);
                            },
                            tsFile, e -> {
                                throw new RuntimeException("Could not load keys from the trust " +
                                        "store: " + e.getClass().getSimpleName(), e);
                            },
                            tsPasswordFile, e -> {
                                throw new RuntimeException("Could not read the trust store " +
                                        "password file: " + e.getClass().getSimpleName(), e);
                            });
            this.tlsMutualAuthEnabled = mutualAuth;
            this.enabledTlsCipherSuites = ciphers;
            this.enabledTlsProtocols = protocols;
            this.tlsEnabled = true;
        }

        public void enableSaslPlainTextAuth() {
            this.saslPlainTextAuthEnabled = true;
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

            final int SO_BACKLOG = 100;
            final int FRAME_SIZE = 4;
            b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .option(ChannelOption.SO_BACKLOG, SO_BACKLOG)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childOption(ChannelOption.SO_REUSEADDR, true)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                    .handler(new LoggingHandler(LogLevel.INFO))
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(io.netty.channel.socket.SocketChannel ch) throws Exception {
                            if (tlsEnabled) {
                                SSLEngine engine = sslContext.newEngine(ch.alloc());
                                engine.setEnabledCipherSuites(enabledTlsCipherSuites);
                                engine.setEnabledProtocols(enabledTlsProtocols);
                                if (tlsMutualAuthEnabled) {
                                    engine.setNeedClientAuth(true);
                                }
                                ch.pipeline().addLast("ssl", new SslHandler(engine));
                            }
                            ch.pipeline().addLast(new LengthFieldPrepender(FRAME_SIZE));
                            ch.pipeline().addLast(new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, FRAME_SIZE, 0, FRAME_SIZE));
                            if (saslPlainTextAuthEnabled) {
                                ch.pipeline().addLast("sasl/plain-text", new PlainTextSaslNettyServer());
                            }
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
