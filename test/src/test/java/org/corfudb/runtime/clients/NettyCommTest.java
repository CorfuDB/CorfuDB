package org.corfudb.runtime.clients;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.SslHandler;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.AbstractCorfuTest;
import org.corfudb.common.config.ConfigParamsHelper;
import org.corfudb.infrastructure.BaseServer;
import org.corfudb.infrastructure.CorfuServerNode;
import org.corfudb.infrastructure.NettyServerRouter;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.ServerContextBuilder;
import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters;
import org.corfudb.runtime.clients.NettyCommTestUtil.CertificateManager;
import org.corfudb.security.tls.TlsUtils.CertStoreConfig.CertManagementConfig;
import org.corfudb.util.NodeLocator;
import org.junit.Test;

import javax.annotation.Nonnull;
import javax.net.ssl.SSLSession;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.corfudb.common.config.ConfigParamsHelper.TlsCiphers.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384;
import static org.corfudb.common.config.ConfigParamsHelper.TlsCiphers.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256;

@Slf4j
public class NettyCommTest extends AbstractCorfuTest {

    private enum KeyStoreType {
        RSA,
        ECDSA,
        RSA_ECDSA,
        RSA_RSA,
        ECDSA_ECDSA
    }

    private Integer findRandomOpenPort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private BaseClient getBaseClient(IClientRouter router) {
        return new BaseClient(router, 0L, UUID.fromString("00000000-0000-0000-0000-000000000000"));
    }

    @Test
    public void nettyServerClientPingable() throws Exception {
        runWithBaseServer(
                (port) -> new NettyServerData(ServerContextBuilder.defaultContext(port)),
                (port) -> new NettyClientRouter("localhost", port),
                (r, d) -> assertThat(getBaseClient(r).pingSync()).isTrue());
    }

    @Test
    public void nettyServerClientPingableAfterFailure() throws Exception {
        runWithBaseServer(
                (port) -> new NettyServerData(ServerContextBuilder.defaultContext(port)),
                (port) -> new NettyClientRouter("localhost", port),
                (r, d) -> {
                    assertThat(getBaseClient(r).pingSync()).isTrue();
                    d.shutdownServer();
                    d.bootstrapServer();

                    getBaseClient(r).pingSync();
                });
    }

    @Test
    public void nettyTlsNoMutualAuth() throws Exception {
        runWithBaseServer(
                (port) ->
                        new NettyServerData(
                                new ServerContextBuilder()
                                        .setTlsEnabled(true)
                                        .setImplementation("auto")
                                        .setTlsCiphers(ConfigParamsHelper.getTlsCiphersCSV())
                                        .setTlsProtocols("TLSv1.2")
                                        .setKeystore("src/test/resources/security/s1.jks")
                                        .setKeystorePasswordFile("src/test/resources/security/storepass")
                                        .setTruststore("src/test/resources/security/s1.jks")
                                        .setTruststorePasswordFile("src/test/resources/security/storepass")
                                        .setPort(port)
                                        .build()),
                (port) ->
                        new NettyClientRouter(
                                NodeLocator.builder().host("localhost").port(port).build(),
                                CorfuRuntimeParameters.builder()
                                        .tlsEnabled(true)
                                        .keyStore("src/test/resources/security/r1.jks")
                                        .ksPasswordFile("src/test/resources/security/storepass")
                                        .trustStore("src/test/resources/security/trust1.jks")
                                        .tsPasswordFile("src/test/resources/security/storepass")
                                        .build()),
                checkPing());
    }

    @Test
    public void nettyTlsMutualAuth() throws Exception {
        runWithBaseServer(
                (port) ->
                        new NettyServerData(
                                new ServerContextBuilder()
                                        .setImplementation("auto")
                                        .setTlsEnabled(true)
                                        .setTlsCiphers(ConfigParamsHelper.getTlsCiphersCSV())
                                        .setTlsProtocols("TLSv1.2")
                                        .setKeystore("src/test/resources/security/s1.jks")
                                        .setKeystorePasswordFile("src/test/resources/security/storepass")
                                        .setTruststore("src/test/resources/security/trust1.jks")
                                        .setTruststorePasswordFile("src/test/resources/security/storepass")
                                        .setTlsMutualAuthEnabled(true)
                                        .setPort(port)
                                        .build()),
                (port) ->
                        new NettyClientRouter(
                                NodeLocator.builder().host("localhost").port(port).build(),
                                CorfuRuntimeParameters.builder()
                                        .tlsEnabled(true)
                                        .keyStore("src/test/resources/security/r1.jks")
                                        .ksPasswordFile("src/test/resources/security/storepass")
                                        .trustStore("src/test/resources/security/trust1.jks")
                                        .tsPasswordFile("src/test/resources/security/storepass")
                                        .build()),
                checkPing());
    }

    private NettyCommFunction checkPing() {
        return (r, d) -> {
            for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_VERY_LOW; i++) {
                boolean ping = getBaseClient(r).pingSync();
                if (ping) {
                    return;
                }
                TimeUnit.MILLISECONDS.sleep(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());
            }
            fail("Broken connection");
        };
    }

    @Test
    public void nettyTlsUnknownServer() throws Exception {
        runWithBaseServer(
                (port) ->
                        new NettyServerData(
                                new ServerContextBuilder()
                                        .setImplementation("auto")
                                        .setTlsEnabled(true)
                                        .setTlsCiphers(ConfigParamsHelper.getTlsCiphersCSV())
                                        .setTlsProtocols("TLSv1.2")
                                        .setKeystore("src/test/resources/security/s3.jks")
                                        .setKeystorePasswordFile("src/test/resources/security/storepass")
                                        .setTruststore("src/test/resources/security/trust1.jks")
                                        .setTruststorePasswordFile("src/test/resources/security/storepass")
                                        .setSaslPlainTextAuth(false)
                                        .setPort(port)
                                        .build()),
                (port) ->
                        new NettyClientRouter(
                                NodeLocator.builder().host("localhost").port(port).build(),
                                CorfuRuntimeParameters.builder()
                                        .tlsEnabled(true)
                                        .keyStore("src/test/resources/security/r1.jks")
                                        .ksPasswordFile("src/test/resources/security/storepass")
                                        .trustStore("src/test/resources/security/trust2.jks")
                                        .tsPasswordFile("src/test/resources/security/storepass")
                                        .build()),
                (r, d) -> assertThat(getBaseClient(r).pingSync()).isFalse());
    }

    @Test
    public void nettyTlsUnknownClient() throws Exception {
        runWithBaseServer(
                (port) ->
                        new NettyServerData(
                                new ServerContextBuilder()
                                        .setImplementation("auto")
                                        .setTlsEnabled(true)
                                        .setTlsCiphers(ConfigParamsHelper.getTlsCiphersCSV())
                                        .setTlsProtocols("TLSv1.2")
                                        .setKeystore("src/test/resources/security/s1.jks")
                                        .setKeystorePasswordFile("src/test/resources/security/storepass")
                                        .setTruststore("src/test/resources/security/trust2.jks")
                                        .setTruststorePasswordFile("src/test/resources/security/storepass")
                                        .setTlsMutualAuthEnabled(true)
                                        .setPort(port)
                                        .build()),
                (port) ->
                        new NettyClientRouter(
                                NodeLocator.builder().host("localhost").port(port).build(),
                                CorfuRuntimeParameters.builder()
                                        .tlsEnabled(true)
                                        .keyStore("src/test/resources/security/r2.jks")
                                        .ksPasswordFile("src/test/resources/security/storepass")
                                        .trustStore("src/test/resources/security/trust1.jks")
                                        .tsPasswordFile("src/test/resources/security/storepass")
                                        .build()),
                (r, d) -> assertThat(getBaseClient(r).pingSync()).isFalse());
    }

    @Test
    public void nettyTlsUnknownClientNoMutualAuth() throws Exception {
        runWithBaseServer(
                (port) ->
                        new NettyServerData(
                                new ServerContextBuilder()
                                        .setImplementation("auto")
                                        .setTlsEnabled(true)
                                        .setTlsCiphers(ConfigParamsHelper.getTlsCiphersCSV())
                                        .setTlsProtocols("TLSv1.2")
                                        .setKeystore("src/test/resources/security/s1.jks")
                                        .setKeystorePasswordFile("src/test/resources/security/storepass")
                                        .setTruststore("src/test/resources/security/trust2.jks")
                                        .setTruststorePasswordFile("src/test/resources/security/storepass")
                                        .setPort(port)
                                        .build()),
                (port) ->
                        new NettyClientRouter(
                                NodeLocator.builder().host("localhost").port(port).build(),
                                CorfuRuntimeParameters.builder()
                                        .tlsEnabled(true)
                                        .keyStore("src/test/resources/security/r2.jks")
                                        .ksPasswordFile("src/test/resources/security/storepass")
                                        .trustStore("src/test/resources/security/trust1.jks")
                                        .tsPasswordFile("src/test/resources/security/storepass")
                                        .build()),
                (r, d) -> assertThat(getBaseClient(r).pingSync()).isTrue());
    }

    @Test
    public void nettySasl() throws Exception {
        runWithBaseServer(
                (port) -> {
                    System.setProperty(
                            "java.security.auth.login.config", "src/test/resources/security/corfudb_jaas.config");
                    return new NettyServerData(
                            new ServerContextBuilder()
                                    .setImplementation("auto")
                                    .setTlsEnabled(true)
                                    .setTlsCiphers(ConfigParamsHelper.getTlsCiphersCSV())
                                    .setTlsProtocols("TLSv1.2")
                                    .setKeystore("src/test/resources/security/s1.jks")
                                    .setKeystorePasswordFile("src/test/resources/security/storepass")
                                    .setTruststore("src/test/resources/security/trust1.jks")
                                    .setTruststorePasswordFile("src/test/resources/security/storepass")
                                    .setSaslPlainTextAuth(true)
                                    .setPort(port)
                                    .build());
                },
                (port) ->
                        new NettyClientRouter(
                                NodeLocator.builder().host("localhost").port(port).build(),
                                CorfuRuntimeParameters.builder()
                                        .tlsEnabled(true)
                                        .keyStore("src/test/resources/security/r1.jks")
                                        .ksPasswordFile("src/test/resources/security/storepass")
                                        .trustStore("src/test/resources/security/trust1.jks")
                                        .tsPasswordFile("src/test/resources/security/storepass")
                                        .saslPlainTextEnabled(true)
                                        .usernameFile("src/test/resources/security/username1")
                                        .passwordFile("src/test/resources/security/userpass1")
                                        .build()),
                (r, d) -> assertThat(getBaseClient(r).pingSync()).isTrue());
    }

    @Test
    public void nettyServerClientHandshakeDefaultId() throws Exception {
        runWithBaseServer(
                (port) -> new NettyServerData(ServerContextBuilder.defaultContext(port)),
                (port) -> {
                    NodeLocator nl =
                            NodeLocator.builder()
                                    .host("localhost")
                                    .port(port)
                                    .nodeId(UUID.fromString("00000000-0000-0000-0000-000000000000"))
                                    .build();
                    return new NettyClientRouter(nl, CorfuRuntimeParameters.builder().build());
                },
                (r, d) -> assertThat(getBaseClient(r).pingSync()).isTrue());
    }

    private UUID nodeId;

    @Test
    public void nettyServerClientHandshakeMatchIds() throws Exception {
        runWithBaseServer(
                (port) -> {
                    ServerContext sc = ServerContextBuilder.defaultContext(port);
                    nodeId = sc.getNodeId();
                    return new NettyServerData(sc);
                },
                (port) -> {
                    NodeLocator nl =
                            NodeLocator.builder().host("localhost").port(port).nodeId(nodeId).build();
                    return new NettyClientRouter(nl, CorfuRuntimeParameters.builder().build());
                },
                (r, d) -> assertThat(getBaseClient(r).pingSync()).isTrue());
    }

    @Test
    public void nettyServerClientHandshakeMismatchId() throws Exception {
        runWithBaseServer(
                (port) -> new NettyServerData(ServerContextBuilder.defaultContext(port)),
                (port) -> {
                    NodeLocator nl =
                            NodeLocator.builder()
                                    .host("localhost")
                                    .port(port)
                                    .nodeId(UUID.nameUUIDFromBytes("test".getBytes()))
                                    .build();
                    return new NettyClientRouter(nl, CorfuRuntimeParameters.builder().build());
                },
                (r, d) -> assertThat(getBaseClient(r).pingSync()).isFalse());
    }

    @Test
    public void nettySaslWrongPassword() throws Exception {
        runWithBaseServer(
                (port) -> {
                    System.setProperty(
                            "java.security.auth.login.config", "src/test/resources/security/corfudb_jaas.config");
                    return new NettyServerData(
                            new ServerContextBuilder()
                                    .setImplementation("auto")
                                    .setTlsEnabled(true)
                                    .setTlsCiphers(ConfigParamsHelper.getTlsCiphersCSV())
                                    .setTlsProtocols("TLSv1.2")
                                    .setKeystore("src/test/resources/security/s1.jks")
                                    .setKeystorePasswordFile("src/test/resources/security/storepass")
                                    .setTruststore("src/test/resources/security/trust1.jks")
                                    .setTruststorePasswordFile("src/test/resources/security/storepass")
                                    .setSaslPlainTextAuth(true)
                                    .setPort(port)
                                    .build());
                },
                (port) ->
                        new NettyClientRouter(
                                NodeLocator.builder().host("localhost").port(port).build(),
                                CorfuRuntimeParameters.builder()
                                        .tlsEnabled(true)
                                        .keyStore("src/test/resources/security/r1.jks")
                                        .ksPasswordFile("src/test/resources/security/storepass")
                                        .trustStore("src/test/resources/security/trust1.jks")
                                        .tsPasswordFile("src/test/resources/security/storepass")
                                        .saslPlainTextEnabled(true)
                                        .usernameFile("src/test/resources/security/username1")
                                        .passwordFile("src/test/resources/security/userpass2")
                                        .build()),
                (r, d) -> assertThat(getBaseClient(r).pingSync()).isFalse());
    }

    @Test
    public void testTlsUpdateServerTrust() throws Exception {
        int port = findRandomOpenPort();

        Path certDir = Paths.get(PARAMETERS.TEST_TEMP_DIR);

        CertificateManager serverCertManager = CertificateManager.buildSHA384withEcDsa(certDir);

        CertificateManager clientCertManager = CertificateManager.buildSHA384withEcDsa(certDir);
        clientCertManager.trustStoreManager.addCertificate(serverCertManager);
        clientCertManager.trustStoreManager.save();

        NettyServerData serverData = new NettyServerData(buildServerContext(port, serverCertManager.certManagementConfig));
        serverData.bootstrapServer();

        NettyClientRouter clientRouter = new NettyClientRouter(
                NodeLocator.builder().host("localhost").port(port).build(),
                buildRuntimeParams(clientCertManager.certManagementConfig)
        );

        assertThat(getBaseClient(clientRouter).pingSync()).isFalse();
        clientRouter.stop();

        serverCertManager.trustStoreManager.addCertificate(clientCertManager);
        serverCertManager.trustStoreManager.save();

        clientRouter = new NettyClientRouter(
                NodeLocator.builder().host("localhost").port(port).build(),
                buildRuntimeParams(clientCertManager.certManagementConfig)
        );

        clientRouter.getConnectionFuture().join();
        assertThat(getBaseClient(clientRouter).pingSync()).isTrue();
        clientRouter.stop();

        serverData.shutdownServer();
    }

    @Test
    public void testTlsUpdateClientTrust() throws Exception {
        int port = findRandomOpenPort();

        Path certDir = Paths.get(PARAMETERS.TEST_TEMP_DIR);

        CertificateManager clientCertManager = CertificateManager.buildSHA384withEcDsa(certDir);

        CertificateManager serverCertManager = CertificateManager.buildSHA384withEcDsa(certDir);
        serverCertManager.trustStoreManager.addCertificate(clientCertManager);
        serverCertManager.trustStoreManager.save();

        NettyServerData serverData = new NettyServerData(buildServerContext(port, serverCertManager.certManagementConfig));
        serverData.bootstrapServer();

        NettyClientRouter clientRouter = new NettyClientRouter(
                NodeLocator.builder().host("localhost").port(port).build(),
                buildRuntimeParams(clientCertManager.certManagementConfig)
        );

        assertThat(getBaseClient(clientRouter).pingSync()).isFalse();
        clientRouter.stop();

        clientCertManager.trustStoreManager.addCertificate(serverCertManager);
        clientCertManager.trustStoreManager.save();

        clientRouter = new NettyClientRouter(
                NodeLocator.builder().host("localhost").port(port).build(),
                buildRuntimeParams(clientCertManager.certManagementConfig)
        );

        clientRouter.getConnectionFuture().join();
        assertThat(getBaseClient(clientRouter).pingSync()).isTrue();
        clientRouter.stop();

        serverData.shutdownServer();
    }

    /**
     * Test RSA Cipher TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256
     * with various keystore combinations.
     *
     * @throws Exception Any Exception thrown during the test
     */
    @Test
    public void testTlsCipherRSA() throws Exception {
        tlsCipherTestHelper(TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256.name(), KeyStoreType.RSA,
                true, TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256);
        tlsCipherTestHelper(TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256.name(), KeyStoreType.ECDSA,
                false, null);
        tlsCipherTestHelper(TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256.name(), KeyStoreType.RSA_ECDSA,
                true, TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256);
        tlsCipherTestHelper(TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256.name(), KeyStoreType.RSA_RSA,
                true, TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256);
        tlsCipherTestHelper(TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256.name(), KeyStoreType.ECDSA_ECDSA,
                false, null);
    }

    /**
     * Test ECDSA Cipher TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384
     * with various keystore combinations.
     *
     * @throws Exception Any Exception thrown during the test
     */
    @Test
    public void testTlsCipherEcdsa() throws Exception {
        tlsCipherTestHelper(TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384.name(), KeyStoreType.RSA,
                false, null);
        tlsCipherTestHelper(TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384.name(), KeyStoreType.ECDSA,
                true, TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384);

        // When OpenSsl is the SSL provider
        // TLS fails sometimes, depending on the environment to find a Cipher when both RSA and ECDSA are in the trust store
        // javax.net.ssl.SSLHandshakeException: error:1417A0C1:SSL routines:tls_post_process_client_hello:no shared cipher
        //
        // It sometimes succeeds by picking TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256 Cipher
        // Skipping this part until a determination is made about this case.
        //
        // tlsCipherTestHelper(TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384.name(), KeyStoreType.RSA_ECDSA, // ssl error
        //         false, null);

        // When JDK is the SSL provider, it works with ECDSA consistently.
        if (!OpenSsl.isAvailable()) {
            tlsCipherTestHelper(TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384.name(), KeyStoreType.RSA_ECDSA,
                    true, TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384);
        }

        tlsCipherTestHelper(TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384.name(), KeyStoreType.RSA_RSA,
                false, null);

        tlsCipherTestHelper(TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384.name(), KeyStoreType.ECDSA_ECDSA,
                true, TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384);
    }

    /**
     * Test both RSA and ECDSA Ciphers (enabled together)
     * with various keystore combinations.
     *
     * @throws Exception Any Exception thrown during the test
     */
    @Test
    public void testTlsCipherRsaAndEcdsa() throws Exception {
        tlsCipherTestHelper(ConfigParamsHelper.getTlsCiphersCSV(), KeyStoreType.RSA,
                true, TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256);

        tlsCipherTestHelper(ConfigParamsHelper.getTlsCiphersCSV(), KeyStoreType.ECDSA,
                true, TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384);

        if (OpenSsl.isAvailable()) {
            // when OpenSsl is the SSL provider
            // Picks RSA Cipher TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256 when both are given
            tlsCipherTestHelper(ConfigParamsHelper.getTlsCiphersCSV(), KeyStoreType.RSA_ECDSA,
                    true, TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256);
        } else {
            // When JDK is the SSL provider
            // Picks ECDSA Cipher TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384 when both are given
            tlsCipherTestHelper(ConfigParamsHelper.getTlsCiphersCSV(), KeyStoreType.RSA_ECDSA,
                    true, TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384);
        }

        tlsCipherTestHelper(ConfigParamsHelper.getTlsCiphersCSV(), KeyStoreType.RSA_RSA,
                true, TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256);

        tlsCipherTestHelper(ConfigParamsHelper.getTlsCiphersCSV(), KeyStoreType.ECDSA_ECDSA,
                true, TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384);
    }

    /**
     * A helper method to test various combination of ciphers and keystore.
     *
     * @param tlsCiphers        The Ciphers to be enabled for the netty connection
     * @param keyStoreType      A keystore containing keys supporting the enabled ciphers
     * @param shouldPingSucceed whether the current combination of ciphers and keys is expected to work or not
     * @param expectedCipher    the expected cipher in a successful connection,
     *                          null if no connection can be established
     * @throws Exception Any Exception thrown during the test
     */
    private void tlsCipherTestHelper(String tlsCiphers, KeyStoreType keyStoreType,
                                     boolean shouldPingSucceed, ConfigParamsHelper.TlsCiphers expectedCipher) throws Exception {
        String serverKeystorePathPrefix = "src/test/resources/security/server_";
        String runtimeKeystorePathPrefix = "src/test/resources/security/runtime_";
        String jksSuffix = ".jks";
        runWithBaseServer(
                (port) ->
                        new NettyServerData(
                                new ServerContextBuilder()
                                        .setImplementation("auto")
                                        .setTlsEnabled(true)
                                        .setTlsCiphers(tlsCiphers)
                                        .setTlsProtocols("TLSv1.2")
                                        .setKeystore(serverKeystorePathPrefix
                                                + keyStoreType.name().toLowerCase() + jksSuffix)
                                        .setKeystorePasswordFile("src/test/resources/security/storepass")
                                        .setTruststore(runtimeKeystorePathPrefix
                                                + keyStoreType.name().toLowerCase() + jksSuffix)
                                        .setTruststorePasswordFile("src/test/resources/security/storepass")
                                        .setTlsMutualAuthEnabled(true)
                                        .setPort(port)
                                        .build()
                        ),
                (port) ->
                        new NettyClientRouter(
                                NodeLocator.builder().host("localhost").port(port).build(),
                                CorfuRuntimeParameters.builder()
                                        .tlsEnabled(true)
                                        .keyStore(runtimeKeystorePathPrefix +
                                                keyStoreType.name().toLowerCase() + jksSuffix)
                                        .ksPasswordFile("src/test/resources/security/storepass")
                                        .trustStore(serverKeystorePathPrefix +
                                                keyStoreType.name().toLowerCase() + jksSuffix)
                                        .tsPasswordFile("src/test/resources/security/storepass")
                                        .build()),
                (r, d) -> {
                    if (shouldPingSucceed) {
                        assertThat(getBaseClient(r).pingSync()).isTrue();
                        SSLSession sslSession =
                                ((SslHandler) r.getChannel().pipeline().get("ssl"))
                                        .engine().getSession();
                        assertThat(sslSession.getProtocol()).isEqualTo("TLSv1.2");
                        assertThat(sslSession.getCipherSuite())
                                .isEqualTo(expectedCipher.name());
                    } else {
                        assertThat(getBaseClient(r).pingSync()).isFalse();
                    }
                });
    }


    private void runWithBaseServer(
            NettyServerDataConstructor nsdc,
            NettyClientRouterConstructor ncrc,
            NettyCommFunction actionFn)
            throws Exception {
        int port = findRandomOpenPort();

        NettyServerData d = nsdc.createNettyServerData(port);
        NettyClientRouter ncr = null;
        try {
            d.bootstrapServer();
            ncr = ncrc.createNettyClientRouter(port);
            ncr.addClient(new BaseHandler());
            actionFn.runTest(ncr, d);
        } catch (Exception ex) {
            log.error("Exception ", ex);
            throw ex;
        } finally {
            try {
                if (ncr != null) {
                    ncr.stop();
                }
            } catch (Exception ex) {
                log.warn("Error shutting down client...", ex);
            }
            d.shutdownServer();
        }
    }

    private ServerContext buildServerContext(int port, CertManagementConfig config) {
        return new ServerContextBuilder()
                .setImplementation("auto")
                .setTlsEnabled(true)
                .setTlsCiphers(ConfigParamsHelper.getTlsCiphersCSV())
                .setTlsProtocols("TLSv1.2")
                .setKeystore(config.getKeyStoreConfig().getKeyStoreFile().toString())
                .setKeystorePasswordFile(config.getKeyStoreConfig().getPasswordFile().toString())
                .setTruststore(config.getTrustStoreConfig().getTrustStoreFile().toString())
                .setTruststorePasswordFile(config.getTrustStoreConfig().getPasswordFile().toString())
                .setTlsMutualAuthEnabled(true)
                .setPort(port)
                .build();
    }

    private CorfuRuntimeParameters buildRuntimeParams(CertManagementConfig config) {
        return CorfuRuntimeParameters.builder()
                .tlsEnabled(true)
                .keyStore(config.getKeyStoreConfig().getKeyStoreFile().toString())
                .ksPasswordFile(config.getKeyStoreConfig().getPasswordFile().toString())
                .trustStore(config.getTrustStoreConfig().getTrustStoreFile().toString())
                .tsPasswordFile(config.getTrustStoreConfig().getPasswordFile().toString())
                .build();
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
    public static class NettyServerData {
        ServerBootstrap b;
        volatile ChannelFuture f;

        final ServerContext serverContext;

        private final String address = "localhost";

        NettyServerData(@Nonnull ServerContext context) {
            this.serverContext = context;
        }

        void bootstrapServer() {
            BaseServer baseServer = new BaseServer(serverContext);
            NettyServerRouter nsr = new NettyServerRouter(ImmutableList.of(baseServer), serverContext);
            CorfuServerNode corfuServerNode =
                    new CorfuServerNode(serverContext, ImmutableMap.of(BaseServer.class, baseServer));
            f =
                    corfuServerNode.bindServer(
                            serverContext.getWorkerGroup(),
                            corfuServerNode::configureBootstrapOptions,
                            serverContext,
                            nsr,
                            address,
                            Integer.parseInt((String) serverContext.getServerConfig().get("<port>")));
        }

        void shutdownServer() {
            f.channel().close().awaitUninterruptibly();
        }
    }
}
