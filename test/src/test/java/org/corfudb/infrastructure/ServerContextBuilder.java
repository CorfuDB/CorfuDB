package org.corfudb.infrastructure;

import com.google.common.collect.ImmutableMap;
import lombok.Data;
import lombok.experimental.Accessors;
import org.corfudb.AbstractCorfuTest;

/**
 * Created by mwei on 6/29/16.
 */
@Accessors(chain = true)
@Data
// Disable magic number check to make defaults readable
@SuppressWarnings("checkstyle:magicnumber")
public class ServerContextBuilder {

    long initialToken = 0L; // for testing, we want to reset the sequencer on each test

    boolean single = true;
    boolean memory = true;
    String logPath = null;
    boolean noVerify = false;

    boolean tlsEnabled = false;
    boolean tlsMutualAuthEnabled = false;
    String tlsProtocols = "";
    String tlsCiphers = "";
    String keystore = "";
    String keystorePasswordFile = "";
    boolean saslPlainTextAuth = false;
    String truststore = "";
    String truststorePasswordFile = "";

    String implementation = "local";

    String cacheSizeHeapRatio = "0.5";
    String address = "test";
    int port = 9000;
    String seqCache = "1000";
    String managementBootstrapEndpoint = null;
    IServerRouter serverRouter;
    String numThreads = "0";
    String handshakeTimeout = "10";
    String prefix = "";

    String clusterId = "auto";
    boolean isTest = true;

    public ServerContextBuilder() {

    }

    public ServerContext build() {
        ImmutableMap.Builder<String,Object> builder =
                new ImmutableMap.Builder<String, Object>()
                .put("--initial-token", initialToken)
                .put("--single", single)
                .put("--memory", memory)
                .put("--Threads", numThreads)
                .put("--HandshakeTimeout", handshakeTimeout)
                .put("--sequencer-cache-size", seqCache);
        if (logPath != null) {
         builder.put("--log-path", logPath);
        }
        if (managementBootstrapEndpoint != null) {
            builder.put("--management-server", managementBootstrapEndpoint);
        }
         builder
                 .put("--no-verify", noVerify)
                 .put("--address", address)
                 .put("--cache-heap-ratio", cacheSizeHeapRatio)
                 .put("--enable-tls", tlsEnabled)
                 .put("--enable-tls-mutual-auth", tlsMutualAuthEnabled)
                 .put("--tls-protocols", tlsProtocols)
                 .put("--tls-ciphers", tlsCiphers)
                 .put("--keystore", keystore)
                 .put("--keystore-password-file", keystorePasswordFile)
                 .put("--truststore", truststore)
                 .put("--truststore-password-file", truststorePasswordFile)
                 .put("--enable-sasl-plain-text-auth", saslPlainTextAuth)
                 .put("--cluster-id", clusterId)
                 .put("--implementation", implementation)
                 .put("<port>", port);

        // Set the prefix to the port number
        if (prefix.equals("")) {
            prefix = "test:" + port;
        }
        builder.put("--Prefix", prefix);

        // Provide the server with event loop groups
        if (implementation.equals("local")) {
            if (AbstractCorfuTest.NETTY_CLIENT_GROUP != null) {
                builder
                    .put("client", AbstractCorfuTest.NETTY_CLIENT_GROUP);
            }
            if (AbstractCorfuTest.NETTY_BOSS_GROUP != null) {
                builder
                .put("boss", AbstractCorfuTest.NETTY_BOSS_GROUP);
            }
            if (AbstractCorfuTest.NETTY_WORKER_GROUP != null) {
                builder
                .put("worker", AbstractCorfuTest.NETTY_WORKER_GROUP);
            }
        }
        ServerContext sc = new ServerContext(builder.build());
        sc.setServerRouter(serverRouter);
        return sc;
    }

    /** Create a test context at a given port with the default settings.
     *
     * @param port  The port to use.
     * @return      A {@link ServerContext} with a {@link TestServerRouter} installed.
     */
    public static ServerContext defaultTestContext(int port) {
        ServerContext sc = new ServerContextBuilder().setPort(port).build();
        sc.setServerRouter(new TestServerRouter());
        return sc;
    }

    /** Create a non-test (socket-based) context at a given port with the default settings.
     *
     * @param port  The port to use
     * @return      A non-test {@link ServerContext}
     */
    public static ServerContext defaultContext(int port) {
        ServerContext sc = new ServerContextBuilder().setPort(port)
            .setImplementation("auto")
            .build();
        return sc;
    }

    public static ServerContext emptyContext() {
        return new ServerContextBuilder().build();
    }

}
