package org.corfudb.infrastructure;

import com.google.common.collect.ImmutableMap;
import lombok.Data;
import lombok.experimental.Accessors;
import org.corfudb.test.concurrent.TestThreadGroups;

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
    boolean noSync = false;

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
    String logSizeLimitPercentage = "100.0";
    String batchSize = "100";
    String managementBootstrapEndpoint = null;
    IServerRouter serverRouter;
    String numThreads = "0";
    String handshakeTimeout = "10";
    String prefix = "";
    String retention = "1000";

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
                .put("--sequencer-cache-size", seqCache)
                .put("--log-size-quota-percentage", logSizeLimitPercentage)
                .put("--batch-size", batchSize)
                .put("--metadata-retention", retention);
        if (logPath != null) {
         builder.put("--log-path", logPath);
        }
        if (managementBootstrapEndpoint != null) {
            builder.put("--management-server", managementBootstrapEndpoint);
        }
         builder
                 .put("--no-verify", noVerify)
                 .put("--no-sync", noSync)
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
            builder.put("client", TestThreadGroups.NETTY_CLIENT_GROUP.get());
            builder.put("boss", TestThreadGroups.NETTY_BOSS_GROUP.get());
            builder.put("worker", TestThreadGroups.NETTY_CLIENT_GROUP.get());
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
