package org.corfudb.infrastructure;

import lombok.Data;
import lombok.experimental.Accessors;
import org.corfudb.comm.ChannelImplementation;
import org.corfudb.infrastructure.configuration.ServerConfiguration;
import org.corfudb.test.concurrent.TestThreadGroups;

/**
 * Created by mwei on 6/29/16.
 */
@Accessors(chain = true)
@Data
// Disable magic number check to make defaults readable
@SuppressWarnings("checkstyle:magicnumber")
public class ServerContextBuilder {
    private boolean single = true;
    private boolean memory = true;
    private String logPath = null;
    private boolean verifyChecksum = true;
    private boolean syncData = true;

    private boolean noAutoCommit = true;

    private boolean tlsEnabled = false;
    private boolean tlsMutualAuthEnabled = false;
    private String tlsProtocols = "";
    private String tlsCiphers = "";
    private String keystore = "";
    private String keystorePasswordFile = "";
    private boolean saslPlainTextAuth = false;
    private String truststore = "";
    private String truststorePasswordFile = "";

    private ChannelImplementation implementation = ChannelImplementation.LOCAL;

    private double cacheSizeHeapRatio = 0.5;
    private String address = "test";
    private int port = 9000;
    private int seqCache = 1000;
    private double logSizeLimitPercentage = 100.0;
    private int batchSize = 100;
    private String managementBootstrapEndpoint = null;
    private IServerRouter serverRouter;

    private int handshakeTimeout = 10;
    private String prefix = "";
    private int retention = 1000;

    private String clusterId = "00000000-0000-0000-0000-000000000000";
    private boolean isTest = true;

    public ServerContextBuilder() {

    }

    public ServerContext build() {
        ServerConfiguration conf = new ServerConfiguration()
                .setSingleMode(single)
                .setInMemoryMode(memory)
                .setHandshakeTimeout(handshakeTimeout)
                .setSequencerCacheSize(seqCache)
                .setLogSizeQuota(logSizeLimitPercentage)
                .setStateTransferBatchSize(batchSize)
                .setMetadataRetention(retention);
        if (logPath != null) {
            conf.setServerDirectory(logPath);
        }

        conf.setVerifyChecksum(verifyChecksum)
                .setSyncData(syncData)
                .setHostAddress(address)
                .setServerPort(port)
                .setLogUnitCacheRatio(cacheSizeHeapRatio)
                .setEnableTls(tlsEnabled)
                .setEnableTlsMutualAuth(tlsMutualAuthEnabled)
                .setTlsProtocols(tlsProtocols)
                .setTlsCiphers(tlsCiphers)
                .setKeystore(keystore)
                .setKeystorePasswordFile(keystorePasswordFile)
                .setTruststore(truststore)
                .setTruststorePasswordFile(truststorePasswordFile)
                .setEnableSaslPlainTextAuth(saslPlainTextAuth)
                .setClusterId(clusterId)
                .setChannelImplementation(implementation);

        // Provide the server with event loop groups
        if (implementation == ChannelImplementation.LOCAL) {
            conf.setTestClientEventLoop(TestThreadGroups.NETTY_CLIENT_GROUP.get())
                    .setTestWorkerEventLoop(TestThreadGroups.NETTY_WORKER_GROUP.get());
        }
        ServerContext sc = new ServerContext(conf);
        sc.setServerRouter(serverRouter);
        return sc;
    }

    /** Create a test context at a given port with the default settings.
     *
     * @param port  The port to use.
     * @return      A {@link ServerContext} with a {@link TestServerRouter} installed.
     */
    public static ServerContext defaultTestContext(int port) {
        ServerContext sc = new ServerContextBuilder()
            .setPort(port).build();
        sc.setServerRouter(new TestServerRouter());
        return sc;
    }

    /** Create a non-test (socket-based) context at a given port with the default settings.
     *
     * @param port  The port to use
     * @return      A non-test {@link ServerContext}
     */
    public static ServerContext defaultContext(int port) {
        ServerContext sc = new ServerContextBuilder()
            .setPort(port)
            .setImplementation(ChannelImplementation.AUTO)
            .build();
        return sc;
    }
}
