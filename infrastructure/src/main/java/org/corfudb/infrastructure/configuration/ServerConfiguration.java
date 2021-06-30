package org.corfudb.infrastructure.configuration;
import static org.corfudb.infrastructure.logreplication.LogReplicationConfig.MAX_DATA_MSG_SIZE_SUPPORTED;
import static org.corfudb.infrastructure.logreplication.LogReplicationConfig.DEFAULT_MAX_NUM_MSG_PER_BATCH;
import static org.corfudb.util.NetworkUtils.getAddressFromInterfaceName;

import ch.qos.logback.classic.Level;
import io.grpc.Server;
import io.netty.channel.EventLoopGroup;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.corfudb.comm.ChannelImplementation;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.utils.lock.Lock;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * This class holds various server configuration parameters.
 *
 * <p>Created by maithem on 12/4/19.
 */

public class ServerConfiguration extends PropertiesConfiguration {
    // Server general parameters
    private static final String SERVER_DIR = "serverDirectory";
    private static final String SINGLE_MODE = "singleMode";
    private static final String CLUSTER_ID = "clusterId";
    private static final String NUM_IO_THREADS = "numIOThreads";
    private static final String HOST_ADDRESS = "hostAddress";
    private static final String SERVER_PORT = "serverPort";
    private static final String NETWORK_INTERFACE = "networkInterface";
    private static final String HANDSHAKE_TIMEOUT = "handshakeTimeout";
    private static final String METADATA_RETENTION = "metadataRetention";
    private static final String LOG_LEVEL = "logLevel";
    private static final String NUM_BASE_SERVER_THREADS = "numBaseServerThreads";
    private static final String METRICS_PROVIDER_ADDRESS = "metricsProviderAddress";
    private static final String CHANNEL_IMPLEMENTATION = "channelImplementation";
    private static final String ENABLE_TLS = "enableTls";
    private static final String ENABLE_TLS_MUTUAL_AUTH = "enableTlsMutualAuth";
    private static final String KEYSTORE = "keystore";
    private static final String KEYSTORE_PASSWORD_FILE = "keystorePasswordFile";
    private static final String TRUSTSTORE = "truststore";
    private static final String TRUSTSTORE_PASSWORD_FILE = "truststorePasswordFile";
    private static final String ENABLE_SASL_PLAIN_TEXT_AUTH = "enableSaslPlainTextAuth";
    private static final String SASL_PLAIN_TEXT_USERNAME_FILE = "saslPlainTextUsernameFile";
    private static final String SASL_PLAIN_TEXT_PASSWORD_FILE = "saslPlainTextPasswordFile";
    private static final String TLS_CIPHERS = "tlsCiphers";
    private static final String TLS_PROTOCOLS = "tlsProtocols";

    // Layout Server parameters
    private static final String NUM_LAYOUT_SERVER_THREADS = "numLayoutServerThreads";

    // LogUnit parameters
    private static final String IN_MEMORY_MODE = "inMemoryMode";
    private static final String LOG_UNIT_CACHE_RATIO = "logUnitCacheRatio";
    private static final String VERIFY_CHECKSUM = "verifyChecksum";
    private static final String SYNC_DATA = "syncData";
    private static final String NUM_LOGUNIT_WORKER_THREADS = "numLogUnitWorkerThreads";
    private static final String LOG_SIZE_QUOTA = "logSizeQuota";


    // Sequencer parameters
    private static final String SEQUENCER_CONFLICT_WINDOW_SIZE = "sequencerConflictWindowSize";

    // Management parameters
    private static final String STATE_TRANSFER_BATCH_SIZE = "stateTransferBatchSize";
    private static final String NUM_MANAGEMENT_SERVER_THREADS = "numManagementServerThreads";

    //Added parameters
    private static final String AUTO_COMMIT = "autoCommit";
    private static final String MAX_REPLICATION_DATA_MESSAGE_SIZE = "maxReplicationDataMessageSize";
    private static final String COMPACT_RATE = "compactRate";
    private static final String PLUGIN_CONFIG_FILE_PATH = "pluginConfigFilePath";
    private static final String ENABLE_METRICS = "enableMetrics";
    private static final String SNAPSHOT_BATCH_SIZE = "snapshotBatchSize";
    private static final String LOCK_LEASE_DURATION = "lockLeaseDuration";
    private static final String THREAD_PREFIX = "threadPrefix";
    private static final String BIND_TO_ALL_INTERFACES = "bindToAllInterfaces";

    // The underlying map of PropertiesConfiguration can't be used to store an EventLoopGroup,
    // so a separate map is needed. This shouldn't be here, but the Unit Tests rely on
    // these event loops to be here
    private Map<String, EventLoopGroup> testEventLoops = new HashMap<>();


    public static ServerConfiguration getServerConfigFromCommandLineArg(CommandLine cmdOptions) {
        ServerConfiguration conf = new ServerConfiguration();
        // merge command line with conf
        // This mapping is a temporary solution until we merge the command line config names
        // with the config names in this file
        if (!cmdOptions.hasOption("memory")) {
            conf.setServerDirectory(cmdOptions.getOptionValue("log-path"));
        }

        conf.setInMemoryMode(cmdOptions.hasOption("memory"));
        conf.setSingleMode(cmdOptions.hasOption("single"));
        conf.setHostAddress(cmdOptions.getOptionValue("address", "localhost"));
        conf.setServerPort(Integer.parseInt(cmdOptions.getOptionValue("port", "9000")));
        conf.setNetworkInterface(cmdOptions.getOptionValue("network-interface"));
        conf.setLogUnitCacheRatio(Double
                .parseDouble(cmdOptions.getOptionValue("cache-heap-ratio", "0.5")));
        conf.setSequencerConflictWindowSize(Integer
                .parseInt(cmdOptions.getOptionValue("sequencer-cache-size", "250000")));
        conf.setLogLevel(cmdOptions.getOptionValue("log-level", "INFO"));
        conf.setEnableTls(cmdOptions.hasOption("enable-tls"));
        conf.setEnableTlsMutualAuth(cmdOptions.hasOption("enable-tls-mutual-auth"));
        conf.setKeystore(cmdOptions.getOptionValue("keystore"));
        conf.setKeystorePasswordFile(cmdOptions.getOptionValue("keystore-password-file"));
        conf.setTruststore(cmdOptions.getOptionValue("truststore"));
        conf.setTruststorePasswordFile(cmdOptions.getOptionValue("truststore-password-file"));
        conf.setLogSizeQuota(Double.parseDouble(cmdOptions
                .getOptionValue("log-size-quota-percentage", "100.0")));
        conf.setMetricsProviderAddress(cmdOptions.hasOption("metrics-port") ?
                Integer.parseInt(cmdOptions.getOptionValue("metrics-port")) : null);

        // Special handling is needed because the port can be specified without
        // an option name
        if (cmdOptions.getArgList().size() == 1) {
            int port = Integer.valueOf(cmdOptions.getArgList().get(0));
            conf.setServerPort(port);
        } else if (!cmdOptions.getArgList().isEmpty()) {
            throw new IllegalArgumentException("Unknown arguments: " + cmdOptions.getArgList());
        }

        return conf;
    }

    public static ServerConfiguration getServerConfigFromMap(Map<String, Object> opts) {
        ServerConfiguration conf = new ServerConfiguration();
        if (opts.containsKey("--memory")) {
            conf.setInMemoryMode(true);
        } else {
            conf.setServerDirectory((String) opts.get("--log-path"));
        }
        conf.setVerifyChecksum(!opts.containsKey("--no-verify"));
        conf.setSyncData(!opts.containsKey("--no-sync"));
        conf.setSingleMode(opts.containsKey("--single"));
        conf.setAutoCommit(opts.containsKey("--no-auto-commit"));

        // Bind to all interfaces only if no address or interface specified by the user.
        // Fetch the address if given a network interface.
        String networkInterfaceName = (String) opts.get("--network-interface");
        String address = (String) opts.get("--address");
        if (networkInterfaceName != null) {
            conf.setHostAddress(getAddressFromInterfaceName(networkInterfaceName));
            conf.setNetworkInterface(networkInterfaceName);
            conf.setBindToAllInterfaces(false);
        } else if (address == null) {
            // Default the address to localhost and set the bind to all interfaces flag to true,
            // if the address and interface is not specified.
            conf.setBindToAllInterfaces(true);
            conf.setHostAddress("localhost");
        } else {
            // Address is specified by the user.
            conf.setHostAddress(address);
            conf.setBindToAllInterfaces(false);
        }

        conf.setMaxReplicationDataMessageSize(Integer.parseInt((String) opts.getOrDefault("--max-replication-data-message-size", Integer.toString(MAX_DATA_MSG_SIZE_SUPPORTED))));
        conf.setLogUnitCacheRatio(Double.parseDouble((String) opts.getOrDefault("--cache-heap-ratio", "0.5")));
        conf.setLogLevel((String) opts.getOrDefault("--log-level", "INFO"));
        conf.setCompactRate(Integer.parseInt((String) opts.getOrDefault("--compact", "60")));
        conf.setPluginConfigFilePath((String) opts.get("--plugin"));
        conf.setNumBaseServerThreads(Integer.parseInt((String) opts.get("--base-server-threads")));
        conf.setLogSizeQuota(Double.parseDouble((String) opts.getOrDefault("--log-size-quota-percentage", "100.0")));

        //TODO(NEIL): double check
        conf.setNumLogUnitWorkerThreads(Integer.parseInt((String) opts.getOrDefault("--logunit-threads", "4")));
        conf.setNumLayoutServerThreads(Integer.parseInt((String) opts.getOrDefault("--management-server-threads", "4")));
        conf.setNumIOThreads(Integer.parseInt((String) opts.getOrDefault("--Threads", "4")));

        conf.setEnableTls(opts.containsKey("--enable-tls"));
        conf.setKeystore((String) opts.get("--keystore"));
        conf.setKeystorePasswordFile((String) opts.get("--keystore-password-file"));
        conf.setTruststore((String) opts.get("--truststore"));
        conf.setTruststorePasswordFile((String) opts.get("--truststore-password-file"));
        conf.setEnableTlsMutualAuth(opts.containsKey("--enable-tls-mutual-auth"));
        conf.setEnableSaslPlainTextAuth(opts.containsKey("--enable-sasl-plain-text-auth"));
        conf.setSaslPlainTextUserFile((String) opts.get("--sasl-plain-text-username-file"));
        conf.setSaslPlainTextPasswordFile((String) opts.get("--sasl-plain-text-password-file"));
        conf.setSequencerConflictWindowSize(Integer.parseInt((String) opts.getOrDefault("--sequencer-cache-size","250000")));

        conf.setStateTransferBatchSize(Integer.parseInt((String) opts.getOrDefault("--batch-size", "100")));

        String implementationType = (String) opts.getOrDefault("--implementation","");
        if (implementationType.equals("auto")) {
            conf.setChannelImplementation(ChannelImplementation.AUTO);
        } else if (implementationType.equals("local")) {
            conf.setChannelImplementation(ChannelImplementation.LOCAL);
        } else if (implementationType.equals("epoll")) {
            conf.setChannelImplementation(ChannelImplementation.EPOLL);
        } else if (implementationType.equals("kqueue")) {
            conf.setChannelImplementation(ChannelImplementation.KQUEUE);
        } else {
            conf.setChannelImplementation(ChannelImplementation.NIO);
        }

        conf.setHandshakeTimeout(Integer.parseInt((String) opts.getOrDefault("--HandshakeTimeout", "10")));
        conf.setClusterId((String) opts.getOrDefault("--cluster-id", "auto"));
        conf.setTlsCiphers((String) opts.getOrDefault("--tls-ciphers","TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"));
        conf.setTlsProtocols((String) opts.getOrDefault("--tls-protocols", "TLSv1.1,TLSv1.2"));

        conf.setEnableMetrics(opts.containsKey("--metrics"));
        conf.setSnapshotBatchSize(Integer.parseInt((String) opts.getOrDefault("--snapshot-batch", Integer.toString(DEFAULT_MAX_NUM_MSG_PER_BATCH))));
        conf.setLockLeaseDuration(Integer.parseInt((String) opts.getOrDefault("--lock-lease", Integer.toString(Lock.leaseDuration))));
        conf.setThreadPrefix((String) opts.getOrDefault("--Prefix", ""));
        conf.setMetadataRetention(Integer.parseInt((String) opts.getOrDefault("--metadata-retention", "1000")));

        conf.setServerPort(Integer.parseInt((String) opts.get("<port>")));

        return conf;
    }


    public ServerConfiguration setServerDirectory(String path) {
        File parentDir = new File(path);
        if (!parentDir.isDirectory()) {
            throw new UnrecoverableCorfuError("Service path " + path + " must be a directory!");
        }

        File corfuServerDir = new File(parentDir.getAbsolutePath()
                + File.separator
                + "corfu");
        // Update the new path with the dedicated child service directory.
        if (!corfuServerDir.exists() && !corfuServerDir.mkdirs()) {
            throw new UnrecoverableCorfuError("Couldn't create " + corfuServerDir);
        }

        setProperty(SERVER_DIR, corfuServerDir.getAbsolutePath());
        return this;
    }

    public String getServerDir() {
        return getString(SERVER_DIR);
    }

    public String getLogDir() {
        return getServerDir() + File.separator + "log";
    }

    public ServerConfiguration setSingleMode(boolean enable) {
        setProperty(SINGLE_MODE, enable);
        return this;
    }

    public boolean isSingleMode() {
        return getBoolean(SINGLE_MODE);
    }

    public ServerConfiguration setClusterId(String clusterId) {
        setProperty(CLUSTER_ID, clusterId);
        return this;
    }

    public String getClusterId() {
        return getString(CLUSTER_ID, "auto");
    }

    public ServerConfiguration setNumIOThreads(int num) {
        setProperty(NUM_IO_THREADS, num);
        return this;
    }

    public int getNumIOThreads() {
        return getInt(NUM_IO_THREADS, Runtime.getRuntime().availableProcessors());
    }

    public ServerConfiguration setHostAddress(String address) {
        setProperty(HOST_ADDRESS, address);
        return this;
    }

    public String getHostAddress() {
        return getString(HOST_ADDRESS);
    }

    public ServerConfiguration setServerPort(int port) {
        setProperty(SERVER_PORT, port);
        return this;
    }

    public String getLocalServerEndpoint() {
        return getHostAddress() + ":" + getServerPort();
    }

    public int getServerPort() {
        return getInt(SERVER_PORT);
    }

    public ServerConfiguration setNetworkInterface(String networkInterface) {
        setProperty(NETWORK_INTERFACE, networkInterface);
        return this;
    }

    public String getNetworkInterface() {
        return getString(NETWORK_INTERFACE);
    }

    public ServerConfiguration setHandshakeTimeout(int timeout) {
        setProperty(HANDSHAKE_TIMEOUT, timeout);
        return this;
    }

    public int getHandshakeTimeout() {
        return getInt(HANDSHAKE_TIMEOUT, 10);
    }

    public ServerConfiguration setMetadataRetention(int numFiles) {
        if (numFiles < 1) {
            throw new IllegalArgumentException("Max number of metadata files to retain must be greater than 0.");
        }
        setProperty(METADATA_RETENTION, numFiles);
        return this;
    }

    public int getMetadataRetention() {
        return getInt(METADATA_RETENTION, 1000);
    }

    public ServerConfiguration setLogLevel(String levelStr) {
        Level level = Level.toLevel(levelStr.toUpperCase());
        setProperty(LOG_LEVEL, level);
        return this;
    }

    public Level getLogLevel() {
        if(getProperty(LOG_LEVEL) == null) {
            return Level.INFO;
        }
        return (Level) getProperty(LOG_LEVEL);
    }

    public ServerConfiguration setNumBaseServerThreads(int numThreads) {
        setProperty(NUM_BASE_SERVER_THREADS, numThreads);
        return this;
    }

    public int getNumBaseServerThreads() {
        return getInt(NUM_BASE_SERVER_THREADS, 8);
    }

    public ServerConfiguration setMetricsProviderAddress(Integer port) {
        setProperty(METRICS_PROVIDER_ADDRESS, port);
        return this;
    }

    public Integer getMetricsProviderAddress() {
        return getInteger(METRICS_PROVIDER_ADDRESS, null);
    }

    public ServerConfiguration setChannelImplementation(ChannelImplementation type) {
        setProperty(CHANNEL_IMPLEMENTATION, type);
        return this;
    }

    public ChannelImplementation getChannelImplementation() {
        if (getProperty(CHANNEL_IMPLEMENTATION) == null) {
            return ChannelImplementation.NIO;
        } else {
            return (ChannelImplementation) getProperty(CHANNEL_IMPLEMENTATION);
        }
    }

    public ServerConfiguration setTestClientEventLoop(EventLoopGroup eventLoopGroup) {
        testEventLoops.put("client", eventLoopGroup);
        return this;
    }

    public EventLoopGroup getTestClientEventLoop() {
        return testEventLoops.get("client");
    }

    public ServerConfiguration setTestBossEventLoop(EventLoopGroup eventLoopGroup) {
        testEventLoops.put("boss", eventLoopGroup);
        return this;
    }

    public EventLoopGroup getTestBossEventLoop() {
        return testEventLoops.get("boss");
    }

    public ServerConfiguration setTestWorkerEventLoop(EventLoopGroup eventLoopGroup) {
        testEventLoops.put("worker", eventLoopGroup);
        return this;
    }

    public EventLoopGroup getTestWorkerEventLoop() {
        return testEventLoops.get("worker");
    }

    public ServerConfiguration setEnableTls(boolean enableTls) {
        setProperty(ENABLE_TLS, enableTls);
        return this;
    }

    public boolean isTlsEnabled() {
        return getBoolean(ENABLE_TLS, false);
    }

    public ServerConfiguration setEnableTlsMutualAuth(boolean enableTlsMutualAuth) {
        setProperty(ENABLE_TLS_MUTUAL_AUTH, enableTlsMutualAuth);
        return this;
    }

    public boolean getEnableTlsMutualAuth() {
        return getBoolean(ENABLE_TLS_MUTUAL_AUTH, false);
    }

    public ServerConfiguration setKeystore(String keystore) {
        setProperty(KEYSTORE, keystore);
        return this;
    }

    public String getKeystore() {
        return getString(KEYSTORE);
    }

    public ServerConfiguration setKeystorePasswordFile(String keystorePasswordFile) {
        setProperty(KEYSTORE_PASSWORD_FILE, keystorePasswordFile);
        return this;
    }

    public String getKeystorePasswordFile() {
        return getString(KEYSTORE_PASSWORD_FILE);
    }

    public ServerConfiguration setTruststore(String truststore) {
        setProperty(TRUSTSTORE, truststore);
        return this;
    }

    public String getTruststore() {
        return getString(TRUSTSTORE);
    }

    public ServerConfiguration setTruststorePasswordFile(String truststorePasswordFile) {
        setProperty(TRUSTSTORE_PASSWORD_FILE, truststorePasswordFile);
        return this;
    }

    public String getTruststorePasswordFile() {
        return getString(TRUSTSTORE_PASSWORD_FILE);
    }


    public ServerConfiguration setEnableSaslPlainTextAuth(boolean enableSaslPlainTextAuth) {
        setProperty(ENABLE_SASL_PLAIN_TEXT_AUTH, enableSaslPlainTextAuth);
        return this;
    }

    public boolean getEnableSaslPlainTextAuth() {
        return getBoolean(ENABLE_SASL_PLAIN_TEXT_AUTH, false);
    }


    public ServerConfiguration setSaslPlainTextUserFile(String saslPlainTextUserFile) {
        setProperty(SASL_PLAIN_TEXT_USERNAME_FILE, saslPlainTextUserFile);
        return this;
    }

    public String getSaslPlainTextUsernameFile() {
        return getString(SASL_PLAIN_TEXT_USERNAME_FILE);
    }

    public ServerConfiguration setSaslPlainTextPasswordFile(String saslPlainTextPasswordFile) {
        setProperty(SASL_PLAIN_TEXT_PASSWORD_FILE, saslPlainTextPasswordFile);
        return this;
    }

    public String getSaslPlainTextPasswordFile() {
        return getString(SASL_PLAIN_TEXT_PASSWORD_FILE);
    }

    public ServerConfiguration setTlsCiphers(String ciphers) {
        setProperty(TLS_CIPHERS, ciphers);
        return this;
    }

    public String getTlsCiphers() {
        return getString(TLS_CIPHERS, "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256");
    }

    public ServerConfiguration setTlsProtocols(String ciphers) {
        setProperty(TLS_PROTOCOLS, ciphers);
        return this;
    }

    public String getTlsProtocols() {
        return getString(TLS_PROTOCOLS, "TLSv1.1,TLSv1.2");
    }

    public ServerConfiguration setNumLayoutServerThreads(int numThreads) {
        setProperty(NUM_LAYOUT_SERVER_THREADS, numThreads);
        return this;
    }

    public int getNumLayoutServerThreads() {
        return getInt(NUM_LAYOUT_SERVER_THREADS, 8);
    }

    public ServerConfiguration setInMemoryMode(boolean inMemoryMode) {
        setProperty(IN_MEMORY_MODE, inMemoryMode);
        return this;
    }

    public boolean isInMemoryMode() {
        return getBoolean(IN_MEMORY_MODE, false);
    }

    public ServerConfiguration setLogUnitCacheRatio(double cacheRatio) {
        setProperty(LOG_UNIT_CACHE_RATIO, cacheRatio);
        return this;
    }

    public double getLogUnitCacheRatio() {
        return getDouble(LOG_UNIT_CACHE_RATIO, 0.5);
    }

    public ServerConfiguration setVerifyChecksum(boolean verifyChecksum) {
        setProperty(VERIFY_CHECKSUM, verifyChecksum);
        return this;
    }

    public boolean getVerifyChecksum() {
        return getBoolean(VERIFY_CHECKSUM, true);
    }

    public ServerConfiguration setSyncData(boolean syncData) {
        setProperty(SYNC_DATA, syncData);
        return this;
    }

    public boolean getSyncData() {
        return getBoolean(SYNC_DATA, true);
    }

    public ServerConfiguration setAutoCommit(boolean autoCommit) {
        setProperty(AUTO_COMMIT, autoCommit);
        return this;
    }

    public boolean getAutoCommit() {
        return getBoolean(AUTO_COMMIT, true);
    }

    public ServerConfiguration setNumLogUnitWorkerThreads(int numThreads) {
        setProperty(NUM_LOGUNIT_WORKER_THREADS, numThreads);
        return this;
    }

    public int getNumLogUnitWorkerThreads() {
        return getInt(NUM_LOGUNIT_WORKER_THREADS, 8);
    }

    public ServerConfiguration setLogSizeQuota(double logSizeQuota) {
        setProperty(LOG_SIZE_QUOTA, logSizeQuota);
        return this;
    }

    public double getLogSizeQuota() {
        return getDouble(LOG_SIZE_QUOTA, 100);
    }

    public long getMaxLogUnitCacheSize() {
        return (long) (Runtime.getRuntime().maxMemory() * getLogUnitCacheRatio());
    }

    public ServerConfiguration setSequencerConflictWindowSize(int size) {
        setProperty(SEQUENCER_CONFLICT_WINDOW_SIZE, size);
        return this;
    }

    public int getSequencerConflictWindowSize() {
        return getInt(SEQUENCER_CONFLICT_WINDOW_SIZE, 250000);
    }

    public ServerConfiguration setStateTransferBatchSize(int batchSize) {
        setProperty(STATE_TRANSFER_BATCH_SIZE, batchSize);
        return this;
    }

    public int getStateTransferBatchSize() {
        return getInt(STATE_TRANSFER_BATCH_SIZE, 100);
    }

    public ServerConfiguration numManagementServerThreads(int numThreads) {
        setProperty(NUM_MANAGEMENT_SERVER_THREADS, numThreads);
        return this;
    }

    public int getNumManagementServerThreads() {
        return getInt(NUM_MANAGEMENT_SERVER_THREADS, 4);
    }

    public ServerConfiguration setMaxReplicationDataMessageSize(int maxReplicationDataMessageSize) {
        setProperty(MAX_REPLICATION_DATA_MESSAGE_SIZE, maxReplicationDataMessageSize);
        return this;
    }

    public int getMaxReplicationDataMessageSize() {
        return getInt(MAX_REPLICATION_DATA_MESSAGE_SIZE,MAX_DATA_MSG_SIZE_SUPPORTED);
    }

    public ServerConfiguration setCompactRate(int compactRate) {
        setProperty(COMPACT_RATE, compactRate);
        return this;
    }

    public int getCompactRate() {
        return getInt(COMPACT_RATE, 60);
    }

    public ServerConfiguration setPluginConfigFilePath(String pluginConfigFilePath) {
        setProperty(PLUGIN_CONFIG_FILE_PATH, pluginConfigFilePath);
        return this;
    }

    public String getPluginConfigFilePath() {
        return getString(PLUGIN_CONFIG_FILE_PATH);
    }

    public ServerConfiguration setEnableMetrics(boolean enableMetrics) {
        setProperty(ENABLE_METRICS, enableMetrics);
        return this;
    }

    public boolean isMetricsEnabled() {
        return getBoolean(ENABLE_METRICS, false);
    }

    public ServerConfiguration setSnapshotBatchSize(int snapshotBatchSize) {
        setProperty(SNAPSHOT_BATCH_SIZE, snapshotBatchSize);
        return this;
    }

    public int getSnapshotBatchSize() {
        return getInt(SNAPSHOT_BATCH_SIZE, DEFAULT_MAX_NUM_MSG_PER_BATCH);
    }

    public ServerConfiguration setLockLeaseDuration(int lockLeaseDuration) {
        setProperty(LOCK_LEASE_DURATION, lockLeaseDuration);
        return this;
    }

    public int getLockLeaseDuration() {
        return getInt(LOCK_LEASE_DURATION, Lock.leaseDuration);
    }

    public ServerConfiguration setThreadPrefix(String threadPrefix) {
        setProperty(THREAD_PREFIX, threadPrefix);
        return this;
    }

    public String getThreadPrefix() {
        return getString(THREAD_PREFIX, "");
    }

    public ServerConfiguration setBindToAllInterfaces(boolean bindToAllInterfaces) {
        setProperty(BIND_TO_ALL_INTERFACES, bindToAllInterfaces);
        return this;
    }

    public boolean getBindToAllInterfaces() {
        return getBoolean(BIND_TO_ALL_INTERFACES, false);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        for (Iterator<String> iter = getKeys(); iter.hasNext();) {
            String key = iter.next();
            Object val = getProperty(key);
            sb.append(key).append("=").append(val).append(", ");
        }
        sb.append("}");
        return sb.toString();
    }
}