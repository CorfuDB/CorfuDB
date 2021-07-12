package org.corfudb.infrastructure.configuration;

import ch.qos.logback.classic.Level;
import com.google.common.collect.ImmutableSet;
import io.netty.channel.EventLoopGroup;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration.BaseConfiguration;
import org.corfudb.comm.ChannelImplementation;
import org.corfudb.utils.lock.Lock;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.corfudb.infrastructure.logreplication.LogReplicationConfig.DEFAULT_MAX_NUM_MSG_PER_BATCH;
import static org.corfudb.infrastructure.logreplication.LogReplicationConfig.MAX_DATA_MSG_SIZE_SUPPORTED;
import static org.corfudb.util.NetworkUtils.getAddressFromInterfaceName;

/**
 * This class holds various server configuration parameters.
 *
 * <p>Created by maithem on 12/4/19.
 */
@Slf4j
public class ServerConfiguration extends BaseConfiguration {
    // Server general parameters
    private static final String SERVER_DIR = "log-path";
    private static final String SINGLE_MODE = "single-mode";
    private static final String CLUSTER_ID = "cluster-id";
    private static final String NUM_IO_THREADS = "io-threads";
    private static final String HOST_ADDRESS = "address";
    private static final String SERVER_PORT = "port";
    private static final String NETWORK_INTERFACE = "network-interface";
    private static final String HANDSHAKE_TIMEOUT = "handshake-timeout";
    private static final String METADATA_RETENTION = "metadata-retention";
    private static final String LOG_LEVEL = "log-level";
    private static final String NUM_BASE_SERVER_THREADS = "base-server-threads";
    private static final String METRICS_PROVIDER_ADDRESS = "metricsProviderAddress";
    private static final String CHANNEL_IMPLEMENTATION = "channel-implementation";
    private static final String ENABLE_TLS = "tls-enabled";
    private static final String ENABLE_TLS_MUTUAL_AUTH = "tls-mutual-auth-enabled";
    private static final String KEYSTORE = "keystore";
    private static final String KEYSTORE_PASSWORD_FILE = "keystore-password-file";
    private static final String TRUSTSTORE = "truststore";
    private static final String TRUSTSTORE_PASSWORD_FILE = "truststore-password-file";
    private static final String ENABLE_SASL_PLAIN_TEXT_AUTH = "sasl-plain-text-auth-enabled";
    private static final String SASL_PLAIN_TEXT_USERNAME_FILE = "sasl-plain-text-username-file";
    private static final String SASL_PLAIN_TEXT_PASSWORD_FILE = "sasl-plain-text-password-file";
    private static final String TLS_CIPHERS = "tls-ciphers";
    private static final String TLS_PROTOCOLS = "tls-protocols";

    // LogUnit parameters
    private static final String IN_MEMORY_MODE = "memory-mode";
    private static final String LOG_UNIT_CACHE_RATIO = "cache-heap-ratio";
    private static final String VERIFY_CHECKSUM = "verify-checksum";
    private static final String SYNC_DATA = "sync-data";
    private static final String NUM_LOGUNIT_WORKER_THREADS = "logunit-threads";
    private static final String LOG_SIZE_QUOTA = "log-size-quota-percentage";


    // Sequencer parameters
    private static final String SEQUENCER_CACHE_SIZE = "sequencer-cache-size";

    // Management parameters
    private static final String STATE_TRANSFER_BATCH_SIZE = "state-transfer-batch-size";
    private static final String NUM_MANAGEMENT_SERVER_THREADS = "management-server-threads";

    //Added parameters
    private static final String AUTO_COMMIT = "auto-commit";
    private static final String MAX_REPLICATION_DATA_MESSAGE_SIZE = "max-replication-data-message-size";
    private static final String COMPACT_RATE = "compact-rate";
    private static final String PLUGIN_CONFIG_FILE_PATH = "plugin-config-file-path";
    private static final String ENABLE_METRICS = "metrics-enabled";
    private static final String SNAPSHOT_BATCH_SIZE = "snapshot-batch";
    private static final String LOCK_LEASE_DURATION = "lock-lease";
    private static final String THREAD_PREFIX = "thread-prefix";
    private static final String BIND_TO_ALL_INTERFACES = "bindToAllInterfaces";

    //Default Configuration Values
    private static final String DEFAULT_LOG_UNIT_CACHE_RATIO = "0.5";
    private static final String DEFAULT_LOG_LEVEL = "INFO";
    private static final String DEFAULT_COMPACT_RATE = "60";
    private static final String DEFAULT_BASE_SERVER_THREADS = "1";
    private static final String DEFAULT_LOG_SIZE_QUOTA = "100.0";
    private static final String DEFAULT_LOG_UNIT_WORKER_THREADS = "4";
    private static final String DEFAULT_MANAGEMENT_SERVER_THREADS = "4";
    private static final String DEFAULT_IO_THREADS = "4";
    private static final String DEFAULT_SEQUENCER_CACHE_SIZE = "250000";
    private static final String DEFAULT_STATE_TRANSFER_BATCH_SIZE = "100";
    private static final String DEFAULT_CHANNEL_IMPLEMENTATION_TYPE = "nio";
    private static final String DEFAULT_HANDSHAKE_TIMEOUT = "10";
    private static final String DEFAULT_CLUSTER_ID = "auto";
    private static final String DEFAULT_TLS_CIPHERS = "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256";
    private static final String DEFAULT_TLS_PROTOCOLS = "TLSv1.1,TLSv1.2";
    private static final String DEFAULT_THREAD_PREFIX = "";
    private static final String DEFAULT_METADATA_RETENTION = "1000";

    // The underlying map of PropertiesConfiguration can't be used to store an EventLoopGroup,
    // so a separate map is needed. This shouldn't be here, but the Unit Tests rely on
    // these event loops to be here
    private final Map<String, EventLoopGroup> testEventLoops = new HashMap<>();




    public static ServerConfiguration getServerConfigFromFile(String configFilePath) {
        FileInputStream configFileStream;
        Properties configProperties = new Properties();

        try {
            configFileStream = new FileInputStream(configFilePath);
            configProperties.load(configFileStream);
        } catch (IOException e) {
            throw new IllegalArgumentException("Unable to load config from file " + configFilePath, e);
        }

        return applyServerConfigurationOptions(configProperties);
    }

    private static ServerConfiguration applyServerConfigurationOptions(Properties configProperties) {

        ServerConfiguration conf = new ServerConfiguration();
        conf.setDelimiterParsingDisabled(true);

        if (Boolean.parseBoolean(configProperties.getProperty(IN_MEMORY_MODE, "false"))) {
            conf.setInMemoryMode(true);
        } else if (!configProperties.containsKey(SERVER_DIR)) {
            throw new IllegalStateException("Configuration must either provide log-path or be set to memory mode");
        } else {
            conf.setServerDirectory(configProperties.getProperty(SERVER_DIR));
        }

        conf.setVerifyChecksum(Boolean.parseBoolean(configProperties.getProperty(VERIFY_CHECKSUM, "true")));
        conf.setSyncData(Boolean.parseBoolean(configProperties.getProperty(SYNC_DATA, "true")));
        conf.setSingleMode(Boolean.parseBoolean(configProperties.getProperty(SINGLE_MODE, "false")));
        conf.setAutoCommit(Boolean.parseBoolean(configProperties.getProperty(AUTO_COMMIT, "true")));

        String networkInterfaceName = configProperties.getProperty(NETWORK_INTERFACE);
        String address = configProperties.getProperty(HOST_ADDRESS);
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

        conf.setMaxReplicationDataMessageSize(Integer.parseInt(configProperties.getProperty(MAX_REPLICATION_DATA_MESSAGE_SIZE, Integer.toString(MAX_DATA_MSG_SIZE_SUPPORTED))));
        conf.setLogUnitCacheRatio(Double.parseDouble(configProperties.getProperty(LOG_UNIT_CACHE_RATIO, DEFAULT_LOG_UNIT_CACHE_RATIO)));
        conf.setLogLevel(configProperties.getProperty(LOG_LEVEL, DEFAULT_LOG_LEVEL));
        conf.setCompactRate(Integer.parseInt(configProperties.getProperty(COMPACT_RATE, DEFAULT_COMPACT_RATE)));
        conf.setPluginConfigFilePath(configProperties.getProperty(PLUGIN_CONFIG_FILE_PATH));
        conf.setNumBaseServerThreads(Integer.parseInt(configProperties.getProperty(NUM_BASE_SERVER_THREADS, DEFAULT_BASE_SERVER_THREADS)));
        conf.setLogSizeQuota(Double.parseDouble(configProperties.getProperty(LOG_SIZE_QUOTA, DEFAULT_LOG_SIZE_QUOTA)));

        conf.setNumLogUnitWorkerThreads(Integer.parseInt(configProperties.getProperty(NUM_LOGUNIT_WORKER_THREADS, DEFAULT_LOG_UNIT_WORKER_THREADS)));
        conf.setNumManagementServerThreads(Integer.parseInt(configProperties.getProperty(NUM_MANAGEMENT_SERVER_THREADS, DEFAULT_MANAGEMENT_SERVER_THREADS)));
        conf.setNumIOThreads(Integer.parseInt(configProperties.getProperty(NUM_IO_THREADS, DEFAULT_IO_THREADS)));

        conf.setEnableTls(Boolean.parseBoolean(configProperties.getProperty(ENABLE_TLS, "false")));
        conf.setKeystore(configProperties.getProperty(KEYSTORE));
        conf.setKeystorePasswordFile(configProperties.getProperty(KEYSTORE_PASSWORD_FILE));
        conf.setTruststore(configProperties.getProperty(TRUSTSTORE));
        conf.setTruststorePasswordFile(configProperties.getProperty(TRUSTSTORE_PASSWORD_FILE));
        conf.setEnableTlsMutualAuth(Boolean.parseBoolean(configProperties.getProperty(ENABLE_TLS_MUTUAL_AUTH, "false")));
        conf.setEnableSaslPlainTextAuth(Boolean.parseBoolean(configProperties.getProperty(ENABLE_SASL_PLAIN_TEXT_AUTH, "false")));
        conf.setSaslPlainTextUserFile(configProperties.getProperty(SASL_PLAIN_TEXT_USERNAME_FILE));
        conf.setSaslPlainTextPasswordFile(configProperties.getProperty(SASL_PLAIN_TEXT_PASSWORD_FILE));
        conf.setSequencerCacheSize(Integer.parseInt(configProperties.getProperty(SEQUENCER_CACHE_SIZE,DEFAULT_SEQUENCER_CACHE_SIZE)));

        conf.setStateTransferBatchSize(Integer.parseInt(configProperties.getProperty(STATE_TRANSFER_BATCH_SIZE, DEFAULT_STATE_TRANSFER_BATCH_SIZE)));

        String implementationType = configProperties.getProperty(CHANNEL_IMPLEMENTATION,DEFAULT_CHANNEL_IMPLEMENTATION_TYPE);
        conf.setChannelImplementation(ChannelImplementation.valueOf(implementationType.toUpperCase()));

        conf.setHandshakeTimeout(Integer.parseInt(configProperties.getProperty(HANDSHAKE_TIMEOUT, DEFAULT_HANDSHAKE_TIMEOUT)));
        conf.setClusterId(configProperties.getProperty(CLUSTER_ID, DEFAULT_CLUSTER_ID));
        conf.setTlsCiphers(configProperties.getProperty(TLS_CIPHERS,DEFAULT_TLS_CIPHERS));
        conf.setTlsProtocols(configProperties.getProperty(TLS_PROTOCOLS, DEFAULT_TLS_PROTOCOLS));

        conf.setEnableMetrics(Boolean.parseBoolean(configProperties.getProperty(ENABLE_METRICS, "false")));
        conf.setSnapshotBatchSize(Integer.parseInt(configProperties.getProperty(SNAPSHOT_BATCH_SIZE, Integer.toString(DEFAULT_MAX_NUM_MSG_PER_BATCH))));
        conf.setLockLeaseDuration(Integer.parseInt(configProperties.getProperty(LOCK_LEASE_DURATION, Integer.toString(Lock.leaseDuration))));
        conf.setThreadPrefix(configProperties.getProperty(THREAD_PREFIX, DEFAULT_THREAD_PREFIX));
        conf.setMetadataRetention(Integer.parseInt(configProperties.getProperty(METADATA_RETENTION, DEFAULT_METADATA_RETENTION)));

        conf.setServerPort(Integer.parseInt(configProperties.getProperty(SERVER_PORT)));

        return conf;
    }

    public static ServerConfiguration getServerConfigFromMap(Map<String, Object> opts) {
        Properties configProperties = new Properties();
        //in the future, if more formats need to be supported, this mapping can be passed as a parameter
        Map<String, String> optionsToPropertiesMapping = getOptionsToPropertiesMapping();
        Set<String> flags = ImmutableSet.of("--memory", "--single", "--enable-tls",
                "--enable-tls-mutual-auth","--enable-sasl-plain-text-auth", "--metrics");
        Set<String> inverseFlags = ImmutableSet.of("--no-verify", "--no-sync", "--no-auto-commit");
        for (String key : opts.keySet()) {
            if (optionsToPropertiesMapping.containsKey(key)) {
                if (opts.get(key) != null) {
                    if (flags.contains(key)) {
                        configProperties.setProperty(optionsToPropertiesMapping.get(key),
                                Boolean.toString((Boolean) opts.get(key)));
                    } else if (inverseFlags.contains(key)) {
                        configProperties.setProperty(optionsToPropertiesMapping.get(key),
                                Boolean.toString(!((Boolean) opts.get(key))));
                    } else {
                        configProperties.setProperty(optionsToPropertiesMapping.get(key), (String) opts.get(key));
                    }
                }
            } else {
                log.warn("Encountered unknown option: {}", key);
            }
        }

        return applyServerConfigurationOptions(configProperties);
    }

    private static Map<String, String> getOptionsToPropertiesMapping() {
        Map<String, String> mapping = new Hashtable<>();
        mapping.put("--memory", IN_MEMORY_MODE);
        mapping.put("--log-path", SERVER_DIR);
        mapping.put("--no-verify", VERIFY_CHECKSUM);
        mapping.put("--no-sync", SYNC_DATA);
        mapping.put("--single", SINGLE_MODE);
        mapping.put("--no-auto-commit", AUTO_COMMIT);
        mapping.put("--network-interface", NETWORK_INTERFACE);
        mapping.put("--address", HOST_ADDRESS);
        mapping.put("--max-replication-data-message-size", MAX_REPLICATION_DATA_MESSAGE_SIZE);
        mapping.put("--cache-heap-ratio", LOG_UNIT_CACHE_RATIO);
        mapping.put("--log-level", LOG_LEVEL);
        mapping.put("--compact", COMPACT_RATE);
        mapping.put("--plugin", PLUGIN_CONFIG_FILE_PATH);
        mapping.put("--base-server-threads", NUM_BASE_SERVER_THREADS);
        mapping.put("--log-size-quota-percentage", LOG_SIZE_QUOTA);
        mapping.put("--logunit-threads", NUM_LOGUNIT_WORKER_THREADS);
        mapping.put("--management-server-threads", NUM_MANAGEMENT_SERVER_THREADS);
        mapping.put("--Threads", NUM_IO_THREADS);
        mapping.put("--enable-tls", ENABLE_TLS);
        mapping.put("--keystore", KEYSTORE);
        mapping.put("--keystore-password-file", KEYSTORE_PASSWORD_FILE);
        mapping.put("--truststore", TRUSTSTORE);
        mapping.put("--truststore-password-file", TRUSTSTORE_PASSWORD_FILE);
        mapping.put("--enable-tls-mutual-auth", ENABLE_TLS_MUTUAL_AUTH);
        mapping.put("--enable-sasl-plain-text-auth", ENABLE_SASL_PLAIN_TEXT_AUTH);
        mapping.put("--sasl-plain-text-username-file", SASL_PLAIN_TEXT_USERNAME_FILE);
        mapping.put("--sasl-plain-text-password-file", SASL_PLAIN_TEXT_PASSWORD_FILE);
        mapping.put("--sequencer-cache-size", SEQUENCER_CACHE_SIZE);
        mapping.put("--batch-size", SNAPSHOT_BATCH_SIZE);
        mapping.put("--implementation", CHANNEL_IMPLEMENTATION);
        mapping.put("--HandshakeTimeout", HANDSHAKE_TIMEOUT);
        mapping.put("--cluster-id", CLUSTER_ID);
        mapping.put("--tls-ciphers", TLS_CIPHERS);
        mapping.put("--tls-protocols", TLS_PROTOCOLS);
        mapping.put("--metrics", ENABLE_METRICS);
        mapping.put("--snapshot-batch", SNAPSHOT_BATCH_SIZE);
        mapping.put("--lock-lease", LOCK_LEASE_DURATION);
        mapping.put("--Prefix", THREAD_PREFIX);
        mapping.put("--metadata-retention", METADATA_RETENTION);
        mapping.put("<port>", SERVER_PORT);

        return mapping;
    }


    public ServerConfiguration setServerDirectory(String path) {
        setProperty(SERVER_DIR, path);
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
        return getString(CLUSTER_ID, DEFAULT_CLUSTER_ID);
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
        return getInt(HANDSHAKE_TIMEOUT, Integer.parseInt(DEFAULT_HANDSHAKE_TIMEOUT));
    }

    public ServerConfiguration setMetadataRetention(int numFiles) {
        if (numFiles < 1) {
            throw new IllegalArgumentException("Max number of metadata files to retain must be greater than 0.");
        }
        setProperty(METADATA_RETENTION, numFiles);
        return this;
    }

    public int getMetadataRetention() {
        return getInt(METADATA_RETENTION, Integer.parseInt(DEFAULT_METADATA_RETENTION));
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
        return getInt(NUM_BASE_SERVER_THREADS, Integer.parseInt(DEFAULT_BASE_SERVER_THREADS));
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
        return getString(TLS_CIPHERS, DEFAULT_TLS_CIPHERS);
    }

    public ServerConfiguration setTlsProtocols(String ciphers) {
        setProperty(TLS_PROTOCOLS, ciphers);
        return this;
    }

    public String getTlsProtocols() {
        return getString(TLS_PROTOCOLS, DEFAULT_TLS_PROTOCOLS);
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
        return getDouble(LOG_UNIT_CACHE_RATIO, Double.parseDouble(DEFAULT_LOG_UNIT_CACHE_RATIO));
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
        return getInt(NUM_LOGUNIT_WORKER_THREADS, Integer.parseInt(DEFAULT_LOG_UNIT_WORKER_THREADS));
    }

    public ServerConfiguration setLogSizeQuota(double logSizeQuota) {
        setProperty(LOG_SIZE_QUOTA, logSizeQuota);
        return this;
    }

    public double getLogSizeQuota() {
        return getDouble(LOG_SIZE_QUOTA, Double.parseDouble(DEFAULT_LOG_SIZE_QUOTA));
    }

    public long getMaxLogUnitCacheSize() {
        return (long) (Runtime.getRuntime().maxMemory() * getLogUnitCacheRatio());
    }

    public ServerConfiguration setSequencerCacheSize(int size) {
        setProperty(SEQUENCER_CACHE_SIZE, size);
        return this;
    }

    public int getSequencerCacheSize() {
        return getInt(SEQUENCER_CACHE_SIZE, Integer.parseInt(DEFAULT_SEQUENCER_CACHE_SIZE));
    }

    public ServerConfiguration setStateTransferBatchSize(int batchSize) {
        setProperty(STATE_TRANSFER_BATCH_SIZE, batchSize);
        return this;
    }

    public int getStateTransferBatchSize() {
        return getInt(STATE_TRANSFER_BATCH_SIZE, Integer.parseInt(DEFAULT_STATE_TRANSFER_BATCH_SIZE));
    }

    public ServerConfiguration setNumManagementServerThreads(int numThreads) {
        setProperty(NUM_MANAGEMENT_SERVER_THREADS, numThreads);
        return this;
    }

    public int getNumManagementServerThreads() {
        return getInt(NUM_MANAGEMENT_SERVER_THREADS, Integer.parseInt(DEFAULT_MANAGEMENT_SERVER_THREADS));
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
        return getInt(COMPACT_RATE, Integer.parseInt(DEFAULT_COMPACT_RATE));
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
        return getString(THREAD_PREFIX, DEFAULT_THREAD_PREFIX);
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