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
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.corfudb.infrastructure.configuration.ServerConfigurationDefaultOptionsConstants.DEFAULT_BASE_SERVER_THREADS;
import static org.corfudb.infrastructure.configuration.ServerConfigurationDefaultOptionsConstants.DEFAULT_CHANNEL_IMPLEMENTATION_TYPE;
import static org.corfudb.infrastructure.configuration.ServerConfigurationDefaultOptionsConstants.DEFAULT_CLUSTER_ID;
import static org.corfudb.infrastructure.configuration.ServerConfigurationDefaultOptionsConstants.DEFAULT_COMPACT_RATE;
import static org.corfudb.infrastructure.configuration.ServerConfigurationDefaultOptionsConstants.DEFAULT_HANDSHAKE_TIMEOUT;
import static org.corfudb.infrastructure.configuration.ServerConfigurationDefaultOptionsConstants.DEFAULT_IO_THREADS;
import static org.corfudb.infrastructure.configuration.ServerConfigurationDefaultOptionsConstants.DEFAULT_MANAGEMENT_SERVER_THREADS;
import static org.corfudb.infrastructure.configuration.ServerConfigurationDefaultOptionsConstants.DEFAULT_METADATA_RETENTION;
import static org.corfudb.infrastructure.configuration.ServerConfigurationDefaultOptionsConstants.DEFAULT_LOG_LEVEL;
import static org.corfudb.infrastructure.configuration.ServerConfigurationDefaultOptionsConstants.DEFAULT_LOG_SIZE_QUOTA;
import static org.corfudb.infrastructure.configuration.ServerConfigurationDefaultOptionsConstants.DEFAULT_LOG_UNIT_CACHE_RATIO;
import static org.corfudb.infrastructure.configuration.ServerConfigurationDefaultOptionsConstants.DEFAULT_LOG_UNIT_WORKER_THREADS;
import static org.corfudb.infrastructure.configuration.ServerConfigurationDefaultOptionsConstants.DEFAULT_SEQUENCER_CACHE_SIZE;
import static org.corfudb.infrastructure.configuration.ServerConfigurationDefaultOptionsConstants.DEFAULT_STATE_TRANSFER_BATCH_SIZE;
import static org.corfudb.infrastructure.configuration.ServerConfigurationDefaultOptionsConstants.DEFAULT_THREAD_PREFIX;
import static org.corfudb.infrastructure.configuration.ServerConfigurationDefaultOptionsConstants.DEFAULT_TLS_CIPHERS;
import static org.corfudb.infrastructure.configuration.ServerConfigurationDefaultOptionsConstants.DEFAULT_TLS_PROTOCOLS;
import static org.corfudb.infrastructure.configuration.ServerConfigurationOptionsConstants.AUTO_COMMIT;
import static org.corfudb.infrastructure.configuration.ServerConfigurationOptionsConstants.BIND_TO_ALL_INTERFACES;
import static org.corfudb.infrastructure.configuration.ServerConfigurationOptionsConstants.CHANNEL_IMPLEMENTATION;
import static org.corfudb.infrastructure.configuration.ServerConfigurationOptionsConstants.CLUSTER_ID;
import static org.corfudb.infrastructure.configuration.ServerConfigurationOptionsConstants.COMPACT_RATE;
import static org.corfudb.infrastructure.configuration.ServerConfigurationOptionsConstants.ENABLE_METRICS;
import static org.corfudb.infrastructure.configuration.ServerConfigurationOptionsConstants.ENABLE_SASL_PLAIN_TEXT_AUTH;
import static org.corfudb.infrastructure.configuration.ServerConfigurationOptionsConstants.ENABLE_TLS;
import static org.corfudb.infrastructure.configuration.ServerConfigurationOptionsConstants.ENABLE_TLS_MUTUAL_AUTH;
import static org.corfudb.infrastructure.configuration.ServerConfigurationOptionsConstants.HANDSHAKE_TIMEOUT;
import static org.corfudb.infrastructure.configuration.ServerConfigurationOptionsConstants.HOST_ADDRESS;
import static org.corfudb.infrastructure.configuration.ServerConfigurationOptionsConstants.IN_MEMORY_MODE;
import static org.corfudb.infrastructure.configuration.ServerConfigurationOptionsConstants.KEYSTORE;
import static org.corfudb.infrastructure.configuration.ServerConfigurationOptionsConstants.KEYSTORE_PASSWORD_FILE;
import static org.corfudb.infrastructure.configuration.ServerConfigurationOptionsConstants.LOCK_LEASE_DURATION;
import static org.corfudb.infrastructure.configuration.ServerConfigurationOptionsConstants.LOG_LEVEL;
import static org.corfudb.infrastructure.configuration.ServerConfigurationOptionsConstants.LOG_SIZE_QUOTA;
import static org.corfudb.infrastructure.configuration.ServerConfigurationOptionsConstants.LOG_UNIT_CACHE_RATIO;
import static org.corfudb.infrastructure.configuration.ServerConfigurationOptionsConstants.MAX_REPLICATION_DATA_MESSAGE_SIZE;
import static org.corfudb.infrastructure.configuration.ServerConfigurationOptionsConstants.METADATA_RETENTION;
import static org.corfudb.infrastructure.configuration.ServerConfigurationOptionsConstants.METRICS_PROVIDER_ADDRESS;
import static org.corfudb.infrastructure.configuration.ServerConfigurationOptionsConstants.NETWORK_INTERFACE;
import static org.corfudb.infrastructure.configuration.ServerConfigurationOptionsConstants.NUM_BASE_SERVER_THREADS;
import static org.corfudb.infrastructure.configuration.ServerConfigurationOptionsConstants.NUM_IO_THREADS;
import static org.corfudb.infrastructure.configuration.ServerConfigurationOptionsConstants.NUM_LOGUNIT_WORKER_THREADS;
import static org.corfudb.infrastructure.configuration.ServerConfigurationOptionsConstants.NUM_MANAGEMENT_SERVER_THREADS;
import static org.corfudb.infrastructure.configuration.ServerConfigurationOptionsConstants.PLUGIN_CONFIG_FILE_PATH;
import static org.corfudb.infrastructure.configuration.ServerConfigurationOptionsConstants.SASL_PLAIN_TEXT_PASSWORD_FILE;
import static org.corfudb.infrastructure.configuration.ServerConfigurationOptionsConstants.SASL_PLAIN_TEXT_USERNAME_FILE;
import static org.corfudb.infrastructure.configuration.ServerConfigurationOptionsConstants.SEQUENCER_CACHE_SIZE;
import static org.corfudb.infrastructure.configuration.ServerConfigurationOptionsConstants.SERVER_DIR;
import static org.corfudb.infrastructure.configuration.ServerConfigurationOptionsConstants.SERVER_PORT;
import static org.corfudb.infrastructure.configuration.ServerConfigurationOptionsConstants.SINGLE_MODE;
import static org.corfudb.infrastructure.configuration.ServerConfigurationOptionsConstants.SNAPSHOT_BATCH_SIZE;
import static org.corfudb.infrastructure.configuration.ServerConfigurationOptionsConstants.STATE_TRANSFER_BATCH_SIZE;
import static org.corfudb.infrastructure.configuration.ServerConfigurationOptionsConstants.SYNC_DATA;
import static org.corfudb.infrastructure.configuration.ServerConfigurationOptionsConstants.THREAD_PREFIX;
import static org.corfudb.infrastructure.configuration.ServerConfigurationOptionsConstants.TLS_CIPHERS;
import static org.corfudb.infrastructure.configuration.ServerConfigurationOptionsConstants.TLS_PROTOCOLS;
import static org.corfudb.infrastructure.configuration.ServerConfigurationOptionsConstants.TRUSTSTORE;
import static org.corfudb.infrastructure.configuration.ServerConfigurationOptionsConstants.TRUSTSTORE_PASSWORD_FILE;
import static org.corfudb.infrastructure.configuration.ServerConfigurationOptionsConstants.VERIFY_CHECKSUM;
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

    // The underlying map of PropertiesConfiguration can't be used to store an EventLoopGroup,
    // so a separate map is needed. This shouldn't be here, but the Unit Tests rely on
    // these event loops to be here
    private final Map<String, EventLoopGroup> testEventLoops = new HashMap<>();




    public static ServerConfiguration getServerConfigFromFile(String configFilePath) {
        Properties configProperties = new Properties();

        try (FileInputStream configFileStream = new FileInputStream(configFilePath)) {
            configProperties.load(configFileStream);
        } catch (IOException e) {
            throw new IllegalArgumentException("Unable to load config from file " + configFilePath, e);
        }

        return applyServerConfigurationOptions(configProperties);
    }

    private static void applyFlags(Properties configProperties, ServerConfiguration conf) {
        conf.setVerifyChecksum(Boolean.parseBoolean(configProperties.getProperty(VERIFY_CHECKSUM, "true")));
        conf.setSyncData(Boolean.parseBoolean(configProperties.getProperty(SYNC_DATA, "true")));
        conf.setSingleMode(Boolean.parseBoolean(configProperties.getProperty(SINGLE_MODE, "false")));
        conf.setAutoCommit(Boolean.parseBoolean(configProperties.getProperty(AUTO_COMMIT, "true")));
        conf.setEnableTls(Boolean.parseBoolean(configProperties.getProperty(ENABLE_TLS, "false")));
        conf.setEnableTlsMutualAuth(Boolean.parseBoolean(configProperties.getProperty(ENABLE_TLS_MUTUAL_AUTH, "false")));
        conf.setEnableSaslPlainTextAuth(Boolean.parseBoolean(configProperties.getProperty(ENABLE_SASL_PLAIN_TEXT_AUTH, "false")));
        conf.setEnableMetrics(Boolean.parseBoolean(configProperties.getProperty(ENABLE_METRICS, "false")));
    }

    private static void applyIntegralOptions(Properties configProperties, ServerConfiguration conf) {
        conf.setMaxReplicationDataMessageSize(Integer.parseInt(configProperties.getProperty(MAX_REPLICATION_DATA_MESSAGE_SIZE, Integer.toString(MAX_DATA_MSG_SIZE_SUPPORTED))));
        conf.setLogUnitCacheRatio(Double.parseDouble(configProperties.getProperty(LOG_UNIT_CACHE_RATIO, DEFAULT_LOG_UNIT_CACHE_RATIO)));
        conf.setCompactRate(Integer.parseInt(configProperties.getProperty(COMPACT_RATE, DEFAULT_COMPACT_RATE)));
        conf.setNumBaseServerThreads(Integer.parseInt(configProperties.getProperty(NUM_BASE_SERVER_THREADS, DEFAULT_BASE_SERVER_THREADS)));
        conf.setLogSizeQuota(Double.parseDouble(configProperties.getProperty(LOG_SIZE_QUOTA, DEFAULT_LOG_SIZE_QUOTA)));

        conf.setNumLogUnitWorkerThreads(Integer.parseInt(configProperties.getProperty(NUM_LOGUNIT_WORKER_THREADS, DEFAULT_LOG_UNIT_WORKER_THREADS)));
        conf.setNumManagementServerThreads(Integer.parseInt(configProperties.getProperty(NUM_MANAGEMENT_SERVER_THREADS, DEFAULT_MANAGEMENT_SERVER_THREADS)));
        conf.setNumIOThreads(Integer.parseInt(configProperties.getProperty(NUM_IO_THREADS, DEFAULT_IO_THREADS)));

        conf.setSequencerCacheSize(Integer.parseInt(configProperties.getProperty(SEQUENCER_CACHE_SIZE,DEFAULT_SEQUENCER_CACHE_SIZE)));

        conf.setStateTransferBatchSize(Integer.parseInt(configProperties.getProperty(STATE_TRANSFER_BATCH_SIZE, DEFAULT_STATE_TRANSFER_BATCH_SIZE)));

        conf.setHandshakeTimeout(Integer.parseInt(configProperties.getProperty(HANDSHAKE_TIMEOUT, DEFAULT_HANDSHAKE_TIMEOUT)));
        conf.setSnapshotBatchSize(Integer.parseInt(configProperties.getProperty(SNAPSHOT_BATCH_SIZE, Integer.toString(DEFAULT_MAX_NUM_MSG_PER_BATCH))));
        conf.setLockLeaseDuration(Integer.parseInt(configProperties.getProperty(LOCK_LEASE_DURATION, Integer.toString(Lock.leaseDuration))));

        conf.setMetadataRetention(Integer.parseInt(configProperties.getProperty(METADATA_RETENTION, DEFAULT_METADATA_RETENTION)));

        conf.setServerPort(Integer.parseInt(configProperties.getProperty(SERVER_PORT)));
    }

    private static void applyStringOptions(Properties configProperties, ServerConfiguration conf) {
        conf.setLogLevel(configProperties.getProperty(LOG_LEVEL, DEFAULT_LOG_LEVEL));
        conf.setPluginConfigFilePath(configProperties.getProperty(PLUGIN_CONFIG_FILE_PATH));


        conf.setKeystore(configProperties.getProperty(KEYSTORE));
        conf.setKeystorePasswordFile(configProperties.getProperty(KEYSTORE_PASSWORD_FILE));
        conf.setTruststore(configProperties.getProperty(TRUSTSTORE));
        conf.setTruststorePasswordFile(configProperties.getProperty(TRUSTSTORE_PASSWORD_FILE));
        conf.setSaslPlainTextUserFile(configProperties.getProperty(SASL_PLAIN_TEXT_USERNAME_FILE));
        conf.setSaslPlainTextPasswordFile(configProperties.getProperty(SASL_PLAIN_TEXT_PASSWORD_FILE));

        conf.setClusterId(configProperties.getProperty(CLUSTER_ID, DEFAULT_CLUSTER_ID));
        conf.setTlsCiphers(configProperties.getProperty(TLS_CIPHERS,DEFAULT_TLS_CIPHERS));
        conf.setTlsProtocols(configProperties.getProperty(TLS_PROTOCOLS, DEFAULT_TLS_PROTOCOLS));

        conf.setThreadPrefix(configProperties.getProperty(THREAD_PREFIX, DEFAULT_THREAD_PREFIX));
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

        String implementationType = configProperties.getProperty(CHANNEL_IMPLEMENTATION,DEFAULT_CHANNEL_IMPLEMENTATION_TYPE);
        conf.setChannelImplementation(ChannelImplementation.valueOf(implementationType.toUpperCase()));

        applyFlags(configProperties, conf);
        applyIntegralOptions(configProperties, conf);
        applyStringOptions(configProperties, conf);

        return conf;
    }

    public static ServerConfiguration getServerConfigFromMap(Map<String, Object> opts, Map<String, String> optionsToPropertiesMapping) {
        Properties configProperties = new Properties();
        Set<String> flags = ImmutableSet.of("--memory", "--single", "--enable-tls",
                "--enable-tls-mutual-auth","--enable-sasl-plain-text-auth", "--metrics");
        Set<String> inverseFlags = ImmutableSet.of("--no-verify", "--no-sync", "--no-auto-commit");
        for (Map.Entry<String,Object> entry : opts.entrySet()) {
            if (!optionsToPropertiesMapping.containsKey(entry.getKey())) {
                log.warn("Encountered unknown option: {}", entry.getKey());
            } else if (entry.getValue() != null) {
                if (flags.contains(entry.getKey())) {
                    configProperties.setProperty(optionsToPropertiesMapping.get(entry.getKey()),
                            Boolean.toString((Boolean) entry.getValue()));
                } else if (inverseFlags.contains(entry.getKey())) {
                    configProperties.setProperty(optionsToPropertiesMapping.get(entry.getKey()),
                            Boolean.toString(!((Boolean) entry.getValue())));
                } else {
                    configProperties.setProperty(optionsToPropertiesMapping.get(entry.getKey()), (String) entry.getValue());
                }
            }
        }

        return applyServerConfigurationOptions(configProperties);
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
