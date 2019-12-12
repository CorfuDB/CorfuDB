package org.corfudb.infrastructure.configuration;


import io.netty.channel.EventLoopGroup;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.corfudb.comm.ChannelImplementation;

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
    private static final String COMPRESSION_CODEC = "compressionCodec";
    private static final String VERIFY_CHECKSUM = "verifyChecksum";
    private static final String SYNC_DATA = "syncData";
    private static final String NUM_LOGUNIT_WORKER_THREADS = "numLogUnitWorkerThreads";
    private static final String LOG_SIZE_QUOTA = "logSizeQuota";


    // Sequencer parameters
    private static final String SEQUENCER_CONFLICT_WINDOW_SIZE = "sequencerConflictWindowSize";

    // Management parameters
    private static final String STATE_TRANSFER_BATCH_SIZE = "stateTransferBatchSize";
    private static final String NUM_MANAGEMENT_SERVER_THREADS = "numManagementServerThreads";

    // use unmodifiableConfiguration



    public static ServerConfiguration getServerConfigFromCommandLineArg(CommandLine cmdOptions) throws
            ConfigurationException {
        ServerConfiguration conf = new ServerConfiguration();
        // merge command line with conf
        // This mapping is a temporary solution until we merge the command line config names
        // with the config names in this file
        conf.setServerDirectory(cmdOptions.getOptionValue("log-path"));
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

        // Special handling is needed because the port can be specified without
        // an option name
        if (cmdOptions.getArgList().size() == 1) {
            int port = Integer.valueOf(cmdOptions.getArgList().get(0));
            conf.setServerPort(port);
        } else if (!cmdOptions.getArgList().isEmpty()) {
            throw new IllegalArgumentException("Unknown arguments " + cmdOptions.getArgList());
        }

        if (cmdOptions.hasOption("conf")) {
            // if a config file is specified, it will override default values and
            // other command like parameters
            String confPath = cmdOptions.getOptionValue("conf");
            conf.load(confPath);
        }

        return conf;
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
        return getString(CLUSTER_ID, "auto");
    }

    public ServerConfiguration setNumIOThreads(int num) {
        setProperty(NUM_IO_THREADS, num);
        return this;
    }

    public int getNumIOThreads() {
        return getInt(NUM_IO_THREADS, 8);
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

    public ServerConfiguration setLogLevel(String level) {
        setProperty(LOG_LEVEL, level);
        return this;
    }

    public String getLogLevel() {
        return getString(LOG_LEVEL, "INFO");
    }

    public ServerConfiguration setNumBaseServerThreads(int numThreads) {
        setProperty(NUM_BASE_SERVER_THREADS, numThreads);
        return this;
    }

    public int getNumBaseServerThreads() {
        return getInt(NUM_BASE_SERVER_THREADS, 4);
    }

    public ServerConfiguration setMetricsProviderAddress(String address) {
        setProperty(METRICS_PROVIDER_ADDRESS, address);
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

    public boolean getBindToAllInterfaces() {
        if (getNetworkInterface() != null || getHostAddress() != null) {
            return false;
        } else {
            return true;
        }
    }

    //todo(maithem) need to remove this, workaround library bug
    Map<String, EventLoopGroup> testEventLoops = new HashMap<>();

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
        return getInt(NUM_LAYOUT_SERVER_THREADS, 4);
    }

    public ServerConfiguration setInMemoryMode(boolean inMemoryMode) {
        setProperty(IN_MEMORY_MODE, inMemoryMode);
        return this;
    }

    // todo(maithem) rename to is inmemory mode?
    public boolean getInMemoryMode() {
        return getBoolean(IN_MEMORY_MODE, false);
    }

    public ServerConfiguration setLogUnitCacheRatio(double cacheRatio) {
        setProperty(LOG_UNIT_CACHE_RATIO, cacheRatio);
        return this;
    }

    public double getLogUnitCacheRatio() {
        return getDouble(LOG_UNIT_CACHE_RATIO, 0.5);
    }

    public ServerConfiguration setCompressionCodec(String codec) {
        //use enums
        setProperty(COMPRESSION_CODEC, codec);
        return this;
    }

    public String getCompressionCodec() {
        return getString(COMPRESSION_CODEC, "ZSTD");
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

    @Override
    public String toString() {
        Iterator<String> keys = getKeys();
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        while (keys.hasNext()) {
            String key = keys.next();
            sb.append(key).append(" = ").append(getProperty(key)).append("\n");
        }
        sb.append("}");
        return sb.toString();
    }

}