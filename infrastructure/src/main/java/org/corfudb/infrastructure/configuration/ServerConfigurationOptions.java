package org.corfudb.infrastructure.configuration;

/**
 * This class holds constant values for server configuration options.
 *
 * <p>Created by nvaishampayan517 on 07/26/21.
 */
public final class ServerConfigurationOptions {
    // Prevent class from being instantiated
    private ServerConfigurationOptions() {}

    // Server general parameters
    public static final String SERVER_DIR = "log-path";
    public static final String SINGLE_MODE = "single-mode";
    public static final String CLUSTER_ID = "cluster-id";
    public static final String NUM_IO_THREADS = "io-threads";
    public static final String HOST_ADDRESS = "address";
    public static final String SERVER_PORT = "port";
    public static final String NETWORK_INTERFACE = "network-interface";
    public static final String HANDSHAKE_TIMEOUT = "handshake-timeout";
    public static final String METADATA_RETENTION = "metadata-retention";
    public static final String LOG_LEVEL = "log-level";
    public static final String NUM_BASE_SERVER_THREADS = "base-server-threads";
    public static final String METRICS_PROVIDER_ADDRESS = "metricsProviderAddress";
    public static final String CHANNEL_IMPLEMENTATION = "channel-implementation";
    public static final String ENABLE_TLS = "tls-enabled";
    public static final String ENABLE_TLS_MUTUAL_AUTH = "tls-mutual-auth-enabled";
    public static final String KEYSTORE = "keystore";
    public static final String KEYSTORE_PASSWORD_FILE = "keystore-password-file";
    public static final String TRUSTSTORE = "truststore";
    public static final String TRUSTSTORE_PASSWORD_FILE = "truststore-password-file";
    public static final String ENABLE_SASL_PLAIN_TEXT_AUTH = "sasl-plain-text-auth-enabled";
    public static final String SASL_PLAIN_TEXT_USERNAME_FILE = "sasl-plain-text-username-file";
    public static final String SASL_PLAIN_TEXT_PASSWORD_FILE = "sasl-plain-text-password-file";
    public static final String TLS_CIPHERS = "tls-ciphers";
    public static final String TLS_PROTOCOLS = "tls-protocols";

    // LogUnit parameters
    public static final String IN_MEMORY_MODE = "memory-mode";
    public static final String LOG_UNIT_CACHE_RATIO = "cache-heap-ratio";
    public static final String VERIFY_CHECKSUM = "verify-checksum";
    public static final String SYNC_DATA = "sync-data";
    public static final String NUM_LOGUNIT_WORKER_THREADS = "logunit-threads";
    public static final String LOG_SIZE_QUOTA = "log-size-quota-percentage";


    // Sequencer parameters
    public static final String SEQUENCER_CACHE_SIZE = "sequencer-cache-size";

    // Management parameters
    public static final String STATE_TRANSFER_BATCH_SIZE = "state-transfer-batch-size";
    public static final String NUM_MANAGEMENT_SERVER_THREADS = "management-server-threads";

    //Added parameters
    public static final String AUTO_COMMIT = "auto-commit";
    public static final String MAX_REPLICATION_DATA_MESSAGE_SIZE = "max-replication-data-message-size";
    public static final String COMPACT_RATE = "compact-rate";
    public static final String PLUGIN_CONFIG_FILE_PATH = "plugin-config-file-path";
    public static final String ENABLE_METRICS = "metrics-enabled";
    public static final String SNAPSHOT_BATCH_SIZE = "snapshot-batch";
    public static final String LOCK_LEASE_DURATION = "lock-lease";
    public static final String THREAD_PREFIX = "thread-prefix";
    public static final String BIND_TO_ALL_INTERFACES = "bindToAllInterfaces";
}
