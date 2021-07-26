package org.corfudb.infrastructure.configuration;

/**
 * This class holds default values for server configuration options.
 *
 * <p>Created by nvaishampayan517 on 07/26/21.
 */
public final class ServerConfigurationDefaultOptions {
    // Prevent class from being instantiated
    private ServerConfigurationDefaultOptions() {}

    //Default Configuration Values
    public static final String DEFAULT_LOG_UNIT_CACHE_RATIO = "0.5";
    public static final String DEFAULT_LOG_LEVEL = "INFO";
    public static final String DEFAULT_COMPACT_RATE = "60";
    public static final String DEFAULT_BASE_SERVER_THREADS = "1";
    public static final String DEFAULT_LOG_SIZE_QUOTA = "100.0";
    public static final String DEFAULT_LOG_UNIT_WORKER_THREADS = "4";
    public static final String DEFAULT_MANAGEMENT_SERVER_THREADS = "4";
    public static final String DEFAULT_IO_THREADS = "4";
    public static final String DEFAULT_SEQUENCER_CACHE_SIZE = "250000";
    public static final String DEFAULT_STATE_TRANSFER_BATCH_SIZE = "100";
    public static final String DEFAULT_CHANNEL_IMPLEMENTATION_TYPE = "nio";
    public static final String DEFAULT_HANDSHAKE_TIMEOUT = "10";
    public static final String DEFAULT_CLUSTER_ID = "auto";
    public static final String DEFAULT_TLS_CIPHERS = "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256";
    public static final String DEFAULT_TLS_PROTOCOLS = "TLSv1.1,TLSv1.2";
    public static final String DEFAULT_THREAD_PREFIX = "";
    public static final String DEFAULT_METADATA_RETENTION = "1000";
}
