package org.corfudb.infrastructure.configuration;

import java.util.HashMap;
import java.util.Map;

import static org.corfudb.infrastructure.configuration.ServerConfigurationOptions.*;
import static org.corfudb.infrastructure.configuration.ServerConfigurationOptions.THREAD_PREFIX;
/**
 * This class creates the mapping from docopt CLI arguments to Server Configuration options.
 *
 * <p>Created by nvaishampayan517 on 07/26/21.
 */
public final class CLIArgumentOptionsMappingUtil {
    // Prevent class from being instantiated
    private CLIArgumentOptionsMappingUtil() {}

    private static void putOptionsToPropertiesFlags(Map<String, String> mapping) {
        mapping.put("--memory", IN_MEMORY_MODE);
        mapping.put("--no-verify", VERIFY_CHECKSUM);
        mapping.put("--no-sync", SYNC_DATA);
        mapping.put("--single", SINGLE_MODE);
        mapping.put("--no-auto-commit", AUTO_COMMIT);
        mapping.put("--enable-tls", ENABLE_TLS);
        mapping.put("--enable-tls-mutual-auth", ENABLE_TLS_MUTUAL_AUTH);
        mapping.put("--enable-sasl-plain-text-auth", ENABLE_SASL_PLAIN_TEXT_AUTH);
        mapping.put("--metrics", ENABLE_METRICS);
    }

    private static void putOptionsToPropertiesIntegral(Map<String,String> mapping) {
        mapping.put("--max-replication-data-message-size", MAX_REPLICATION_DATA_MESSAGE_SIZE);
        mapping.put("--cache-heap-ratio", LOG_UNIT_CACHE_RATIO);
        mapping.put("--compact", COMPACT_RATE);
        mapping.put("--base-server-threads", NUM_BASE_SERVER_THREADS);
        mapping.put("--log-size-quota-percentage", LOG_SIZE_QUOTA);
        mapping.put("--logunit-threads", NUM_LOGUNIT_WORKER_THREADS);
        mapping.put("--management-server-threads", NUM_MANAGEMENT_SERVER_THREADS);
        mapping.put("--Threads", NUM_IO_THREADS);
        mapping.put("--sequencer-cache-size", SEQUENCER_CACHE_SIZE);
        mapping.put("--batch-size", SNAPSHOT_BATCH_SIZE);
        mapping.put("--HandshakeTimeout", HANDSHAKE_TIMEOUT);
        mapping.put("--snapshot-batch", SNAPSHOT_BATCH_SIZE);
        mapping.put("--lock-lease", LOCK_LEASE_DURATION);
        mapping.put("--metadata-retention", METADATA_RETENTION);
        mapping.put("<port>", SERVER_PORT);
    }

    private static void putOptionsToPropertiesString(Map<String,String> mapping) {
        mapping.put("--log-path", SERVER_DIR);
        mapping.put("--network-interface", NETWORK_INTERFACE);
        mapping.put("--address", HOST_ADDRESS);
        mapping.put("--log-level", LOG_LEVEL);
        mapping.put("--plugin", PLUGIN_CONFIG_FILE_PATH);
        mapping.put("--keystore", KEYSTORE);
        mapping.put("--keystore-password-file", KEYSTORE_PASSWORD_FILE);
        mapping.put("--truststore", TRUSTSTORE);
        mapping.put("--truststore-password-file", TRUSTSTORE_PASSWORD_FILE);
        mapping.put("--sasl-plain-text-username-file", SASL_PLAIN_TEXT_USERNAME_FILE);
        mapping.put("--sasl-plain-text-password-file", SASL_PLAIN_TEXT_PASSWORD_FILE);
        mapping.put("--implementation", CHANNEL_IMPLEMENTATION);
        mapping.put("--cluster-id", CLUSTER_ID);
        mapping.put("--tls-ciphers", TLS_CIPHERS);
        mapping.put("--tls-protocols", TLS_PROTOCOLS);
        mapping.put("--Prefix", THREAD_PREFIX);
    }

    public static Map<String, String> getOptionsToPropertiesMapping() {
        Map<String, String> mapping = new HashMap<>();

        putOptionsToPropertiesFlags(mapping);
        putOptionsToPropertiesIntegral(mapping);
        putOptionsToPropertiesString(mapping);

        return mapping;
    }
}
