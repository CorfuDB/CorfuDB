package org.corfudb.infrastructure.logreplication;

import lombok.Data;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

@Data
@Slf4j
public class LogReplicationConfig {

    /*
     * Unique identifiers for all streams to be replicated across sites.
     */
    private Set<String> streamsToReplicate;

    /*
     * Unique identifier of the current site ID.
     */
    private UUID siteID;

    /*
     * Unique identifier of the remote/destination site ID.
     */
    private UUID remoteSiteID;

    private static final String STREAMS_TO_REPLICATE_KEY = "StreamsToReplicate";
    private static final String REMOTE_SITE_UUID_KEY = "RemoteSiteId";

    /**
     * Constructor
     *
     * @param streamsToReplicate Unique identifiers for all streams to be replicated across sites.
     * @param remoteSiteID Unique identifier of the remote/destination site.
     */
    public LogReplicationConfig(Set<String> streamsToReplicate, @NonNull UUID remoteSiteID) {
        this.streamsToReplicate = streamsToReplicate;
        this.remoteSiteID = remoteSiteID;
    }

    public static LogReplicationConfig fromFile(String filePath) {

        try (InputStream input = new FileInputStream(filePath)) {

            Properties prop = new Properties();

            if (input == null) {
                log.info("Log Replication Config {} not found, DEFAULT values will be used.", filePath);
                return getDefaultConfig();

            }

            // Load the properties file
            prop.load(input);

            // Get Values by Keys from Properties File
            Set<String> streamsToReplicate = new HashSet<>(Arrays.asList(prop.getProperty(STREAMS_TO_REPLICATE_KEY).split(",")));
            UUID remoteSiteId = UUID.fromString(prop.getProperty(REMOTE_SITE_UUID_KEY));

            log.info("Loaded log replication config: streams to replicate [{}], remote site id {}", streamsToReplicate, remoteSiteId);
            return new LogReplicationConfig(streamsToReplicate, remoteSiteId);

        } catch (Exception e) {
            log.error("Caught exception while reading log replication config file {}", filePath, e);
        }

        return getDefaultConfig();
    }

    private static LogReplicationConfig getDefaultConfig() {
        Set<String> streamsToReplicate = new HashSet<>(Arrays.asList("Table001", "Table002", "Table003"));
        return new LogReplicationConfig(streamsToReplicate, UUID.randomUUID());
    }
}