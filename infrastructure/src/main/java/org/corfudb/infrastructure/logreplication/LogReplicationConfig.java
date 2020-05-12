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

    private static final String REMOTE_SITE_UUID_KEY = "RemoteSiteId";

    /**
     * Constructor
     *
     * @param streamsToReplicate Unique identifiers for all streams to be replicated across sites.
     * @param remoteSiteID Unique identifier of the remote/destination site.
     */
    public LogReplicationConfig(Set<String> streamsToReplicate, @NonNull UUID siteID, @NonNull UUID remoteSiteID) {
        this.streamsToReplicate = streamsToReplicate;
        this.siteID = siteID;
        this.remoteSiteID = remoteSiteID;
    }

    /**
     * TODO: Perhaps we want to keep this for testing?
     * @return
     */
    private static LogReplicationConfig getDefaultConfig() {
        Set<String> streamsToReplicate = new HashSet<>(Arrays.asList("Table001", "Table002", "Table003"));
        return new LogReplicationConfig(streamsToReplicate, UUID.randomUUID(), UUID.randomUUID());
    }
}