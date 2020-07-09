package org.corfudb.infrastructure.logreplication;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.Set;

/**
 * This class represents any Log Replication Configuration,
 * i.e., set of parameters common across all Clusters.
 */
@Slf4j
@Data
public class LogReplicationConfig {

    /*
     * Unique identifiers for all streams to be replicated across sites.
     */
    private Set<String> streamsToReplicate;

    /*
     * Snapshot Sync Batch Size (number of entries)
     */
    private int snapshotSyncBatchSize;

    /**
     * Constructor
     *
     * @param streamsToReplicate Unique identifiers for all streams to be replicated across sites.
     */
    public LogReplicationConfig(Set<String> streamsToReplicate) {
        this.streamsToReplicate = streamsToReplicate;
    }

    /**
     * Constructor
     *
     * @param streamsToReplicate Unique identifiers for all streams to be replicated across sites.
     * @param snapshotSyncBatchSize snapshot sync batch size (number of entries per batch)
     */
    public LogReplicationConfig(Set<String> streamsToReplicate, int snapshotSyncBatchSize) {
        this(streamsToReplicate);
        this.snapshotSyncBatchSize = snapshotSyncBatchSize;
    }
}