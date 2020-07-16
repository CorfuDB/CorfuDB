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

    // Log Replication message timeout time in milliseconds.
    public static final int DEFAULT_TIMEOUT = 5000;

    // Log Replication default max number of message generated at the active cluster for each run cycle.
    public static final int DEFAULT_MAX_NUM_SNAPSHOT_MSG_PER_CYCLE = 100;

    // Log Replication default max data message size is 64KB.
    public static final int DEFAULT_LOG_REPLICATION_DATA_MSG_SIZE = (64 << 10);

    /*
     * Unique identifiers for all streams to be replicated across sites.
     */
    private Set<String> streamsToReplicate;

    /*
     * Snapshot Sync Batch Size Per Cycle(number of messages)
     */
    private int maxNumSnapshotMsgPerCycle;

    /*
     * The Max Size of Log Replication Data Message.
     */
    private int maxDataMsgSize;

    /**
     * Constructor
     *
     * @param streamsToReplicate Unique identifiers for all streams to be replicated across sites.
     */
    public LogReplicationConfig(Set<String> streamsToReplicate) {
        this.streamsToReplicate = streamsToReplicate;
        this.maxNumSnapshotMsgPerCycle = DEFAULT_MAX_NUM_SNAPSHOT_MSG_PER_CYCLE;
        this.maxDataMsgSize = DEFAULT_LOG_REPLICATION_DATA_MSG_SIZE;
    }

    /**
     * Constructor
     *
     * @param streamsToReplicate Unique identifiers for all streams to be replicated across sites.
     * @param maxNumSnapshotMsgPerCycle snapshot sync batch size (number of entries per batch)
     */
    public LogReplicationConfig(Set<String> streamsToReplicate, int maxNumSnapshotMsgPerCycle, int maxDataMsgSize) {
        this(streamsToReplicate);
        this.maxNumSnapshotMsgPerCycle = maxNumSnapshotMsgPerCycle;
        this.maxDataMsgSize = maxDataMsgSize;
    }
}