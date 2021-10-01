package org.corfudb.infrastructure.logreplication;

import com.google.common.annotations.VisibleForTesting;
import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * This class represents any Log Replication Configuration,
 * i.e., set of parameters common across all Clusters.
 */
@Slf4j
@Data
@ToString
public class LogReplicationConfig {

    // Log Replication message timeout time in milliseconds
    public static final int DEFAULT_TIMEOUT_MS = 5000;

    // Log Replication default max number of messages generated at the active cluster for each batch
    public static final int DEFAULT_MAX_NUM_MSG_PER_BATCH = 10;

    // Log Replication default max data message size is 64MB
    public static final int MAX_DATA_MSG_SIZE_SUPPORTED = (64 << 20);

    // Percentage of log data per log replication message
    public static final int DATA_FRACTION_PER_MSG = 90;

    // Unique identifiers for all streams to be replicated across sites
    private Set<String> streamsToReplicate;

    // Streaming tags on Sink/Standby (map data stream id to list of tags associated to it)
    private Map<UUID, List<UUID>> dataStreamToTagsMap = new HashMap<>();

    // Snapshot Sync Batch Size(number of messages)
    private int maxNumMsgPerBatch;

    // Max Size of Log Replication Data Message
    private int maxMsgSize;

    /**
     * The max size of data payload for the log replication message.
     */
    private int maxDataSizePerMsg;

    /**
     * Constructor
     *
     * @param streamsToReplicate Unique identifiers for all streams to be replicated across sites.
     */
    @VisibleForTesting
    public LogReplicationConfig(Set<String> streamsToReplicate) {
        this(streamsToReplicate, DEFAULT_MAX_NUM_MSG_PER_BATCH, MAX_DATA_MSG_SIZE_SUPPORTED);
    }

    /**
     * Constructor
     *
     * @param streamsToReplicate Unique identifiers for all streams to be replicated across sites.
     * @param maxNumMsgPerBatch snapshot sync batch size (number of entries per batch)
     */
    public LogReplicationConfig(Set<String> streamsToReplicate, int maxNumMsgPerBatch, int maxMsgSize) {
        this.streamsToReplicate = streamsToReplicate;
        this.maxNumMsgPerBatch = maxNumMsgPerBatch;
        this.maxMsgSize = maxMsgSize;
        this.maxDataSizePerMsg = maxMsgSize * DATA_FRACTION_PER_MSG / 100;
    }

    public LogReplicationConfig(Set<String> streamsToReplicate, Map<UUID, List<UUID>> streamingTagsMap,
                                int maxNumMsgPerBatch, int maxMsgSize) {
        this(streamsToReplicate, maxNumMsgPerBatch, maxMsgSize);
        this.dataStreamToTagsMap = streamingTagsMap;
    }
}