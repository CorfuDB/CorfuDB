package org.corfudb.infrastructure.logreplication;

import com.google.common.annotations.VisibleForTesting;
import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.ReplicationSubscriber;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.TableRegistry;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;
import static org.corfudb.runtime.view.TableRegistry.getFullyQualifiedTableName;

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

    // Log Replication default max number of messages generated at the source cluster for each batch
    public static final int DEFAULT_MAX_NUM_MSG_PER_BATCH = 10;

    // Log Replication default max data message size is 64MB
    public static final int MAX_DATA_MSG_SIZE_SUPPORTED = (64 << 20);

    // Log Replication default max cache number of entries
    // Note: if we want to improve performance for large scale this value should be tuned as it
    // used in snapshot sync to quickly access shadow stream entries, written locally.
    // This value is exposed as a configuration parameter for LR.
    public static final int MAX_CACHE_NUM_ENTRIES = 200;

    // Percentage of log data per log replication message
    public static final int DATA_FRACTION_PER_MSG = 90;

    public static final UUID REGISTRY_TABLE_ID = CorfuRuntime.getStreamID(
        getFullyQualifiedTableName(CORFU_SYSTEM_NAMESPACE, TableRegistry.REGISTRY_TABLE_NAME));

    // A map consisting of the streams to replicate for each supported replication model
    private Map<ReplicationSubscriber, Set<String>> replicationSubscriberToStreamsMap = new HashMap<>();

    public static final UUID PROTOBUF_TABLE_ID = CorfuRuntime.getStreamID(
            getFullyQualifiedTableName(CORFU_SYSTEM_NAMESPACE, TableRegistry.PROTOBUF_DESCRIPTOR_TABLE_NAME));


    // Set of streams that shouldn't be cleared on snapshot apply phase, as these streams should be the result of
    // "merging" the replicated data (from source) + local data (on sink).
    // For instance, RegistryTable (to avoid losing local opened tables on sink)
    public static final Set<UUID> MERGE_ONLY_STREAMS = new HashSet<>(Arrays.asList(
            REGISTRY_TABLE_ID,
            PROTOBUF_TABLE_ID
    ));

    // Unique identifiers for all streams to be replicated across sites
    private Set<String> streamsToReplicate;

    // Streaming tags on Sink (map data stream id to list of tags associated to it)
    private Map<UUID, List<UUID>> dataStreamToTagsMap = new HashMap<>();

    // Snapshot Sync Batch Size(number of messages)
    private int maxNumMsgPerBatch;

    // Max Size of Log Replication Data Message
    private int maxMsgSize;

    // Max Cache number of entries
    private int maxCacheSize;

    /**
     * The max size of data payload for the log replication message.
     */
    private int maxDataSizePerMsg;

    /**
     * Constructor
     *
     * @param streamsToReplicateMap A map consisting of the streams to replicate for each supported replication model
     */
    @VisibleForTesting
    public LogReplicationConfig(Map<ReplicationSubscriber, Set<String>> streamsToReplicateMap) {
        this(streamsToReplicateMap, DEFAULT_MAX_NUM_MSG_PER_BATCH, MAX_DATA_MSG_SIZE_SUPPORTED, MAX_CACHE_NUM_ENTRIES);
    }

    /**
     * Constructor
     *
     * @param streamsToReplicateMap A map consisting of the streams to replicate for each supported replication model
     * @param maxNumMsgPerBatch snapshot sync batch size (number of entries per batch)
     */
    public LogReplicationConfig(Map<ReplicationSubscriber, Set<String>> streamsToReplicateMap, int maxNumMsgPerBatch,
                                int maxMsgSize, int cacheSize) {
        replicationSubscriberToStreamsMap = streamsToReplicateMap;
        this.maxNumMsgPerBatch = maxNumMsgPerBatch;
        this.maxMsgSize = maxMsgSize;
        this.maxCacheSize = cacheSize;
        this.maxDataSizePerMsg = maxMsgSize * DATA_FRACTION_PER_MSG / 100;
    }

    /**
     * Constructor
     *
     * @param streamsToReplicateMap A map consisting of the streams to replicate for each supported replication model
     * @param maxNumMsgPerBatch snapshot sync batch size (number of entries per batch)
     */
    public LogReplicationConfig(Map<ReplicationSubscriber, Set<String>> streamsToReplicateMap,
        int maxNumMsgPerBatch, int maxMsgSize) {
        this(streamsToReplicateMap, maxNumMsgPerBatch, maxMsgSize, MAX_CACHE_NUM_ENTRIES);
    }

    public LogReplicationConfig(Map<ReplicationSubscriber, Set<String>> streamsToReplicateMap,
        Map<UUID, List<UUID>> streamingTagsMap, int maxNumMsgPerBatch, int maxMsgSize,
        int cacheSize) {
        this(streamsToReplicateMap, maxNumMsgPerBatch, maxMsgSize, cacheSize);
        this.dataStreamToTagsMap = streamingTagsMap;
    }

}
