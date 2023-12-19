package org.corfudb.infrastructure.logreplication.config;

import lombok.Data;
import lombok.NonNull;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.corfudb.runtime.view.TableRegistry;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

/**
 * This class represents the Log Replication Configuration field(s) that are common to all replication models.
 */
@Data
public abstract class LogReplicationConfig {
    // Log Replication message timeout time in milliseconds
    public static final int DEFAULT_TIMEOUT_MS = 5000;

    // Log Replication default max number of messages generated at the source cluster for each batch
    public static final int DEFAULT_MAX_NUM_MSG_PER_BATCH = 10;

    // Default value for the max number of entries applied in a single transaction on Sink during snapshot sync
    public static final int DEFAULT_MAX_SNAPSHOT_ENTRIES_APPLIED = 50;

    // Max message size supported by protocol buffers is 64MB. Log Replication uses this limit as the default message
    // size to batch and send data across over to the other side.
    public static final int DEFAULT_MAX_DATA_MSG_SIZE = 64 << 20;

    // Log Replication default max cache number of entries
    // Note: if we want to improve performance for large scale this value should be tuned as it
    // used in snapshot sync to quickly access shadow stream entries, written locally.
    // This value is exposed as a configuration parameter for LR.
    public static final int DEFAULT_MAX_CACHE_NUM_ENTRIES = 200;

    // Percentage of log data per log replication message
    public static final int DATA_FRACTION_OF_UNCOMPRESSED_WRITE_SIZE = 85;

    public static final UUID REGISTRY_TABLE_ID = CorfuRuntime.getStreamID(
        TableRegistry.getFullyQualifiedTableName(CORFU_SYSTEM_NAMESPACE, TableRegistry.REGISTRY_TABLE_NAME));

    public static final UUID PROTOBUF_TABLE_ID = CorfuRuntime.getStreamID(
            TableRegistry.getFullyQualifiedTableName(CORFU_SYSTEM_NAMESPACE, TableRegistry.PROTOBUF_DESCRIPTOR_TABLE_NAME));

    // Set of streams that shouldn't be cleared on snapshot apply phase, as these streams should be the result of
    // "merging" the replicated data (from source) + local data (on sink).
    // For instance, RegistryTable (to avoid losing local opened tables on sink)
    public static final Set<UUID> MERGE_ONLY_STREAMS = new HashSet<>(Arrays.asList(
            REGISTRY_TABLE_ID,
            PROTOBUF_TABLE_ID
    ));

    // Snapshot Sync Batch Size(number of messages)
    private int maxNumMsgPerBatch;

    // Max Size of Log Replication Data Message
    private int maxMsgSize;

    // Max Cache number of entries
    private int maxCacheSize;

    /**
     * The max size of replicated data payload transferred at a time.
     */
    private int maxTransferSize;

    /**
     * Max number of entries to be applied during a snapshot sync.  For special tables only.
     */
    private int maxSnapshotEntriesApplied;

    private LogReplicationSession session;

    // A map consisting of the streams to replicate for each supported replication model
    private Set<String> streamsToReplicate;

    // Streaming tags on Sink (map data stream id to list of tags associated to it)
    private Map<UUID, List<UUID>> dataStreamToTagsMap;

    public LogReplicationConfig(@NonNull LogReplicationSession session,
                                @NonNull Set<String> streamsToReplicate,
                                @NonNull Map<UUID, List<UUID>> dataStreamToTagsMap,
                                ServerContext serverContext) {
        this.session = session;
        this.streamsToReplicate = streamsToReplicate;
        this.dataStreamToTagsMap = dataStreamToTagsMap;

        if (serverContext == null) {
            this.maxNumMsgPerBatch = DEFAULT_MAX_NUM_MSG_PER_BATCH;
            this.maxMsgSize = DEFAULT_MAX_DATA_MSG_SIZE;
            this.maxCacheSize = DEFAULT_MAX_CACHE_NUM_ENTRIES;
            this.maxSnapshotEntriesApplied = DEFAULT_MAX_SNAPSHOT_ENTRIES_APPLIED;
        } else {
            this.maxNumMsgPerBatch = serverContext.getLogReplicationMaxNumMsgPerBatch();
            this.maxMsgSize = serverContext.getLogReplicationMaxDataMessageSize();
            this.maxCacheSize = serverContext.getLogReplicationCacheMaxSize();
            this.maxSnapshotEntriesApplied = serverContext.getMaxSnapshotEntriesApplied();
        }
        this.maxTransferSize = Math.min(maxMsgSize,
                serverContext.getMaxUncompressedTxSize() * DATA_FRACTION_OF_UNCOMPRESSED_WRITE_SIZE / 100);
    }
}
