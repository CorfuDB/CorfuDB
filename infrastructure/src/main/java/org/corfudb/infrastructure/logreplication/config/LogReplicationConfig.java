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

    // The recommended max message size of a protobuf message is 64MB. Log Replication uses this limit as the default
    // message size to batch and send data across over to the other side.
    //
    // For routing queue model, this number was reduced to ~17MB when testing with multiple (upto 64) reader threads sending
    // data concurrently.  With this concurrency, LR can run out-of-memory as each thread created a payload to send.
    // In future with a fixed size thread pool, the empirical 17MB limit can be revisited.
    public static final int DEFAULT_MAX_DATA_MSG_SIZE = 64 << 20;

    // Log Replication default max cache number of entries
    // Note: if we want to improve performance for large scale this value should be tuned as it
    // used in snapshot sync to quickly access shadow stream entries, written locally.
    // This value is exposed as a configuration parameter for LR.
    // TODO: Tune this better.
    public static final int DEFAULT_MAX_CACHE_NUM_ENTRIES = 10;

    // Corfu runtime's max uncompressed write size is used to calculate the payload transfer and apply sizes in
    // LR.  To account for extra bytes added in logData.serialize(), consider a fraction of this max write size.
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

    // Max Cache number of entries
    private int maxCacheSize;

    /**
     * The max size of replicated data payload transferred at a time.
     */
    private long maxTransferSize;

    /**
     * The max size of data payload written in a single transaction during the Apply phase of Snapshot Sync
     */
    private long maxApplySize;

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
            this.maxCacheSize = DEFAULT_MAX_CACHE_NUM_ENTRIES;
            this.maxSnapshotEntriesApplied = DEFAULT_MAX_SNAPSHOT_ENTRIES_APPLIED;
            this.maxApplySize = CorfuRuntime.MAX_UNCOMPRESSED_WRITE_SIZE * DATA_FRACTION_OF_UNCOMPRESSED_WRITE_SIZE / 100;
        } else {
            this.maxNumMsgPerBatch = serverContext.getLogReplicationMaxNumMsgPerBatch();
            this.maxCacheSize = serverContext.getLogReplicationCacheMaxSize();
            this.maxSnapshotEntriesApplied = serverContext.getMaxSnapshotEntriesApplied();
            this.maxApplySize = serverContext.getMaxUncompressedTxSize() * DATA_FRACTION_OF_UNCOMPRESSED_WRITE_SIZE / 100;
        }
        // The transfer size determines the amount of data sent at a time from the sender and, in case of
        // Snapshot Sync, applied in a single transaction to the shadow stream.  Write to the shadow stream also adds
        // metadata on the Sink so we consider the min of maxApplySize and DEFAULT_MAX_DATA_MSG_SIZE to account for the
        // LR metadata.
        // No LR metadata is written during the 'apply' phase so it is enough to include just
        // DATA_FRACTION_OF_UNCOMPRESSED_WRITE_SIZE as a buffer.
        this.maxTransferSize = Math.min(DEFAULT_MAX_DATA_MSG_SIZE, maxApplySize);
    }
}
