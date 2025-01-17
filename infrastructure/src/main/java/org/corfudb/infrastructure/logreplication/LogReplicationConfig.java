package org.corfudb.infrastructure.logreplication;

import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.CorfuReplicationDiscoveryService;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationConfigManager;
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

    // Log Replication default max number of messages generated at the active cluster for each batch
    public static final int DEFAULT_MAX_NUM_MSG_PER_BATCH = 5;

    // Default value for the max number of entries applied in a single transaction on Sink during snapshot sync
    public static final int DEFAULT_MAX_SNAPSHOT_ENTRIES_APPLIED = 50;

    // Log Replication uses 15MB limit as the default max message size to batch and
    // send data across over to the other side. (Arbitrary limit, can be changed)
    public static final int DEFAULT_MAX_MSG_BATCH_SIZE = (15 << 20);

    // Log Replication default max cache number of entries
    // Note: if we want to improve performance for large scale this value should be tuned as it
    // used in snapshot sync to quickly access shadow stream entries, written locally.
    // This value is exposed as a configuration parameter for LR.
    public static final int MAX_CACHE_NUM_ENTRIES = 200;

    // Percentage of log data per log replication message
    public static final long DATA_FRACTION_PER_MSG = 90L;

    public static final UUID REGISTRY_TABLE_ID = CorfuRuntime.getStreamID(
            getFullyQualifiedTableName(CORFU_SYSTEM_NAMESPACE, TableRegistry.REGISTRY_TABLE_NAME));

    public static final UUID PROTOBUF_TABLE_ID = CorfuRuntime.getStreamID(
            getFullyQualifiedTableName(CORFU_SYSTEM_NAMESPACE, TableRegistry.PROTOBUF_DESCRIPTOR_TABLE_NAME));

    // Set of streams that shouldn't be cleared on snapshot apply phase, as these
    // streams should be the result of "merging" the replicated data (from active) + local data (on standby).
    // For instance, RegistryTable (to avoid losing local opened tables on standby)
    public static final Set<UUID> MERGE_ONLY_STREAMS = new HashSet<>(Arrays.asList(
            REGISTRY_TABLE_ID,
            PROTOBUF_TABLE_ID
    ));

    // Suite of utility methods for updating the configuration
    private LogReplicationConfigManager configManager;

    // Unique identifiers for all streams to be replicated across sites
    private Set<String> streamsToReplicate;

    // Mapping from stream ids to their fully qualified names.
    private Map<UUID, String> streamsIdToNameMap;

    // Streaming tags on Sink (map data stream id to list of tags associated to it)
    private Map<UUID, List<UUID>> dataStreamToTagsMap = new HashMap<>();

    // Set of streams to drop on Sink if federated flag differs from Source during cluster upgrades.
    private Set<UUID> replicatedStreamsToDrop;

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
     * Max number of entries to be applied during a snapshot sync.  For special tables only.
     */
    private int maxSnapshotEntriesApplied = DEFAULT_MAX_SNAPSHOT_ENTRIES_APPLIED;

    /**
     * Constructor exposed to {@link CorfuReplicationDiscoveryService}
     */
    public LogReplicationConfig(LogReplicationConfigManager configManager,
                                int maxNumMsgPerBatch, int maxMsgSize, int cacheSize, int maxSnapshotEntriesApplied) {
        this.configManager = configManager;
        this.maxNumMsgPerBatch = maxNumMsgPerBatch;
        this.maxCacheSize = cacheSize;
        this.maxMsgSize = maxMsgSize;
        this.maxDataSizePerMsg = (int) ((maxMsgSize * DATA_FRACTION_PER_MSG) / 100L);
        this.maxSnapshotEntriesApplied = maxSnapshotEntriesApplied;
        syncWithRegistry();
    }

    /**
     * Provide the ability to sync LogReplicationConfig with the latest registry table.
     */
    public void syncWithRegistry() {
        if (configManager.loadRegistryTableEntries()) {
            update();
            log.info("Synced with registry table. Streams to replicate total = {}, streams names = {}",
                    streamsToReplicate.size(), streamsToReplicate);
        } else {
            log.trace("Registry table address space did not change, using last fetched config.");
        }
    }

    /**
     * Update LogReplicationConfig fields. This method should be invoked after successfully refreshing the in-memory
     * registry table entries in {@link LogReplicationConfigManager}.
     */
    private void update() {
        this.streamsToReplicate = configManager.getStreamsToReplicate();
        this.dataStreamToTagsMap = configManager.getStreamToTagsMap();
        this.replicatedStreamsToDrop = configManager.getStreamsToDrop();
        this.streamsIdToNameMap = new HashMap<>();
        streamsToReplicate.forEach(stream -> streamsIdToNameMap.put(CorfuRuntime.getStreamID(stream), stream));
    }
}
