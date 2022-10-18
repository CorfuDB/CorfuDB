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

    public static final UUID PROTOBUF_TABLE_ID = CorfuRuntime.getStreamID(
            getFullyQualifiedTableName(CORFU_SYSTEM_NAMESPACE, TableRegistry.PROTOBUF_DESCRIPTOR_TABLE_NAME));

    // Set of streams that shouldn't be cleared on snapshot apply phase, as these
    // streams should be the result of "merging" the replicated data (from active) + local data (on standby).
    // For instance, RegistryTable (to avoid losing local opened tables on standby)
    public static final Set<UUID> MERGE_ONLY_STREAMS = new HashSet<>(Arrays.asList(
            REGISTRY_TABLE_ID,
            PROTOBUF_TABLE_ID
    ));

    // LogReplicationConfigManager contains a suite of utility methods for updating LogReplicationConfig
    private LogReplicationConfigManager configManager;

    // Unique identifiers for all streams to be replicated across sites
    private Set<String> streamsToReplicate;

    // Mapping from stream ids to their fully qualified names.
    private Map<UUID, String> streamsIdToNameMap;

    // Streaming tags on Sink (map data stream id to list of tags associated to it)
    private Map<UUID, List<UUID>> dataStreamToTagsMap = new HashMap<>();

    // If streams have explicitly set is_federated flag to false on Sink side, their data as well as their registry
    // table records should be dropped during log replication.
    private Set<UUID> confirmedNoisyStreams;

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
     * Constructor exposed to {@link CorfuReplicationDiscoveryService}
     */
    public LogReplicationConfig(LogReplicationConfigManager configManager,
                                int maxNumMsgPerBatch, int maxMsgSize, int cacheSize) {
        this.configManager = configManager;
        this.maxNumMsgPerBatch = maxNumMsgPerBatch;
        this.maxMsgSize = maxMsgSize;
        this.maxCacheSize = cacheSize;
        this.maxDataSizePerMsg = maxMsgSize * DATA_FRACTION_PER_MSG / 100;
        syncWithRegistry();
    }

    /**
     * Provide the ability to sync LogReplicationConfig with the latest registry table.
     */
    public void syncWithRegistry() {
        if (configManager.loadRegistryTableEntries()) {
            updateConfig();
            log.info("Synced with registry table. Streams to replicate total = {}, streams names = {}",
                    streamsToReplicate.size(), streamsToReplicate);
        } else {
            log.info("Registry table address space did not change, using last fetched config.");
        }
    }

    /**
     * Provide the ability to sync LogReplicationConfig with registry table at a certain timestamp.
     */
    public void syncWithRegistry(long snapshotTimestamp) {
        configManager.loadRegistryTableEntriesAtSnapshot(snapshotTimestamp);
        updateConfig();
        log.info("Synced with registry table at timestamp {}. Streams to replicate total = {}, streams names = {}",
                snapshotTimestamp, streamsToReplicate.size(), streamsToReplicate);
    }

    /**
     * Update LogReplicationConfig fields. This method should be invoked after successfully refreshing the in-memory
     * registry table entries in {@link LogReplicationConfigManager}.
     */
    private void updateConfig() {
        this.streamsToReplicate = configManager.getStreamsToReplicate();
        this.dataStreamToTagsMap = configManager.getStreamToTagsMap();
        this.confirmedNoisyStreams = configManager.getConfirmedNoisyStreams();
        this.streamsIdToNameMap = new HashMap<>();
        streamsToReplicate.forEach(stream -> streamsIdToNameMap.put(CorfuRuntime.getStreamID(stream), stream));
    }
}
