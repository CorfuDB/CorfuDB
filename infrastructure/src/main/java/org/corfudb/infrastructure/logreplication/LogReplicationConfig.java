package org.corfudb.infrastructure.logreplication;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationConfigManager;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.utils.LogReplicationStreams.TableInfo;

import java.util.HashMap;
import java.util.HashSet;
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

    // Log Replication default max cache number of entries
    // Note: if we want to improve performance for large scale this value should be tuned as it
    // used in snapshot sync to quickly access shadow stream entries, written locally.
    // This value is exposed as a configuration parameter for LR.
    public static final int MAX_CACHE_NUM_ENTRIES = 200;

    // Percentage of log data per log replication message
    public static final int DATA_FRACTION_PER_MSG = 90;

    // For querying version table, external plugin, and registry table to update
    // the set of streams to replicate.
    private final LogReplicationConfigManager configManager;

    // Unique identifiers for all streams to be replicated across sites
    private final Set<UUID> streamsToReplicate;

    // Map data stream id to list of tags associated to it
    private Map<UUID, Set<UUID>> dataStreamToTagsMap = new HashMap<>();

    // Set of streams that shouldn't be cleared on snapshot apply phase, as these
    // streams should be the result of "merging" the replicated data (from active) + local data (on standby).
    // For now, it includes RegistryTable and ProtobufDescriptorTable (to avoid losing local opened tables on standby)
    private Set<UUID> mergeOnlyStreams = new HashSet<>();

    // Set of streams that were previously supposed to be replicated, but no longer needed after
    // upgrade (is_federated = false). We need to skip applying those streams during replication.
    // Note: this field is used for Standby under rolling upgrade case.
    private Set<UUID> noisyStreams = new HashSet<>();

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
     * Constructor that actually initiate the fields
     *
     * @param fetchedStreams Info for all streams to be replicated across sites.
     * @param maxNumMsgPerBatch snapshot sync batch size (number of entries per batch)
     */
    private LogReplicationConfig(Set<TableInfo> fetchedStreams, int maxNumMsgPerBatch, int maxMsgSize,
                                int cacheSize, LogReplicationConfigManager configManager) {
        this.streamsToReplicate = new HashSet<>();
        this.configManager = configManager;
        this.maxNumMsgPerBatch = maxNumMsgPerBatch;
        this.maxMsgSize = maxMsgSize;
        this.maxCacheSize = cacheSize;
        this.maxDataSizePerMsg = maxMsgSize * DATA_FRACTION_PER_MSG / 100;
        if (configManager != null) {
            this.noisyStreams.addAll(configManager.fetchNoisyStreams());
        }
        updateStreamsToReplicate(fetchedStreams, false);
    }

    /**
     * Constructor exposed for discovery service to initiate LogReplicationConfig
     */
    public LogReplicationConfig(Set<TableInfo> fetchedStreams,
                                Set<UUID> mergeOnlyStreams, int maxNumMsgPerBatch, int maxMsgSize,
                                int cacheSize, LogReplicationConfigManager configManager) {
        this(fetchedStreams, maxNumMsgPerBatch, maxMsgSize, cacheSize, configManager);
        this.mergeOnlyStreams = mergeOnlyStreams;
    }

    /**
     * Constructor used for testing purpose only
     *
     * @param fetchedStreams Info for all streams to be replicated across sites
     * @param maxNumMsgPerBatch snapshot sync batch size (number of entries per batch)
     */
    @VisibleForTesting
    public LogReplicationConfig(Set<TableInfo> fetchedStreams, int maxNumMsgPerBatch, int maxMsgSize) {
        this(fetchedStreams, maxNumMsgPerBatch, maxMsgSize, MAX_CACHE_NUM_ENTRIES, null);
    }

    /**
     * Constructor used for testing purpose only
     *
     * @param fetchedStreams Info for all streams to be replicated across sites.
     */
    @VisibleForTesting
    public LogReplicationConfig(Set<TableInfo> fetchedStreams) {
        this(fetchedStreams, DEFAULT_MAX_NUM_MSG_PER_BATCH, MAX_DATA_MSG_SIZE_SUPPORTED,
                MAX_CACHE_NUM_ENTRIES, null);
    }

    /**
     * This method is supposed to be invoked at the STANDBY side cluster to update the streams to their
     * tags map during snapshot sync and delta sync.
     *
     * @param newStreamTagsMap The newly discovered / collected stream to tags map
     * @param refresh Boolean field to check if we need to clear the present map. Set to true in Snapshot sync.
     */
    public void updateDataStreamToTagsMap(Map<UUID, Set<UUID>> newStreamTagsMap, boolean refresh) {
        if (refresh) {
            this.dataStreamToTagsMap.clear();
        }
        dataStreamToTagsMap.putAll(newStreamTagsMap);
    }

    /**
     * Update the set of stream id for replicate
     *
     * @param streamsToReplicate Set of stream id for replicate
     * @param refresh            True when need to clear current set of stream id
     */
    private void updateStreamsToReplicate(Set<TableInfo> streamsToReplicate, boolean refresh) {
        if (refresh) {
            this.streamsToReplicate.clear();
        }

        for (TableInfo info : streamsToReplicate) {
            // The name field here should be a fully qualified table name
            if (info.hasField(TableInfo.getDescriptor().findFieldByName("name"))) {
                this.streamsToReplicate.add(CorfuRuntime.getStreamID(info.getName()));
            } else if (info.hasField(TableInfo.getDescriptor().findFieldByName("id"))) {
                this.streamsToReplicate.add(UUID.fromString(info.getId()));
            }
        }
    }

    /**
     * This method is invoked during snapshot and log entry sync at Standby side to update the
     * set of stream id to replicate
     */
    public void updateStreamsToReplicateStandby(Set<UUID> streamsToReplicate, boolean refresh) {
        if (refresh) {
            this.streamsToReplicate.clear();
        }

        this.streamsToReplicate.addAll(streamsToReplicate);
    }

    /**
     * Sync with Info table upon cluster role change (leader node acquired at Active) and
     * before a new start of snapshot sync. We do this a bit redundantly to avoid any loss
     * of streams to replicate
     */
    public void syncWithInfoTable() {
        if (configManager == null) {
            log.warn("configManager is null! skipping sync!");
            return;
        }

        Set<TableInfo> fetched = this.configManager.fetchStreamsToReplicate();
        // We should refresh the stream ids here because this method is called before snapshot
        // sync and under leadership acquire
        updateStreamsToReplicate(fetched, true);
    }

    /**
     * Sync with RegistryTable before a new start of snapshot sync. We do this a bit redundantly
     * to avoid loss of any streams to replicate
     */
    public void syncWithTableRegistry(long timestamp) {
        if (configManager == null) {
            log.warn("configManager is null! skipping sync!");
            return;
        }

        Set<TableInfo> registryInfo = configManager.readStreamsToReplicateFromRegistry(timestamp);
        // We don't refresh the stream ids here because this method is called only before snapshot
        // sync as a supplementary
        updateStreamsToReplicate(registryInfo, false);
    }

    /**
     * Getter method that returns an immutable copy of streams to replicate
     */
    public Set<UUID> getStreamsToReplicate() {
        return ImmutableSet.copyOf(streamsToReplicate);
    }

    /**
     * Getter method that returns an immutable copy of noisy streams
     */
    public Set<UUID> getNoisyStreams() {
        return ImmutableSet.copyOf(noisyStreams);
    }

    /**
     * Add a set of streams newly discovered during log entry sync. This new set of streams
     * should be populated to both InfoTable (stream backed) and in-memory state (this class)
     *
     * @param streamIdSet A set of streams newly discovered during log entry sync.
     */
    public void addStreams(Set<UUID> streamIdSet) {
        configManager.addStreamsToInfoTable(streamIdSet);
        Set<TableInfo> fetched = this.configManager.fetchStreamsToReplicate();
        updateStreamsToReplicate(fetched, false);
        log.debug("Added new streams {} to streamIds in config", streamIdSet);
    }
}
