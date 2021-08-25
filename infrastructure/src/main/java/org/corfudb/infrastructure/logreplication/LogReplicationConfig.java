package org.corfudb.infrastructure.logreplication;

import com.google.common.collect.ImmutableSet;
import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationStreamNameTableManager;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.utils.LogReplicationStreams.TableInfo;

import java.util.HashSet;
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


    /**
     * This class represents Log Replication Stream Info.
     */
    public static class StreamInfo {
        private final Set<UUID> streamIds;
        private final LogReplicationStreamNameTableManager streamInfoManager;

        public StreamInfo(Set<TableInfo> streamsToReplicate,
                          LogReplicationStreamNameTableManager streamInfoManager) {
            this.streamInfoManager = streamInfoManager;
            this.streamIds = new HashSet<>();

            refreshStreamIds(streamsToReplicate, true);
        }

        private synchronized void refreshStreamIds(Set<TableInfo> infoSet, boolean clear) {
            if (clear) {
                this.streamIds.clear();
            }
            for (TableInfo info : infoSet) {
                if (info.getName() != null) {
                    streamIds.add(CorfuRuntime.getStreamID(info.getName()));
                } else if (info.getId() != null) {
                    streamIds.add(UUID.fromString(info.getId()));
                }
            }
        }

        public void syncWithInfoTable() {
            if (streamInfoManager == null) {
                log.warn("streamInfoManager is null! skipping sync!");
                return;
            }

            Set<TableInfo> fetched = streamInfoManager.getStreamsToReplicate();
            refreshStreamIds(fetched, true);
        }

        // sync with table registry then union two sets
        public void syncWithTableRegistry(long ts) {
            if (streamInfoManager == null) {
                log.warn("streamInfoManager is null! skipping sync!");
                return;
            }

            Set<TableInfo> registryInfo = streamInfoManager.readStreamsToReplicatedFromRegistry(ts);
            refreshStreamIds(registryInfo, false);
        }

        public synchronized Set<UUID> getStreamIds() {
            return ImmutableSet.copyOf(streamIds);
        }

        public synchronized void addStreams(Set<UUID> streamIdSet) {
            streamIds.addAll(streamIdSet);
            if (streamInfoManager != null) {
                streamInfoManager.addStreamsToInfoTable(streamIdSet);
            } else {
                log.warn("streamInfoManager is null! Failed to update info in metadata table.");
            }
        }
    }

    // Log Replication message timeout time in milliseconds.
    public static final int DEFAULT_TIMEOUT_MS = 5000;

    // Log Replication default max number of messages generated at the active cluster for each batch.
    public static final int DEFAULT_MAX_NUM_MSG_PER_BATCH = 10;

    // Log Replication default max data message size is 64MB.
    public static final int MAX_DATA_MSG_SIZE_SUPPORTED = (64 << 20);

    /**
     * percentage of log data per log replication message
     */
    public static final int DATA_FRACTION_PER_MSG = 90;

    /**
     * Info for all streams to be replicated across sites.
     */
    private final StreamInfo streamInfo;

    /**
     * Snapshot Sync Batch Size(number of messages)
     */
    private int maxNumMsgPerBatch;

    /**
     * The Max Size of Log Replication Data Message.
     */
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
    public LogReplicationConfig(Set<TableInfo> streamsToReplicate) {
        this(streamsToReplicate, DEFAULT_MAX_NUM_MSG_PER_BATCH, MAX_DATA_MSG_SIZE_SUPPORTED, null);
    }

    /**
     * Constructor
     *
     * @param streamsToReplicate Unique identifiers for all streams to be replicated across sites.
     * @param maxNumMsgPerBatch snapshot sync batch size (number of entries per batch)
     */
    public LogReplicationConfig(Set<TableInfo> streamsToReplicate,
                                int maxNumMsgPerBatch, int maxMsgSize,
                                LogReplicationStreamNameTableManager streamInfoManager) {
        this.maxNumMsgPerBatch = maxNumMsgPerBatch;
        this.maxMsgSize = maxMsgSize;
        this.maxDataSizePerMsg = maxMsgSize * DATA_FRACTION_PER_MSG / 100;
        this.streamInfo = new StreamInfo(streamsToReplicate, streamInfoManager);
    }
}