package org.corfudb.infrastructure.logreplication;

import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.ServerContext;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@Slf4j
@ToString
public class LogicalGroupBasedReplicationConfig extends LogReplicationConfig {

    // Set of streams to replicate for each destination
    private Map<String, Set<String>> remoteClusterToStreamsToReplicateMap = new HashMap<>();

    // Set of streams to drop for each destination
    private Map<String, Set<UUID>> remoteClusterToStreamsToDropMap = new HashMap<>();

    public LogicalGroupBasedReplicationConfig(Set<String> streamsToReplicate, Set<UUID> streamsToDrop, Map<UUID, List<UUID>> streamToTagsMap, ServerContext serverContext) {
        super(streamsToReplicate, streamsToDrop, streamToTagsMap, serverContext);
    }

    /**
     * TODO: Fix this class - what about destination?
    public LogicalGroupBasedReplicationConfig(Map<String, Set<String>> remoteClusterToStreamsToReplicateMap,
                                              Map<String, Set<UUID>> remoteClusterToStreamsToDropMap,
                                              Map<UUID, List<UUID>> streamToTagsMap, ServerContext serverContext) {
        this.remoteClusterToStreamsToReplicateMap = remoteClusterToStreamsToReplicateMap;
        this.remoteClusterToStreamsToDropMap = remoteClusterToStreamsToDropMap;
        this.dataStreamToTagsMap = streamToTagsMap;

        if (serverContext == null) {
            this.maxNumMsgPerBatch = DEFAULT_MAX_NUM_MSG_PER_BATCH;
            this.maxMsgSize = MAX_DATA_MSG_SIZE_SUPPORTED;
            this.maxCacheSize = MAX_CACHE_NUM_ENTRIES;
        } else {
            this.maxNumMsgPerBatch = serverContext.getLogReplicationMaxNumMsgPerBatch();
            this.maxMsgSize = serverContext.getLogReplicationMaxDataMessageSize();
            this.maxCacheSize = serverContext.getLogReplicationCacheMaxSize();
        }
        this.maxDataSizePerMsg = maxMsgSize * DATA_FRACTION_PER_MSG / 100;
    }

    public Set<String> getStreamsToReplicate(String destination) {
        return remoteClusterToStreamsToReplicateMap.get(destination);
    }

    public Set<UUID> getStreamsToDrop(String destination) {
        return remoteClusterToStreamsToDropMap.get(destination);
    }
     */
}
