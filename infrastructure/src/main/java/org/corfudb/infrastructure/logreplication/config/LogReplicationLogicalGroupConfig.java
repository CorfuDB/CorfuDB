package org.corfudb.infrastructure.logreplication.config;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.runtime.LogReplication.LogReplicationSession;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * This class represents the Log Replication Configuration field(s) for LOGICAL_GROUP replication model.
 */
public class LogReplicationLogicalGroupConfig extends LogReplicationConfig {

    /**
     * Stream tag that is used by the client config listener to get updates from LR client configuration tables.
     * This tag is dedicated to LR client configuration tables in LOGICAL_GROUP use case. Changes could include
     * new client registration and group-destinations information updates.
     */
    public static final String CLIENT_CONFIG_TAG = "lr_sessions";

    /**
     * Logical groups to streams mapping, which are read from RegistryTable.
     */
    @Getter
    @Setter
    private Map<String, Set<String>> logicalGroupToStreams;


    public LogReplicationLogicalGroupConfig(@NonNull LogReplicationSession session,
                                            @NonNull Set<String> streamsToReplicate,
                                            @NonNull Map<UUID, List<UUID>> dataStreamToTagsMap,
                                            ServerContext serverContext,
                                            @NonNull Map<String, Set<String>> logicalGroupToStreams) {
        super(session, streamsToReplicate, dataStreamToTagsMap, serverContext);
        this.logicalGroupToStreams = logicalGroupToStreams;
    }
}
