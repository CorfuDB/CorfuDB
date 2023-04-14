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
 * This class represents any Log Replication Configuration,
 * i.e., set of parameters common across all Clusters.
 */
public class LogReplicationFullTableConfig extends LogReplicationConfig {

    /**
     * Set of streams to drop on Sink if streams to replicate info differs from Source, which should only happen
     * when Source and Sink are on different versions.
     */
    @Getter
    @Setter
    private Set<UUID> streamsToDrop;

    public LogReplicationFullTableConfig(@NonNull LogReplicationSession session,
                                         @NonNull Set<String> streamsToReplicate,
                                         @NonNull Map<UUID, List<UUID>> dataStreamToTagsMap,
                                         ServerContext serverContext,
                                         @NonNull Set<UUID> streamsToDrop) {
        super(session, streamsToReplicate, dataStreamToTagsMap, serverContext);
        this.streamsToDrop = streamsToDrop;
    }
}
