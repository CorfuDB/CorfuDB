package org.corfudb.infrastructure.logreplication;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.Data;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationConfigManager;

import java.util.List;
import java.util.Set;
import java.util.UUID;

@Data
public abstract class ReplicationConfig {
    LogReplicationConfigManager configManager;

    Set<String> streamsToReplicate;

    Set<UUID> replicatedStreamsToDrop;

    public void syncWithRegistry() {}
}
