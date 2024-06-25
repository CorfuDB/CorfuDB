package org.corfudb.infrastructure.logreplication;

import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.CorfuReplicationDiscoveryService;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationConfigManager;

/**
 * This class is used to interface with Postgres and represents anything needed on the local
 * node to bootstrap itself and perform actions on topology changes.
 */
@Slf4j
@ToString
public class PostgresReplicationConfig extends ReplicationConfig {
    private final String streamsToReplicatePath;
    private final String streamsToCreatePath;

    /**
     * Constructor exposed to {@link CorfuReplicationDiscoveryService}
     */
    public PostgresReplicationConfig(LogReplicationConfigManager configManager, PostgresReplicationConnectionConfig config) {
        this.configManager = configManager;
        this.streamsToReplicatePath = config.getTABLES_TO_REPLICATE_PATH();
        this.streamsToCreatePath = config.getTABLES_TO_CREATE_PATH();
        syncWithRegistry();
    }

    /**
     * Provide the ability to sync LogReplicationConfig with the latest registry table.
     */
    public void syncWithRegistry() {
        try {
            update();
            log.info("Synced with registry table. Streams to replicate total = {}, streams names = {}",
                    streamsToReplicate.size(), streamsToReplicate);
        } catch (Exception e) {
            log.trace("Registry table address space did not change, using last fetched config.", e);
        }
    }

    /**
     * Update LogReplicationConfig fields. This method should be invoked after successfully refreshing the in-memory
     * registry table entries in {@link LogReplicationConfigManager}.
     */
    private void update() {
        this.streamsToCreate = configManager.loadTablesToCreate(streamsToCreatePath);
        this.streamsToReplicate = configManager.loadTablesToReplicate(streamsToReplicatePath);

        // TODO (Postgres): might need to take care of these outside of happy path
        // this.replicatedStreamsToDrop = configManager.getStreamsToDrop();
        // streamsToReplicate.forEach(stream -> streamsIdToNameMap.put(CorfuRuntime.getStreamID(stream), stream));
    }
}
