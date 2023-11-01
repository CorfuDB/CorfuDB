package org.corfudb.infrastructure.logreplication.infrastructure;

import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import lombok.Setter;
import org.corfudb.infrastructure.logreplication.config.LogReplicationConfig;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.LogReplicationPluginConfig;
import org.corfudb.infrastructure.logreplication.FsmTaskManager;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationConfigManager;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.corfudb.util.serializer.ISerializer;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.corfudb.util.serializer.ProtobufSerializer.PROTOBUF_SERIALIZER_CODE;

/**
 * This class represents the Log Replication Context.
 *
 * It contains all abstractions required to initiate log replication either as
 * a source or as sink cluster.
 *
 * @author amartinezman
 */
public class LogReplicationContext {

    @Getter
    private final LogReplicationConfigManager configManager;

    @Getter
    private final String localCorfuEndpoint;

    @Getter
    @Setter
    private long topologyConfigId;

    @Getter
    private final AtomicBoolean isLeader;

    @Getter
    private final LogReplicationPluginConfig pluginConfig;

    @Getter
    private final CorfuRuntime corfuRuntime;
    /**
     * Constructor
     **/
    public LogReplicationContext(LogReplicationConfigManager configManager, long topologyConfigId,
                                 String localCorfuEndpoint, LogReplicationPluginConfig pluginConfig,
                                 CorfuRuntime runtime) {
        this.configManager = configManager;
        this.topologyConfigId = topologyConfigId;
        this.localCorfuEndpoint = localCorfuEndpoint;
        this.pluginConfig = pluginConfig;
        this.isLeader = new AtomicBoolean(false);
        this.corfuRuntime = runtime;
    }

    @VisibleForTesting
    public LogReplicationContext(LogReplicationConfigManager configManager, long topologyConfigId,
                                 String localCorfuEndpoint, boolean isLeader, LogReplicationPluginConfig pluginConfig,
                                 CorfuRuntime runtime) {
        this.configManager = configManager;
        this.topologyConfigId = topologyConfigId;
        this.localCorfuEndpoint = localCorfuEndpoint;
        this.pluginConfig = pluginConfig;
        this.isLeader = new AtomicBoolean(isLeader);
        this.corfuRuntime = runtime;
    }

    public void setIsLeader(boolean newValue) {
        this.isLeader.set(newValue);
    }

    /**
     * This method will be invoked when it is needed to get the up-to-date LogReplicationConfig by checking registry
     * table or other config related tables.
     *
     * @param session LogReplicationSession to refresh the config.
     * @param updateGroupDestinationConfig True if group destination config needs to be updated.
     */
    public void refreshConfig(LogReplicationSession session, boolean updateGroupDestinationConfig) {
        this.configManager.getUpdatedConfig(session, updateGroupDestinationConfig);
    }

    /**
     * Exposed method for log replicator to get current config.
     *
     * @return Current config in config manager.
     */
    public LogReplicationConfig getConfig(LogReplicationSession session) {
        return this.configManager.getSessionToConfigMap().get(session);
    }

    /**
     * In general, log replication is happening in stream layer and data should not be materialized. Currently,
     * the only case we need to deserialize data is in log replication writers, where registry table entries need
     * to be deserialized to read the schema options.
     *
     * @return ProtobufSerializer for deserialize registry table entries in log replication writer.
     */
    public ISerializer getProtobufSerializer() {
        return configManager.getRuntime().getSerializers().getSerializer(PROTOBUF_SERIALIZER_CODE);
    }
}
