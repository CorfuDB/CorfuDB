package org.corfudb.infrastructure.logreplication.infrastructure;

import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import lombok.Setter;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationConfigManager;
import org.corfudb.util.serializer.ISerializer;

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


    /**
     * Constructor
     **/
    public LogReplicationContext(LogReplicationConfigManager configManager, long topologyConfigId, String localCorfuEndpoint) {
        this.configManager = configManager;
        this.topologyConfigId = topologyConfigId;
        this.localCorfuEndpoint = localCorfuEndpoint;
    }

    /**
     * This method will be invoked when it is needed to check if registry has new entries, to get the up-to-date
     * LogReplicationConfig, which mainly includes streams to replicate and data streams to tags map.
     *
     * @return The updated LogReplicationConfig that is up-to-date with registry table
     */
    public LogReplicationConfig refresh() {
        return this.configManager.getUpdatedConfig();
    }

    /**
     * Exposed method for log replicator to get current config.
     *
     * @return Current config in config manager.
     */
    public LogReplicationConfig getConfig() {
        return this.configManager.getConfig();
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
