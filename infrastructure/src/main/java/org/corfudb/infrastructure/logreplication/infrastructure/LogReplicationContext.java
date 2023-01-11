package org.corfudb.infrastructure.logreplication.infrastructure;

import lombok.Getter;
import lombok.Setter;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationConfigManager;

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
    private LogReplicationConfigManager configManager;

    @Getter
    @Setter
    private long topologyConfigId;

    @Getter
    private String localCorfuEndpoint;

    /**
     * Constructor
     **/
    public LogReplicationContext(LogReplicationConfigManager configManager, long topologyConfigId,
                                 String localCorfuEndpoint) {
        this.configManager = configManager;
        this.topologyConfigId = topologyConfigId;
        this.localCorfuEndpoint = localCorfuEndpoint;
    }

    public LogReplicationConfig refresh() {
        return this.configManager.getUpdatedConfig();
    }

    public LogReplicationConfig getConfig() {
        return this.configManager.getConfig();
    }
}
