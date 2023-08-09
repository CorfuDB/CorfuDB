package org.corfudb.infrastructure.logreplication.utils;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.LRRollingUpgradeHandler;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.ILogReplicationVersionAdapter;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuStore;

/**
 * Manages version and upgrade-related metadata and provides utility methods for the same.
 */
@Slf4j
public class LogReplicationUpgradeManager {
    public static final String LOG_REPLICATION_PLUGIN_VERSION_TABLE = "LogReplicationPluginVersion";

    @Getter
    private LRRollingUpgradeHandler lrRollingUpgradeHandler;

    private ILogReplicationVersionAdapter logReplicationVersionAdapter;

    // The current version running on the local node, extracted from the plugin
    @Getter
    private static String nodeVersion;

    public LogReplicationUpgradeManager(CorfuStore corfuStore, ILogReplicationVersionAdapter versionPlugin) {
        logReplicationVersionAdapter = versionPlugin;
        openTableAndGetNodeVersion(corfuStore.getRuntime());
        initLogReplicationRollingUpgradeHandler(corfuStore);
    }

    /**
     * Instantiate the LogReplicator's Rolling Upgrade Handler
     */
    private void initLogReplicationRollingUpgradeHandler(CorfuStore corfuStore) {
        this.lrRollingUpgradeHandler = new LRRollingUpgradeHandler(logReplicationVersionAdapter, corfuStore);
    }

    private void openTableAndGetNodeVersion(CorfuRuntime runtime) {
        logReplicationVersionAdapter.openVersionTable(runtime);
        nodeVersion = logReplicationVersionAdapter.getNodeVersion();
    }
}
