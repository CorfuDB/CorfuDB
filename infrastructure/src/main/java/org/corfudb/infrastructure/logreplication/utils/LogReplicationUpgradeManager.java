package org.corfudb.infrastructure.logreplication.utils;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.LRRollingUpgradeHandler;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.ILogReplicationVersionAdapter;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.LogReplicationPluginConfig;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

/**
 * Manages version and upgrade-related metadata and provides utility methods for the same.
 */
@Slf4j
public class LogReplicationUpgradeManager {
    public static final String LOG_REPLICATION_PLUGIN_VERSION_TABLE = "LogReplicationPluginVersion";

    @Getter
    private LRRollingUpgradeHandler lrRollingUpgradeHandler;

    private ILogReplicationVersionAdapter logReplicationVersionAdapter;

    private final String pluginConfigFilePath;

    // The current version running on the local node, extracted from the plugin
    @Getter
    private static String nodeVersion;

    public LogReplicationUpgradeManager(CorfuRuntime runtime, String pluginConfigFilePath) {
        this.pluginConfigFilePath = pluginConfigFilePath;
        CorfuStore corfuStore = new CorfuStore(runtime);
        initVersionPlugin(runtime);
        initLogReplicationRollingUpgradeHandler(corfuStore);
    }

    /**
     * Instantiate the LogReplicator's Rolling Upgrade Handler and invoke its
     * check the first time, so it can cache the result in the common case
     * where there is no rolling upgrade in progress.
     *
     * @param corfuStore - instance of the store to which the check is made with.
     */
    private void initLogReplicationRollingUpgradeHandler(CorfuStore corfuStore) {
        this.lrRollingUpgradeHandler = new LRRollingUpgradeHandler(logReplicationVersionAdapter);
        final int retries = 3;
        for (int i = retries; i>=0; i--) {
            try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                log.info("LRRollingUpgradeHandler: Prestart check isUpgradeOn: {}",
                    lrRollingUpgradeHandler.isLRUpgradeInProgress(txn));
                txn.commit();
                break;
            } catch (Exception ex) {
                log.error("Fatal error: Failed to get LR upgrade status", ex);
            }
        }
    }

    private void initVersionPlugin(CorfuRuntime runtime) {
        log.info("Version plugin :: {}", pluginConfigFilePath);
        LogReplicationPluginConfig config = new LogReplicationPluginConfig(pluginConfigFilePath);
        File jar = new File(config.getStreamFetcherPluginJARPath());
        try (URLClassLoader child = new URLClassLoader(new URL[]{jar.toURI().toURL()}, this.getClass().getClassLoader())) {
            Class plugin = Class.forName(config.getStreamFetcherClassCanonicalName(), true, child);
            logReplicationVersionAdapter = (ILogReplicationVersionAdapter)
                plugin.getDeclaredConstructor(CorfuRuntime.class).newInstance(runtime);
            nodeVersion = logReplicationVersionAdapter.getNodeVersion();
        } catch (Exception e) {
            log.error("Fatal error: Failed to get Log Replicator Version Plugin", e);
            throw new UnrecoverableCorfuError(e);
        }
    }
}
