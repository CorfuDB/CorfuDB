package org.corfudb.infrastructure.logreplication.utils;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.LRRollingUpgradeHandler;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.ILogReplicationVersionAdapter;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.LogReplicationPluginConfig;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStoreEntry;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.IntervalRetry;
import org.corfudb.util.retry.RetryNeededException;
import org.corfudb.utils.CommonTypes.Uuid;
import org.corfudb.utils.LogReplicationStreams.Version;
import org.corfudb.utils.LogReplicationStreams.VersionString;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.NoSuchElementException;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

/**
 * Manages version and upgrade-related metadata and provides utility methods for the same.
 */
@Slf4j
public class LogReplicationUpgradeManager {
    public static final String LOG_REPLICATION_PLUGIN_VERSION_TABLE = "LogReplicationPluginVersion";

    // TODO: During a rolling upgrade, all nodes will not be on the same version.  This key type must change to
    //  reflect the version per node.
    public static final String VERSION_PLUGIN_KEY = "VERSION";

    private final VersionString versionString = VersionString.newBuilder().setName(VERSION_PLUGIN_KEY).build();

    private final CorfuStore corfuStore;

    @Getter
    private LRRollingUpgradeHandler lrRollingUpgradeHandler;

    private ILogReplicationVersionAdapter logReplicationVersionAdapter;

    private final String pluginConfigFilePath;

    // The current version running on the local node, extracted from the plugin
    @Getter
    private static String nodeVersion;

    // TODO: Metadata type need not be uuid.  It can be a generic protobuf message.
    private Table<VersionString, Version, Uuid> pluginVersionTable;

    public LogReplicationUpgradeManager(CorfuRuntime runtime, String pluginConfigFilePath) {
        this.pluginConfigFilePath = pluginConfigFilePath;
        this.corfuStore = new CorfuStore(runtime);
        initVersionPlugin(runtime);
        setupVersionTable();
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
        } catch (Exception e) {
            log.error("Fatal error: Failed to get Log Replicator Version Plugin", e);
            throw new UnrecoverableCorfuError(e);
        }
    }

    /**
     * Initiate version table for upcoming version checks.
     */
    private void setupVersionTable() {
        try {
            pluginVersionTable = corfuStore.openTable(CORFU_SYSTEM_NAMESPACE, LOG_REPLICATION_PLUGIN_VERSION_TABLE,
                VersionString.class, Version.class, Uuid.class, TableOptions.builder().build());
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            log.warn("Exception when opening version table", e);
            throw new UnrecoverableCorfuError(e);
        }
        try {
            if (nodeVersion == null) {
                nodeVersion = logReplicationVersionAdapter.getNodeVersion();
            }
            IRetry.build(IntervalRetry.class, () -> {
                try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                    VersionResult result = verifyVersionResult(txn);
                    if (result.equals(VersionResult.UNSET) || result.equals(VersionResult.CHANGE)) {
                        // Case of upgrade or initial boot: sync version table with plugin info
                        boolean isUpgraded = result.equals(VersionResult.CHANGE);
                        log.info("Current version from plugin = {}, isUpgraded = {}", nodeVersion, isUpgraded);
                        // Persist upgrade flag so a snapshot-sync is enforced upon negotiation
                        // (when it is set to true)
                        Version version = Version.newBuilder()
                            .setVersion(nodeVersion)
                            .setIsUpgraded(isUpgraded)
                            .build();
                        txn.putRecord(pluginVersionTable, versionString, version, null);
                    }
                    txn.commit();
                    return null;
                } catch (TransactionAbortedException e) {
                    log.warn("Exception on getStreamsToReplicate()", e);
                    throw new RetryNeededException();
                }
            }).run();
        } catch (InterruptedException e) {
            log.error("Unrecoverable exception when updating version table", e);
            throw new UnrecoverableCorfuInterruptedError(e);
        }
    }

    /**
     * Helper method for flipping the boolean flag back to false in version table which
     * indicates the LR upgrading path is complete.
     */
    public void resetUpgradeFlag() {
        log.info("Reset isUpgraded flag");
        try {
            IRetry.build(IntervalRetry.class, () -> {
                try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                    VersionString versionString = VersionString.newBuilder().setName(VERSION_PLUGIN_KEY).build();
                    CorfuStoreEntry<VersionString, Version, Uuid> versionEntry =
                        txn.getRecord(LOG_REPLICATION_PLUGIN_VERSION_TABLE, versionString);
                    Version version = Version.newBuilder().mergeFrom(versionEntry.getPayload()).setIsUpgraded(false).build();
                    log.info("Old Version Info: {}.  Updating to {}", versionEntry.getPayload(), version);
                    txn.putRecord(pluginVersionTable, versionString, version, null);
                    txn.commit();
                } catch (TransactionAbortedException e) {
                    log.warn("Exception when resetting upgrade flag in version table", e);
                    throw new RetryNeededException();
                }
                return null;
            }).run();
        } catch (InterruptedException e) {
            log.error("Unrecoverable exception when resetting upgrade flag in version table", e);
            throw new UnrecoverableCorfuInterruptedError(e);
        }
    }

    /**
     * Helper method for checking whether LR is in upgrading path or not.
     * Note that the default boolean value for ProtoBuf message is false.
     *
     * @return True if LR is in upgrading path, false otherwise.
     */
    public boolean isUpgraded() {
        VersionString versionString = VersionString.newBuilder().setName(VERSION_PLUGIN_KEY).build();
        CorfuStoreEntry<VersionString, Version, Uuid> entry;

        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            entry = txn.getRecord(LOG_REPLICATION_PLUGIN_VERSION_TABLE, versionString);
            txn.commit();
        } catch (NoSuchElementException e) {
            // Normally this will not happen as version table should be initialized during bootstrap
            log.error("Version table has not been initialized", e);
            return false;
        }
        return entry.getPayload().getIsUpgraded();
    }

    private VersionResult verifyVersionResult(TxnContext txn) {
        CorfuStoreEntry<VersionString, Version, Uuid> entry =
            txn.getRecord(LOG_REPLICATION_PLUGIN_VERSION_TABLE, versionString);
        if (entry.getPayload() == null) {
            log.info("LR initializing. Version unset");
            return VersionResult.UNSET;
        } else if (!entry.getPayload().getVersion().equals(nodeVersion)) {
            log.info("LR upgraded. Version changed from {} to {}", nodeVersion, entry.getPayload().getVersion());
            return VersionResult.CHANGE;
        }
        return VersionResult.SAME;
    }

    private static enum VersionResult {
        UNSET,
        CHANGE,
        SAME
    }
}
