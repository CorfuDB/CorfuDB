package org.corfudb.infrastructure.logreplication.utils;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.ServerContext;
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
import org.corfudb.utils.CommonTypes;
import org.corfudb.utils.LogReplicationStreams;
import org.corfudb.utils.LogReplicationStreams.Version;
import org.corfudb.utils.LogReplicationStreams.VersionString;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.NoSuchElementException;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

/**
 * Represents Log Replicator upgrade manager which aims to support rolling upgrade workflow
 */
@Slf4j
public class UpgradeManager {

    public static final String LOG_REPLICATION_PLUGIN_VERSION_TABLE = "LogReplicationPluginVersion";
    public static final String VERSION_PLUGIN_KEY = "VERSION";

    private static final CommonTypes.Uuid defaultMetadata = CommonTypes.Uuid.newBuilder().setLsb(0).setMsb(0).build();

    private final VersionString versionString = LogReplicationStreams.VersionString
            .newBuilder().setName(VERSION_PLUGIN_KEY).build();

    private final CorfuStore corfuStore;

    @Getter
    private LRRollingUpgradeHandler lrRollingUpgradeHandler;

    private ILogReplicationVersionAdapter logReplicationVersionAdapter;

    private final String pluginConfigFilePath;

    @Getter
    private static String currentVersion;

    private Table<LogReplicationStreams.VersionString, LogReplicationStreams.Version, CommonTypes.Uuid> pluginVersionTable;

    public UpgradeManager(CorfuRuntime runtime, String pluginConfigFilePath) {
        this.pluginConfigFilePath = pluginConfigFilePath;
        this.corfuStore = new CorfuStore(runtime);
        initVersionPlugin(runtime);
        setupVersionTable();
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
                    LogReplicationStreams.VersionString.class, Version.class, CommonTypes.Uuid.class, TableOptions.builder().build());
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            log.warn("Exception when opening version table", e);
            throw new UnrecoverableCorfuError(e);
        }
        try {
            if (currentVersion == null) {
                currentVersion = logReplicationVersionAdapter.getNodeVersion();
            }
            IRetry.build(IntervalRetry.class, () -> {
                try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                    UpgradeManager.VersionResult result = verifyVersionResult(txn);
                    if (result.equals(UpgradeManager.VersionResult.UNSET) || result.equals(UpgradeManager.VersionResult.CHANGE)) {
                        // Case of upgrade or initial boot: sync version table with plugin info
                        boolean isUpgraded = result.equals(UpgradeManager.VersionResult.CHANGE);
                        log.info("Current version from plugin = {}, isUpgraded = {}", currentVersion, isUpgraded);
                        // Persist upgrade flag so a snapshot-sync is enforced upon negotiation
                        // (when it is set to true)
                        LogReplicationStreams.Version version = LogReplicationStreams.Version.newBuilder()
                                .setVersion(currentVersion)
                                .setIsUpgraded(isUpgraded)
                                .build();
                        txn.putRecord(pluginVersionTable, versionString, version, defaultMetadata);
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
                    LogReplicationStreams.VersionString versionString = VersionString.newBuilder().setName(VERSION_PLUGIN_KEY).build();
                    CorfuStoreEntry<LogReplicationStreams.VersionString, LogReplicationStreams.Version, CommonTypes.Uuid> versionEntry =
                            txn.getRecord(LOG_REPLICATION_PLUGIN_VERSION_TABLE, versionString);
                    LogReplicationStreams.Version version = LogReplicationStreams.Version.newBuilder().mergeFrom(versionEntry.getPayload()).setIsUpgraded(false).build();
                    txn.putRecord(pluginVersionTable, versionString, version, defaultMetadata);
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
        CorfuStoreEntry<VersionString, Version, CommonTypes.Uuid> record;

        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            record = txn.getRecord(LOG_REPLICATION_PLUGIN_VERSION_TABLE, versionString);
            txn.commit();
        } catch (NoSuchElementException e) {
            // Normally this will not happen as version table should be initialized during bootstrap
            log.error("Version table has not been initialized", e);
            return false;
        }
        return record.getPayload().getIsUpgraded();
    }

    private VersionResult verifyVersionResult(TxnContext txn) {
        CorfuStoreEntry<LogReplicationStreams.VersionString, LogReplicationStreams.Version, CommonTypes.Uuid> record =
                txn.getRecord(LOG_REPLICATION_PLUGIN_VERSION_TABLE, versionString);
        if (record.getPayload() == null) {
            log.info("LR initializing. Version unset");
            return VersionResult.UNSET;
        } else if (!record.getPayload().getVersion().equals(currentVersion)) {
            log.info("LR upgraded. Version changed from {} to {}", currentVersion, record.getPayload().getVersion());
            return VersionResult.CHANGE;
        }
        return VersionResult.SAME;
    }

    private enum VersionResult {
        UNSET,
        CHANGE,
        SAME
    }
}
