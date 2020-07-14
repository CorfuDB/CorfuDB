package org.corfudb.infrastructure.logreplication.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.ILogReplicationConfigAdapter;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.LogReplicationPluginConfig;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.Query;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxBuilder;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.utils.CommonTypes;
import org.corfudb.utils.LogReplicationStreams;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

/**
 * Handle creation and maintenance of the Corfu table/s containing names of tables
 * to be replicated.
 * @author pankti-m
 */
@Slf4j
public class LogReplicationStreamNameTableManager {

    private static final String LOG_REPLICATION_STREAMS_NAME_TABLE = "LogReplicationStreams";
    private static final String LOG_REPLICATION_PLUGIN_VERSION_TABLE = "LogReplicationPluginVersion";
    private static final String EMPTY_STR = "";

    private ILogReplicationConfigAdapter logReplicationConfigAdapter;

    private CorfuRuntime corfuRuntime;

    private String pluginConfigFilePath;

    private String versionInTableOnStartup;

    // If the new version was persisted in the version table after detecting an upgrade
    private boolean upgradeProcessed;

    // If there was an upgrade in this boot cycle, this is the set of streams not present in the new streams to replicate
    private Set<String> diffStreamsAfterUpgrade;


    public LogReplicationStreamNameTableManager(CorfuRuntime runtime, String pluginConfigFilePath) {
        this.pluginConfigFilePath = pluginConfigFilePath;
        this.corfuRuntime = runtime;
        logReplicationConfigAdapter = initStreamNameFetcherPlugin();
        versionInTableOnStartup = getVersionInTableOnStartup();
        upgradeProcessed = false;
    }

    private ILogReplicationConfigAdapter initStreamNameFetcherPlugin() {
        LogReplicationPluginConfig config = new LogReplicationPluginConfig(pluginConfigFilePath);
        File jar = new File(config.getStreamFetcherPluginJARPath());
        try (URLClassLoader child = new URLClassLoader(new URL[]{jar.toURI().toURL()}, this.getClass().getClassLoader())) {
            Class plugin = Class.forName(config.getStreamFetcherClassCanonicalName(), true, child);
            ILogReplicationConfigAdapter adapter = (ILogReplicationConfigAdapter) plugin.getDeclaredConstructor()
                    .newInstance();
            return adapter;
        } catch (Exception e) {
            log.error("Fatal error: Failed to get Stream Fetcher Plugin", e);
            throw new UnrecoverableCorfuError(e);
        }
    }

    private String getVersionInTableOnStartup() {
        if (!tableExists(LOG_REPLICATION_PLUGIN_VERSION_TABLE)) {
            // Table does not exist.  This means LogReplication has never run on this setup
            return EMPTY_STR;
        }
        CorfuStore corfuStore = new CorfuStore(corfuRuntime);
        corfuStore.openTable(CORFU_SYSTEM_NAMESPACE, LOG_REPLICATION_PLUGIN_VERSION_TABLE);
        LogReplicationStreams.VersionString versionString = LogReplicationStreams.VersionString.newBuilder().setName("VERSION").build();
        Query q = corfuStore.query(CORFU_SYSTEM_NAMESPACE);

        // If the version table is dropped using the CorfuStoreBrowser(UFO) for testing, it will be empty.
        // In this case, return an empty string, equivalent to an invalid version
        if (q.getRecord(LOG_REPLICATION_PLUGIN_VERSION_TABLE, versionString) == null) {
            return EMPTY_STR;
        }
        LogReplicationStreams.Version version = (LogReplicationStreams.Version) q.getRecord(LOG_REPLICATION_PLUGIN_VERSION_TABLE,
                versionString).getPayload();
        return version.getVersion();
    }

    private boolean tableExists(String tableName) {
        CorfuStore corfuStore = new CorfuStore(corfuRuntime);
        try {
            corfuStore.openTable(CORFU_SYSTEM_NAMESPACE, tableName);
        } catch (NoSuchElementException e) {
            // Table does not exist
            return false;
        } catch (IllegalArgumentException e) { }
        return true;
    }

    public Set<String> getStreamsToReplicate(boolean refresh) {
        if (refresh) {
            deleteExistingStreamNameAndVersionTables();
            createStreamNameAndVersionTables(
                    logReplicationConfigAdapter.fetchStreamsToReplicate());
        }
        else {
            // Initialize the streamsToReplicate
            if (tableExists(LOG_REPLICATION_PLUGIN_VERSION_TABLE) &&
                    tableExists(LOG_REPLICATION_STREAMS_NAME_TABLE)) {
                // The tables exist but may have been created by another runtime in which case they have to be opened with
                // key/value/metadata type info
                openExistingTable(LOG_REPLICATION_PLUGIN_VERSION_TABLE);
                openExistingTable(LOG_REPLICATION_STREAMS_NAME_TABLE);
                if (wasUpgraded() && !upgradeProcessed) {
                    // delete the tables and recreate them
                    Set<String> oldStreams = readStreamsToReplicateFromTable();
                    deleteExistingStreamNameAndVersionTables();
                    Map<String, String> newStreamsMap = logReplicationConfigAdapter.fetchStreamsToReplicate();
                    createStreamNameAndVersionTables(newStreamsMap);
                    diffStreamsAfterUpgrade = calculateStreamsDiff(oldStreams, newStreamsMap.keySet());
                    upgradeProcessed = true;
                }
            } else {
                // If any 1 or both tables does not exist, delete and recreate them both as they may have been corrupted.
                // TODO pankti: This may be an error scenario and should be flagged.
                deleteExistingStreamNameAndVersionTables();
                createStreamNameAndVersionTables(
                        logReplicationConfigAdapter.fetchStreamsToReplicate());
            }
        }
        return readStreamsToReplicateFromTable();
    }

    private void openExistingTable(String tableName) {
        CorfuStore corfuStore = new CorfuStore(corfuRuntime);
        try {
            if (Objects.equals(tableName, LOG_REPLICATION_STREAMS_NAME_TABLE)) {
                corfuStore.openTable(CORFU_SYSTEM_NAMESPACE, tableName, LogReplicationStreams.TableInfo.class,
                        LogReplicationStreams.Namespace.class, CommonTypes.Uuid.class, TableOptions.builder().build());
            } else {
                corfuStore.openTable(CORFU_SYSTEM_NAMESPACE, LOG_REPLICATION_PLUGIN_VERSION_TABLE, LogReplicationStreams.VersionString.class,
                        LogReplicationStreams.Version.class, CommonTypes.Uuid.class, TableOptions.builder().build());
            }
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            log.warn("Exception when opening existing table {}", e);
        }
    }

    private void deleteExistingStreamNameAndVersionTables() {
        CorfuStore corfuStore = new CorfuStore(corfuRuntime);
        try {
            corfuStore.deleteTable(CORFU_SYSTEM_NAMESPACE, LOG_REPLICATION_STREAMS_NAME_TABLE);
            corfuStore.deleteTable(CORFU_SYSTEM_NAMESPACE, LOG_REPLICATION_PLUGIN_VERSION_TABLE);
        } catch (NoSuchElementException e) {
            // If the table does not exist, simply return
            return;
        }
    }

    private void createStreamNameAndVersionTables(Map<String, String> streams) {
        CorfuStore corfuStore = new CorfuStore(corfuRuntime);
        try {
            corfuStore.openTable(CORFU_SYSTEM_NAMESPACE, LOG_REPLICATION_STREAMS_NAME_TABLE, LogReplicationStreams.TableInfo.class,
                    LogReplicationStreams.Namespace.class, CommonTypes.Uuid.class, TableOptions.builder().build());
            corfuStore.openTable(CORFU_SYSTEM_NAMESPACE, LOG_REPLICATION_PLUGIN_VERSION_TABLE, LogReplicationStreams.VersionString.class,
                    LogReplicationStreams.Version.class, CommonTypes.Uuid.class, TableOptions.builder().build());
            TxBuilder tx = corfuStore.tx(CORFU_SYSTEM_NAMESPACE);

            // Populate the plugin version in the version table
            LogReplicationStreams.VersionString versionString = LogReplicationStreams.VersionString.newBuilder().setName("VERSION").build();
            LogReplicationStreams.Version version = LogReplicationStreams.Version.newBuilder().setVersion(logReplicationConfigAdapter.getVersion()).build();
            CommonTypes.Uuid uuid = CommonTypes.Uuid.newBuilder().setLsb(0L).setMsb(0L).build();
            tx.create(LOG_REPLICATION_PLUGIN_VERSION_TABLE, versionString, version, uuid);

            // Copy all stream names to the stream names table
            for (Map.Entry<String, String> entry : streams.entrySet()) {
                LogReplicationStreams.TableInfo tableInfo = LogReplicationStreams.TableInfo.newBuilder().setName(entry.getKey()).build();
                LogReplicationStreams.Namespace namespace = LogReplicationStreams.Namespace.newBuilder().setName(entry.getValue()).build();
                uuid = CommonTypes.Uuid.newBuilder().setLsb(0L).setMsb(0L).build();
                tx.create(LOG_REPLICATION_STREAMS_NAME_TABLE, tableInfo, namespace, uuid);
            }
            tx.commit();
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            log.warn("Exception when opening the table {}", e);
        }
    }

    private Set<String> readStreamsToReplicateFromTable() {
        CorfuStore corfuStore = new CorfuStore(corfuRuntime);
        corfuStore.openTable(CORFU_SYSTEM_NAMESPACE, LOG_REPLICATION_STREAMS_NAME_TABLE);
        Query q = corfuStore.query(CORFU_SYSTEM_NAMESPACE);
        Set<LogReplicationStreams.TableInfo> tables = q.keySet(LOG_REPLICATION_STREAMS_NAME_TABLE, null);
        Set<String> tableNames = new HashSet<>();
        tables.forEach(table -> {
            tableNames.add(table.getName());
        });
        return tableNames;
    }

    /**
     * Returns whether the version changed on this boot cycle.
     * @return if the version changed in this boot cycle
     */
    public boolean wasUpgraded() {
        return Objects.equals(versionInTableOnStartup, logReplicationConfigAdapter.getVersion());
    }

    private Set<String> calculateStreamsDiff(Set<String> oldSet, Set<String> newSet) {
        Set<String> difference = new HashSet<>(oldSet);
        difference.removeAll(newSet);
        return difference;
    }

    /**
     * Returns the streams no longer replicated in the new version.  This method is assumed to be invoked after the diff
     * has been calculated, i.e., after upgrade has been detected and processed in getStreamsToReplicate()
     * @return Set of streams no longer replicated in the new version.
     */
    public Set<String> getStreamsDiffBetweenVersions() {
        return diffStreamsAfterUpgrade;
    }
}
