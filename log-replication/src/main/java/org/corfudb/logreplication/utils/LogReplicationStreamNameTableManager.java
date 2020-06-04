package org.corfudb.logreplication.utils;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.LogReplicationPluginConfig;
import org.corfudb.logreplication.runtime.LogReplicationStreamNameFetcher;
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

@Slf4j
public class LogReplicationStreamNameTableManager {

    private static final String LOG_REPLICATION_STREAMS_NAME_TABLE = "LogReplicationStreams";
    private static final String LOG_REPLICATION_PLUGIN_VERSION_TABLE = "LogReplicationPluginVersion";

    private static final String PLUGIN_CONFIG_FILE_PATH = "/config/corfu/corfu_plugin_config.properties";

    private LogReplicationStreamNameFetcher logReplicationStreamNameFetcher;

    private CorfuRuntime corfuRuntime;

    public static Set<String> getStreamsToReplicate(String corfuPort) {
        LogReplicationStreamNameTableManager streamNameTableManager =
            new LogReplicationStreamNameTableManager(corfuPort);
        return streamNameTableManager.readStreamsToReplicateFromTable();
    }

    private LogReplicationStreamNameTableManager(String corfuPort) {
        // Create Corfu Table with Names of Tables to be replicated
        connectToCorfuRuntime(corfuPort);
        initStreamNameFetcherPlugin();

        // Initialize the streamsToReplicate
        if (verifyTableExists(LOG_REPLICATION_PLUGIN_VERSION_TABLE) &&
                verifyTableExists(LOG_REPLICATION_STREAMS_NAME_TABLE)) {
            // The tables exist but may have been created by another runtime in which case they have to be opened with
            // key/value/metadata type info
            openExistingStreamNameAndVersionTables();
            if (!tableVersionMatchesPlugin()) {
                // delete the tables and recreate them
                deleteExistingStreamNameAndVersionTables();
                createStreamNameAndVersionTables(logReplicationStreamNameFetcher.fetchStreamsToReplicate());
            }
        } else {
            // If any 1 of the 2 tables does not exist, delete and recreate them both as they may have been corrupted.
            deleteExistingStreamNameAndVersionTables();
            createStreamNameAndVersionTables(logReplicationStreamNameFetcher.fetchStreamsToReplicate());
        }
    }

    private void connectToCorfuRuntime(String corfuPort) {
        String corfuEndpoint = "localhost" + corfuPort;
        corfuRuntime = CorfuRuntime.fromParameters(CorfuRuntime.CorfuRuntimeParameters.builder().build())
                .parseConfigurationString(corfuEndpoint);
        corfuRuntime.connect();
    }

    private void initStreamNameFetcherPlugin() {
        LogReplicationPluginConfig config = new LogReplicationPluginConfig(PLUGIN_CONFIG_FILE_PATH);
        File jar = new File(config.getStreamFetcherPluginJARPath());
        try (URLClassLoader child = new URLClassLoader(new URL[]{jar.toURI().toURL()}, this.getClass().getClassLoader())) {
            Class plugin = Class.forName(config.getStreamFetcherClassCanonicalName(), true, child);
            logReplicationStreamNameFetcher = (LogReplicationStreamNameFetcher) plugin.getDeclaredConstructor()
                    .newInstance();
        } catch (Exception e) {
            log.error("Fatal error: Failed to get Stream Fetcher Plugin", e);
            throw new UnrecoverableCorfuError(e);
        }
    }

    private boolean verifyTableExists(String tableName) {
        CorfuStore corfuStore = new CorfuStore(corfuRuntime);
        try {
            corfuStore.openTable(CORFU_SYSTEM_NAMESPACE, tableName);
        } catch (NoSuchElementException e) {
            // Table does not exist
            return false;
        } catch (IllegalArgumentException e) { }
        return true;
    }

    private void openExistingStreamNameAndVersionTables() {
        CorfuStore corfuStore = new CorfuStore(corfuRuntime);
        try {
            corfuStore.openTable(CORFU_SYSTEM_NAMESPACE, LOG_REPLICATION_STREAMS_NAME_TABLE, LogReplicationStreams.TableInfo.class,
                    LogReplicationStreams.Namespace.class, CommonTypes.Uuid.class, TableOptions.builder().build());
            corfuStore.openTable(CORFU_SYSTEM_NAMESPACE, LOG_REPLICATION_PLUGIN_VERSION_TABLE, LogReplicationStreams.VersionString.class,
                    LogReplicationStreams.Version.class, CommonTypes.Uuid.class, TableOptions.builder().build());
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            log.warn("Exception when opening existing table {}", e);
        }
    }

    private boolean tableVersionMatchesPlugin() {
        CorfuStore corfuStore = new CorfuStore(corfuRuntime);
        corfuStore.openTable(CORFU_SYSTEM_NAMESPACE, LOG_REPLICATION_PLUGIN_VERSION_TABLE);
        LogReplicationStreams.VersionString versionString = LogReplicationStreams.VersionString.newBuilder().setName("Version").build();
        Query q = corfuStore.query(CORFU_SYSTEM_NAMESPACE);
        LogReplicationStreams.Version version = (LogReplicationStreams.Version) q.getRecord(LOG_REPLICATION_PLUGIN_VERSION_TABLE, versionString).getPayload();
        return (Objects.equals(version.getVersion(), logReplicationStreamNameFetcher.getVersion()));
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
            LogReplicationStreams.VersionString versionString = LogReplicationStreams.VersionString.newBuilder().setName("Version").build();
            LogReplicationStreams.Version version = LogReplicationStreams.Version.newBuilder().setVersion(logReplicationStreamNameFetcher.getVersion()).build();
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
}
