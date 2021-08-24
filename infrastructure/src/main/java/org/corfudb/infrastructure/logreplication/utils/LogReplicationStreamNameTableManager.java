package org.corfudb.infrastructure.logreplication.utils;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.ILogReplicationConfigAdapter;
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
import org.corfudb.utils.LogReplicationStreams.VersionString;
import org.corfudb.utils.LogReplicationStreams.Version;
import org.corfudb.utils.LogReplicationStreams.TableInfo;
import org.corfudb.utils.LogReplicationStreams.Namespace;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

/**
 * Handle creation and maintenance of the Corfu table/s containing names of tables
 * to be replicated.
 * @author pankti-m
 */
@Slf4j
public class LogReplicationStreamNameTableManager {

    public static final String LOG_REPLICATION_STREAMS_INFO_TABLE = "LogReplicationStreams";
    public static final String LOG_REPLICATION_PLUGIN_VERSION_TABLE = "LogReplicationPluginVersion";

    private ILogReplicationConfigAdapter logReplicationConfigAdapter;

    private String pluginConfigFilePath;

    private CorfuStore corfuStore;

    private static final String EMPTY_STR = "";

    private static final CommonTypes.Uuid defaultMetadata =
        CommonTypes.Uuid.newBuilder().setLsb(0).setMsb(0).build();

    public LogReplicationStreamNameTableManager(CorfuRuntime runtime, String pluginConfigFilePath) {
        this.pluginConfigFilePath = pluginConfigFilePath;
        this.corfuStore = new CorfuStore(runtime);

        initStreamNameFetcherPlugin();
    }

    public Set<TableInfo> getStreamsToReplicate() {
        // Initialize the streamsToReplicate
        if (verifyTableExists(LOG_REPLICATION_PLUGIN_VERSION_TABLE) &&
            verifyTableExists(LOG_REPLICATION_STREAMS_INFO_TABLE)) {
            // The tables exist but may have been created by another runtime in which case they have to be opened with
            // key/value/metadata type info
            openExistingTable(LOG_REPLICATION_PLUGIN_VERSION_TABLE);
            openExistingTable(LOG_REPLICATION_STREAMS_INFO_TABLE);
            if (!tableVersionMatchesPlugin()) {
                // delete the tables and recreate them
                deleteExistingStreamNameAndVersionTables();
                createStreamNameAndVersionTables(
                    logReplicationConfigAdapter.fetchStreamsToReplicate());
            }
        } else {
            // If any 1 of the 2 tables does not exist, delete and recreate them both as they may have been corrupted.
            deleteExistingStreamNameAndVersionTables();
            createStreamNameAndVersionTables(
                logReplicationConfigAdapter.fetchStreamsToReplicate());
        }
        return readStreamsToReplicateFromTable();
    }
    
    public void addStreamsToInfoTable(Set<UUID> streamIdSet) {
        try {
            Table<TableInfo, Namespace, CommonTypes.Uuid> streamsInfoTable = corfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                    LOG_REPLICATION_STREAMS_INFO_TABLE,
                    TableInfo.class, Namespace.class, CommonTypes.Uuid.class,
                    TableOptions.builder().build());

            addStreamsToInfoTableWithRetry(streamIdSet, streamsInfoTable);
        } catch (Exception e) {
            log.warn("Exception when opening LR stream info table ", e);
        }
    }

    private void addStreamsToInfoTableWithRetry(Set<UUID> streamIdSet,
                                                Table<TableInfo, Namespace, CommonTypes.Uuid> streamsInfoTable) {
        try {
            IRetry.build(IntervalRetry.class, () -> {
                try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                    for (UUID id : streamIdSet) {
                        LogReplicationStreams.TableInfo tableInfo =
                                LogReplicationStreams.TableInfo.newBuilder()
                                        .setId(id.toString())
                                        .build();

                        LogReplicationStreams.Namespace namespace =
                                LogReplicationStreams.Namespace.newBuilder()
                                        .setName(EMPTY_STR)
                                        .build();
                        txn.putRecord(streamsInfoTable, tableInfo, namespace, defaultMetadata);
                    }
                    txn.commit();
                } catch (TransactionAbortedException tae) {
                    throw new RetryNeededException();
                }

                log.debug("Successfully added streams {} to the info table.", streamIdSet);
                return null;
            }).run();
        } catch (InterruptedException ie) {
            log.error("Unrecoverable exception when attempting to add streams.", ie);
            throw new UnrecoverableCorfuInterruptedError(ie);
        }
    }

    public boolean isUpgraded() {
        if (verifyTableExists(LOG_REPLICATION_PLUGIN_VERSION_TABLE)) {
            openExistingTable(LOG_REPLICATION_PLUGIN_VERSION_TABLE);
            return !tableVersionMatchesPlugin();
        }
        // TODO pankti: this may be the first time the replication server is initialized, so return false.
        //  But what about a case if the user has deleted the table?
        return false;
    }

    private void initStreamNameFetcherPlugin() {
        LogReplicationPluginConfig config = new LogReplicationPluginConfig(pluginConfigFilePath);
        File jar = new File(config.getStreamFetcherPluginJARPath());
        try (URLClassLoader child = new URLClassLoader(new URL[]{jar.toURI().toURL()}, this.getClass().getClassLoader())) {
            Class plugin = Class.forName(config.getStreamFetcherClassCanonicalName(), true, child);
            logReplicationConfigAdapter = (ILogReplicationConfigAdapter) plugin.getDeclaredConstructor()
                    .newInstance();
        } catch (Exception e) {
            log.error("Fatal error: Failed to get Stream Fetcher Plugin", e);
            throw new UnrecoverableCorfuError(e);
        }
    }

    private boolean verifyTableExists(String tableName) {
        try {
            corfuStore.getTable(CORFU_SYSTEM_NAMESPACE, tableName);
        } catch (NoSuchElementException e) {
            // Table does not exist
            return false;
        } catch (IllegalArgumentException e) {
            // Table exists
        }
        return true;
    }

    private void openExistingTable(String tableName) {
        try {
            if (Objects.equals(tableName, LOG_REPLICATION_STREAMS_INFO_TABLE)) {
                corfuStore.openTable(CORFU_SYSTEM_NAMESPACE, tableName, LogReplicationStreams.TableInfo.class,
                    LogReplicationStreams.Namespace.class, CommonTypes.Uuid.class, TableOptions.builder().build());
            } else {
                corfuStore.openTable(CORFU_SYSTEM_NAMESPACE, LOG_REPLICATION_PLUGIN_VERSION_TABLE, LogReplicationStreams.VersionString.class,
                        LogReplicationStreams.Version.class, CommonTypes.Uuid.class, TableOptions.builder().build());
            }
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            log.warn("Exception when opening existing table {}", tableName, e);
        }
    }

    private boolean tableVersionMatchesPlugin() {
        VersionString versionString = LogReplicationStreams.VersionString.newBuilder().setName("VERSION").build();
        CorfuStoreEntry<VersionString, Version, CommonTypes.Uuid> record;

        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            // If the version table is dropped using the CorfuStoreBrowser(UFO) for testing, it will be empty.
            // In this case, it should be re-created
            record = txn.getRecord(LOG_REPLICATION_PLUGIN_VERSION_TABLE, versionString);
            txn.commit();
        }

        return record == null ? false : (Objects.equals(record.getPayload().getVersion(),
                logReplicationConfigAdapter.getVersion()));
    }

    private void deleteExistingStreamNameAndVersionTables() {
        try {
            corfuStore.deleteTable(CORFU_SYSTEM_NAMESPACE, LOG_REPLICATION_STREAMS_INFO_TABLE);
            corfuStore.deleteTable(CORFU_SYSTEM_NAMESPACE, LOG_REPLICATION_PLUGIN_VERSION_TABLE);
        } catch (NoSuchElementException e) {
            // If the table does not exist, simply return
        }
    }

    private void createStreamNameAndVersionTables(Set<String> streams) {
        try {
            Table<TableInfo, Namespace, CommonTypes.Uuid> streamsInfoTable = corfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                    LOG_REPLICATION_STREAMS_INFO_TABLE,
                TableInfo.class, Namespace.class, CommonTypes.Uuid.class,
                TableOptions.builder().build());

            Table<VersionString, Version, CommonTypes.Uuid> pluginVersionTable = corfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                LOG_REPLICATION_PLUGIN_VERSION_TABLE,
                VersionString.class, Version.class, CommonTypes.Uuid.class,
                TableOptions.builder().build());

            try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                // Populate the plugin version in the version table
                LogReplicationStreams.VersionString versionString =
                        LogReplicationStreams.VersionString.newBuilder()
                                .setName("VERSION").build();
                LogReplicationStreams.Version version =
                        LogReplicationStreams.Version.newBuilder()
                                .setVersion(logReplicationConfigAdapter.getVersion()).build();
                txn.putRecord(pluginVersionTable, versionString, version, defaultMetadata);

                // Copy all stream names to the stream names table.  Each name is
                // a fully qualified stream name
                for (String entry : streams) {
                    LogReplicationStreams.TableInfo tableInfo =
                            LogReplicationStreams.TableInfo.newBuilder().setName(entry)
                                    .build();

                    // As each name is fully qualified, no need to insert the
                    // namespace.  Simply insert an empty string there.
                    // TODO: Ideally the Namespace protobuf can be removed but it
                    //  will involve data migration on upgrade as it is a schema
                    //  change
                    LogReplicationStreams.Namespace namespace =
                            LogReplicationStreams.Namespace.newBuilder().setName(
                                    EMPTY_STR)
                                    .build();
                    txn.putRecord(streamsInfoTable, tableInfo, namespace, defaultMetadata);
                }
                txn.commit();
            }
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            log.warn("Exception when opening LR version/stream table ", e);
        }
    }

    private Set<TableInfo> readStreamsToReplicateFromTable() {
        Set<TableInfo> tableInfoSet = new HashSet<>();
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            Set<TableInfo> tables = txn.keySet(LOG_REPLICATION_STREAMS_INFO_TABLE);
            tableInfoSet.addAll(tables);
            txn.commit();
        }
        return tableInfoSet;
    }
}
