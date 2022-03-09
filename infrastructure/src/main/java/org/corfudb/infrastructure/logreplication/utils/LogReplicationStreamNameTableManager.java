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
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.view.TableRegistry;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

import static org.corfudb.runtime.view.ObjectsView.LOG_REPLICATOR_STREAM_INFO;
import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;
import static org.corfudb.runtime.view.TableRegistry.getFullyQualifiedTableName;

/**
 * Handle creation and maintenance of the Corfu table/s containing names of tables
 * to be replicated.
 * @author pankti-m
 */
@Slf4j
public class LogReplicationStreamNameTableManager {

    public static final String LOG_REPLICATION_STREAMS_NAME_TABLE = "LogReplicationStreams";
    public static final String LOG_REPLICATION_PLUGIN_VERSION_TABLE = "LogReplicationPluginVersion";

    private ILogReplicationConfigAdapter logReplicationConfigAdapter;

    private final String pluginConfigFilePath;

    private final CorfuStore corfuStore;

    private static final String EMPTY_STR = "";

    private static final CommonTypes.Uuid defaultMetadata =
        CommonTypes.Uuid.newBuilder().setLsb(0).setMsb(0).build();

    private static final Set<UUID> MERGE_ONLY_STREAM_ID_LIST = new HashSet<>(Arrays.asList(
            CorfuRuntime.getStreamID(getFullyQualifiedTableName(CORFU_SYSTEM_NAMESPACE,
                    TableRegistry.REGISTRY_TABLE_NAME)),
            CorfuRuntime.getStreamID(getFullyQualifiedTableName(CORFU_SYSTEM_NAMESPACE,
                    TableRegistry.PROTOBUF_DESCRIPTOR_TABLE_NAME))
    ));

    public LogReplicationStreamNameTableManager(CorfuRuntime runtime, String pluginConfigFilePath) {
        this.pluginConfigFilePath = pluginConfigFilePath;
        this.corfuStore = new CorfuStore(runtime);

        initStreamNameFetcherPlugin();
    }

    public Set<String> getStreamsToReplicate() {
        // Initialize the streamsToReplicate
        if (verifyTableExists(LOG_REPLICATION_PLUGIN_VERSION_TABLE) &&
            verifyTableExists(LOG_REPLICATION_STREAMS_NAME_TABLE)) {
            // The tables exist but may have been created by another runtime in which case they have to be opened with
            // key/value/metadata type info
            openExistingTable(LOG_REPLICATION_PLUGIN_VERSION_TABLE);
            openExistingTable(LOG_REPLICATION_STREAMS_NAME_TABLE);
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
        log.info(">> PLUGIN IS: {}", pluginConfigFilePath);
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
            // Ignore
        }
        return true;
    }

    private void openExistingTable(String tableName) {
        try {
            if (Objects.equals(tableName, LOG_REPLICATION_STREAMS_NAME_TABLE)) {
                corfuStore.openTable(CORFU_SYSTEM_NAMESPACE, tableName, TableInfo.class,
                    Namespace.class, CommonTypes.Uuid.class, TableOptions.builder().build());
            } else {
                corfuStore.openTable(CORFU_SYSTEM_NAMESPACE, LOG_REPLICATION_PLUGIN_VERSION_TABLE, VersionString.class,
                        Version.class, CommonTypes.Uuid.class, TableOptions.builder().build());
            }
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            log.warn("Exception when opening existing table ", e);
        }
    }

    private boolean tableVersionMatchesPlugin() {
        VersionString versionString = VersionString.newBuilder().setName("VERSION").build();
        CorfuStoreEntry<VersionString, Version, CommonTypes.Uuid> record;

        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            // If the version table is dropped using the CorfuStoreBrowser(UFO) for testing, it will be empty.
            // In this case, it should be re-created
            record = txn.getRecord(LOG_REPLICATION_PLUGIN_VERSION_TABLE, versionString);
            txn.commit();
        }

        return record.getPayload() != null && (Objects.equals(record.getPayload().getVersion(),
                logReplicationConfigAdapter.getVersion()));
    }

    private void deleteExistingStreamNameAndVersionTables() {
        try {
            corfuStore.deleteTable(CORFU_SYSTEM_NAMESPACE, LOG_REPLICATION_STREAMS_NAME_TABLE);
            corfuStore.deleteTable(CORFU_SYSTEM_NAMESPACE, LOG_REPLICATION_PLUGIN_VERSION_TABLE);
        } catch (NoSuchElementException e) {
            // If the table does not exist, simply return
        }
    }

    private void createStreamNameAndVersionTables(Set<String> streams) {
        try {
            Table<TableInfo, Namespace, CommonTypes.Uuid> streamsNameTable = corfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                LOG_REPLICATION_STREAMS_NAME_TABLE,
                TableInfo.class, Namespace.class, CommonTypes.Uuid.class,
                TableOptions.builder().build());

            Table<VersionString, Version, CommonTypes.Uuid> pluginVersionTable = corfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                LOG_REPLICATION_PLUGIN_VERSION_TABLE,
                VersionString.class, Version.class, CommonTypes.Uuid.class,
                TableOptions.builder().build());

            // add registryTable to the streams
            String registryTable = getFullyQualifiedTableName(
                    CORFU_SYSTEM_NAMESPACE, TableRegistry.REGISTRY_TABLE_NAME);
            streams.add(registryTable);

            // add protobufDescriptorTable to the streams
            String protoTable = getFullyQualifiedTableName(
                    CORFU_SYSTEM_NAMESPACE, TableRegistry.PROTOBUF_DESCRIPTOR_TABLE_NAME);
            streams.add(protoTable);

            try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                // Populate the plugin version in the version table
                LogReplicationStreams.VersionString versionString = VersionString.newBuilder()
                                .setName("VERSION").build();
                LogReplicationStreams.Version version = Version.newBuilder()
                                .setVersion(logReplicationConfigAdapter.getVersion()).build();
                txn.putRecord(pluginVersionTable, versionString, version, defaultMetadata);

                // Copy all stream names to the stream names table.  Each name is
                // a fully qualified stream name
                for (String entry : streams) {
                    LogReplicationStreams.TableInfo tableInfo = TableInfo.newBuilder().setName(entry).build();

                    // As each name is fully qualified, no need to insert the
                    // namespace.  Simply insert an empty string there.
                    // TODO: Ideally the Namespace protobuf can be removed but it
                    //  will involve data migration on upgrade as it is a schema
                    //  change
                    LogReplicationStreams.Namespace namespace = Namespace.newBuilder().setName(
                                    EMPTY_STR)
                                    .build();
                    txn.putRecord(streamsNameTable, tableInfo, namespace, defaultMetadata);
                }
                txn.commit();
            }
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            log.warn("Exception when opening the table", e);
        }
    }

    private Set<String> readStreamsToReplicateFromTable() {
        Set<String> tableNames = new HashSet<>();
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            Set<TableInfo> tables = txn.keySet(LOG_REPLICATION_STREAMS_NAME_TABLE);
            tables.forEach(table -> tableNames.add(table.getName()));
            txn.commit();
        }
        return tableNames;
    }

    /**
     * Get stream tags to send data change notifications to on the receiver (sink / standby site)
     *
     * Stream tags will be read from a static configuration file. This file should contain not only
     * the stream tag of interest (namespace, tag), i.e., the stream tag we wish to receive notifications on
     * but also the table names of interest within that tag.
     *
     * Note that, we need the mapping as we cannot infer the stream tag from the replicated data on the sink.
     * Data is replicated at the stream level and not deserialized, hence we cannot infer from the transferred log
     * entries, the tags associated to them.
     *
     * @return map of stream tag UUID to data streams UUIDs.
     */
    public Map<UUID, List<UUID>> getStreamingConfigOnSink() {
        Map<UUID, List<UUID>> streamingConfig = logReplicationConfigAdapter.getStreamingConfigOnSink();
        for (UUID id : MERGE_ONLY_STREAM_ID_LIST) {
            streamingConfig.put(id,
                    Collections.singletonList(LOG_REPLICATOR_STREAM_INFO.getStreamId()));
        }
        return streamingConfig;
    }

    public static Set<UUID> getMergeOnlyStreamIdList() {
        return MERGE_ONLY_STREAM_ID_LIST;
    }
}