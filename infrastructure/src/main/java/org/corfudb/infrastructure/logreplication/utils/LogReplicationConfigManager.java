package org.corfudb.infrastructure.logreplication.utils;

import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.ILogReplicationConfigAdapter;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.LogReplicationPluginConfig;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.collections.CorfuRecord;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStoreEntry;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.corfudb.runtime.view.TableRegistry;
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
import java.util.Arrays;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;
import static org.corfudb.runtime.view.TableRegistry.getFullyQualifiedTableName;

/**
 * Handle creation and maintenance of the Corfu table/s containing names of tables
 * to be replicated.
 * @author pankti-m
 */
@Slf4j
public class LogReplicationConfigManager {

    public static final String LOG_REPLICATION_STREAMS_INFO_TABLE = "LogReplicationStreams";
    public static final String LOG_REPLICATION_PLUGIN_VERSION_TABLE = "LogReplicationPluginVersion";
    public static final String VERSION_PLUGIN_KEY = "VERSION";
    private static final String EMPTY_STR = "";

    private ILogReplicationConfigAdapter logReplicationConfigAdapter;

    private final String pluginConfigFilePath;

    private final VersionString versionString = VersionString.newBuilder().setName(VERSION_PLUGIN_KEY).build();

    private final LogReplicationStreams.Namespace defaultNamespace = Namespace
            .newBuilder()
            .setName(EMPTY_STR)
            .build();

    private final CorfuStore corfuStore;

    private final CorfuRuntime runtime;

    private static final CommonTypes.Uuid defaultMetadata =
        CommonTypes.Uuid.newBuilder().setLsb(0).setMsb(0).build();

    private static final Set<UUID> MERGE_ONLY_STREAM_ID_LIST = new HashSet<>(Arrays.asList(
            CorfuRuntime.getStreamID(getFullyQualifiedTableName(CORFU_SYSTEM_NAMESPACE,
                    TableRegistry.REGISTRY_TABLE_NAME)),
            CorfuRuntime.getStreamID(getFullyQualifiedTableName(CORFU_SYSTEM_NAMESPACE,
                    TableRegistry.PROTOBUF_DESCRIPTOR_TABLE_NAME))
    ));

    @Getter
    private static String currentVersion;

    private Table<TableInfo, Namespace, CommonTypes.Uuid> streamsInfoTable;

    private Table<VersionString, Version, CommonTypes.Uuid> pluginVersionTable;

    /**
     * Used for testing purpose only.
     */
    @VisibleForTesting
    public LogReplicationConfigManager(CorfuRuntime runtime) {
        this.corfuStore = new CorfuStore(runtime);
        this.runtime = runtime;
        this.pluginConfigFilePath = EMPTY_STR;
    }

    public LogReplicationConfigManager(CorfuRuntime runtime, String pluginConfigFilePath) {
        this.pluginConfigFilePath = pluginConfigFilePath;
        this.corfuStore = new CorfuStore(runtime);
        this.runtime = runtime;

        initStreamNameFetcherPlugin();
        openTables();
    }

    private void openTables() {
        try {
            streamsInfoTable = corfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                    LOG_REPLICATION_STREAMS_INFO_TABLE, TableInfo.class,
                    Namespace.class, CommonTypes.Uuid.class, TableOptions.builder().build());
            pluginVersionTable = corfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                    LOG_REPLICATION_PLUGIN_VERSION_TABLE, VersionString.class,
                    Version.class, CommonTypes.Uuid.class, TableOptions.builder().build());
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            log.warn("Exception when opening config tables", e);
            throw new UnrecoverableCorfuError(e);
        }
    }

    /**
     * This method is for ACTIVE to fetch streams to replicate from LOG_REPLICATION_STREAMS_INFO_TABLE.
     * If it doesn't exist, we will create info table by reading from registry table.
     *
     * @return Set of TableInfo for streams to replicate from ACTIVE
     */
    public Set<TableInfo> fetchStreamsToReplicate() {
        try {
            currentVersion = logReplicationConfigAdapter.getVersion();
            Set<TableInfo> fetchedStreams = readStreamsToReplicateFromRegistry(this.runtime.getAddressSpaceView().getLogTail());

            return IRetry.build(IntervalRetry.class, () -> {
                Set<TableInfo> streamsInfo = new HashSet<>();
                try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                    VersionResult result = verifyVersionResult(txn);
                    if (result.equals(VersionResult.UNSET) || result.equals(VersionResult.CHANGE)) {
                        boolean isUpgraded = result.equals(VersionResult.CHANGE);
                        // Case of upgrade or initial boot: clear table entries and sync with plugin info
                        clearTables();
                        updateVersionTable(txn, isUpgraded);
                        streamsInfo = updateStreamsTable(txn, fetchedStreams);
                    }
                    txn.commit();
                    return streamsInfo.isEmpty() ? readStreamsToReplicateFromTable() : streamsInfo;
                } catch (TransactionAbortedException e) {
                    log.warn("Exception on getStreamsToReplicate()", e);
                    throw new RetryNeededException();
                }
            }).run();
        } catch (InterruptedException e) {
            log.error("Unrecoverable exception when updating Config Tables", e);
            throw new UnrecoverableCorfuInterruptedError(e);
        }
    }

    public Set<UUID> fetchNoisyStreams() {
        CorfuTable<CorfuStoreMetadata.TableName, CorfuRecord<CorfuStoreMetadata.TableDescriptors,
                CorfuStoreMetadata.TableMetadata>>
                registryTable = runtime.getTableRegistry().getRegistryTable();

        // Get streams to replicate from registry table and add to tableInfoSet.
        return registryTable.entryStream()
                .filter(entry -> !entry.getValue().getMetadata().getTableOptions().getIsFederated())
                .map(entry -> CorfuRuntime.getStreamID(getFullyQualifiedTableName(entry.getKey())))
                .filter(streamId -> !MERGE_ONLY_STREAM_ID_LIST.contains(streamId)).collect(Collectors.toSet());
    }

    private void initStreamNameFetcherPlugin() {
        log.info("Plugin :: {}", pluginConfigFilePath);
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

    private VersionResult verifyVersionResult(TxnContext txn) {
        CorfuStoreEntry<VersionString, Version, CommonTypes.Uuid> record =
                txn.getRecord(LOG_REPLICATION_PLUGIN_VERSION_TABLE, versionString);

        if (record.getPayload() == null) {
            // Initializing
            log.info("LR initializing. Version unset");
            return VersionResult.UNSET;
        } else if (!record.getPayload().getVersion().equals(currentVersion)) {
            // Upgrading
            log.info("LR upgraded. Version changed from {} to {}", currentVersion, record.getPayload().getVersion());
            return VersionResult.CHANGE;
        }

        return VersionResult.SAME;
    }

    private void clearTables() {
        log.info("Clearing streams name table and plugin version table");
        streamsInfoTable.clearAll();
        pluginVersionTable.clearAll();
    }

    private void updateVersionTable(TxnContext txn, boolean isUpgraded) {
        log.info("Current version from plugin = {}, isUpgraded = {}", currentVersion, isUpgraded);
        // Persist upgrade flag so a snapshot-sync is enforced upon negotiation (when it is set to true)
        Version version = Version.newBuilder()
                .setVersion(currentVersion)
                .setIsUpgraded(isUpgraded)
                .build();
        txn.putRecord(pluginVersionTable, versionString, version, defaultMetadata);
    }

    private Set<TableInfo> updateStreamsTable(TxnContext txn, Set<TableInfo> streamsInfo) {
        // add registryTable to the streams
        String registryTableName = getFullyQualifiedTableName(
                CORFU_SYSTEM_NAMESPACE, TableRegistry.REGISTRY_TABLE_NAME);
        TableInfo registryTable = TableInfo.newBuilder().setName(registryTableName).build();
        streamsInfo.add(registryTable);

        // add protobufDescriptorTable to the streams
        String protoTableName = getFullyQualifiedTableName(
                CORFU_SYSTEM_NAMESPACE, TableRegistry.PROTOBUF_DESCRIPTOR_TABLE_NAME);
        TableInfo protoTable = TableInfo.newBuilder().setName(protoTableName).build();
        streamsInfo.add(protoTable);

        // Copy all stream names to the stream names table.  Each name is
        // a fully qualified stream name
        for (TableInfo tableInfo : streamsInfo) {
            // As each name is fully qualified, no need to insert the
            // namespace.  Simply insert an empty string there.
            // Note: Ideally the namespace protoBuf can be removed, but it
            // will involve data migration on upgrade as it is a schema change
            txn.putRecord(streamsInfoTable, tableInfo, defaultNamespace, defaultMetadata);
        }

        return streamsInfo;
    }

    private Set<TableInfo> readStreamsToReplicateFromTable() {
        Set<TableInfo> streamsInfo = new HashSet<>();
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            Set<TableInfo> tables = txn.keySet(LOG_REPLICATION_STREAMS_INFO_TABLE);
            streamsInfo.addAll(tables);
            txn.commit();
        }
        return streamsInfo;
    }

    /**
     * Helper method for checking whether LR is in upgrading path or not.
     * Note that the default boolean value for ProtoBuf message is false.
     *
     * @return True if LR is in upgrading path, false otherwise.
     */
    public boolean isUpgraded() {
        VersionString versionString = VersionString.newBuilder()
                .setName(VERSION_PLUGIN_KEY).build();
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

    /**
     * Helper method for flipping the boolean flag back to false in version table which
     * indicates the LR upgrading path is complete.
     */
    public void resetUpgradeFlag() {

        log.info("Reset isUpgraded flag");

        try {
            IRetry.build(IntervalRetry.class, () -> {
                try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                    VersionString versionString = VersionString.newBuilder()
                            .setName(VERSION_PLUGIN_KEY).build();
                    CorfuStoreEntry<VersionString, Version, CommonTypes.Uuid> versionEntry =
                            txn.getRecord(LOG_REPLICATION_PLUGIN_VERSION_TABLE, versionString);
                    Version version = Version.newBuilder().mergeFrom(versionEntry.getPayload()).setIsUpgraded(false).build();

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

    private void addStreamsToInfoTable(Set<UUID> streamIdSet,
                                       Table<TableInfo, Namespace, CommonTypes.Uuid> streamsInfoTable)
            throws RetryNeededException {
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            for (UUID id : streamIdSet) {
                TableInfo tableInfo = TableInfo.newBuilder().setId(id.toString()).build();
                Namespace namespace = Namespace.newBuilder().setName(EMPTY_STR).build();
                txn.putRecord(streamsInfoTable, tableInfo, namespace, defaultMetadata);
            }
            txn.commit();
        } catch (TransactionAbortedException tae) {
            throw new RetryNeededException();
        }
    }

    private void addStreamsToInfoTableWithRetry(Set<UUID> streamIdSet,
                                                Table<TableInfo, Namespace, CommonTypes.Uuid> streamsInfoTable) {
        try {
            IRetry.build(IntervalRetry.class, () -> {
                addStreamsToInfoTable(streamIdSet, streamsInfoTable);
                log.debug("Successfully added streams {} to the info table.", streamIdSet);
                return null;
            }).run();
        } catch (InterruptedException ie) {
            log.error("Unrecoverable exception when attempting to add streams.", ie);
            throw new UnrecoverableCorfuInterruptedError(ie);
        }
    }

    public Set<TableInfo> readStreamsToReplicateFromRegistry(long ts) {

        Set<TableInfo> tableInfoSet = new HashSet<>();
        CorfuTable<CorfuStoreMetadata.TableName, CorfuRecord<CorfuStoreMetadata.TableDescriptors,
                CorfuStoreMetadata.TableMetadata>>
                registryTable = runtime.getTableRegistry().getRegistryTable();
        Token snapshotToken = Token.of(0L, ts);
        runtime.getObjectsView().TXBuild()
                .type(TransactionType.SNAPSHOT)
                .snapshot(snapshotToken)
                .build()
                .begin();

        // Get streams to replicate from registry table and add to tableInfoSet.
        registryTable.entryStream()
                .filter(entry -> entry.getValue().getMetadata().getTableOptions().getIsFederated())
                .map(entry -> TableInfo
                        .newBuilder()
                        .setName(getFullyQualifiedTableName(entry.getKey()))
                        .build())
                .forEachOrdered(tableInfoSet::add);
        runtime.getObjectsView().TXEnd();

        return tableInfoSet;
    }

    public static Set<UUID> getMergeOnlyStreamIdList() {
        return MERGE_ONLY_STREAM_ID_LIST;
    }

    private enum VersionResult {
        UNSET,
        CHANGE,
        SAME
    }
}
