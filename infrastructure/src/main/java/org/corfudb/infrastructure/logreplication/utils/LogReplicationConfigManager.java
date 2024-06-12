package org.corfudb.infrastructure.logreplication.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.LRRollingUpgradeHandler;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.ILogReplicationVersionAdapter;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.LogReplicationPluginConfig;
import org.corfudb.infrastructure.logreplication.replication.receive.LogEntryWriter;
import org.corfudb.infrastructure.logreplication.replication.receive.SnapshotWriter;
import org.corfudb.protocols.wireprotocol.StreamAddressRange;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata.TableDescriptors;
import org.corfudb.runtime.CorfuStoreMetadata.TableMetadata;
import org.corfudb.runtime.CorfuStoreMetadata.TableName;
import org.corfudb.runtime.collections.CorfuRecord;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStoreEntry;
import org.corfudb.runtime.collections.ICorfuTable;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.runtime.view.stream.StreamAddressSpace;
import org.corfudb.util.retry.ExponentialBackoffRetry;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.IntervalRetry;
import org.corfudb.util.retry.RetryNeededException;
import org.corfudb.utils.CommonTypes.Uuid;
import org.corfudb.utils.LogReplicationStreams.Version;
import org.corfudb.utils.LogReplicationStreams.VersionString;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLClassLoader;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.corfudb.infrastructure.logreplication.LogReplicationConfig.MERGE_ONLY_STREAMS;
import static org.corfudb.infrastructure.logreplication.LogReplicationConfig.REGISTRY_TABLE_ID;
import static org.corfudb.runtime.view.ObjectsView.LOG_REPLICATOR_STREAM_INFO;
import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;
import static org.corfudb.runtime.view.TableRegistry.getFullyQualifiedTableName;

/**
 * Handle creation and maintenance of the Corfu table/s containing names of tables
 * to be replicated.
 * @author pankti-m
 */
@Slf4j
public class LogReplicationConfigManager {

    public static final String LOG_REPLICATION_PLUGIN_VERSION_TABLE = "LogReplicationPluginVersion";
    public static final String VERSION_PLUGIN_KEY = "VERSION";
    private static final String EMPTY_STR = "";

    private static final int SYNC_THRESHOLD = 120;

    private ILogReplicationVersionAdapter logReplicationVersionAdapter;

    @Getter
    private LRRollingUpgradeHandler lrRollingUpgradeHandler;

    private final String pluginConfigFilePath;

    private final VersionString versionString = VersionString.newBuilder().setName(VERSION_PLUGIN_KEY).build();

    private CorfuRuntime rt = null;

    private CorfuStore corfuStore = null;

    private static final Uuid defaultMetadata =
        Uuid.newBuilder().setLsb(0).setMsb(0).build();

    @Getter
    private static String currentVersion;

    private Table<VersionString, Version, Uuid> pluginVersionTable;

    // In memory entries of registry table, which will be refreshed when config is syncing with registry table.
    // This is needed to avoid inconsistency between config fields (for example, after streamsToReplicate is fetched,
    // some tables opened and confirmedNoisyStreams will be read with potential conflict info)
    private List<Map.Entry<TableName, CorfuRecord<TableDescriptors, TableMetadata>>> registryTableEntries;

    private long lastRegistryTableLogTail;

    /**
     * Used for non-upgrade testing purpose only. Note that this constructor will keep the version table in
     * uninitialized state, in which case LR will be constantly considered to be not upgraded.
     */
    @VisibleForTesting
    public LogReplicationConfigManager(CorfuRuntime runtime) {
        this.rt = runtime;
        this.corfuStore = new CorfuStore(runtime);
        this.pluginConfigFilePath = EMPTY_STR;
        this.lastRegistryTableLogTail = Address.NON_ADDRESS;
    }

    public LogReplicationConfigManager(String pluginConfigFilePath) {
        this.pluginConfigFilePath = pluginConfigFilePath;
        this.lastRegistryTableLogTail = Address.NON_ADDRESS;
    }

    public LogReplicationConfigManager(CorfuRuntime runtime, String pluginConfigFilePath) {
        this.rt = runtime;
        this.pluginConfigFilePath = pluginConfigFilePath;
        this.corfuStore = new CorfuStore(runtime);
        this.lastRegistryTableLogTail = Address.NON_ADDRESS;
        initLogReplicationVersionPlugin(runtime);
        initLogReplicationRollingUpgradeHandler(corfuStore);
        setupVersionTable();
    }

    private void initLogReplicationVersionPlugin(CorfuRuntime runtime) {
        log.info("Plugin :: {}", pluginConfigFilePath);
        LogReplicationPluginConfig config = new LogReplicationPluginConfig(pluginConfigFilePath);
        File jar = new File(config.getStreamFetcherPluginJARPath());
        try (URLClassLoader child = new URLClassLoader(new URL[]{jar.toURI().toURL()}, this.getClass().getClassLoader())) {
            Class plugin = Class.forName(config.getStreamFetcherClassCanonicalName(), true, child);
            logReplicationVersionAdapter = (ILogReplicationVersionAdapter)
                    plugin.getDeclaredConstructor(CorfuRuntime.class).newInstance(runtime);
        } catch (Exception e) {
            log.error("Fatal error: Failed to get Log Replicatior Version Plugin", e);
            throw new UnrecoverableCorfuError(e);
        }
    }

    /**
     * Instantiate the LogReplicator's Rolling Upgrade Handler and invoke its
     * check the first time, so it can cache the result in the common case
     * where there is no rolling upgrade in progress.
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

    /**
     * Read registry table's latest entries into memory for config synchronization.
     *
     * @return True if registry table address space changed and the in-memory entries are refreshed.
     */
    public boolean loadRegistryTableEntries() {
        try {
            return IRetry.build(ExponentialBackoffRetry.class, () -> {
                try {
                    StreamAddressSpace currentAddressSpace = rt.getSequencerView().getStreamAddressSpace(
                            new StreamAddressRange(REGISTRY_TABLE_ID, Long.MAX_VALUE, Address.NON_ADDRESS));
                    long currentLogTail = currentAddressSpace.getTail();
                    if (currentLogTail != lastRegistryTableLogTail) {
                        lastRegistryTableLogTail = currentLogTail;
                        ICorfuTable<TableName, CorfuRecord<TableDescriptors, TableMetadata>> registryTable =
                                rt.getTableRegistry().getRegistryTable();
                        registryTableEntries = registryTable.entryStream().collect(Collectors.toList());
                        return true;
                    }
                } catch (Exception e) {
                    log.error("Exception caught when syncing with registry table, retry needed", e);
                    throw new RetryNeededException();
                }
                return false;
            }).setOptions(x -> x.setMaxRetryThreshold(Duration.ofSeconds(SYNC_THRESHOLD))).run();
        } catch (InterruptedException e) {
            log.error("Cannot sync with registry table, LR stopped", e);
            throw new UnrecoverableCorfuInterruptedError(e);
        }
    }

    public Set<String> loadTablesToReplicate(String tablesToReplicatePath) {
        try {
            return IRetry.build(ExponentialBackoffRetry.class, () -> {
                Set<String> tablesToReplicate = new HashSet<>();
                try {
                    BufferedReader reader = new BufferedReader(new FileReader(tablesToReplicatePath));
                    String line;
                    while ((line = reader.readLine()) != null) {
                        tablesToReplicate.add(line.trim());
                    }
                    return tablesToReplicate;
                } catch (Exception e) {
                    log.error("Exception caught fetching tables to replicate, retry needed", e);
                    throw new RetryNeededException();
                }
            }).setOptions(x -> x.setMaxRetryThreshold(Duration.ofSeconds(SYNC_THRESHOLD))).run();
        } catch (InterruptedException e) {
            log.error("Cannot load tables to replicate, LR stopped", e);
            throw new UnrecoverableCorfuInterruptedError(e);
        }
    }

    public List<JsonNode> loadTablesToCreate(String tablesToCreatePath) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return IRetry.build(ExponentialBackoffRetry.class, () -> {
                List<JsonNode> tablesToCreate = new ArrayList<>();
                try {
                    JsonNode tables = objectMapper.readTree(new File(tablesToCreatePath));
                    for (JsonNode tableToCreate : tables.get("tables")) {
                        tablesToCreate.add(tableToCreate);
                    }
                    return tablesToCreate;
                } catch (Exception e) {
                    log.error("Exception caught fetching tables to create, retry needed", e);
                    throw new RetryNeededException();
                }
            }).setOptions(x -> x.setMaxRetryThreshold(Duration.ofSeconds(SYNC_THRESHOLD))).run();
        } catch (InterruptedException e) {
            log.error("Cannot get tables to create, LR stopped", e);
            throw new UnrecoverableCorfuInterruptedError(e);
        }
    }

    /**
     * Construct streams to replicate from the cached copy of registry table entries.
     *
     * @return Set of fully qualified names of streams to replicate
     */
    public Set<String> getStreamsToReplicate() {
        Set<String> streamNameSet = new HashSet<>();
        // Get streams to replicate from registry table and add to streamNameSet.
        registryTableEntries.stream()
                .filter(entry -> entry.getValue().getMetadata().getTableOptions().getIsFederated())
                .map(entry -> getFullyQualifiedTableName(entry.getKey()))
                .forEachOrdered(streamNameSet::add);
        // Add registryTable to the streams
        String registryTable = getFullyQualifiedTableName(
                CORFU_SYSTEM_NAMESPACE, TableRegistry.REGISTRY_TABLE_NAME);
        streamNameSet.add(registryTable);

        // Add protoBufDescriptorTable to the streams
        String protoTable = getFullyQualifiedTableName(
                CORFU_SYSTEM_NAMESPACE, TableRegistry.PROTOBUF_DESCRIPTOR_TABLE_NAME);
        streamNameSet.add(protoTable);
        return streamNameSet;
    }

    /**
     * Get stream tags to send data change notifications to on the sink side.
     *
     * Stream tags maps are constructed from the cached copy of registry table entries, and some streams' (opened on
     * Source but not opened on Sink) tags could not be known before registry table is updated during log replication.
     * In that case, LogReplicationConfig needs to sync with registry table to avoid data loss.
     *
     * @return map of stream-UUID to list of stream-tag UUIDs.
     */
    public Map<UUID, List<UUID>> getStreamToTagsMap() {
        Map<UUID, List<UUID>> streamToTagsMap = new HashMap<>();

        registryTableEntries.forEach(entry -> {
            TableName tableName = entry.getKey();
            CorfuRecord<TableDescriptors, TableMetadata> tableRecord = entry.getValue();
            UUID streamId = CorfuRuntime.getStreamID(getFullyQualifiedTableName(tableName));
            streamToTagsMap.putIfAbsent(streamId, new ArrayList<>());
            streamToTagsMap.get(streamId).addAll(
                    tableRecord.getMetadata()
                            .getTableOptions()
                            .getStreamTagList()
                            .stream()
                            .map(streamTag -> TableRegistry.getStreamIdForStreamTag(
                                    tableName.getNamespace(), streamTag))
                            .collect(Collectors.toList()));
        });

        // Add stream tags for merge only streams
        for (UUID id : MERGE_ONLY_STREAMS) {
            streamToTagsMap.put(id,
                    Collections.singletonList(LOG_REPLICATOR_STREAM_INFO.getStreamId()));
        }
        return streamToTagsMap;
    }

    /**
     * Collect streams that have explicitly set is_federated flag to false, which will be used by {@link SnapshotWriter}
     * and {@link LogEntryWriter} to drop incoming data on those streams. Note that if streams have not been opened
     * (no records in registry table), they should still be applied
     *
     * @return Set of streams ids for those have explicitly set is_federated flag to false
     */
    public Set<UUID> getStreamsToDrop() {
        Set<UUID> streamsToDrop = new HashSet<>();
        // Get streams to replicate from registry table and add to streamNameSet.
        registryTableEntries.stream()
                .filter(entry -> !entry.getValue().getMetadata().getTableOptions().getIsFederated())
                .map(entry -> CorfuRuntime.getStreamID(getFullyQualifiedTableName(entry.getKey())))
                .filter(streamId -> !MERGE_ONLY_STREAMS.contains(streamId))
                .forEachOrdered(streamsToDrop::add);

        return streamsToDrop;
    }

    /**
     * Initiate version table during constructing LogReplicationConfigManager for upcoming version checks.
     */
    private void setupVersionTable() {
        try {
            pluginVersionTable = corfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                    LOG_REPLICATION_PLUGIN_VERSION_TABLE, VersionString.class,
                    Version.class, Uuid.class, TableOptions.builder().build());
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
                    VersionResult result = verifyVersionResult(txn);
                    if (result.equals(VersionResult.UNSET) || result.equals(VersionResult.CHANGE)) {
                        // Case of upgrade or initial boot: sync version table with plugin info
                        boolean isUpgraded = result.equals(VersionResult.CHANGE);
                        log.info("Current version from plugin = {}, isUpgraded = {}", currentVersion, isUpgraded);
                        // Persist upgrade flag so a snapshot-sync is enforced upon negotiation
                        // (when it is set to true)
                        Version version = Version.newBuilder()
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

    private VersionResult verifyVersionResult(TxnContext txn) {
        CorfuStoreEntry<VersionString, Version, Uuid> record =
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

    /**
     * Helper method for checking whether LR is in upgrading path or not.
     * Note that the default boolean value for ProtoBuf message is false.
     *
     * @return True if LR is in upgrading path, false otherwise.
     */
    public boolean isUpgraded() {
        VersionString versionString = VersionString.newBuilder()
                .setName(VERSION_PLUGIN_KEY).build();
        CorfuStoreEntry<VersionString, Version, Uuid> record;
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
                    CorfuStoreEntry<VersionString, Version, Uuid> versionEntry =
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

    public CorfuRuntime getConfigRuntime() {
        return rt;
    }

    private enum VersionResult {
        UNSET,
        CHANGE,
        SAME
    }
}
