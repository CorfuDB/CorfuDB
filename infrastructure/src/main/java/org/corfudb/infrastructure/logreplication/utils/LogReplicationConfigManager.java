package org.corfudb.infrastructure.logreplication.utils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.LRRollingUpgradeHandler;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.infrastructure.logreplication.infrastructure.ReplicationSubscriber;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.ILogReplicationVersionAdapter;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.LogReplicationPluginConfig;
import org.corfudb.protocols.wireprotocol.StreamAddressRange;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata.TableDescriptors;
import org.corfudb.runtime.CorfuStoreMetadata.TableMetadata;
import org.corfudb.runtime.CorfuStoreMetadata.TableName;
import org.corfudb.runtime.collections.CorfuRecord;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStoreEntry;
import org.corfudb.runtime.collections.PersistentCorfuTable;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.runtime.view.stream.StreamAddressSpace;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.IntervalRetry;
import org.corfudb.util.retry.RetryNeededException;
import org.corfudb.utils.CommonTypes.Uuid;
import org.corfudb.utils.LogReplicationStreams.Version;
import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.corfudb.infrastructure.logreplication.LogReplicationConfig.REGISTRY_TABLE_ID;
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

    private ILogReplicationVersionAdapter logReplicationVersionAdapter;

    @Getter
    private LRRollingUpgradeHandler lrRollingUpgradeHandler;

    private final String pluginConfigFilePath;

    private final VersionString versionString = VersionString.newBuilder().setName(VERSION_PLUGIN_KEY).build();

    @Getter
    private final CorfuRuntime runtime;

    private final CorfuStore corfuStore;

    private static final Uuid defaultMetadata = Uuid.newBuilder().setLsb(0).setMsb(0).build();

    @Getter
    private static String currentVersion;

    private Table<VersionString, Version, Uuid> pluginVersionTable;

    // In-memory list of registry table entries
    private List<Map.Entry<TableName, CorfuRecord<TableDescriptors, TableMetadata>>> registryTableEntries =
        new ArrayList<>();

    private long lastRegistryTableLogTail = Address.NON_ADDRESS;

    @Getter
    private LogReplicationConfig config;

    private ServerContext serverContext;

    /**
     * Used for non-upgrade testing purpose only. Note that this constructor will keep the version table in
     * uninitialized state, in which case LR will be constantly considered to be not upgraded.
     */
    @VisibleForTesting
    public LogReplicationConfigManager(CorfuRuntime runtime) {
        this.runtime = runtime;
        this.corfuStore = new CorfuStore(runtime);
        this.pluginConfigFilePath = EMPTY_STR;
        config = generateConfig();
    }

    public LogReplicationConfigManager(CorfuRuntime runtime, ServerContext serverContext) {
        this.runtime = runtime;
        this.corfuStore = new CorfuStore(runtime);
        this.serverContext = serverContext;
        this.pluginConfigFilePath = serverContext == null ? EMPTY_STR : serverContext.getPluginConfigFilePath();
        initLogReplicationVersionPlugin(runtime);
        setupVersionTable();
        config = generateConfig();
    }

    private LogReplicationConfig generateConfig() {
        PersistentCorfuTable<TableName, CorfuRecord<TableDescriptors, TableMetadata>> registryTable =
            runtime.getTableRegistry().getRegistryTable();
        registryTableEntries = registryTable.entryStream().collect(Collectors.toList());

        Map<ReplicationSubscriber, Set<String>> replicationSubscriberToStreamsMap = new HashMap<>();
        Map<UUID, List<UUID>> streamToTagsMap = new HashMap<>();

        registryTableEntries.forEach(entry -> {

            if (entry.getValue().getMetadata().getTableOptions().getIsFederated()) {
                ReplicationSubscriber subscriber = ReplicationSubscriber.getDefaultReplicationSubscriber();
                Set<String> streamsToReplicate =
                    replicationSubscriberToStreamsMap.getOrDefault(subscriber, new HashSet<>());
                streamsToReplicate.add(getFullyQualifiedTableName(entry.getKey()));
                replicationSubscriberToStreamsMap.put(subscriber, streamsToReplicate);

                // Collect tags for this stream
                UUID streamId = CorfuRuntime.getStreamID(getFullyQualifiedTableName(entry.getKey()));
                List<UUID> tags = streamToTagsMap.getOrDefault(streamId, new ArrayList<>());
                tags.addAll(entry
                    .getValue()
                    .getMetadata()
                    .getTableOptions()
                    .getStreamTagList()
                    .stream()
                    .map(streamTag -> TableRegistry.getStreamIdForStreamTag(entry.getKey().getNamespace(), streamTag))
                    .collect(Collectors.toList()));
                streamToTagsMap.put(streamId, tags);
            }

            // TODO: Add other cases once the protobuf options for other subscribers are available
        });

        // For each subscriber, add the Registry and Protobuf descriptor tables to the streams to replicate.
        // Also construct the set of streams which must not be replicated.
        Set<ReplicationSubscriber> subscribers = replicationSubscriberToStreamsMap.keySet();
        Set<String> registryTableStreamNames = new HashSet<>();
        registryTableEntries.forEach(entry -> registryTableStreamNames.add(getFullyQualifiedTableName(entry.getKey())));
        Map<ReplicationSubscriber, Set<UUID>> subscriberToNonReplicatedStreamsMap = new HashMap<>();

        for (ReplicationSubscriber subscriber : subscribers) {
            Set<String> streamsToReplicate = replicationSubscriberToStreamsMap.get(subscriber);

            String registryTableName = getFullyQualifiedTableName(CORFU_SYSTEM_NAMESPACE,
                TableRegistry.REGISTRY_TABLE_NAME);
            String protoTableName = getFullyQualifiedTableName(CORFU_SYSTEM_NAMESPACE,
                TableRegistry.PROTOBUF_DESCRIPTOR_TABLE_NAME);

            streamsToReplicate.add(registryTableName);
            streamsToReplicate.add(protoTableName);

            // Set of streams to drop
            Set<UUID> set = registryTableStreamNames.stream().filter(stream -> !streamsToReplicate.contains(stream))
                .map(CorfuRuntime::getStreamID).collect(Collectors.toSet());
            subscriberToNonReplicatedStreamsMap.put(subscriber, set);
        }

        LogReplicationConfig config = new LogReplicationConfig(replicationSubscriberToStreamsMap,
            subscriberToNonReplicatedStreamsMap, streamToTagsMap, serverContext);
        return config;
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

    public LogReplicationConfig getUpdatedConfig() {

        // Check if the registry table has new entries.  Otherwise, no update is necessary.
        if (registryTableHasNewEntries()) {
            LogReplicationConfig updatedConfig = generateConfig();

            config.setReplicationSubscriberToStreamsMap(updatedConfig.getReplicationSubscriberToStreamsMap());
            config.setSubscriberToNonReplicatedStreamsMap(updatedConfig.getSubscriberToNonReplicatedStreamsMap());
            config.setDataStreamToTagsMap(updatedConfig.getDataStreamToTagsMap());
        }
        return config;
    }

    private boolean registryTableHasNewEntries() {
        StreamAddressSpace currentAddressSpace = runtime.getSequencerView().getStreamAddressSpace(
            new StreamAddressRange(REGISTRY_TABLE_ID, Long.MAX_VALUE, Address.NON_ADDRESS));
        long currentLogTail = currentAddressSpace.getTail();
        return (currentLogTail != lastRegistryTableLogTail);
    }

    /**
     * Initiate version table during constructing LogReplicationConfigManager for upcoming version checks.
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
        VersionString versionString = VersionString.newBuilder().setName(VERSION_PLUGIN_KEY).build();
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
                    VersionString versionString = VersionString.newBuilder().setName(VERSION_PLUGIN_KEY).build();
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

    private enum VersionResult {
        UNSET,
        CHANGE,
        SAME
    }

}
