package org.corfudb.infrastructure.logreplication.utils;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Message;
import com.google.protobuf.TextFormat;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.logreplication.config.LogReplicationConfig;
import org.corfudb.infrastructure.logreplication.config.LogReplicationFullTableConfig;
import org.corfudb.infrastructure.logreplication.config.LogReplicationLogicalGroupConfig;
import org.corfudb.protocols.wireprotocol.StreamAddressRange;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.CorfuStoreMetadata.TableDescriptors;
import org.corfudb.runtime.CorfuStoreMetadata.TableMetadata;
import org.corfudb.runtime.CorfuStoreMetadata.TableName;
import org.corfudb.runtime.LogReplication.ClientDestinationInfoKey;
import org.corfudb.runtime.LogReplication.ClientRegistrationId;
import org.corfudb.runtime.LogReplication.ClientRegistrationInfo;
import org.corfudb.runtime.LogReplication.DestinationInfoVal;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.corfudb.runtime.LogReplication.ReplicationModel;
import org.corfudb.runtime.LogReplication.ReplicationSubscriber;
import org.corfudb.runtime.collections.CorfuRecord;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStoreEntry;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.ObjectsView;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.runtime.view.stream.StreamAddressSpace;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.IntervalRetry;
import org.corfudb.util.retry.RetryNeededException;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.corfudb.infrastructure.logreplication.config.LogReplicationConfig.MERGE_ONLY_STREAMS;
import static org.corfudb.infrastructure.logreplication.config.LogReplicationConfig.REGISTRY_TABLE_ID;
import static org.corfudb.infrastructure.logreplication.replication.send.logreader.LogicalGroupLogEntryReader.CLIENT_CONFIG_TABLE_ID;
import static org.corfudb.runtime.LogReplicationLogicalGroupClient.DEFAULT_LOGICAL_GROUP_CLIENT;
import static org.corfudb.runtime.LogReplicationLogicalGroupClient.LR_MODEL_METADATA_TABLE_NAME;
import static org.corfudb.runtime.LogReplicationLogicalGroupClient.LR_REGISTRATION_TABLE_NAME;
import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

/**
 * Handle construction and maintenance of the streams to replicate for all replication models supported in LR.
 * @author pankti-m
 */
@Slf4j
public class LogReplicationConfigManager {

    // Represents default client for LR V1, i.e., use case where tables are tagged with
    // 'is_federated' flag, yet no client is specified in proto
    private static final String DEFAULT_CLIENT = "00000000-0000-0000-0000-000000000000";

    @Getter
    private final CorfuRuntime runtime;

    private final CorfuStore corfuStore;

    private final String localClusterId;

    private long lastRegistryTableLogTail = Address.NON_ADDRESS;

    private long lastClientConfigTableLogTail = Address.NON_ADDRESS;

    private Table<ClientRegistrationId, ClientRegistrationInfo, Message> clientRegistrationTable;

    private Table<ClientDestinationInfoKey, DestinationInfoVal, Message> clientConfigTable;

    @Getter
    private ServerContext serverContext;

    // Map from a session to its corresponding config.
    @Getter
    private final Map<LogReplicationSession, LogReplicationConfig> sessionToConfigMap = new ConcurrentHashMap<>();

    // Set of registered log replication subscribers.
    @Getter
    private final Set<ReplicationSubscriber> registeredSubscribers = ConcurrentHashMap.newKeySet();

    // In-memory list of registry table entries for finding streams to replicate
    private List<Map.Entry<TableName, CorfuRecord<TableDescriptors, TableMetadata>>> registryTableEntries =
            new ArrayList<>();

    // In-memory list of client config table entries for finding group to Sinks map
    private List<CorfuStoreEntry<ClientDestinationInfoKey, DestinationInfoVal, Message>> clientConfigTableEntries =
            new ArrayList<>();


    /**
     * Used for non-upgrade testing purpose only. Note that this constructor will keep the version table in
     * uninitialized state, in which case LR will be constantly considered to be not upgraded.
     */
    @VisibleForTesting
    public LogReplicationConfigManager(CorfuRuntime runtime, String localClusterId) {
        this.runtime = runtime;
        this.corfuStore = new CorfuStore(runtime);
        this.localClusterId = localClusterId;
        init();
    }

    public LogReplicationConfigManager(CorfuRuntime runtime, ServerContext serverContext, String localClusterId) {
        this.runtime = runtime;
        this.corfuStore = new CorfuStore(runtime);
        this.serverContext = serverContext;
        this.localClusterId = localClusterId;
        init();
    }

    // TODO (V2): This builder should be removed after the rpc stream is added for Sink side session creation.
    public static ReplicationSubscriber getDefaultLogicalGroupSubscriber() {
        return ReplicationSubscriber.newBuilder()
                .setClientName(DEFAULT_LOGICAL_GROUP_CLIENT)
                .setModel(ReplicationModel.LOGICAL_GROUPS)
                .build();
    }

    public static ReplicationSubscriber getDefaultSubscriber() {
        return ReplicationSubscriber.newBuilder()
                .setClientName(DEFAULT_CLIENT)
                .setModel(ReplicationModel.FULL_TABLE)
                .build();
    }

    /**
     * Init config manager:
     * 1. Adding default subscriber to registeredSubscribers
     * 2. Open client configuration tables to make them available in corfuStore
     * 3. Initialize registry table log tail and in-memory entries
     */
    private void init() {
        registeredSubscribers.add(getDefaultSubscriber());
        // TODO (V2): This builder should be removed after the rpc stream is added for Sink side session creation.
        //  and logical group subscribers should come from client registration.
        registeredSubscribers.add(getDefaultLogicalGroupSubscriber());
        openClientConfigTables();
        syncWithRegistryTable();
        syncWithClientConfigTable();
    }

    private void openClientConfigTables() {
        try {
            IRetry.build(IntervalRetry.class, () -> {
                try {
                    clientRegistrationTable = corfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                            LR_REGISTRATION_TABLE_NAME,
                            ClientRegistrationId.class,
                            ClientRegistrationInfo.class,
                            null,
                            TableOptions.fromProtoSchema(ClientRegistrationInfo.class));
                    clientConfigTable = corfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                            LR_MODEL_METADATA_TABLE_NAME,
                            ClientDestinationInfoKey.class,
                            DestinationInfoVal.class,
                            null,
                            TableOptions.fromProtoSchema(ClientRegistrationInfo.class));
                } catch (InvocationTargetException | NoSuchMethodException | IllegalAccessException e) {
                    log.error("Cannot open client config tables, retrying.", e);
                    throw new RetryNeededException();
                }
                return null;
            }).run();
        } catch (InterruptedException e) {
            log.error("Failed to open client config tables", e);
            throw new UnrecoverableCorfuInterruptedError(e);
        }
    }

    /**
     * Generate LogReplicationConfig for all given sessions based on there replication model.
     *
     * @param sessions set of sessions for which to generate config.
     * @param updateGroupDestinationConfig True if group destination config needs to be updated.
     */
    public void generateConfig(Set<LogReplicationSession> sessions, boolean updateGroupDestinationConfig) {
        sessions.forEach(session -> {
                switch (session.getSubscriber().getModel()) {
                    case FULL_TABLE:
                        log.debug("Generating FULL_TABLE config for session {}",
                                TextFormat.shortDebugString(session));
                        generateFullTableConfig(session);
                        break;
                    case LOGICAL_GROUPS:
                        log.debug("Generating LOGICAL_GROUP config for session {}",
                                TextFormat.shortDebugString(session));
                        generateLogicalGroupConfig(session, updateGroupDestinationConfig);
                        break;
                    default:
                        throw new IllegalArgumentException("Invalid replication model: " +
                                session.getSubscriber().getModel());
                }
        });
    }

    private void generateLogicalGroupConfig(LogReplicationSession session, boolean updateGroupDestinationConfig) {
        Map<UUID, List<UUID>> streamToTagsMap = new HashMap<>();
        Set<String> streamsToReplicate = new HashSet<>();
        // Check if the local cluster is the Sink for this session. Sink side will honor whatever Source side send
        // instead of relying on groupSinksMap to filter the streams to replicate.
        boolean isSink = session.getSinkClusterId().equals(localClusterId);
        Map<String, Set<String>> groupToSinksMap = getGroupToSinksMapForSubscriber(session.getSubscriber());
        Map<String, Set<String>> logicalGroupToStreams = new HashMap<>();

        if (!updateGroupDestinationConfig) {
            // If group config is not supposed to be updated, get it from existing config
            logicalGroupToStreams.putAll(((LogReplicationLogicalGroupConfig) sessionToConfigMap.get(session))
                    .getLogicalGroupToStreams());
        } else if (!isSink) {
            groupToSinksMap.entrySet().stream()
                    .filter(entry -> entry.getValue().contains(session.getSinkClusterId()))
                    .map(Map.Entry::getKey)
                    .forEach(group -> logicalGroupToStreams.put(group, new HashSet<>()));
            log.debug("Targeted logical groups={}", logicalGroupToStreams.keySet());
        }

        registryTableEntries.forEach(entry -> {
            String tableName = TableRegistry.getFullyQualifiedTableName(entry.getKey());
            UUID streamId = CorfuRuntime.getStreamID(tableName);

            // Add merge only streams first
            if (MERGE_ONLY_STREAMS.contains(streamId)) {
                streamsToReplicate.add(tableName);
                // Collect tags for merge-only stream
                streamToTagsMap.put(streamId, Collections.singletonList(getOpaqueStreamToTrack(session.getSubscriber())));
            } else if (entry.getValue().getMetadata().getTableOptions().hasReplicationGroup() &&
                    !entry.getValue().getMetadata().getTableOptions().getReplicationGroup().getLogicalGroup().isEmpty()) {
                // Find streams to replicate for every logical group for this session
                String logicalGroup = entry.getValue().getMetadata().getTableOptions()
                        .getReplicationGroup().getLogicalGroup();
                // TODO (V2): Client name should be checked after the rpc stream is added for Sink side session creation.
                // if (session.getSubscriber().getClientName().equals(clientName) && groups.contains(logicalGroup)) {
                if (isSink || logicalGroupToStreams.containsKey(logicalGroup)) {
                    streamsToReplicate.add(tableName);
                    Set<String> relatedStreams = logicalGroupToStreams.getOrDefault(logicalGroup, new HashSet<>());
                    relatedStreams.add(tableName);
                    logicalGroupToStreams.put(logicalGroup, relatedStreams);
                    // Collect tags for this stream
                    List<UUID> tags = streamToTagsMap.getOrDefault(streamId, new ArrayList<>());
                    entry.getValue().getMetadata().getTableOptions().getStreamTagList().stream()
                            .map(streamTag -> TableRegistry.getStreamIdForStreamTag(entry.getKey().getNamespace(), streamTag))
                            .forEach(tags::add);
                    streamToTagsMap.put(streamId, tags);
                }
            }
        });

        if (sessionToConfigMap.containsKey(session)) {
            LogReplicationLogicalGroupConfig config = (LogReplicationLogicalGroupConfig) sessionToConfigMap.get(session);
            config.setStreamsToReplicate(streamsToReplicate);
            config.setDataStreamToTagsMap(streamToTagsMap);
            config.setLogicalGroupToStreams(logicalGroupToStreams);
            log.info("LogReplicationLogicalGroupConfig updated for session={}, streams to replicate={}, groups={}",
                    TextFormat.shortDebugString(session), streamsToReplicate, logicalGroupToStreams);
        } else {
            LogReplicationLogicalGroupConfig logicalGroupConfig = new LogReplicationLogicalGroupConfig(session,
                    streamsToReplicate, streamToTagsMap, serverContext, logicalGroupToStreams);
            sessionToConfigMap.put(session, logicalGroupConfig);
            log.info("LogReplicationLogicalGroupConfig generated for session={}, streams to replicate={}, groups={}",
                    TextFormat.shortDebugString(session), streamsToReplicate, logicalGroupToStreams);
        }
    }

    private void generateFullTableConfig(LogReplicationSession session) {
        Map<UUID, List<UUID>> streamToTagsMap = new HashMap<>();
        Set<UUID> streamsToDrop = new HashSet<>();
        Set<String> streamsToReplicate = new HashSet<>();

        registryTableEntries.forEach(entry -> {
            String tableName = TableRegistry.getFullyQualifiedTableName(entry.getKey());
            boolean isFederated = entry.getValue().getMetadata().getTableOptions().getIsFederated();
            UUID streamId = CorfuRuntime.getStreamID(tableName);

            // Find federated tables that will be used by FULL_TABLE replication model
            if (isFederated || MERGE_ONLY_STREAMS.contains(streamId)) {
                streamsToReplicate.add(tableName);
                if (MERGE_ONLY_STREAMS.contains(streamId)) {
                    // Collect tags for merge-only stream
                    streamToTagsMap.put(streamId, Collections.singletonList(getOpaqueStreamToTrack(session.getSubscriber())));
                } else {
                    // Collect tags for normal stream
                    List<UUID> tags = streamToTagsMap.getOrDefault(streamId, new ArrayList<>());
                    tags.addAll(entry.getValue().getMetadata().getTableOptions().getStreamTagList().stream()
                            .map(streamTag -> TableRegistry.getStreamIdForStreamTag(entry.getKey().getNamespace(), streamTag))
                            .collect(Collectors.toList()));
                    streamToTagsMap.put(streamId, tags);
                }
            } else {
                streamsToDrop.add(streamId);
            }
        });

        if (sessionToConfigMap.containsKey(session)) {
            LogReplicationFullTableConfig config = (LogReplicationFullTableConfig) sessionToConfigMap.get(session);
            config.setStreamsToReplicate(streamsToReplicate);
            config.setDataStreamToTagsMap(streamToTagsMap);
            config.setStreamsToDrop(streamsToDrop);
            log.info("LogReplicationFullTableConfig updated for session={}, streams to replicate={}, streamsToDrop={}",
                    TextFormat.shortDebugString(session), streamsToReplicate, streamsToDrop);
        } else {
            LogReplicationFullTableConfig fullTableConfig = new LogReplicationFullTableConfig(session, streamsToReplicate,
                    streamToTagsMap, serverContext, streamsToDrop);
            sessionToConfigMap.put(session, fullTableConfig);
            log.info("LogReplicationFullTableConfig generated for session={}, streams to replicate={}, streamsToDrop={}",
                    TextFormat.shortDebugString(session), streamsToReplicate, streamsToDrop);
        }
    }

    /**
     * Get updated LogReplicationConfig for the given session.
     *
     * @param session LogReplicationSession to get updated config for.
     * @param updateGroupDestinationConfig True if group destination config needs to be updated.
     */
    public void getUpdatedConfig(LogReplicationSession session, boolean updateGroupDestinationConfig) {
        syncWithRegistryTable();

        switch (session.getSubscriber().getModel()) {
            case FULL_TABLE:
                generateConfig(Collections.singleton(session), updateGroupDestinationConfig);
                break;
            case LOGICAL_GROUPS:
                syncWithClientConfigTable();
                generateConfig(Collections.singleton(session), updateGroupDestinationConfig);
                break;
            default:
                throw new IllegalArgumentException("Invalid replication model: " +
                        session.getSubscriber().getModel());
        }
    }

    /**
     * Check if the registry table's log tail moved or not. If it has moved ahead, we need to update
     * lastRegistryTableLogTail and in-memory registry table entries, which will be used for construct
     * LogReplicationConfig objects.
     */
    private synchronized void syncWithRegistryTable() {
        StreamAddressSpace currentAddressSpace = runtime.getSequencerView().getStreamAddressSpace(
            new StreamAddressRange(REGISTRY_TABLE_ID, Long.MAX_VALUE, Address.NON_ADDRESS));
        long currentLogTail = currentAddressSpace.getTail();

        // Check if the log tail of registry table moved ahead
        if (currentLogTail != lastRegistryTableLogTail) {
            registryTableEntries = runtime.getTableRegistry().getRegistryTable().entryStream().collect(Collectors.toList());
            lastRegistryTableLogTail = currentLogTail;
        }
    }

    /**
     * Check if the client config table (groups to Sinks map for LOGICAL_GROUP use case) has new updates or not.
     * If the table's log tail moved, update the in-memory client config table entry with the latest table content.
     */
    private synchronized void syncWithClientConfigTable() {
        StreamAddressSpace currentAddressSpace = runtime.getSequencerView().getStreamAddressSpace(
                new StreamAddressRange(CLIENT_CONFIG_TABLE_ID, Long.MAX_VALUE, Address.NON_ADDRESS));
        long currentLogTail = currentAddressSpace.getTail();

        // Check if the log tail of client config table moved ahead
        if (currentLogTail != lastClientConfigTableLogTail) {
            List<CorfuStoreEntry<ClientDestinationInfoKey, DestinationInfoVal, Message>> newEntries = new ArrayList<>();
            try {
                IRetry.build(IntervalRetry.class, () -> {
                    try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                        newEntries.addAll(txn.executeQuery(clientConfigTable, e -> true));
                        txn.commit();
                        return null;
                    } catch (TransactionAbortedException tae) {
                        log.error("Failed to sync with client configuration table table due to Txn Abort, retrying", tae);
                        throw new RetryNeededException();
                    }
                }).run();
            } catch (InterruptedException e) {
                log.error("Unable to sync with client configuration table", e);
                throw new UnrecoverableCorfuInterruptedError(e);
            }
            clientConfigTableEntries = newEntries;
            lastClientConfigTableLogTail = currentLogTail;
        }
    }

    /**
     * For LOGICAL_GROUP model, the groups to Sinks map could be different between subscribers.
     *
     * @param subscriber Subscriber whose groups to Sinks map will be returned
     * @return Given subscriber's groups to Sinks map.
     */
    private Map<String, Set<String>> getGroupToSinksMapForSubscriber(ReplicationSubscriber subscriber) {
        Map<String, Set<String>> groupSinksMap = new HashMap<>();
        clientConfigTableEntries.stream()
                .filter(corfuStreamEntry ->
                        corfuStreamEntry.getKey().getClientName().equals(subscriber.getClientName()) &&
                        corfuStreamEntry.getKey().getModel().equals(subscriber.getModel()))
                .forEach(corfuStreamEntry -> {
                    String groupName = corfuStreamEntry.getKey().getGroupName();
                    Set<String> destinations = new HashSet<>(corfuStreamEntry.getPayload().getDestinationIdsList());
                    groupSinksMap.put(groupName, destinations);
                });

        return groupSinksMap;
    }

    /**
     * Preprocess the client registration table, get the log tail that will be the start point for client register
     * listener to monitor updates.
     *
     * @return Log tail hat will be the start point for client register listener to monitor updates.
     */
    public CorfuStoreMetadata.Timestamp preprocessAndGetTail() {
        try {
            return IRetry.build(IntervalRetry.class, () -> {
                try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                    List<CorfuStoreEntry<ClientRegistrationId, ClientRegistrationInfo, Message>> registrationResults =
                            txn.executeQuery(clientRegistrationTable, p -> true);
                    registrationResults.forEach(entry -> {
                        String clientName = entry.getKey().getClientName();
                        ReplicationModel model = entry.getPayload().getModel();
                        ReplicationSubscriber subscriber = ReplicationSubscriber.newBuilder()
                                .setClientName(clientName).setModel(model).build();
                        // TODO (V2): currently we don't support customized client name, default logical group subscriber
                        //  should be removed after the grpc stream for Sink session creation is created.
                        if (model.equals(ReplicationModel.LOGICAL_GROUPS)) {
                            subscriber = getDefaultLogicalGroupSubscriber();
                        }
                        registeredSubscribers.add(subscriber);
                    });

                    return txn.commit();
                } catch (TransactionAbortedException tae) {
                    log.error("Failed to preprocess client registration table due to Txn Abort, retrying", tae);
                    throw new RetryNeededException();
                }
            }).run();
        } catch (InterruptedException e) {
            log.error("Unable to preprocess client configuration tables", e);
            throw new UnrecoverableCorfuInterruptedError(e);
        }
    }

    /**
     * Callback method which is invoked when the client config listener receives update for new client registration.
     */
    public void onNewClientRegister(ReplicationSubscriber subscriber) {
        if (registeredSubscribers.contains(subscriber)) {
            log.warn("Client {} with model {} already registered!", subscriber.getClientName(), subscriber.getModel());
            return;
        }
        // TODO (V2): Currently we add a default subscriber for logical group use case instead of listening on client
        //  registration. Subscriber should be added upon registration after grpc stream for session creation is added.
        registeredSubscribers.add(subscriber);
    }

    /**
     * Client listener will perform a full sync upon its resume. We perform a re-processing of the client config
     * tables in the same transaction (as the listener finds the timestamp from which to subscribe for deltas) to
     * avoid data loss.
     */
    public CorfuStoreMetadata.Timestamp onClientListenerResume() {
        registeredSubscribers.clear();
        return preprocessAndGetTail();
    }

    /**
     * Log entry readers will be tracking different opaque streams to get entries for log entry sync, based on the
     * log replication subscriber's model and client name.
     *
     * @param subscriber Replication subscriber based on which the opaque stream id will be determined
     * @return Stream id of the opaque stream to track for a session's log entry reader.
     */
    public UUID getOpaqueStreamToTrack(ReplicationSubscriber subscriber) {
        switch(subscriber.getModel()) {
            case FULL_TABLE:
                return ObjectsView.getLogReplicatorStreamId();
            case LOGICAL_GROUPS:
                return ObjectsView.getLogicalGroupStreamTagInfo(subscriber.getClientName()).getStreamId();
            default:
                throw new IllegalArgumentException("Subscriber with invalid replication model: " + subscriber.getModel());
        }
    }
}
