package org.corfudb.infrastructure.logreplication.utils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.infrastructure.logreplication.infrastructure.ReplicationSubscriber;
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
import org.corfudb.runtime.collections.PersistentCorfuTable;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.ObjectsView;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.runtime.view.stream.StreamAddressSpace;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.corfudb.infrastructure.logreplication.LogReplicationConfig.REGISTRY_TABLE_ID;
import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

/**
 * Handle construction and maintenance of the streams to replicate for all replication models supported in LR.
 * @author pankti-m
 */
@Slf4j
public class LogReplicationConfigManager {

    @Getter
    private final CorfuRuntime runtime;

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
        config = generateConfig();
    }

    public LogReplicationConfigManager(CorfuRuntime runtime, ServerContext serverContext) {
        this.runtime = runtime;
        this.serverContext = serverContext;
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
        // TODO (V2 / Chris): the operation of check current tail + generate updated config + update last log tail
        //  should be atomic operation in multi replication session env.
        StreamAddressSpace currentAddressSpace = runtime.getSequencerView().getStreamAddressSpace(
            new StreamAddressRange(REGISTRY_TABLE_ID, Long.MAX_VALUE, Address.NON_ADDRESS));
        long currentLogTail = currentAddressSpace.getTail();

        // Check if the log tail of registry table moved ahead
        if (currentLogTail != lastRegistryTableLogTail) {
            lastRegistryTableLogTail = currentLogTail;
            return true;
        }

        return false;
    }
}
