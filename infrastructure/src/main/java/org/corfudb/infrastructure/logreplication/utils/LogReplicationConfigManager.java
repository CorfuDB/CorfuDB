package org.corfudb.infrastructure.logreplication.utils;

import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.protocols.wireprotocol.StreamAddressRange;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata.TableDescriptors;
import org.corfudb.runtime.CorfuStoreMetadata.TableMetadata;
import org.corfudb.runtime.CorfuStoreMetadata.TableName;
import org.corfudb.runtime.collections.CorfuRecord;
import org.corfudb.runtime.collections.PersistentCorfuTable;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.runtime.view.stream.StreamAddressSpace;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.corfudb.infrastructure.logreplication.LogReplicationConfig.REGISTRY_TABLE_ID;
import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;
import static org.corfudb.runtime.view.TableRegistry.getFullyQualifiedTableName;

/**
 * Handle construction and maintenance of the streams to replicate for all replication models supported in LR.
 * @author pankti-m
 */
@Slf4j
public class LogReplicationConfigManager {

    @Getter
    private final CorfuRuntime runtime;

    @Getter
    private LogReplicationConfig config;

    @Getter
    private ServerContext serverContext;

    private List<Entry<TableName, CorfuRecord<TableDescriptors, TableMetadata>>> registryTableEntries =
            new ArrayList<>();

    private long lastRegistryTableLogTail = Address.NON_ADDRESS;

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

    public LogReplicationConfig generateConfig() {
        PersistentCorfuTable<TableName, CorfuRecord<TableDescriptors, TableMetadata>> registryTable =
            runtime.getTableRegistry().getRegistryTable();
        registryTableEntries = registryTable.entryStream().collect(Collectors.toList());

        Set<String> streamsToReplicate = new HashSet<>();
        Set<UUID> streamsToDrop = new HashSet<>();
        streamsToReplicate.add(getFullyQualifiedTableName(CORFU_SYSTEM_NAMESPACE, TableRegistry.REGISTRY_TABLE_NAME));
        streamsToReplicate.add(getFullyQualifiedTableName(CORFU_SYSTEM_NAMESPACE, TableRegistry.PROTOBUF_DESCRIPTOR_TABLE_NAME));
        Map<UUID, List<UUID>> streamToTagsMap = new HashMap<>();

        registryTableEntries.forEach(entry -> {
            String tableName = getFullyQualifiedTableName(entry.getKey());

            if (entry.getValue().getMetadata().getTableOptions().getIsFederated()) {
                streamsToReplicate.add(tableName);

                // Collect tags for this stream
                UUID streamId = CorfuRuntime.getStreamID(tableName);
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
            } else {
                streamsToDrop.add(CorfuRuntime.getStreamID(tableName));
            }

            // TODO: Add other cases once the protobuf options for other subscribers are available
        });

        return new LogReplicationConfig(streamsToReplicate, streamsToDrop, streamToTagsMap, serverContext, runtime);
    }

    public LogReplicationConfig getUpdatedConfig() {
        // Check if the registry table has new entries.  Otherwise, no update is necessary.
        if (registryTableHasNewEntries()) {
            LogReplicationConfig updatedConfig = generateConfig();
            config.setStreamsToReplicate(updatedConfig.getStreamsToReplicate());
            config.setStreamsToDrop(updatedConfig.getStreamsToDrop());
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
