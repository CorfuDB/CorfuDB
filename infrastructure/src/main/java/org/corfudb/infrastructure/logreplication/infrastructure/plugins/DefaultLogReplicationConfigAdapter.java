package org.corfudb.infrastructure.logreplication.infrastructure.plugins;

import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.infrastructure.logreplication.infrastructure.ReplicationSubscriber;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.TableRegistry;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;
import static org.corfudb.runtime.view.TableRegistry.getFullyQualifiedTableName;

/**
 * Default testing implementation of a Log Replication Config Provider
 *
 * This implementation retrieves a fixed set of tables, which are used for testing purposes.
 */
public class DefaultLogReplicationConfigAdapter implements ILogReplicationConfigAdapter{

    private Map<ReplicationSubscriber, Set<String>> streamsToReplicateMap;

    public static final int MAP_COUNT = 10;
    public static final String TABLE_PREFIX = "Table00";
    public static final String NAMESPACE = "LR-Test";
    public static final String TAG_ONE = "tag_one";
    public static final String SAMPLE_CLIENT = "SampleClient";
    private static final String SEPARATOR = "$";
    private static final int STREAMING_CONFIG_TABLES_COUNT = 3;

    public DefaultLogReplicationConfigAdapter() {
        Set<String> streams = new HashSet<>();
        streams.add("Table001");
        streams.add("Table002");
        streams.add("Table003");

        // Support for UFO
        for (int i = 1; i <= MAP_COUNT; i++) {
            streams.add(NAMESPACE + SEPARATOR + TABLE_PREFIX + i);
        }
        String registryTable = getFullyQualifiedTableName(CORFU_SYSTEM_NAMESPACE, TableRegistry.REGISTRY_TABLE_NAME);

        String protoTable = getFullyQualifiedTableName(CORFU_SYSTEM_NAMESPACE,
            TableRegistry.PROTOBUF_DESCRIPTOR_TABLE_NAME);
        streams.add(registryTable);
        streams.add(protoTable);

        streamsToReplicateMap = new HashMap<>();
        streamsToReplicateMap.put(
            new ReplicationSubscriber(LogReplicationMetadata.ReplicationModels.REPLICATE_FULL_TABLES, SAMPLE_CLIENT),
            streams);
    }

    @Override
    public String getVersion() {
        return "version_latest";
    }

    @Override
    public Map<UUID, List<UUID>> getStreamingConfigOnSink() {
        Map<UUID, List<UUID>> streamsToTagsMaps = new HashMap<>();
        UUID streamTagOneDefaultId = TableRegistry.getStreamIdForStreamTag(NAMESPACE, TAG_ONE);

        for (int i = 1; i <= STREAMING_CONFIG_TABLES_COUNT; i++) {
            streamsToTagsMaps.put(CorfuRuntime.getStreamID(NAMESPACE + SEPARATOR + TABLE_PREFIX + i),
                Collections.singletonList(streamTagOneDefaultId));
        }
        return streamsToTagsMaps;
    }

    @Override
    public Map<ReplicationSubscriber, Set<String>> getSubscriberToStreamsMap() {
        return streamsToReplicateMap;
    }
}
