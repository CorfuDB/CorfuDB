package org.corfudb.infrastructure.logreplication;

import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.ReplicationSubscriber;
import org.corfudb.infrastructure.logreplication.proto.Sample.Metadata;
import org.corfudb.infrastructure.logreplication.proto.Sample.IntValue;
import org.corfudb.infrastructure.logreplication.proto.Sample.IntValueTag;
import org.corfudb.infrastructure.logreplication.proto.Sample.StringKey;
import org.corfudb.test.SampleSchema.ValueFieldTagOne;
import org.corfudb.test.SampleSchema.Uuid;

import org.corfudb.infrastructure.logreplication.utils.LogReplicationConfigManager;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.view.AbstractViewTest;

import org.corfudb.runtime.view.TableRegistry;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.corfudb.runtime.view.TableRegistry.REGISTRY_TABLE_NAME;
import static org.corfudb.runtime.view.TableRegistry.PROTOBUF_DESCRIPTOR_TABLE_NAME;
import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

@Slf4j
public class LogReplicationConfigManagerTest extends AbstractViewTest {

    private CorfuRuntime runtime;
    private CorfuStore corfuStore;

    private Map<ReplicationSubscriber, Set<String>> expectedSubscriberToStreamsToReplicateMap = new HashMap<>();
    private Map<ReplicationSubscriber, Set<UUID>> expectedSubscriberToStreamsToDropMap = new HashMap<>();
    private Map<UUID, List<UUID>> streamToTagsMap = new HashMap<>();

    private static final String NAMESPACE = "LR-Test";
    private static final String TABLE1 = "Table001";
    private static final String TABLE2 = "Table002";
    private static final String TABLE3 = "Table003";
    private static final String TABLE4 = "Table004";
    private static final String TABLE5 = "Table005";
    private static final String TABLE6 = "Table006";

    @Before
    public void setup() throws Exception {
        runtime = getDefaultRuntime();
        corfuStore = new CorfuStore(runtime);

        Set<String> streamNamesToReplicate = new HashSet<>();

        // Merge-only streams which must be replicated
        streamNamesToReplicate.add(REGISTRY_TABLE_NAME);
        streamNamesToReplicate.add(PROTOBUF_DESCRIPTOR_TABLE_NAME);

        // 2 tables which should be replicated, opened with schema IntValueTag which has is_federated=true
        streamNamesToReplicate.add(TABLE1);
        streamNamesToReplicate.add(TABLE2);

        // Open tables to be replicated and setup the expected streams to replicated and tags maps
        setupStreamsToReplicateAndTagsMap(streamNamesToReplicate, ReplicationSubscriber.getDefaultReplicationSubscriber(),
            IntValueTag.class);

        // 2 tables which should not be replicated, opened with schema IntValue which has is_federated=false
        Set<String> streamNamesToDrop = new HashSet<>();
        streamNamesToDrop.add(TABLE3);
        streamNamesToDrop.add(TABLE4);
        setupStreamsToDrop(streamNamesToDrop, ReplicationSubscriber.getDefaultReplicationSubscriber(), IntValue.class);
    }

    private <K extends Message> void setupStreamsToReplicateAndTagsMap(Set<String> streamNamesToReplicate,
                                                            ReplicationSubscriber subscriber,
                                                            Class<K> valueSchema) throws Exception {

        TableOptions tableOptions = TableOptions.fromProtoSchema(valueSchema);

        for (String stream : streamNamesToReplicate) {

            if (!stream.equals(REGISTRY_TABLE_NAME) && !stream.equals(PROTOBUF_DESCRIPTOR_TABLE_NAME)) {
                corfuStore.openTable(NAMESPACE, stream, StringKey.class, valueSchema, Metadata.class, tableOptions);
            }

            Set<String> streamsToReplicate = expectedSubscriberToStreamsToReplicateMap.getOrDefault(
                subscriber, new HashSet<>());

            String fullyQualifiedTableName;
            if (stream.equals(REGISTRY_TABLE_NAME) || stream.equals(PROTOBUF_DESCRIPTOR_TABLE_NAME)) {
                fullyQualifiedTableName = TableRegistry.getFullyQualifiedTableName(CORFU_SYSTEM_NAMESPACE, stream);
            } else {
                fullyQualifiedTableName = TableRegistry.getFullyQualifiedTableName(NAMESPACE, stream);
            }
            streamsToReplicate.add(fullyQualifiedTableName);
            expectedSubscriberToStreamsToReplicateMap.put(subscriber, streamsToReplicate);

            if (!stream.equals(REGISTRY_TABLE_NAME) && !stream.equals(PROTOBUF_DESCRIPTOR_TABLE_NAME)) {
                List<UUID> streamTags = tableOptions.getSchemaOptions().getStreamTagList().stream().map(streamTag ->
                    TableRegistry.getStreamIdForStreamTag(NAMESPACE, streamTag)).collect(Collectors.toList());

                streamToTagsMap.put(CorfuRuntime.getStreamID(fullyQualifiedTableName), streamTags);
            }
        }

    }

    private <K extends Message> void setupStreamsToDrop(Set<String> streamNamesToDrop, ReplicationSubscriber subscriber,
                                                        Class<K> valueSchema) throws Exception {

        for (String streamToDrop : streamNamesToDrop) {
            corfuStore.openTable(NAMESPACE, streamToDrop, StringKey.class, valueSchema, Metadata.class,
                TableOptions.fromProtoSchema(valueSchema));

            Set<UUID> streamIdsToDrop = expectedSubscriberToStreamsToDropMap.getOrDefault(subscriber, new HashSet<>());

            String fullyQualifiedTableName = TableRegistry.getFullyQualifiedTableName(NAMESPACE, streamToDrop);
            streamIdsToDrop.add(CorfuRuntime.getStreamID(fullyQualifiedTableName));
            expectedSubscriberToStreamsToDropMap.put(subscriber, streamIdsToDrop);
        }
    }

    @Test
    public void testConfigGeneration() {
        LogReplicationConfigManager configManager = new LogReplicationConfigManager(runtime);
        verifyExpectedConfigGenerated(configManager.getConfig());
    }

    @Test
    public void testConfigUpdate() throws Exception {
        LogReplicationConfigManager configManager = new LogReplicationConfigManager(runtime);
        verifyExpectedConfigGenerated(configManager.getConfig());

        // Open new tables and update the expected streams to replicate map, streams to drop and stream tags
        setupStreamsToReplicateAndTagsMap(Collections.singleton(TABLE5),
            ReplicationSubscriber.getDefaultReplicationSubscriber(), ValueFieldTagOne.class);

        setupStreamsToDrop(Collections.singleton(TABLE6), ReplicationSubscriber.getDefaultReplicationSubscriber(),
            Uuid.class);

        verifyExpectedConfigGenerated(configManager.getUpdatedConfig());
    }

    private void verifyExpectedConfigGenerated(LogReplicationConfig actualConfig) {
        Assert.assertTrue(Objects.equals(expectedSubscriberToStreamsToReplicateMap,
            actualConfig.getReplicationSubscriberToStreamsMap()));

        Assert.assertTrue(Objects.equals(expectedSubscriberToStreamsToDropMap,
            actualConfig.getSubscriberToNonReplicatedStreamsMap()));

        Assert.assertTrue(Objects.equals(streamToTagsMap, actualConfig.getDataStreamToTagsMap()));
    }
}
