package org.corfudb.infrastructure.logreplication;

import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import org.corfudb.infrastructure.logreplication.infrastructure.LogReplicationClientConfigListener;
import org.corfudb.infrastructure.logreplication.infrastructure.LogReplicationContext;
import org.corfudb.infrastructure.logreplication.infrastructure.SessionManager;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterConfig;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationConfigManager;
import org.corfudb.runtime.CorfuOptions;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.LogReplication.ClientDestinationInfoKey;
import org.corfudb.runtime.LogReplication.ClientRegistrationId;
import org.corfudb.runtime.LogReplication.ClientRegistrationInfo;
import org.corfudb.runtime.LogReplication.DestinationInfoVal;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.corfudb.runtime.LogReplication.ReplicationModel;
import org.corfudb.runtime.LogReplication.ReplicationSubscriber;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStreamEntries;
import org.corfudb.runtime.collections.CorfuStreamEntry;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TableSchema;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.TableRegistry;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.corfudb.infrastructure.logreplication.infrastructure.SessionManager.getDefaultLogicalGroupSubscriber;
import static org.corfudb.runtime.LogReplicationLogicalGroupClient.LR_MODEL_METADATA_TABLE_NAME;
import static org.corfudb.runtime.LogReplicationLogicalGroupClient.LR_REGISTRATION_TABLE_NAME;
import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;
import static org.mockito.Mockito.mock;


public class LogReplicationClientConfigTest extends AbstractViewTest {

    private CorfuRuntime corfuRuntime;
    private CorfuStore corfuStore;
    private LogReplicationConfigManager configManager;
    private final SessionManager sessionManager = mock(SessionManager.class);
    private LogReplicationClientConfigListener clientConfigListener;

    private final TableSchema<ClientRegistrationId, ClientRegistrationInfo, Message> registrationTableSchema =
            new TableSchema<>(LR_REGISTRATION_TABLE_NAME, ClientRegistrationId.class, ClientRegistrationInfo.class, Message.class);

    private final TableSchema<ClientDestinationInfoKey, DestinationInfoVal, Message> clientMetadataTableSchema =
            new TableSchema<>(LR_MODEL_METADATA_TABLE_NAME, ClientDestinationInfoKey.class, DestinationInfoVal.class, Message.class);

    @Before
    public void setUp() {
        corfuRuntime = getDefaultRuntime();
        corfuStore = new CorfuStore(corfuRuntime);
        configManager = new LogReplicationConfigManager(corfuRuntime);
        final long topologyConfigId = 5L;
        LogReplicationContext replicationContext = new LogReplicationContext(configManager, topologyConfigId,
                getEndpoint(SERVERS.PORT_0));
        clientConfigListener = new LogReplicationClientConfigListener(sessionManager,
                replicationContext.getConfigManager(), corfuStore);
    }

    @After
    public void tearDown() {
        corfuRuntime.shutdown();
    }

    @Test
    public void testOpenClientConfigTables() {
        // Test configManger constructor has opened client config tables
        Set<String> expectedTables = new HashSet<>(Arrays.asList(LR_REGISTRATION_TABLE_NAME, LR_MODEL_METADATA_TABLE_NAME));
        Set<String> currentTables = new HashSet<>();
        for (CorfuStoreMetadata.TableName tn : corfuStore.listTables(CORFU_SYSTEM_NAMESPACE)){
            currentTables.add(tn.getTableName());
        }
        Assert.assertTrue(currentTables.containsAll(expectedTables));
    }

    @Test
    public void testGenerateLogicalGroupConfig() throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
//        TODO (V2): Currently we only add default subscriber this will change to the below
//        final String clientName = "clientName";
        final String clientName = "00000000-0000-0000-0000-0000000000001";
        final String logicalGroup = "logicalGroup";
        final String sampleTableName = "sampleTable";
        final String sampleStreamTag = "sampleTag";
//        TODO (V2): This will change as well to the below
//        ReplicationSubscriber logicalGroupSubscriber = ReplicationSubscriber.newBuilder()
//                .setClientName(clientName)
//                .setModel(ReplicationModel.LOGICAL_GROUPS)
//                .build();
        ReplicationSubscriber logicalGroupSubscriber = getDefaultLogicalGroupSubscriber();
        LogReplicationSession session = LogReplicationSession.newBuilder()
                .setSourceClusterId(new DefaultClusterConfig().getSourceClusterIds().get(0))
                .setSinkClusterId(new DefaultClusterConfig().getSinkClusterIds().get(0))
                .setSubscriber(logicalGroupSubscriber)
                .build();
        final List<String> destinations = new ArrayList<>(Collections.singletonList(session.getSinkClusterId()));

        CorfuOptions.ReplicationLogicalGroup replicationLogicalGroup = CorfuOptions.ReplicationLogicalGroup.newBuilder()
                .setLogicalGroup(logicalGroup)
                .setClientName(clientName)
                .build();
        CorfuOptions.SchemaOptions options = CorfuOptions.SchemaOptions.newBuilder()
                .setReplicationGroup(replicationLogicalGroup)
                .addStreamTag(sampleStreamTag)
                .build();
        corfuStore.openTable(CORFU_SYSTEM_NAMESPACE, sampleTableName,
                ClientRegistrationId.class, ClientRegistrationInfo.class, null,
                TableOptions.builder().schemaOptions(options).build());

        configManager.onNewClientRegister(logicalGroupSubscriber);
        configManager.onGroupDestinationsChange(logicalGroupSubscriber, logicalGroup, destinations);
        configManager.generateConfig(new HashSet<>(Collections.singletonList(session)));
        clientConfigListener.start();
        configManager.getUpdatedConfig();

        final String fullyQualifiedTableName = corfuStore.getTable(CORFU_SYSTEM_NAMESPACE, sampleTableName).getFullyQualifiedTableName();
        final Set<UUID> sampleTableStreamTags = corfuStore.getTable(CORFU_SYSTEM_NAMESPACE, sampleTableName).getStreamTags();
        final Set<String> expectedStreams = new HashSet<>(Arrays.asList(fullyQualifiedTableName,
                TableRegistry.getFullyQualifiedTableName(CORFU_SYSTEM_NAMESPACE, TableRegistry.REGISTRY_TABLE_NAME),
                TableRegistry.getFullyQualifiedTableName(CORFU_SYSTEM_NAMESPACE, TableRegistry.PROTOBUF_DESCRIPTOR_TABLE_NAME)));

        Assert.assertTrue(configManager.getSessionToConfigMap().containsKey(session));
        Assert.assertTrue(configManager.getSessionToConfigMap().get(session).getStreamsToReplicate()
                .containsAll(expectedStreams));
        Assert.assertTrue(configManager.getSessionToConfigMap().get(session).getDataStreamToTagsMap()
                .containsKey(CorfuRuntime.getStreamID(fullyQualifiedTableName)));
        Assert.assertTrue(sampleTableStreamTags.containsAll(configManager.getSessionToConfigMap()
                .get(session).getDataStreamToTagsMap().get(CorfuRuntime.getStreamID(fullyQualifiedTableName))));
    }

    @Test
    public void testGenerateFullTableConfig() throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        final String clientName = "clientName";
        final String sampleTableName = "sampleTable";
        final String sampleStreamTag = "sampleTag";
        ReplicationSubscriber fullTableSubscriber = ReplicationSubscriber.newBuilder()
                .setClientName(clientName)
                .setModel(ReplicationModel.FULL_TABLE)
                .build();
        LogReplicationSession session = LogReplicationSession.newBuilder()
                .setSourceClusterId(new DefaultClusterConfig().getSourceClusterIds().get(0))
                .setSinkClusterId(new DefaultClusterConfig().getSinkClusterIds().get(0))
                .setSubscriber(fullTableSubscriber)
                .build();

        CorfuOptions.SchemaOptions options = CorfuOptions.SchemaOptions.newBuilder()
                .setIsFederated(true)
                .addStreamTag(sampleStreamTag)
                .build();
        corfuStore.openTable(CORFU_SYSTEM_NAMESPACE, sampleTableName,
                ClientRegistrationId.class, ClientRegistrationInfo.class, null,
                TableOptions.builder().schemaOptions(options).build());

        configManager.onNewClientRegister(fullTableSubscriber);
        configManager.generateConfig(new HashSet<>(Collections.singletonList(session)));
        clientConfigListener.start();
        configManager.getUpdatedConfig();

        final String fullyQualifiedTableName = corfuStore.getTable(CORFU_SYSTEM_NAMESPACE, sampleTableName).getFullyQualifiedTableName();
        final Set<UUID> sampleTableStreamTags = corfuStore.getTable(CORFU_SYSTEM_NAMESPACE, sampleTableName).getStreamTags();
        final Set<String> expectedStreams = new HashSet<>(Arrays.asList(fullyQualifiedTableName,
                TableRegistry.getFullyQualifiedTableName(CORFU_SYSTEM_NAMESPACE, TableRegistry.REGISTRY_TABLE_NAME),
                TableRegistry.getFullyQualifiedTableName(CORFU_SYSTEM_NAMESPACE, TableRegistry.PROTOBUF_DESCRIPTOR_TABLE_NAME)));

        Assert.assertTrue(configManager.getSessionToConfigMap().containsKey(session));
        Assert.assertTrue(configManager.getSessionToConfigMap().get(session).getStreamsToReplicate()
                .containsAll(expectedStreams));
        Assert.assertTrue(configManager.getSessionToConfigMap().get(session).getDataStreamToTagsMap()
                .containsKey(CorfuRuntime.getStreamID(fullyQualifiedTableName)));
        Assert.assertTrue(sampleTableStreamTags.containsAll(configManager.getSessionToConfigMap()
                .get(session).getDataStreamToTagsMap().get(CorfuRuntime.getStreamID(fullyQualifiedTableName))));
    }

    @Test
    public void testPreProcessAngGetTail() {
        final String clientName = "clientName";
        final String logicalGroup = "logicalGroup";
        final List<String> destinations = new ArrayList<>(Arrays.asList("d1", "d2", "d3"));
        final List<String> destinations1 = new ArrayList<>(Arrays.asList("d4", "d5", "d6"));
        ReplicationSubscriber replicationSubscriber = ReplicationSubscriber.newBuilder()
                .setClientName(clientName)
                .setModel(ReplicationModel.LOGICAL_GROUPS)
                .build();

        addClient(clientName);
        addDestination(clientName, logicalGroup, destinations);
        CorfuStoreMetadata.Timestamp ts = configManager.preprocessAndGetTail();
        Assert.assertEquals(new HashSet<>(destinations), configManager.getGroupSinksMap().get(logicalGroup));

        addDestination(clientName, logicalGroup, destinations1);
        CorfuStoreMetadata.Timestamp ts1 = configManager.preprocessAndGetTail();
        Assert.assertEquals(new HashSet<>(destinations1), configManager.getGroupSinksMap().get(logicalGroup));

        Assert.assertTrue(configManager.getRegisteredSubscribers().contains(replicationSubscriber));
        Assert.assertTrue(ts1.getSequence() > ts.getSequence());
    }

    @Test
    public void testListenerStart() {
        final String clientName = "clientName";
        final String logicalGroup = "logicalGroup";
        final List<String> destinations = new ArrayList<>(Arrays.asList("d1", "d2", "d3"));
        final ReplicationSubscriber replicationSubscriber = ReplicationSubscriber.newBuilder()
                .setClientName(clientName)
                .setModel(ReplicationModel.LOGICAL_GROUPS)
                .build();

        addClient(clientName);
        addDestination(clientName, logicalGroup, destinations);

        clientConfigListener.start();

        Assert.assertTrue(configManager.getRegisteredSubscribers().contains(replicationSubscriber));
        Assert.assertEquals(new HashSet<>(destinations), configManager.getGroupSinksMap().get(logicalGroup));
    }

    @Test
    public void testListenerNext() {
//        TODO (V2): Currently we only add default subscriber this will change to the below
//        final String clientName = "clientName";
        final String clientName = "00000000-0000-0000-0000-0000000000001";
        final String logicalGroup = "logicalGroup";
        final List<String> destinations = new ArrayList<>(Arrays.asList("d1", "d2", "d3"));
//        TODO (V2): This will change as well to the below
//        ReplicationSubscriber replicationSubscriber = ReplicationSubscriber.newBuilder()
//                .setClientName(clientName)
//                .setModel(ReplicationModel.LOGICAL_GROUPS)
//                .build();
        ReplicationSubscriber replicationSubscriber = getDefaultLogicalGroupSubscriber();

        ClientRegistrationId clientKey = getRegistrationId(clientName);
        ClientRegistrationInfo clientInfo = getRegistrationInfo(clientName);

        ClientDestinationInfoKey destinationKey = getDestinationKey(clientName, logicalGroup);
        DestinationInfoVal destinationVal = getDestinationVal(destinations);

        CorfuStreamEntry<ClientRegistrationId, ClientRegistrationInfo, Message> registrationEntry =
                new CorfuStreamEntry<>(clientKey, clientInfo, null, CorfuStreamEntry.OperationType.UPDATE);
        CorfuStreamEntry<ClientDestinationInfoKey, DestinationInfoVal, Message> destinationEntry =
                new CorfuStreamEntry<>(destinationKey, destinationVal, null, CorfuStreamEntry.OperationType.UPDATE);

        Map<TableSchema, List<CorfuStreamEntry>> entries = new HashMap<>();
        entries.put(registrationTableSchema, Collections.singletonList(registrationEntry));
        entries.put(clientMetadataTableSchema, Collections.singletonList(destinationEntry));
        CorfuStoreMetadata.Timestamp ts = CorfuStoreMetadata.Timestamp.newBuilder().setEpoch(0L).setSequence(Address.NON_ADDRESS).build();
        CorfuStreamEntries corfuStreamEntries = new CorfuStreamEntries(entries, ts);

        clientConfigListener.onNext(corfuStreamEntries);

        Assert.assertTrue(configManager.getRegisteredSubscribers().contains(replicationSubscriber));
        System.out.println(configManager.getGroupSinksMap());
        Assert.assertEquals(new HashSet<>(destinations), configManager.getGroupSinksMap().get(logicalGroup));

        // Test deletion entries (client deletion currently are no-ops)
        CorfuStreamEntry<ClientRegistrationId, ClientRegistrationInfo, Message> deleteRegistrationEntry =
                new CorfuStreamEntry<>(clientKey, clientInfo, null, CorfuStreamEntry.OperationType.DELETE);
        CorfuStreamEntry<ClientDestinationInfoKey, DestinationInfoVal, Message> deleteDestinationEntry =
                new CorfuStreamEntry<>(destinationKey, destinationVal, null, CorfuStreamEntry.OperationType.DELETE);

        Map<TableSchema, List<CorfuStreamEntry>> deletionEntries = new HashMap<>();
        deletionEntries.put(registrationTableSchema, Collections.singletonList(deleteRegistrationEntry));
        deletionEntries.put(clientMetadataTableSchema, Collections.singletonList(deleteDestinationEntry));
        CorfuStreamEntries corfuStreamDeletionEntries = new CorfuStreamEntries(deletionEntries, ts);

        clientConfigListener.onNext(corfuStreamDeletionEntries);

        //TODO: Assert registered subscribers are deleted after client deletion implemented
        Assert.assertEquals(configManager.getGroupSinksMap().get(logicalGroup), new HashSet<>());
    }

    @Test
    public void testListenerStop() {
        clientConfigListener.stop();
    }

    private ClientRegistrationId getRegistrationId(String clientName) {
        return ClientRegistrationId.newBuilder()
                .setClientName(clientName)
                .build();
    }

    private ClientRegistrationInfo getRegistrationInfo(String clientName) {
        Instant time = Instant.now();
        Timestamp timestamp = Timestamp.newBuilder().setSeconds(time.getEpochSecond())
                .setNanos(time.getNano()).build();
        return ClientRegistrationInfo.newBuilder()
                .setClientName(clientName)
                .setModel(ReplicationModel.LOGICAL_GROUPS)
                .setRegistrationTime(timestamp)
                .build();
    }

    private ClientDestinationInfoKey getDestinationKey(String clientName, String logicalGroup) {
        return ClientDestinationInfoKey.newBuilder()
                .setClientName(clientName)
                .setModel(ReplicationModel.LOGICAL_GROUPS)
                .setGroupName(logicalGroup)
                .build();
    }

    private DestinationInfoVal getDestinationVal(List<String> destinations) {
        return DestinationInfoVal.newBuilder()
                .addAllDestinationIds(destinations)
                .build();
    }

    private void addClient(String clientName) {
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            txn.putRecord(corfuStore.getTable(CORFU_SYSTEM_NAMESPACE, LR_REGISTRATION_TABLE_NAME),
                    getRegistrationId(clientName), getRegistrationInfo(clientName), null);
            txn.commit();
        }
    }

    private void addDestination(String clientName, String logicalGroup, List<String> destinations) {
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            txn.putRecord(corfuStore.getTable(CORFU_SYSTEM_NAMESPACE, LR_MODEL_METADATA_TABLE_NAME),
                    getDestinationKey(clientName, logicalGroup), getDestinationVal(destinations), null);
            txn.commit();
        }
    }
}
