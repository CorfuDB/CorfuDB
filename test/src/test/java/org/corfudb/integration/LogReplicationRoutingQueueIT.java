package org.corfudb.integration;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.browser.CorfuStoreBrowserEditor;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterConfig;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterManager;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.runtime.CorfuOptions;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.ExampleSchemas;
import org.corfudb.runtime.exceptions.LogReplicationClientException;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.runtime.LRFullStateReplicationContext;
import org.corfudb.runtime.LRSiteDiscoveryListener;
import org.corfudb.runtime.LiteRoutingQueueListener;
import org.corfudb.runtime.LogReplication;
import org.corfudb.runtime.Queue;
import org.corfudb.runtime.RoutingQueueSenderClient;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStoreEntry;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.proto.RpcCommon;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.runtime.LogReplicationUtils.LOG_ENTRY_SYNC_QUEUE_TAG_SENDER_PREFIX;
import static org.corfudb.runtime.LogReplicationUtils.REPLICATED_RECV_Q_PREFIX;
import static org.corfudb.runtime.LogReplicationUtils.REPLICATED_QUEUE_TAG;
import static org.corfudb.runtime.LogReplicationUtils.REPLICATION_STATUS_TABLE_NAME;
import static org.corfudb.runtime.RoutingQueueSenderClient.DEFAULT_ROUTING_QUEUE_CLIENT;
import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

@SuppressWarnings("checkstyle:magicnumber")
@Slf4j
public class LogReplicationRoutingQueueIT extends CorfuReplicationMultiSourceSinkIT {

    private int numSource = 1;

    private int numLogEntryUpdates = 10;

    /**
     * Get the client runtime that connects to Source cluster node.
     *
     * @return CorfuRuntime for client
     */
    private CorfuRuntime getClientRuntime() {
        return CorfuRuntime.fromParameters(CorfuRuntime.CorfuRuntimeParameters.builder().build())
                .parseConfigurationString(DEFAULT_HOST + ":" + DEFAULT_PORT)
                .connect();
    }

    @Before
    public void setUp() throws Exception {
        super.setUp(1, 1, DefaultClusterManager.TP_SINGLE_SOURCE_SINK_ROUTING_QUEUE);
        openLogReplicationStatusTable();
    }

    /**
     * This test verifies successful snapshot and log entry sync replication for Routing Queue model
     * @throws Exception
     */
    @Test
    public void testRoutingQueueReplication() throws Exception {

        // Create the Routing Queue Sender client and subscribe a listener to get snapshot sync requests.
        CorfuRuntime clientRuntime = getClientRuntime();
        CorfuStore clientCorfuStore = new CorfuStore(clientRuntime);
        String clientName = DEFAULT_ROUTING_QUEUE_CLIENT;
        RoutingQueueSenderClient queueSenderClient = new RoutingQueueSenderClient(clientCorfuStore, clientName);
        SnapshotProvider snapshotProvider = new SnapshotProvider(clientCorfuStore);
        queueSenderClient.startLRSnapshotTransmitter(snapshotProvider); // starts a listener on event table

        try {
            RoutingQueueListener listener = new RoutingQueueListener(sinkCorfuStores.get(0),
                    DefaultClusterConfig.getSourceClusterIds().get(0), clientName);
            sinkCorfuStores.get(0).subscribeRoutingQListener(listener);
            startReplicationServers();

            // Wait till all snapshot data is provided by the replication client
            while (!snapshotProvider.isSnapshotSent) {
                Thread.sleep(5000);
            }

            // Generate some incremental updates which will get sent as Log Entry Sync
            generateLogEntryData(clientCorfuStore, queueSenderClient);

            // Wait until these updates are received on the Sink
            int numLogEntriesReceived = listener.logEntryMsgCnt;
            while (numLogEntriesReceived < numLogEntryUpdates) {
                Thread.sleep(5000);
                numLogEntriesReceived = listener.logEntryMsgCnt;
                log.info("Entries got on receiver {}", numLogEntriesReceived);
            }

            // Verify that all expected data through snapshot and log entry sync was received.
            log.info("Expected num entries on the Sink Received");
            assertThat(listener.logEntryMsgCnt).isEqualTo(numLogEntryUpdates);
            assertThat(listener.snapSyncMsgCnt).isEqualTo(5);
            log.info("Sink replicated queue size: {}", listener.snapSyncMsgCnt);
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            throw new LogReplicationClientException(e);
        }
    }

    /**
     * This test verifies the behavior of snapshot sync requested through multiple workflows
     * 1. Negotiation
     * 2. Global snapshot sync(to all Sink clusters) via an LR tool
     * 3. Application(client)-requested snapshot sync
     * @throws Exception
     */
    @Test
    public void testRoutingQueueFullSyncs() throws Exception {

        // Create the Routing Queue Sender client and subscribe a listener to get snapshot sync requests.
        CorfuRuntime clientRuntime = getClientRuntime();
        CorfuStore clientCorfuStore = new CorfuStore(clientRuntime);
        String sourceSiteId = DefaultClusterConfig.getSourceClusterIds().get(0);
        CorfuStoreBrowserEditor editor = new CorfuStoreBrowserEditor(clientRuntime, null, true);
        String clientName = DEFAULT_ROUTING_QUEUE_CLIENT;
        RoutingQueueSenderClient queueSenderClient = new RoutingQueueSenderClient(clientCorfuStore, clientName);

        // SnapshotProvider implements RoutingQueueSenderClient.LRTransmitterReplicationModule
        SnapshotProvider snapshotProvider = new SnapshotProvider(clientCorfuStore);
        queueSenderClient.startLRSnapshotTransmitter(snapshotProvider); // starts a listener on event table

        log.info("Sink Queue name: {}", REPLICATED_RECV_Q_PREFIX + sourceSiteId);
        startReplicationServers();

        // Wait till all snapshot data is provided by the replication client
        while (!snapshotProvider.isSnapshotSent) {
            Thread.sleep(5000);
        }

        RoutingQueueListener listener = new RoutingQueueListener(sinkCorfuStores.get(0),
            sourceSiteId, clientName);
        sinkCorfuStores.get(0).subscribeRoutingQListener(listener);

        int numFullSyncMsgsGot = listener.snapSyncMsgCnt;
        while (numFullSyncMsgsGot < 5) {
            Thread.sleep(5000);
            numFullSyncMsgsGot = listener.snapSyncMsgCnt;
            log.info("Entries got on receiver {}", numFullSyncMsgsGot);
        }
        // Verify that all snapshot sync data was replicated.
        assertThat(listener.snapSyncMsgCnt).isEqualTo(5);

        // Now request a snapshot sync again for all sites!
        snapshotProvider.isSnapshotSent = false;
        editor.requestGlobalFullSync();

        // Verify that all data was received from this cycle
        while (numFullSyncMsgsGot < 10) {
            Thread.sleep(5000);
            numFullSyncMsgsGot = listener.snapSyncMsgCnt;
            log.info("Entries got on receiver after 2nd full sync {}", numFullSyncMsgsGot);
        }
        assertThat(listener.snapSyncMsgCnt).isEqualTo(10);

        // Now request a full sync this time for just one site!
        snapshotProvider.isSnapshotSent = false;
        queueSenderClient.requestSnapshotSync(DefaultClusterConfig.getSourceClusterIds().get(0),
            DefaultClusterConfig.getSinkClusterIds().get(0));

        // Verify that all data was received on the Sink
        while (numFullSyncMsgsGot < 15) {
            Thread.sleep(5000);
            numFullSyncMsgsGot = listener.snapSyncMsgCnt;
            log.info("Entries got on receiver after 3rd full sync {}", numFullSyncMsgsGot);
        }
        assertThat(listener.snapSyncMsgCnt).isEqualTo(15);
    }

    /**
     * This test verifies the behavior of 2-way Snapshot sync, i.e., Cluster A -> B and B -> A
     * @throws Exception
     */
    @Test
    public void testRoutingQueue2way() throws Exception {

        // Create the Routing Queue Sender client and subscribe a listener to get snapshot sync requests.
        CorfuRuntime clientRuntime = getClientRuntime();
        CorfuStore clientCorfuStore = new CorfuStore(clientRuntime);
        String sourceSiteId = DefaultClusterConfig.getSourceClusterIds().get(0);
        final String clientName = DEFAULT_ROUTING_QUEUE_CLIENT;

        RoutingQueueSenderClient queueSenderClient = new RoutingQueueSenderClient(clientCorfuStore, clientName);
        // SnapshotProvider implements RoutingQueueSenderClient.LRTransmitterReplicationModule
        SnapshotProvider snapshotProvider = new SnapshotProvider(clientCorfuStore);
        queueSenderClient.startLRSnapshotTransmitter(snapshotProvider); // starts a listener on event table

        // Also start a Site Discovery Service on the Source Cluster that listens for new sites & starts up
        // RoutingQueueListeners when a sink comes up
        RoutingQSiteDiscoverer remoteSiteDiscoverer = new RoutingQSiteDiscoverer(clientCorfuStore,
                LogReplication.ReplicationModel.ROUTING_QUEUES, clientName);

        // Start up snapshot sync providers on both sink sites to test reverse replication
        sinkCorfuStores.forEach(sinkCorfuStore -> {
            RoutingQueueSenderClient sinkSideQsender = null;
            sinkSideQsender = new RoutingQueueSenderClient(sinkCorfuStore, clientName);
            // SnapshotProvider implements RoutingQueueSenderClient.LRTransmitterReplicationModule
            SnapshotProvider sinkSideSnapProvider = new SnapshotProvider(sinkCorfuStore);
            sinkSideQsender.startLRSnapshotTransmitter(sinkSideSnapProvider); // starts a listener on event table
        });

        // Open queue on sink
        log.info("Sink Queue name: {}", REPLICATED_RECV_Q_PREFIX + sourceSiteId);
        sinkCorfuStores.get(0).openQueue(CORFU_SYSTEM_NAMESPACE, REPLICATED_RECV_Q_PREFIX + sourceSiteId,
            Queue.RoutingTableEntryMsg.class, TableOptions.builder().schemaOptions(
                CorfuOptions.SchemaOptions.newBuilder().addStreamTag(REPLICATED_QUEUE_TAG).build()).build());

        startReplicationServers();
        while (!snapshotProvider.isSnapshotSent) {
            Thread.sleep(5000);
        }

        // Test forward replication source -> sink
        RoutingQueueListener listener = new RoutingQueueListener(sinkCorfuStores.get(0),
            sourceSiteId, clientName);
        sinkCorfuStores.get(0).subscribeRoutingQListener(listener);

        int numFullSyncMsgsGot = listener.snapSyncMsgCnt;
        while (numFullSyncMsgsGot < 5) {
            Thread.sleep(5000);
            numFullSyncMsgsGot = listener.snapSyncMsgCnt;
            log.info("Entries got on receiver {}", numFullSyncMsgsGot);
        }
        assertThat(listener.snapSyncMsgCnt).isEqualTo(5);

        // Test reverse replication from the 2 sinks to the source
        remoteSiteDiscoverer.iGot.values().forEach(sourceListener -> {
            assertThat(sourceListener.snapSyncMsgCnt).isEqualTo(5);
        });
    }

    /**
     * This test verifies the behavior when an ongoing snapshot sync gets cancelled.
     * @throws Exception
     */
    @Test
    public void testSnapshotSyncCancel() throws Exception {
        // Create the Routing Queue Sender client and subscribe a listener to get snapshot sync requests.
        CorfuRuntime clientRuntime = getClientRuntime();
        CorfuStore clientCorfuStore = new CorfuStore(clientRuntime);
        String sourceSiteId = DefaultClusterConfig.getSourceClusterIds().get(0);
        CorfuStoreBrowserEditor editor = new CorfuStoreBrowserEditor(clientRuntime, null, true);
        String clientName = DEFAULT_ROUTING_QUEUE_CLIENT;
        RoutingQueueSenderClient queueSenderClient = new RoutingQueueSenderClient(clientCorfuStore, clientName);
        CountDownLatch startedTransmitting = new CountDownLatch(1);
        SnapshotProvider snapshotProvider = new SnapshotProvider(clientCorfuStore, startedTransmitting);
        queueSenderClient.startLRSnapshotTransmitter(snapshotProvider);

        RoutingQueueListener listener = new RoutingQueueListener(sinkCorfuStores.get(0),
                sourceSiteId, clientName);
        sinkCorfuStores.get(0).subscribeRoutingQListener(listener);

        startReplicationServers();

        int numFullSyncMsgsGot = listener.snapSyncMsgCnt;
        boolean sentDuplicateSync = false;
        boolean cancelledInitialSync = false;

        snapshotProvider.setNumFullSyncBatches(200);

        startedTransmitting.await();
        log.info("Negotiation snapshot sync has started!!!");

        while (numFullSyncMsgsGot < snapshotProvider.getNumFullSyncBatches()) {
            if (!cancelledInitialSync) {
                assertThat(queueSenderClient.getPendingFullSyncs()
                        .containsKey(DefaultClusterConfig.getSinkClusterIds().get(0)))
                        .isTrue();
                snapshotProvider.cancel(queueSenderClient.getPendingFullSyncs()
                        .get(DefaultClusterConfig.getSinkClusterIds().get(0)));
                cancelledInitialSync = true;
            }

            if (!sentDuplicateSync) {
                editor.requestGlobalFullSync();
                sentDuplicateSync = true;
            }

            numFullSyncMsgsGot = listener.snapSyncMsgCnt;
        }
        assertThat(listener.snapSyncMsgCnt).isEqualTo(snapshotProvider.getNumFullSyncBatches());
    }

    private void generateLogEntryData(CorfuStore corfuStore, RoutingQueueSenderClient client) {
        String namespace = CORFU_SYSTEM_NAMESPACE;

        String streamTagFollowed =
            LOG_ENTRY_SYNC_QUEUE_TAG_SENDER_PREFIX + DefaultClusterConfig.getSinkClusterIds().get(0);
        log.info("Stream UUID: {}", TableRegistry.getStreamIdForStreamTag(CORFU_SYSTEM_NAMESPACE, streamTagFollowed));

        for (int i = 0; i < numLogEntryUpdates; i++) {
            ByteBuffer buffer = ByteBuffer.allocate(Integer.SIZE);
            buffer.putInt(i);
            Queue.RoutingTableEntryMsg val =
                Queue.RoutingTableEntryMsg.newBuilder()
                        .setSourceClusterId(DefaultClusterConfig.getSourceClusterIds().get(0))
                        .addAllDestinations(Arrays.asList(DefaultClusterConfig.getSinkClusterIds().get(0),
                                DefaultClusterConfig.getSinkClusterIds().get(1)))
                        .setOpaquePayload(ByteString.copyFrom(buffer.array()))
                        .setReplicationType(Queue.ReplicationType.LOG_ENTRY_SYNC)
                        .build();

            try (TxnContext txnContext = corfuStore.txn(namespace)) {
                client.transmitDeltaMessages(txnContext, Collections.singletonList(val), corfuStore);
                log.info("Committed at {}", txnContext.commit());
            } catch (Exception e) {
                log.error("Failed to add data to the queue", e);
            }
        }
    }

    /**
     * This test verifies the behavior of Snapshot and Log Entry Sync when using Scoped Transactions.
     * @throws Exception
     */
    @Test
    public void testRoutingQueueReplicationWithScopedTxn() throws Exception {
        // Create the Routing Queue Sender client and subscribe a listener to get snapshot sync requests.
        CorfuRuntime clientRuntime = getClientRuntime();
        CorfuStore clientCorfuStore = new CorfuStore(clientRuntime);
        String clientName = DEFAULT_ROUTING_QUEUE_CLIENT;
        RoutingQueueSenderClient queueSenderClient = new RoutingQueueSenderClient(clientCorfuStore, clientName);

        // SnapshotProvider implements RoutingQueueSenderClient.LRTransmitterReplicationModule
        ScopedSnapshotProvider snapshotProvider = new ScopedSnapshotProvider(clientCorfuStore);

        // starts a listener on event table
        queueSenderClient.startLRSnapshotTransmitter(snapshotProvider);

        try {
            RoutingQueueListener listener = new RoutingQueueListener(sinkCorfuStores.get(0),
                    DefaultClusterConfig.getSourceClusterIds().get(0), clientName);
            sinkCorfuStores.get(0).subscribeRoutingQListener(listener);
            startReplicationServers();
            while (!snapshotProvider.isSnapshotSent) {
                Thread.sleep(5000);
            }
            log.info("Snapshot Sent");

            // Generate some incremental updates which will get sent as Log Entry Sync
            generateLogEntryData(clientCorfuStore, queueSenderClient);

            // Verify that all data generated through Snapshot and Log Entry Sync is received on the Sink cluster
            int numLogEntriesReceived = listener.logEntryMsgCnt;
            while (numLogEntriesReceived < 10) {
                Thread.sleep(5000);
                numLogEntriesReceived = listener.logEntryMsgCnt;
                log.info("Entries got on receiver {}", numLogEntriesReceived);
            }

            log.info("Expected num entries on the Sink Received");
            assertThat(listener.logEntryMsgCnt).isEqualTo(numLogEntryUpdates);
            assertThat(listener.snapSyncMsgCnt).isEqualTo(5);
            log.info("Sink replicated queue size: {}", listener.snapSyncMsgCnt);
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Provide a full sync or snapshot
     */
    public static class SnapshotProvider implements RoutingQueueSenderClient.LRTransmitterReplicationModule {

        public final String someNamespace = "testNamespace";

        @Getter
        @Setter
        public int numFullSyncBatches = 5;
        public CountDownLatch startedTransmitting;
        private int recordId = 0;
        CorfuStore corfuStore;
        public boolean isSnapshotSent = false;

        SnapshotProvider(CorfuStore corfuStore) {
            this.corfuStore = corfuStore;
        }

        SnapshotProvider(CorfuStore corfuStore, CountDownLatch latch) {
            this.corfuStore = corfuStore;
            this.startedTransmitting = latch;
        }

        @Override
        public void provideFullStateData(LRFullStateReplicationContext context) {
            for (int i = 0; i < numFullSyncBatches; i++) {
                try (TxnContext tx = corfuStore.txn(someNamespace)) {
                    Queue.RoutingTableEntryMsg message = Queue.RoutingTableEntryMsg.newBuilder()
                            .addDestinations(context.getDestinationSiteId())
                            .setOpaquePayload(ByteString.copyFromUtf8("opaquetxn"+recordId))
                            .setReplicationType(Queue.ReplicationType.SNAPSHOT_SYNC)
                            .buildPartial();
                    recordId++;
                    context.transmit(message);

                    log.info("Transmitting full sync message{}", message);
                    // For debugging Q's stream id should be "61d2fc0f-315a-3d87-a982-24fb36932050"
                    log.info("FS Committed at {}", tx.commit());

                    if (startedTransmitting != null && startedTransmitting.getCount() > 0) {
                        startedTransmitting.countDown();
                    }
                }
            }
            context.markCompleted();
            isSnapshotSent = true;
        }

        @Override
        public void cancel(LRFullStateReplicationContext context) {
            context.cancel();
        }
    }

    public class ScopedSnapshotProvider extends SnapshotProvider {
        Table<RpcCommon.UuidMsg, ExampleSchemas.ExampleValue, Message> someTable;
        public ScopedSnapshotProvider(CorfuStore corfuStore) {
            super(corfuStore);
            Table<RpcCommon.UuidMsg, ExampleSchemas.ExampleValue, Message> someTableLcl = null;
            try {
                someTableLcl = corfuStore.openTable(someNamespace,
                        "testSnapshotProvider",
                        RpcCommon.UuidMsg.class,
                        ExampleSchemas.ExampleValue.class, null,
                        TableOptions.fromProtoSchema(ExampleSchemas.ExampleValue.class));
            } catch (Exception e) {
                assertThat(e).isNull();
            }
            this.someTable = someTableLcl;
            // Now insert some data into it
            try (TxnContext tx = corfuStore.txn(someNamespace)) {
                for (int i = 0; i < numFullSyncBatches; i++) {
                    tx.putRecord(someTable, RpcCommon.UuidMsg.newBuilder().setMsb(i).build(),
                            ExampleSchemas.ExampleValue.newBuilder()
                                    .setPayload("opaque_payload" + i)
                                    .build(), null);
                }
                tx.commit();
            }
        }

        @Override
        public void provideFullStateData(LRFullStateReplicationContext context) {
            for (int i = 0; i < numFullSyncBatches; i++) {
                try (TxnContext tx = corfuStore.txn(someNamespace)) {
                    if (context.getSnapshot() == null) {
                        context.setSnapshot(
                            CorfuStore.snapshotFederatedTables(someNamespace, corfuStore.getRuntime()));
                    }
                    CorfuStoreEntry<RpcCommon.UuidMsg, ExampleSchemas.ExampleValue, Message> entry =
                        context.getSnapshot().getRecord(someTable, RpcCommon.UuidMsg.newBuilder().setMsb(i).build());
                    Queue.RoutingTableEntryMsg message = Queue.RoutingTableEntryMsg.newBuilder()
                            .addDestinations(context.getDestinationSiteId())
                            .setOpaquePayload(ByteString.copyFromUtf8(entry.getPayload().getPayload()))
                            .setReplicationType(Queue.ReplicationType.SNAPSHOT_SYNC)
                            .buildPartial();
                    context.transmit(message);
                    log.info("Transmitting full sync message{}", message);
                    // For debugging Q's stream id should be "61d2fc0f-315a-3d87-a982-24fb36932050"
                    log.info("FS Committed at {}", tx.commit());
                }
            }
            context.markCompleted();
            assertThat(context.getSnapshot()).isNull();
            super.isSnapshotSent = true;
        }
    }

    /**
     * Open replication status table on each Sink for verify replication status.
     */
    private void openLogReplicationStatusTable() throws Exception {
        for (int i = 0; i < numSource; i++) {
            sourceCorfuStores.get(i).openTable(
                LogReplicationMetadataManager.NAMESPACE,
                REPLICATION_STATUS_TABLE_NAME,
                LogReplication.LogReplicationSession.class,
                LogReplication.ReplicationStatus.class,
                null,
                TableOptions.fromProtoSchema(LogReplication.ReplicationStatus.class)
            );
        }
    }

    static class RoutingQueueListener extends LiteRoutingQueueListener {
        public volatile int logEntryMsgCnt = 0;
        public volatile int snapSyncMsgCnt = 0;

        public RoutingQueueListener(CorfuStore corfuStore, String sourceSiteId, String clientName) {
            super(corfuStore, sourceSiteId, clientName); // performFullSync is called here
        }

        @Override
        protected boolean processUpdatesInSnapshotSync(List<Queue.RoutingTableEntryMsg> updates) {
            log.info("RQListener got {} FULL_SYNC updates from site {}", updates.size(), sourceSiteId);
            snapSyncMsgCnt += updates.size();

            updates.forEach(u -> {
                log.info("RQListener got FULL_SYNC update {} from {}", u, sourceSiteId);
            });
            return true;
        }

        @Override
        protected boolean processUpdatesInLogEntrySync(List<Queue.RoutingTableEntryMsg> updates) {
            log.info("RQListener:: got LOG_ENTRY {} updates from site {}", updates.size(), sourceSiteId);
            logEntryMsgCnt += updates.size();
            updates.forEach(u -> {
                log.info("LitQListener:logEntrySync update {} from {}", u, sourceSiteId);
            });
            return true;
        }

        @Override
        protected void onSnapshotSyncComplete() {
            log.info("Snapshot Sync Completed");
        }
    }

    static class RoutingQSiteDiscoverer extends LRSiteDiscoveryListener {
        private final String clientName;
        public final Map<String, RoutingQueueListener> iGot = new HashMap<>();

        public RoutingQSiteDiscoverer(CorfuStore corfuStore, LogReplication.ReplicationModel replicationModel,
                                      String clientName) {
            super(corfuStore, replicationModel, clientName);
            this.clientName = clientName;
        }

        @Override
        public void onNewSiteUp(String siteId) {
            if (!iGot.containsKey(siteId)) {
                iGot.put(siteId, new RoutingQueueListener(corfuStore, siteId, clientName));
            }
        }
    }
}
