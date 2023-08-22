package org.corfudb.integration;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.browser.CorfuStoreBrowserEditor;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterConfig;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterManager;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.runtime.CorfuOptions;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.ExampleSchemas;
import org.corfudb.runtime.LRFullStateReplicationContext;
import org.corfudb.runtime.LRSiteDiscoveryListener;
import org.corfudb.runtime.LiteRoutingQueueListener;
import org.corfudb.runtime.LogReplication;
import org.corfudb.runtime.Queue;
import org.corfudb.runtime.RoutingQueueSenderClient;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStoreEntry;
import org.corfudb.runtime.collections.IsolationLevel;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.object.transactions.AbstractTransactionalContext;
import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.corfudb.runtime.proto.RpcCommon;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.runtime.LogReplicationUtils.LOG_ENTRY_SYNC_QUEUE_TAG_SENDER_PREFIX;
import static org.corfudb.runtime.LogReplicationUtils.REPLICATED_RECV_Q_PREFIX;
import static org.corfudb.runtime.LogReplicationUtils.REPLICATED_QUEUE_TAG;
import static org.corfudb.runtime.LogReplicationUtils.REPLICATION_STATUS_TABLE_NAME;
import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

@SuppressWarnings("checkstyle:magicnumber")
@Slf4j
public class LogReplicationRoutingQueueIT extends CorfuReplicationMultiSourceSinkIT {

    private int numSource = 1;

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

    @Test
    public void testRoutingQueueReplication() throws Exception {
        // Register client and setup initial group destinations mapping
        CorfuRuntime clientRuntime = getClientRuntime();
        CorfuStore clientCorfuStore = new CorfuStore(clientRuntime);
        String clientName = "testClient";
        String sourceSiteId = DefaultClusterConfig.getSourceClusterIds().get(0);
        RoutingQueueSenderClient queueSenderClient = new RoutingQueueSenderClient(clientCorfuStore, clientName);
        // SnapshotProvider implements RoutingQueueSenderClient.LRTransmitterReplicationModule
        SnapshotProvider snapshotProvider = new SnapshotProvider(clientCorfuStore);
        queueSenderClient.startLRSnapshotTransmitter(snapshotProvider); // starts a listener on event table

        try {
            RoutingQueueListener listener = new RoutingQueueListener(sinkCorfuStores.get(0),
                    DefaultClusterConfig.getSourceClusterIds().get(0));
            sinkCorfuStores.get(0).subscribeRoutingQListener(listener);
            startReplicationServers();
            while (!snapshotProvider.isSnapshotSent) {
                Thread.sleep(5000);
            }
            generateData(clientCorfuStore, queueSenderClient);

            RoutingQueueListener listener = new RoutingQueueListener(sinkCorfuStores.get(0));
            sinkCorfuStores.get(0).subscribeRoutingQListener(listener);

            int numLogEntriesReceived = listener.logEntryMsgCnt;
            while (numLogEntriesReceived < 10) {
                Thread.sleep(5000);
                numLogEntriesReceived = listener.logEntryMsgCnt;
                log.info("Entries got on receiver {}", numLogEntriesReceived);
            }

            log.info("Expected num entries on the Sink Received");
            assertThat(listener.logEntryMsgCnt).isEqualTo(10);
            assertThat(listener.snapSyncMsgCnt).isEqualTo(5);
            log.info("Sink replicated queue size: {}", listener.snapSyncMsgCnt);
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testRoutingQueueFullSyncs() throws Exception {

        // Register client and setup initial group destinations mapping
        CorfuRuntime clientRuntime = getClientRuntime();
        CorfuStore clientCorfuStore = new CorfuStore(clientRuntime);
        String sourceSiteId = DefaultClusterConfig.getSourceClusterIds().get(0);
        CorfuStoreBrowserEditor editor = new CorfuStoreBrowserEditor(clientRuntime, null, true);
        String clientName = RoutingQueueSenderClient.DEFAULT_ROUTING_QUEUE_CLIENT;
        RoutingQueueSenderClient queueSenderClient = new RoutingQueueSenderClient(clientCorfuStore, clientName);
        // SnapshotProvider implements RoutingQueueSenderClient.LRTransmitterReplicationModule
        SnapshotProvider snapshotProvider = new SnapshotProvider(clientCorfuStore);
        queueSenderClient.startLRSnapshotTransmitter(snapshotProvider); // starts a listener on event table

        // Open queue on sink
        try {
            log.info("Sink Queue name: {}", REPLICATED_RECV_Q_PREFIX+sourceSiteId);
            Table<Queue.CorfuGuidMsg, Queue.RoutingTableEntryMsg, Queue.CorfuQueueMetadataMsg> replicatedQueueSink
                    = sinkCorfuStores.get(0).openQueue(CORFU_SYSTEM_NAMESPACE,
                    REPLICATED_RECV_Q_PREFIX+sourceSiteId,
                    Queue.RoutingTableEntryMsg.class, TableOptions.builder().schemaOptions(CorfuOptions.SchemaOptions.newBuilder()
                            .addStreamTag(REPLICATED_QUEUE_TAG).build()).build());

            startReplicationServers();
            while (!snapshotProvider.isSnapshotSent) {
                Thread.sleep(5000);
            }

            RoutingQueueListener listener = new RoutingQueueListener(sinkCorfuStores.get(0),
                    sourceSiteId);
            sinkCorfuStores.get(0).subscribeRoutingQListener(listener);

            int numFullSyncMsgsGot = listener.snapSyncMsgCnt;
            while (numFullSyncMsgsGot < 5) {
                Thread.sleep(5000);
                numFullSyncMsgsGot = listener.snapSyncMsgCnt;
                log.info("Entries got on receiver {}", numFullSyncMsgsGot);
            }
            assertThat(listener.snapSyncMsgCnt).isEqualTo(5);

            // Now request a full sync again for all sites!
            snapshotProvider.isSnapshotSent = false;
            editor.requestGlobalFullSync();

            while (numFullSyncMsgsGot < 10) {
                Thread.sleep(5000);
                numFullSyncMsgsGot = listener.snapSyncMsgCnt;
                log.info("Entries got on receiver after 2nd full sync {}", numFullSyncMsgsGot);
            }
            assertThat(listener.snapSyncMsgCnt).isEqualTo(10);

            // Now request a full sync this time for just one site!
            snapshotProvider.isSnapshotSent = false;
            queueSenderClient.requestSnapshotSync(UUID.fromString(DefaultClusterConfig.getSourceClusterIds().get(0)),
                    UUID.fromString(DefaultClusterConfig.getSinkClusterIds().get(0)));

            while (numFullSyncMsgsGot < 15) {
                Thread.sleep(5000);
                numFullSyncMsgsGot = listener.snapSyncMsgCnt;
                log.info("Entries got on receiver after 3rd full sync {}", numFullSyncMsgsGot);
            }
            assertThat(listener.snapSyncMsgCnt).isEqualTo(15);
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testRoutingQueue2way() throws Exception {

        // Register client and setup initial group destinations mapping
        CorfuRuntime clientRuntime = getClientRuntime();
        CorfuStore clientCorfuStore = new CorfuStore(clientRuntime);
        String sourceSiteId = DefaultClusterConfig.getSourceClusterIds().get(0);
        final String clientName = RoutingQueueSenderClient.DEFAULT_ROUTING_QUEUE_CLIENT;

        RoutingQueueSenderClient queueSenderClient = new RoutingQueueSenderClient(clientCorfuStore, clientName);
        // SnapshotProvider implements RoutingQueueSenderClient.LRTransmitterReplicationModule
        SnapshotProvider snapshotProvider = new SnapshotProvider(clientCorfuStore);
        queueSenderClient.startLRSnapshotTransmitter(snapshotProvider); // starts a listener on event table

        // Also start a Site Discovery Service on the Source Cluster that listens for new sites & starts up
        // RoutingQueueListeners when a sink comes up
        RoutingQSiteDiscoverer remoteSiteDiscoverer = new RoutingQSiteDiscoverer(clientCorfuStore,
                LogReplication.ReplicationModel.ROUTING_QUEUES);

        // Start up Full Sync providers on both sink sites to test reverse replication
        sinkCorfuStores.forEach(sinkCorfuStore -> {
            RoutingQueueSenderClient sinkSideQsender = null;
            try {
                sinkSideQsender = new RoutingQueueSenderClient(sinkCorfuStore, clientName);
            } catch (NoSuchMethodException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }
            // SnapshotProvider implements RoutingQueueSenderClient.LRTransmitterReplicationModule
            SnapshotProvider sinkSideSnapProvider = new SnapshotProvider(sinkCorfuStore);
            sinkSideQsender.startLRSnapshotTransmitter(sinkSideSnapProvider); // starts a listener on event table
        });

        // Open queue on sink
        try {
            log.info("Sink Queue name: {}", REPLICATED_RECV_Q_PREFIX+sourceSiteId);
            Table<Queue.CorfuGuidMsg, Queue.RoutingTableEntryMsg, Queue.CorfuQueueMetadataMsg> replicatedQueueSink
                    = sinkCorfuStores.get(0).openQueue(CORFU_SYSTEM_NAMESPACE,
                    REPLICATED_RECV_Q_PREFIX+sourceSiteId,
                    Queue.RoutingTableEntryMsg.class, TableOptions.builder().schemaOptions(CorfuOptions.SchemaOptions.newBuilder()
                            .addStreamTag(REPLICATED_QUEUE_TAG).build()).build());

            startReplicationServers();
            while (!snapshotProvider.isSnapshotSent) {
                Thread.sleep(5000);
            }

            // Test forward replication source -> sink
            RoutingQueueListener listener = new RoutingQueueListener(sinkCorfuStores.get(0),
                    sourceSiteId);
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
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private void generateData(CorfuStore corfuStore, RoutingQueueSenderClient client) throws Exception {
        String namespace = CORFU_SYSTEM_NAMESPACE;

        String streamTagFollowed =
            LOG_ENTRY_SYNC_QUEUE_TAG_SENDER_PREFIX + DefaultClusterConfig.getSinkClusterIds().get(0);
        log.info("Stream UUID: {}", TableRegistry.getStreamIdForStreamTag(CORFU_SYSTEM_NAMESPACE, streamTagFollowed));

        for (int i = 0; i < 10; i++) {
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

    @Test
    public void testRoutingQueueReplicationWithScopedTxn() throws Exception {
        // Register client and setup initial group destinations mapping
        CorfuRuntime clientRuntime = getClientRuntime();
        CorfuStore clientCorfuStore = new CorfuStore(clientRuntime);
        String clientName = "testClient";
        String sourceSiteId = DefaultClusterConfig.getSourceClusterIds().get(0);
        RoutingQueueSenderClient queueSenderClient = new RoutingQueueSenderClient(clientCorfuStore, clientName);
        // SnapshotProvider implements RoutingQueueSenderClient.LRTransmitterReplicationModule
        ScopedSnapshotProvider snapshotProvider = new ScopedSnapshotProvider(clientCorfuStore);
        queueSenderClient.startLRSnapshotTransmitter(snapshotProvider); // starts a listener on event table

        try {
            RoutingQueueListener listener = new RoutingQueueListener(sinkCorfuStores.get(0),
                    DefaultClusterConfig.getSourceClusterIds().get(0));
            sinkCorfuStores.get(0).subscribeRoutingQListener(listener);
            startReplicationServers();
            while (!snapshotProvider.isSnapshotSent) {
                Thread.sleep(5000);
            }
            generateData(clientCorfuStore, queueSenderClient);

            int numLogEntriesReceived = listener.logEntryMsgCnt;
            while (numLogEntriesReceived < 10) {
                Thread.sleep(5000);
                numLogEntriesReceived = listener.logEntryMsgCnt;
                log.info("Entries got on receiver {}", numLogEntriesReceived);
            }

            log.info("Expected num entries on the Sink Received");
            assertThat(listener.logEntryMsgCnt).isEqualTo(10);
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
        public final int numFullSyncBatches = 5;
        private int recordId = 0;
        CorfuStore corfuStore;
        public boolean isSnapshotSent = false;
        SnapshotProvider(CorfuStore corfuStore) {
            this.corfuStore = corfuStore;
        }

        @Override
        public void provideFullStateData(LRFullStateReplicationContext context) {
            for (int i = 0; i < numFullSyncBatches; i++) {
                try (TxnContext tx = corfuStore.txn(someNamespace)) {
                    Queue.RoutingTableEntryMsg message = Queue.RoutingTableEntryMsg.newBuilder()
                            .addDestinations(context.getDestinationSiteId())
                            .setOpaquePayload(ByteString.copyFromUtf8("opaquetxn"+recordId))
                            .buildPartial();
                    recordId++;
                    context.transmit(message);
                    log.info("Transmitting full sync message{}", message);
                    // For debugging Q's stream id should be "61d2fc0f-315a-3d87-a982-24fb36932050"
                    AbstractTransactionalContext txCtx = TransactionalContext.getRootContext();
                    log.info("FS Committed at {}", tx.commit());
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
            Table<RpcCommon.UuidMsg, ExampleSchemas.ExampleValue, Message> someTable_lcl = null;
            try {
                someTable_lcl = corfuStore.openTable(someNamespace,
                        "testSnapshotProvider",
                        RpcCommon.UuidMsg.class,
                        ExampleSchemas.ExampleValue.class, null,
                        TableOptions.fromProtoSchema(ExampleSchemas.ExampleValue.class));
            } catch (Exception e) {
                assertThat(e).isNull();
            }
            this.someTable = someTable_lcl;
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
            RpcCommon.UuidMsg key = RpcCommon.UuidMsg.newBuilder()
                    .setMsb(0).setLsb(numFullSyncBatches).build();
            for (int i = 0; i < numFullSyncBatches; i++) {
                try (TxnContext tx = corfuStore.txn(someNamespace)) {
                    if (context.getSnapshot() == null) {
                        context.setSnapshot(
                                corfuStore.scopedTxn(someNamespace, IsolationLevel.snapshot(),
                                        (Table<?, ?, ?>) corfuStore.getRuntime().getTableRegistry().getAllOpenTables())
                        );
                    }
                    CorfuStoreEntry<RpcCommon.UuidMsg, ExampleSchemas.ExampleValue, Message> record = context.getSnapshot()
                            .getRecord(someTable, RpcCommon.UuidMsg.newBuilder().setMsb(i).build());
                    Queue.RoutingTableEntryMsg message = Queue.RoutingTableEntryMsg.newBuilder()
                            .addDestinations(context.getDestinationSiteId())
                            .setOpaquePayload(ByteString.copyFromUtf8(record.getPayload().getPayload()))
                            .buildPartial();
                    context.transmit(message);
                    log.info("Transmitting full sync message{}", message);
                    // For debugging Q's stream id should be "61d2fc0f-315a-3d87-a982-24fb36932050"
                    AbstractTransactionalContext txCtx = TransactionalContext.getRootContext();
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

        public RoutingQueueListener(CorfuStore corfuStore, String sourceSiteId) {
            super(corfuStore, sourceSiteId); // performFullSync is called here
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
    }

    static class RoutingQSiteDiscoverer extends LRSiteDiscoveryListener {
        public RoutingQSiteDiscoverer(CorfuStore corfuStore, LogReplication.ReplicationModel replicationModel) {
            super(corfuStore, replicationModel);
        }
        public final Map<String, RoutingQueueListener> iGot = new HashMap<>();

        @Override
        public void onNewSiteUp(String siteId) {
            if (!iGot.containsKey(siteId)) {
                iGot.put(siteId, new RoutingQueueListener(corfuStore, siteId));
            }
        }
    }
}
