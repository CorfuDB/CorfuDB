package org.corfudb.integration;

import com.google.protobuf.ByteString;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterConfig;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterManager;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationConfigManager;
import org.corfudb.runtime.CorfuOptions;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LRFullStateReplicationContext;
import org.corfudb.runtime.LiteRoutingQueueListener;
import org.corfudb.runtime.LogReplication;
import org.corfudb.runtime.Queue;
import org.corfudb.runtime.Queue.RoutingTableEntryMsg;
import org.corfudb.runtime.RoutingQueueSenderClient;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.object.transactions.AbstractTransactionalContext;
import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.corfudb.runtime.view.TableRegistry;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.integration.CorfuReplicationClusterConfigIT.checkpointAndTrimCorfuStore;
import static org.corfudb.runtime.LogReplicationUtils.LOG_ENTRY_SYNC_QUEUE_NAME_SENDER;
import static org.corfudb.runtime.LogReplicationUtils.LOG_ENTRY_SYNC_QUEUE_TAG_SENDER_PREFIX;
import static org.corfudb.runtime.LogReplicationUtils.REPLICATED_QUEUE_NAME;
import static org.corfudb.runtime.LogReplicationUtils.REPLICATED_QUEUE_TAG;
import static org.corfudb.runtime.LogReplicationUtils.REPLICATION_STATUS_TABLE_NAME;
import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

@SuppressWarnings("checkstyle:magicnumber")
@Slf4j
public class LogReplicationRoutingQueueIT extends CorfuReplicationMultiSourceSinkIT {
    private final int numSource = 1;
    private final int numSink = 1;
    private final int SINK1_INDEX = 0;
    private final int SINK2_INDEX = 1;
    private final int SOURCE_INDEX = 0;
    private final int generateDataNumEntries = 10;
    private final int numFullSyncEntries = 5;

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
        super.setUp(numSource, numSink, DefaultClusterManager.TP_SINGLE_SOURCE_SINK_ROUTING_QUEUE);
    }

    @Test
    public void testRoutingQueueReplicationWithFullSyncData() throws Exception {
        testRoutingQueueReplication(numFullSyncEntries);
    }

    @Test
    public void testRoutingQueueReplicationNoFullSyncData() throws Exception {
        int zeroEntries = 0;
        testRoutingQueueReplication(zeroEntries);
    }

    private void testRoutingQueueReplication(int numFullSyncEntries) throws Exception {

        // Register client and setup initial group destinations mapping
        CorfuRuntime clientRuntime = getClientRuntime();
        CorfuStore clientCorfuStore = new CorfuStore(clientRuntime);
        String clientName = "testClient";
        RoutingQueueSenderClient queueSenderClient = new RoutingQueueSenderClient(clientCorfuStore, clientName);
        // SnapshotProvider implements RoutingQueueSenderClient.LRTransmitterReplicationModule
        SnapshotProvider snapshotProvider = new SnapshotProvider(clientCorfuStore);
        snapshotProvider.setNumFullSyncBatches(numFullSyncEntries);
        queueSenderClient.startLRSnapshotTransmitter(snapshotProvider); // starts a listener on event table

        // Open queue on sink
        try {
            log.info("Sink Queue name: {}", REPLICATED_QUEUE_NAME);
            Table<Queue.CorfuGuidMsg, Queue.RoutingTableEntryMsg, Queue.CorfuQueueMetadataMsg> replicatedQueueSink
                    = sinkCorfuStores.get(0).openQueue(CORFU_SYSTEM_NAMESPACE, REPLICATED_QUEUE_NAME,
                    Queue.RoutingTableEntryMsg.class, TableOptions.builder().schemaOptions(CorfuOptions.SchemaOptions.newBuilder()
                            .addStreamTag(REPLICATED_QUEUE_TAG).build()).build());

            startReplicationServers();

            // Wait until snapshot data is sent
            while (!snapshotProvider.isSnapshotSent) {
                Thread.sleep(5000);
            }

            // Wait for transition to log entry sync
            openLogReplicationStatusTable(clientCorfuStore);
            verifySessionInLogEntrySyncState(clientCorfuStore, SINK1_INDEX, LogReplicationConfigManager.getDefaultRoutingQueueSubscriber());

            RoutingQueueListener listener = new RoutingQueueListener(sinkCorfuStores.get(SINK1_INDEX));
            sinkCorfuStores.get(SINK1_INDEX).subscribeRoutingQListener(listener);

            generateData(clientCorfuStore, queueSenderClient);

            int numLogEntriesReceived = listener.logEntryMsgCnt;
            while (numLogEntriesReceived < generateDataNumEntries) {
                Thread.sleep(5000);
                numLogEntriesReceived = listener.logEntryMsgCnt;
                log.info("Entries got on receiver {}", numLogEntriesReceived);
            }

            log.info("Expected num entries on the Sink Received");
            assertThat(listener.logEntryMsgCnt).isEqualTo(generateDataNumEntries);
            assertThat(listener.snapSyncMsgCnt).isEqualTo(snapshotProvider.getNumFullSyncBatches());
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
        String clientName = RoutingQueueSenderClient.DEFAULT_ROUTING_QUEUE_CLIENT;
        RoutingQueueSenderClient queueSenderClient = new RoutingQueueSenderClient(clientCorfuStore, clientName);
        // SnapshotProvider implements RoutingQueueSenderClient.LRTransmitterReplicationModule
        SnapshotProvider snapshotProvider = new SnapshotProvider(clientCorfuStore);
        snapshotProvider.setNumFullSyncBatches(numFullSyncEntries);
        queueSenderClient.startLRSnapshotTransmitter(snapshotProvider); // starts a listener on event table

        // Open queue on sink
        try {
            log.info("Sink Queue name: {}", REPLICATED_QUEUE_NAME);
            Table<Queue.CorfuGuidMsg, Queue.RoutingTableEntryMsg, Queue.CorfuQueueMetadataMsg> replicatedQueueSink
                    = sinkCorfuStores.get(SINK1_INDEX).openQueue(CORFU_SYSTEM_NAMESPACE, REPLICATED_QUEUE_NAME,
                    Queue.RoutingTableEntryMsg.class, TableOptions.builder().schemaOptions(CorfuOptions.SchemaOptions.newBuilder()
                            .addStreamTag(REPLICATED_QUEUE_TAG).build()).build());

            startReplicationServers();
            while (!snapshotProvider.isSnapshotSent) {
                Thread.sleep(5000);
            }

            RoutingQueueListener listener = new RoutingQueueListener(sinkCorfuStores.get(SINK1_INDEX));
            sinkCorfuStores.get(SINK1_INDEX).subscribeRoutingQListener(listener);

            int numFullSyncMsgsGot = listener.snapSyncMsgCnt;
            while (numFullSyncMsgsGot < 5) {
                Thread.sleep(5000);
                numFullSyncMsgsGot = listener.snapSyncMsgCnt;
                log.info("Entries got on receiver {}", numFullSyncMsgsGot);
            }
            assertThat(listener.snapSyncMsgCnt).isEqualTo(5);

            // Now request a full sync again for all sites!
            snapshotProvider.isSnapshotSent = false;
            queueSenderClient.requestGlobalSnapshotSync(UUID.fromString(DefaultClusterConfig.getSourceClusterIds().get(SOURCE_INDEX)),
                    UUID.fromString(DefaultClusterConfig.getSinkClusterIds().get(SINK1_INDEX)));

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
    public void testRoutingQueueReplicationWithCpAndTrim() throws Exception {

        // Register client and setup initial group destinations mapping
        CorfuRuntime clientRuntime = getClientRuntime();
        CorfuStore clientCorfuStore = new CorfuStore(clientRuntime);
        String clientName = "testClient";
        RoutingQueueSenderClient queueSenderClient = new RoutingQueueSenderClient(clientCorfuStore, clientName);

        // SnapshotProvider implements RoutingQueueSenderClient.LRTransmitterReplicationModule
        SnapshotProvider snapshotProvider = new SnapshotProvider(clientCorfuStore);
        snapshotProvider.setNumFullSyncBatches(numFullSyncEntries);
        queueSenderClient.startLRSnapshotTransmitter(snapshotProvider); // starts a listener on event table

        // Open queue on sink
        try {
            log.info("Sink Queue name: {}", REPLICATED_QUEUE_NAME);
            Table<Queue.CorfuGuidMsg, Queue.RoutingTableEntryMsg, Queue.CorfuQueueMetadataMsg> replicatedQueueSink
                    = sinkCorfuStores.get(0).openQueue(CORFU_SYSTEM_NAMESPACE, REPLICATED_QUEUE_NAME,
                    Queue.RoutingTableEntryMsg.class, TableOptions.builder().schemaOptions(CorfuOptions.SchemaOptions.newBuilder()
                            .addStreamTag(REPLICATED_QUEUE_TAG).build()).build());

            RoutingQueueListener listener = new RoutingQueueListener(sinkCorfuStores.get(SINK1_INDEX));
            sinkCorfuStores.get(SINK1_INDEX).subscribeRoutingQListener(listener);

            startReplicationServers();

            // Wait until snapshot data is sent
            while (!snapshotProvider.isSnapshotSent) {
                Thread.sleep(5000);
            }

            // Wait for transition to log entry sync
            openLogReplicationStatusTable(clientCorfuStore);
            verifySessionInLogEntrySyncState(clientCorfuStore, SINK1_INDEX, LogReplicationConfigManager.getDefaultRoutingQueueSubscriber());

            log.info("Expected number of entries are received in sink");
            assertThat(listener.snapSyncMsgCnt).isEqualTo(snapshotProvider.getNumFullSyncBatches());

            // Stop Source replication server
            stopReplicationServer(sourceReplicationPorts.get(SOURCE_INDEX), sourceReplicationServers.get(SOURCE_INDEX));
            sourceReplicationServers.clear();

            // Stop Sink Replication server
            stopReplicationServer(sinkReplicationPorts.get(SINK1_INDEX), sinkReplicationServers.get(SINK1_INDEX));
            sinkReplicationServers.clear();

            // Checkpoint & trim on the Sink such that shadow streams are trimmed
            checkpointAndTrimCorfuStore(createRuntimeWithCache(sinkEndpoints.get(SINK1_INDEX)));

            // Write data to source queue
            generateData(clientCorfuStore, queueSenderClient);

            Table<Queue.CorfuGuidMsg, RoutingTableEntryMsg, Queue.CorfuQueueMetadataMsg> logEntryQ =
                    clientCorfuStore.openQueue(CORFU_SYSTEM_NAMESPACE, LOG_ENTRY_SYNC_QUEUE_NAME_SENDER,
                            RoutingTableEntryMsg.class, TableOptions.fromProtoSchema(RoutingTableEntryMsg.class));

            // Verify data exists on source
            int logEntryQSize = logEntryQ.count();
            while (logEntryQSize != generateDataNumEntries) {
                Thread.sleep(5000);
                logEntryQSize = logEntryQ.count();
                log.info("Source delta queue size: {}", logEntryQSize);
            }

            // Verify new data does not exist on sink
            assertThat(replicatedQueueSink.count()).isLessThanOrEqualTo(snapshotProvider.getNumFullSyncBatches());
            log.info("Sink replicated queue size: {}", replicatedQueueSink.count());

            // Start source and sink replication servers again
            log.info("Starting source and sink servers...");
            startReplicationServers();

            // Delta messages should be transferred after recovery from prior snapshot
            int numLogEntriesReceived = listener.logEntryMsgCnt;
            while (numLogEntriesReceived < generateDataNumEntries) {
                Thread.sleep(5000);
                numLogEntriesReceived = listener.logEntryMsgCnt;
                log.info("Entries got on receiver {}", numLogEntriesReceived);
            }
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private void verifySessionInLogEntrySyncState(CorfuStore corfuStore, int sinkIndex, LogReplication.ReplicationSubscriber subscriber) {
        LogReplication.LogReplicationSession session = LogReplication.LogReplicationSession.newBuilder()
                .setSourceClusterId(DefaultClusterConfig.getSourceClusterIds().get(SOURCE_INDEX))
                .setSinkClusterId(DefaultClusterConfig.getSinkClusterIds().get(sinkIndex))
                .setSubscriber(subscriber)
                .build();

        LogReplication.ReplicationStatus status = null;

        while (status == null || !status.getSourceStatus().getReplicationInfo().getSyncType().equals(LogReplication.SyncType.LOG_ENTRY)
                || !status.getSourceStatus().getReplicationInfo().getSnapshotSyncInfo().getStatus()
                .equals(LogReplication.SyncStatus.COMPLETED)) {
            try (TxnContext txn = corfuStore.txn(LogReplicationMetadataManager.NAMESPACE)) {
                status = (LogReplication.ReplicationStatus) txn.getRecord(REPLICATION_STATUS_TABLE_NAME, session).getPayload();
                txn.commit();
            }
        }

        // Snapshot sync should have completed and log entry sync is ongoing
        assertThat(status.getSourceStatus().getReplicationInfo().getSyncType()).isEqualTo(LogReplication.SyncType.LOG_ENTRY);
        assertThat(status.getSourceStatus().getReplicationInfo().getStatus())
                .isEqualTo(LogReplication.SyncStatus.ONGOING);

        assertThat(status.getSourceStatus().getReplicationInfo().getSnapshotSyncInfo().getType())
                .isEqualTo(LogReplication.SnapshotSyncInfo.SnapshotSyncType.DEFAULT);
        assertThat(status.getSourceStatus().getReplicationInfo().getSnapshotSyncInfo().getStatus())
                .isEqualTo(LogReplication.SyncStatus.COMPLETED);
    }

    private void generateData(CorfuStore corfuStore, RoutingQueueSenderClient client) throws Exception {
        String namespace = CORFU_SYSTEM_NAMESPACE;

        String streamTagFollowed =
            LOG_ENTRY_SYNC_QUEUE_TAG_SENDER_PREFIX + DefaultClusterConfig.getSinkClusterIds().get(SINK1_INDEX);
        log.info("Stream UUID: {}", TableRegistry.getStreamIdForStreamTag(CORFU_SYSTEM_NAMESPACE, streamTagFollowed));

        for (int i = 0; i < generateDataNumEntries; i++) {
            ByteBuffer buffer = ByteBuffer.allocate(Integer.SIZE);
            buffer.putInt(i);
            Queue.RoutingTableEntryMsg val =
                Queue.RoutingTableEntryMsg.newBuilder()
                        .setSourceClusterId(DefaultClusterConfig.getSourceClusterIds().get(SOURCE_INDEX))
                        .addAllDestinations(Arrays.asList(DefaultClusterConfig.getSinkClusterIds().get(SINK1_INDEX),
                                DefaultClusterConfig.getSinkClusterIds().get(SINK2_INDEX)))
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
     * Provide a full sync or snapshot
     */
    public static class SnapshotProvider implements RoutingQueueSenderClient.LRTransmitterReplicationModule {

        public final String someNamespace = "testNamespace";
        @Getter
        @Setter
        private int numFullSyncBatches;
        private int recordId = 0;
        CorfuStore corfuStore;
        public boolean isSnapshotSent = false;
        SnapshotProvider(CorfuStore corfuStore) {
            this.corfuStore = corfuStore;
        }

        @Override
        public void provideFullStateData(LRFullStateReplicationContext context) {
            log.info("Providing {} number of full sync batches.", numFullSyncBatches);
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

    /**
     * Open replication status table on each Sink for verify replication status.
     */
    private void openLogReplicationStatusTable(CorfuStore corfuStore) throws Exception {
        for (int i = 0; i < numSource; i++) {
            corfuStore.openTable(
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

        public RoutingQueueListener(CorfuStore corfuStore) {
            super(corfuStore); // performFullSync is called here
        }

        @Override
        protected boolean processUpdatesInSnapshotSync(List<Queue.RoutingTableEntryMsg> updates) {
            log.info("LiteRoutingQueueListener::SnapshotSync:: got {} updates", updates.size());
            snapSyncMsgCnt += updates.size();

            updates.forEach(u -> {
                log.info("LitQListener:fullSyncMsg update {}", u);
            });
            return true;
        }

        @Override
        protected boolean processUpdatesInLogEntrySync(List<Queue.RoutingTableEntryMsg> updates) {
            log.info("LiteRoutingQueueListener::processUpdatesInLogEntrySync:: got {} updates", updates.size());
            logEntryMsgCnt += updates.size();
            updates.forEach(u -> {
                log.info("LitQListener:logEntrySync update {}", u);
            });
            return true;
        }
    }
}
