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
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.CorfuOptions;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.ExampleSchemas;
import org.corfudb.runtime.LogReplicationUtils;
import org.corfudb.runtime.collections.CorfuRecord;
import org.corfudb.runtime.collections.CorfuStreamEntries;
import org.corfudb.runtime.collections.StreamListener;
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
import org.corfudb.util.serializer.ProtobufSerializer;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager.REPLICATION_EVENT_TABLE_NAME;
import static org.corfudb.runtime.LogReplication.ReplicationEvent.ReplicationEventType.SERVER_REQUESTED_SNAPSHOT_SYNC;
import static org.corfudb.runtime.LogReplicationUtils.LOG_ENTRY_SYNC_QUEUE_NAME_SENDER;
import static org.corfudb.runtime.LogReplicationUtils.LOG_ENTRY_SYNC_QUEUE_TAG_SENDER_PREFIX;
import static org.corfudb.runtime.LogReplicationUtils.REPLICATED_RECV_Q_PREFIX;
import static org.corfudb.runtime.LogReplicationUtils.REPLICATED_QUEUE_TAG;
import static org.corfudb.runtime.LogReplicationUtils.REPLICATION_STATUS_TABLE_NAME;
import static org.corfudb.runtime.LogReplicationUtils.SNAPSHOT_SYNC_QUEUE_NAME_SENDER;
import static org.corfudb.runtime.LogReplicationUtils.SNAPSHOT_SYNC_QUEUE_TAG_SENDER_PREFIX;
import static org.corfudb.runtime.LogReplicationUtils.SNAP_SYNC_TXN_ENVELOPE_TABLE;
import static org.corfudb.runtime.LogReplicationUtils.lrSnapSyncTxnEnvelopeStreamId;
import static org.corfudb.runtime.RoutingQueueSenderClient.DEFAULT_ROUTING_QUEUE_CLIENT;
import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

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

    @Test
    public void testRoutingQueueReplication() throws Exception {
        // Register client and setup initial group destinations mapping
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
            while (!snapshotProvider.isSnapshotSent) {
                Thread.sleep(5000);
            }
            generateData(clientCorfuStore, queueSenderClient);
            int numLogEntriesReceived = listener.logEntryMsgCnt;
            while (numLogEntriesReceived < numLogEntryUpdates + 1) {
                Thread.sleep(5000);
                numLogEntriesReceived = listener.logEntryMsgCnt;
                log.info("Entries got on receiver {}", numLogEntriesReceived);
            }

            log.info("Expected num entries on the Sink Received");
            assertThat(listener.logEntryMsgCnt).isEqualTo(numLogEntryUpdates + 1);
            assertThat(listener.snapSyncMsgCnt).isEqualTo(5);
            log.info("Sink replicated queue size: {}", listener.snapSyncMsgCnt);
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            throw new LogReplicationClientException(e);
        }
    }

    @Test
    public void testRoutingQueueFullSyncs() throws Exception {

        // Register client and setup initial group destinations mapping
        CorfuRuntime clientRuntime = getClientRuntime();
        CorfuStore clientCorfuStore = new CorfuStore(clientRuntime);
        String sourceSiteId = DefaultClusterConfig.getSourceClusterIds().get(0);
        CorfuStoreBrowserEditor editor = new CorfuStoreBrowserEditor(clientRuntime, null, true);
        String clientName = DEFAULT_ROUTING_QUEUE_CLIENT;
        RoutingQueueSenderClient queueSenderClient = new RoutingQueueSenderClient(clientCorfuStore, clientName);
        // SnapshotProvider implements RoutingQueueSenderClient.LRTransmitterReplicationModule
        SnapshotProvider snapshotProvider = new SnapshotProvider(clientCorfuStore);
        queueSenderClient.startLRSnapshotTransmitter(snapshotProvider); // starts a listener on event table

        // Open queue on sink

        log.info("Sink Queue name: {}", REPLICATED_RECV_Q_PREFIX + sourceSiteId);
        startReplicationServers();
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
        queueSenderClient.requestSnapshotSync(DefaultClusterConfig.getSourceClusterIds().get(0),
            DefaultClusterConfig.getSinkClusterIds().get(0));

        while (numFullSyncMsgsGot < 15) {
            Thread.sleep(5000);
            numFullSyncMsgsGot = listener.snapSyncMsgCnt;
            log.info("Entries got on receiver after 3rd full sync {}", numFullSyncMsgsGot);
        }
        assertThat(listener.snapSyncMsgCnt).isEqualTo(15);

    }

    @Test
    public void testRoutingQueue2way() throws Exception {

        // Register client and setup initial group destinations mapping
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

        // Start up Full Sync providers on both sink sites to test reverse replication
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

    @Test
    public void testSnapshotSyncCancel() throws Exception {

        // Register client and setup initial group destinations mapping
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

    private void generateData(CorfuStore corfuStore, RoutingQueueSenderClient client) throws Exception {
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

    @Test
    public void testRoutingQueueReplicationWithScopedTxn() throws Exception {
        // Register client and setup initial group destinations mapping
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
            generateData(clientCorfuStore, queueSenderClient);

            int numLogEntriesReceived = listener.logEntryMsgCnt;
            while (numLogEntriesReceived < 10) {
                Thread.sleep(5000);
                numLogEntriesReceived = listener.logEntryMsgCnt;
                log.info("Entries got on receiver {}", numLogEntriesReceived);
            }

            log.info("Expected num entries on the Sink Received");
            assertThat(listener.logEntryMsgCnt).isEqualTo(numLogEntryUpdates + 1);
            assertThat(listener.snapSyncMsgCnt).isEqualTo(5);
            log.info("Sink replicated queue size: {}", listener.snapSyncMsgCnt);
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Test the order of the records on the SINK side using the entryList operation.
     * This is effectively testing the order on cp/trim, even though the test is not really simulating a cp/trim. The
     * reason being that the entryList operation is what will be used on an event of cp/trim.
     */
    @Test
    public void testOrderOfDataOnReceiveQueue() throws Exception {
        CorfuRuntime runtime = getClientRuntime();
        CorfuStore corfuStore = new CorfuStore(runtime);

        //open snapshot and delta queues on source
        Table<Queue.CorfuGuidMsg, Queue.RoutingTableEntryMsg, Queue.CorfuQueueMetadataMsg> logEntryQ;
        Table<Queue.CorfuGuidMsg, Queue.RoutingTableEntryMsg, Queue.CorfuQueueMetadataMsg> snapSyncQ;

        try {
            logEntryQ = corfuStore.openQueue(CORFU_SYSTEM_NAMESPACE, LOG_ENTRY_SYNC_QUEUE_NAME_SENDER,
                    Queue.RoutingTableEntryMsg.class, TableOptions.fromProtoSchema(Queue.RoutingTableEntryMsg.class));
            snapSyncQ = corfuStore.openQueue(CORFU_SYSTEM_NAMESPACE, SNAPSHOT_SYNC_QUEUE_NAME_SENDER,
                    Queue.RoutingTableEntryMsg.class, TableOptions.fromProtoSchema(Queue.RoutingTableEntryMsg.class));
            corfuStore.openTable(CORFU_SYSTEM_NAMESPACE, SNAP_SYNC_TXN_ENVELOPE_TABLE,
                    Queue.RoutingQSnapSyncHeaderKeyMsg.class, Queue.RoutingQSnapSyncHeaderMsg.class, null,
                    TableOptions.fromProtoSchema(Queue.RoutingTableEntryMsg.class));
            corfuStore.openTable(CORFU_SYSTEM_NAMESPACE, REPLICATION_EVENT_TABLE_NAME,
                    LogReplication.ReplicationEventInfoKey.class,
                    LogReplication.ReplicationEvent.class,
                    null,
                    TableOptions.fromProtoSchema(LogReplication.ReplicationEvent.class));
        } catch (InvocationTargetException | IllegalAccessException | NoSuchMethodException e) {
            log.error("Failed to open table/queue", e);
            throw new RuntimeException(e);
        }

        //subscribe to eventTable
        CountDownLatch snapshotSyncRequestReceived = new CountDownLatch(1);
        SnapshotRequestListener snapReqListener = new SnapshotRequestListener(snapshotSyncRequestReceived);
        corfuStore.subscribeListener(snapReqListener, CORFU_SYSTEM_NAMESPACE, "log_replication");

        // start source/sink LR
        startReplicationServers();

        // wait until the snapshot request is received from the LR source.
        snapshotSyncRequestReceived.await();

        // alternate writes between snapshot sync queue and the delta queue
        long tail = snapReqListener.seqNumber;
        String destinationId = DefaultClusterConfig.getSinkClusterIds().get(0);


        int numEntryUpdates = numLogEntryUpdates;
        for(int i = 0; i < numEntryUpdates; ++i) {
            try(TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {

                if (i % 2 == 0) {
                    Queue.RoutingTableEntryMsg snapshotMessage = Queue.RoutingTableEntryMsg.newBuilder()
                            .setReplicationType(Queue.ReplicationType.SNAPSHOT_SYNC)
                            .addDestinations(destinationId)
                            .setOpaquePayload(ByteString.copyFromUtf8("opaquetxn"+i))
                            .buildPartial();
                    txn.logUpdateEnqueue(snapSyncQ.getStreamUUID(), snapshotMessage, Collections.singletonList(TableRegistry.getStreamIdForStreamTag(CORFU_SYSTEM_NAMESPACE,
                            SNAPSHOT_SYNC_QUEUE_TAG_SENDER_PREFIX + destinationId + "_" + DEFAULT_ROUTING_QUEUE_CLIENT)), corfuStore);
                    addMarker(tail, true, destinationId, corfuStore, txn);
                } else {
                    Queue.RoutingTableEntryMsg deltaMessage = Queue.RoutingTableEntryMsg.newBuilder()
                            .setReplicationType(Queue.ReplicationType.LOG_ENTRY_SYNC)
                            .addDestinations(destinationId)
                            .setOpaquePayload(ByteString.copyFromUtf8("opaquetxn"+i))
                            .buildPartial();
                    txn.logUpdateEnqueue(logEntryQ.getStreamUUID(), deltaMessage,
                            Collections.singletonList(TableRegistry.getStreamIdForStreamTag(CORFU_SYSTEM_NAMESPACE,
                            LOG_ENTRY_SYNC_QUEUE_TAG_SENDER_PREFIX + destinationId + "_" + DEFAULT_ROUTING_QUEUE_CLIENT)), corfuStore);
                }
                txn.commit();
            }
        }

        // add end marker to denote end of snapshot sync
        try(TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            addMarker(tail, false, DefaultClusterConfig.getSinkClusterIds().get(0), corfuStore, txn);
            txn.commit();
        }


        // open the recvQ on SINK
        Table<Queue.CorfuGuidMsg, Queue.RoutingTableEntryMsg, Queue.CorfuQueueMetadataMsg> recvQ = sinkCorfuStores.get(0).openQueue(
                CORFU_SYSTEM_NAMESPACE, LogReplicationUtils.REPLICATED_RECV_Q_PREFIX + DefaultClusterConfig.getSourceClusterIds().get(0) + "_" + DEFAULT_ROUTING_QUEUE_CLIENT,
                Queue.RoutingTableEntryMsg.class, TableOptions.builder().schemaOptions(
                        CorfuOptions.SchemaOptions.newBuilder().addStreamTag(REPLICATED_QUEUE_TAG).build()).build());

        // wait until the recv side count == data written on source
        while(recvQ.count() < (numEntryUpdates + 1)) {

        }

        // the "+1" is for the dummy record that is written at the end of snapshot sync
        assertThat(recvQ.count()).isEqualTo(numEntryUpdates + 1);


        AtomicBoolean snapshotSyncData = new AtomicBoolean(true);
        AtomicInteger countOfSnapshotSyncData = new AtomicInteger(0);
        AtomicInteger countOfDeltaSyncData = new AtomicInteger(0);

        // validate that the order is maintained.
        recvQ.entryList().forEach(entry -> {
            if (snapshotSyncData.get()) {
                assertEquals(((Queue.RoutingTableEntryMsg) entry.getEntry()).getReplicationType(), Queue.ReplicationType.SNAPSHOT_SYNC);
                countOfSnapshotSyncData.incrementAndGet();
            } else {
                assertEquals(((Queue.RoutingTableEntryMsg) entry.getEntry()).getReplicationType(), Queue.ReplicationType.LOG_ENTRY_SYNC);
                countOfDeltaSyncData.incrementAndGet();
            }
            // flip the flag after all the snapshot sync data is received. From now, there should only be delta sync data
            if(countOfSnapshotSyncData.get() == numLogEntryUpdates/2 && countOfDeltaSyncData.get() == 0) {
                snapshotSyncData.set(false);
            }

        });
    }

    class SnapshotRequestListener implements StreamListener {

        CountDownLatch latch;
        Long seqNumber;

        SnapshotRequestListener(CountDownLatch latch) {
            this.latch = latch;
        }

        public void onNext(CorfuStreamEntries results) {
            results.getEntries().forEach((schema, entries) -> {
                if (schema.getTableName().equals(REPLICATION_EVENT_TABLE_NAME)) {
                    entries.forEach(e -> {
                        if (((LogReplication.ReplicationEventInfoKey)e.getKey()).getSession().getSubscriber().getClientName()
                                .equals(DEFAULT_ROUTING_QUEUE_CLIENT)) {
                            LogReplication.ReplicationEvent event = (LogReplication.ReplicationEvent) e.getPayload();
                            if (event.getType().equals(SERVER_REQUESTED_SNAPSHOT_SYNC)) {
                                seqNumber = event.getEventTimestamp().getSeconds();
                                latch.countDown();
                            }
                        }
                    });
                }
            });
        }

        @Override
        public void onError(Throwable throwable) {
            fail("onError for SnapshotRequestListener : " + throwable.toString());
        }
    }

    private void addMarker(long baseSnapshotTimestamp, boolean isStartMarker, String destinationId, CorfuStore store, TxnContext txn) {
        Queue.RoutingQSnapSyncHeaderKeyMsg key = Queue.RoutingQSnapSyncHeaderKeyMsg.newBuilder()
                .setSnapSyncRequestId(baseSnapshotTimestamp)
                .setDestination(destinationId).build();
        Queue.RoutingQSnapSyncHeaderMsg marker = Queue.RoutingQSnapSyncHeaderMsg.newBuilder()
                .setSnapshotStartTimestamp(isStartMarker ? baseSnapshotTimestamp : -baseSnapshotTimestamp) // This is the base snapshot timestamp
                .build();

        CorfuRecord<Queue.RoutingQSnapSyncHeaderMsg, Message> markerEntry = new CorfuRecord<>(marker, null);

        Object[] smrArgs = new Object[2];
        smrArgs[0] = key;
        smrArgs[1] = markerEntry;
        txn.logUpdate(lrSnapSyncTxnEnvelopeStreamId,
                new SMREntry("put", smrArgs,
                        store.getRuntime().getSerializers().getSerializer(ProtobufSerializer.PROTOBUF_SERIALIZER_CODE)),
                Collections.singletonList(TableRegistry.getStreamIdForStreamTag(CORFU_SYSTEM_NAMESPACE,
                        SNAPSHOT_SYNC_QUEUE_TAG_SENDER_PREFIX + destinationId + "_" + DEFAULT_ROUTING_QUEUE_CLIENT))
        );
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
