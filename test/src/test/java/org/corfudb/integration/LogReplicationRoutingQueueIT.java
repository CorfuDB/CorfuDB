package org.corfudb.integration;

import com.google.protobuf.ByteString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.browser.CorfuStoreBrowserEditor;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterConfig;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterManager;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.runtime.CorfuOptions;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LRFullStateReplicationContext;
import org.corfudb.runtime.LiteRoutingQueueListener;
import org.corfudb.runtime.LogReplication;
import org.corfudb.runtime.Queue;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.infrastructure.logreplication.utils.LogReplicationConfigManager.getDefaultRoutingQueueSubscriber;
import static org.corfudb.integration.LogReplicationAbstractIT.NAMESPACE;
import static org.corfudb.runtime.LogReplicationUtils.LOG_ENTRY_SYNC_QUEUE_TAG_SENDER_PREFIX;
import static org.corfudb.runtime.LogReplicationUtils.LR_STATUS_STREAM_TAG;
import static org.corfudb.runtime.LogReplicationUtils.REPLICATED_QUEUE_NAME;
import static org.corfudb.runtime.LogReplicationUtils.REPLICATED_QUEUE_TAG;
import static org.corfudb.runtime.LogReplicationUtils.REPLICATION_STATUS_TABLE_NAME;
import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

@SuppressWarnings("checkstyle:magicnumber")
@Slf4j
public class LogReplicationRoutingQueueIT extends CorfuReplicationMultiSourceSinkIT {

    final int numDeltaMsgs = 50;

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
        super.setUp(1, DefaultClusterManager.TP_SINGLE_SOURCE_MULTI_SINK_ROUTING_QUEUE.getValue(),
                DefaultClusterManager.TP_SINGLE_SOURCE_MULTI_SINK_ROUTING_QUEUE);
        openLogReplicationStatusTable();
    }

    @Test
    public void testRoutingQueueReplication() throws Exception {
        System.out.println();

        // Register client and setup initial group destinations mapping
        CorfuRuntime clientRuntime = getClientRuntime();
        CorfuStore clientCorfuStore = new CorfuStore(clientRuntime);
        String clientName = "testClient";
        RoutingQueueSenderClient queueSenderClient = new RoutingQueueSenderClient(clientCorfuStore, clientName);
        // SnapshotProvider implements RoutingQueueSenderClient.LRTransmitterReplicationModule
        SnapshotProvider snapshotProvider = new SnapshotProvider(clientCorfuStore);
        queueSenderClient.startLRSnapshotTransmitter(snapshotProvider); // starts a listener on event table


        // Open queue on sink
        try {
            log.info("Sink Queue name: {}", REPLICATED_QUEUE_NAME);
            Table<Queue.CorfuGuidMsg, Queue.RoutingTableEntryMsg, Queue.CorfuQueueMetadataMsg> replicatedQueueSink
                    = sinkCorfuStores.get(0).openQueue(CORFU_SYSTEM_NAMESPACE, REPLICATED_QUEUE_NAME,
                    Queue.RoutingTableEntryMsg.class, TableOptions.builder().schemaOptions(CorfuOptions.SchemaOptions.newBuilder()
                            .addStreamTag(REPLICATED_QUEUE_TAG).build()).build());
            System.out.println("Starting LR..so starting full sync");

            startReplicationServers();
            while (!snapshotProvider.isSnapshotSent) {
                Thread.sleep(5000);
            }

            CountDownLatch latch = new CountDownLatch(getNumSinkClusters());
            ReplicationStatusListener listener = new ReplicationStatusListener(latch, true);
            sourceCorfuStores.get(0).subscribeListener(listener, LogReplicationMetadataManager.NAMESPACE,
                    LR_STATUS_STREAM_TAG);

            latch.await();

            System.out.println("Generating deltas");
            generateData(clientCorfuStore, queueSenderClient);

            List<Table<Queue.CorfuGuidMsg, Queue.RoutingTableEntryMsg, Queue.CorfuQueueMetadataMsg>> sinkReplicatedQueues = new ArrayList<>();
            openSinkReplicatedQueues(sinkReplicatedQueues);

            for (int i = 0; i < sinkReplicatedQueues.size(); ++i) {
                int expectedCount = i == 0 ? SnapshotProvider.numFullSyncBatches + numDeltaMsgs :
                        SnapshotProvider.numFullSyncBatches + (numDeltaMsgs * 2);
                while(sinkReplicatedQueues.get(i).count() != expectedCount) {
                    // wait
//                System.out.println("count is {}"+ sinkReplicatedQueues.get(i).count());
                }
                System.out.println("table is " + i);
            }

        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private void openSinkReplicatedQueues(
            List<Table<Queue.CorfuGuidMsg, Queue.RoutingTableEntryMsg, Queue.CorfuQueueMetadataMsg>> sinkReplicatedQueues) {

        log.info("Sink Queue name: {}", REPLICATED_QUEUE_NAME);
        for(int i = 0; i < getNumSinkClusters(); ++i) {
            try {
                sinkReplicatedQueues.add(sinkCorfuStores.get(i).openQueue(CORFU_SYSTEM_NAMESPACE, REPLICATED_QUEUE_NAME,
                        Queue.RoutingTableEntryMsg.class, TableOptions.builder().schemaOptions(CorfuOptions.SchemaOptions.newBuilder()
                                .addStreamTag(REPLICATED_QUEUE_TAG).build()).build()));
            } catch(NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void subscribeSinkListeners(Map<Integer, RoutingQueueListener> sinkCounterToListener) {
        for(int i = 0; i < getNumSinkClusters(); ++i) {
            RoutingQueueListener listener = new RoutingQueueListener(sinkCorfuStores.get(i));
            sinkCorfuStores.get(i).subscribeRoutingQListener(listener);
            sinkCounterToListener.put(i, listener);
        }
    }

    @Test
    public void testRoutingQueueFullSyncs() throws Exception {

        // Register client and setup initial group destinations mapping
        CorfuRuntime clientRuntime = getClientRuntime();
        CorfuStore clientCorfuStore = new CorfuStore(clientRuntime);
        CorfuStoreBrowserEditor editor = new CorfuStoreBrowserEditor(clientRuntime, null, true);
        String clientName = RoutingQueueSenderClient.DEFAULT_ROUTING_QUEUE_CLIENT;
        RoutingQueueSenderClient queueSenderClient = new RoutingQueueSenderClient(clientCorfuStore, clientName);
        // SnapshotProvider implements RoutingQueueSenderClient.LRTransmitterReplicationModule
        SnapshotProvider snapshotProvider = new SnapshotProvider(clientCorfuStore);
        queueSenderClient.startLRSnapshotTransmitter(snapshotProvider); // starts a listener on event table

        // Open queue on sink
        try {
            log.info("Sink Queue name: {}", REPLICATED_QUEUE_NAME);
            Table<Queue.CorfuGuidMsg, Queue.RoutingTableEntryMsg, Queue.CorfuQueueMetadataMsg> replicatedQueueSink
                    = sinkCorfuStores.get(0).openQueue(CORFU_SYSTEM_NAMESPACE, REPLICATED_QUEUE_NAME,
                    Queue.RoutingTableEntryMsg.class, TableOptions.builder().schemaOptions(CorfuOptions.SchemaOptions.newBuilder()
                            .addStreamTag(REPLICATED_QUEUE_TAG).build()).build());

            startReplicationServers();
            while (!snapshotProvider.isSnapshotSent) {
                Thread.sleep(5000);
            }

            RoutingQueueListener listener = new RoutingQueueListener(sinkCorfuStores.get(0));
            sinkCorfuStores.get(0).subscribeRoutingQListener(listener);

            int numFullSyncMsgsGot = listener.snapSyncMsgCnt;
            while (numFullSyncMsgsGot < SnapshotProvider.numFullSyncBatches) {
                Thread.sleep(5000);
                numFullSyncMsgsGot = listener.snapSyncMsgCnt;
                log.info("Entries got on receiver {}", numFullSyncMsgsGot);
            }
            assertThat(listener.snapSyncMsgCnt).isEqualTo(SnapshotProvider.numFullSyncBatches);

            // Now request a full sync again for all sites!
            snapshotProvider.isSnapshotSent = false;
            editor.requestGlobalFullSync();

            while (numFullSyncMsgsGot < SnapshotProvider.numFullSyncBatches * 2) {
                Thread.sleep(5000);
                numFullSyncMsgsGot = listener.snapSyncMsgCnt;
                log.info("Entries got on receiver after 2nd full sync {}", numFullSyncMsgsGot);
            }
            assertThat(listener.snapSyncMsgCnt).isEqualTo(SnapshotProvider.numFullSyncBatches * 2);

            // Now request a full sync this time for just one site!
            snapshotProvider.isSnapshotSent = false;
            queueSenderClient.requestSnapshotSync(UUID.fromString(DefaultClusterConfig.getClusterId(true, 0)),
                    UUID.fromString(DefaultClusterConfig.getClusterId(false, 0)));

            while (numFullSyncMsgsGot < SnapshotProvider.numFullSyncBatches * 3) {
                Thread.sleep(5000);
                numFullSyncMsgsGot = listener.snapSyncMsgCnt;
                log.info("Entries got on receiver after 3rd full sync {}", numFullSyncMsgsGot);
            }
            assertThat(listener.snapSyncMsgCnt).isEqualTo(SnapshotProvider.numFullSyncBatches * 3);
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }


    private void generateData(CorfuStore corfuStore, RoutingQueueSenderClient client) throws Exception {
        String namespace = CORFU_SYSTEM_NAMESPACE;

        int clusterCounter = 0;
        for(int j = 0; j < getNumSinkClusters(); ++j) {
            String currSinkClusterId = DefaultClusterConfig.getClusterId(false, clusterCounter);
            List<String> destinationList = new ArrayList<>();
            destinationList.add(currSinkClusterId);

            if (j < getNumSinkClusters() - 1) {
                String nextSinkClusterId = DefaultClusterConfig.getClusterId(false, clusterCounter + 2);
                destinationList.add(nextSinkClusterId);
            }

            for (int i = 0; i < numDeltaMsgs; i++) {
                ByteBuffer buffer = ByteBuffer.allocate(Integer.SIZE);
                buffer.putInt(i);
                Queue.RoutingTableEntryMsg val = Queue.RoutingTableEntryMsg.newBuilder()
                        .setSourceClusterId(DefaultClusterConfig.getClusterId(true, 0))
                        .addAllDestinations(destinationList)
                        .setOpaquePayload(ByteString.copyFrom(buffer.array()))
                        .setReplicationType(Queue.ReplicationType.LOG_ENTRY_SYNC)
                        .build();

                try (TxnContext txnContext = corfuStore.txn(namespace)) {
                    client.transmitDeltaMessages(txnContext, Collections.singletonList(val), corfuStore);
                    long commitedTs = txnContext.commit().getSequence();
                    log.info("Committed at {}", commitedTs);
//                    System.out.println("commited to " + commitedTs);
                } catch (Exception e) {
                    log.error("Failed to add data to the queue", e);
                }
            }
//            System.out.println("====> "+j);
            clusterCounter += 2;
        }
    }

    /**
     * Provide a full sync or snapshot
     */
    public static class SnapshotProvider implements RoutingQueueSenderClient.LRTransmitterReplicationModule {

        public final String someNamespace = "testNamespace";
        public static final int numFullSyncBatches = 20;
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
