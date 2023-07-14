package org.corfudb.integration;

import com.google.protobuf.ByteString;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterConfig;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterManager;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.runtime.CorfuOptions;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LogReplication;
import org.corfudb.runtime.LogReplicationRoutingQueueListener;
import org.corfudb.runtime.LogReplicationUtils;
import org.corfudb.runtime.Queue;
import org.corfudb.runtime.RoutingQueueSenderClient;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStreamEntries;
import org.corfudb.runtime.collections.CorfuStreamEntry;
import org.corfudb.runtime.collections.StreamListener;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.runtime.LogReplicationUtils.DEMO_NAMESPACE;
import static org.corfudb.runtime.LogReplicationUtils.LOG_ENTRY_SYNC_QUEUE_NAME_SENDER;
import static org.corfudb.runtime.LogReplicationUtils.LOG_ENTRY_SYNC_QUEUE_TAG_SENDER_PREFIX;
import static org.corfudb.runtime.LogReplicationUtils.REPLICATED_QUEUE_NAME_PREFIX;
import static org.corfudb.runtime.LogReplicationUtils.REPLICATED_QUEUE_TAG;
import static org.corfudb.runtime.LogReplicationUtils.REPLICATION_STATUS_TABLE_NAME;

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
    public void testLogEntrySync() throws Exception {

        // Register client and setup initial group destinations mapping
        CorfuRuntime clientRuntime = getClientRuntime();
        CorfuStore clientCorfuStore = new CorfuStore(clientRuntime);
        RoutingQueueSenderClient queueSenderClient = new RoutingQueueSenderClient();

        // Open queue on sink
        try {
            log.info("Sink queue name: {}", String.join("", REPLICATED_QUEUE_NAME_PREFIX,
                    DefaultClusterConfig.getSourceClusterIds().get(0)));
            log.info("Sink Queue name: {}", REPLICATED_QUEUE_NAME_PREFIX + DefaultClusterConfig.getSourceClusterIds().get(0));
            Table<Queue.CorfuGuidMsg, Queue.RoutingTableEntryMsg, Queue.CorfuQueueMetadataMsg> replicatedQueueSink
                    = sinkCorfuStores.get(0).openQueue(DEMO_NAMESPACE, String.join("",
                            REPLICATED_QUEUE_NAME_PREFIX,
                            DefaultClusterConfig.getSourceClusterIds().get(0)),
                    Queue.RoutingTableEntryMsg.class, TableOptions.builder().schemaOptions(CorfuOptions.SchemaOptions.newBuilder()
                            .addStreamTag(REPLICATED_QUEUE_TAG).build()).build());

            RoutingQueueListener listener = new RoutingQueueListener();
            sinkCorfuStores.get(0).subscribeListener(listener, DEMO_NAMESPACE, REPLICATED_QUEUE_TAG);

            RoutingQueueMultiNamespaceListener nsListener =
                    new RoutingQueueMultiNamespaceListener(sinkCorfuStores.get(0), DEMO_NAMESPACE);
            LogReplicationUtils.subscribeRqListener(nsListener, DEMO_NAMESPACE, 5, sinkCorfuStores.get(0));

            startReplicationServers();
            generateData(clientCorfuStore, queueSenderClient);
            Thread.sleep(5000);

            int sinkQueueSize = replicatedQueueSink.count();
            while (sinkQueueSize != 10) {
                Thread.sleep(5000);
                sinkQueueSize = replicatedQueueSink.count();
                log.info("Sink replicated queue size: {}", sinkQueueSize);
            }
            sinkQueueSize = replicatedQueueSink.count();
            replicatedQueueSink.entryStream().forEach(e -> {
                log.info("{}", e.getPayload());
            });
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private void generateData(CorfuStore corfuStore, RoutingQueueSenderClient client) throws Exception {
        String namespace = DEMO_NAMESPACE;

        Table<Queue.CorfuGuidMsg, Queue.RoutingTableEntryMsg, Queue.CorfuQueueMetadataMsg> q =
            corfuStore.openQueue(namespace, LOG_ENTRY_SYNC_QUEUE_NAME_SENDER, Queue.RoutingTableEntryMsg.class,
                TableOptions.fromProtoSchema(Queue.RoutingTableEntryMsg.class));

        String streamTagFollowed =
            LOG_ENTRY_SYNC_QUEUE_TAG_SENDER_PREFIX + DefaultClusterConfig.getSinkClusterIds().get(0);
        log.info("Stream UUID: {}", CorfuRuntime.getStreamID(streamTagFollowed));

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

    private void verifySessionInLogEntrySyncState(int sinkIndex, LogReplication.ReplicationSubscriber subscriber) {
        LogReplication.LogReplicationSession session = LogReplication.LogReplicationSession.newBuilder()
            .setSourceClusterId(DefaultClusterConfig.getSourceClusterIds().get(0))
            .setSinkClusterId(DefaultClusterConfig.getSinkClusterIds().get(sinkIndex))
            .setSubscriber(subscriber)
            .build();

        LogReplication.ReplicationStatus status = null;

        while (status == null || !status.getSourceStatus().getReplicationInfo().getSyncType().equals(LogReplication.SyncType.LOG_ENTRY)
            || !status.getSourceStatus().getReplicationInfo().getSnapshotSyncInfo().getStatus()
            .equals(LogReplication.SyncStatus.COMPLETED)) {
            try (TxnContext txn = sourceCorfuStores.get(0).txn(LogReplicationMetadataManager.NAMESPACE)) {
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

    static class RoutingQueueListener implements StreamListener {

        @Override
        public void onNext(CorfuStreamEntries results) {
            List<CorfuStreamEntry> entries = results.getEntries().entrySet().stream()
                    .map(Map.Entry::getValue).findFirst().get();
            for (CorfuStreamEntry entry : entries) {
                if (((Queue.RoutingTableEntryMsg) entry.getPayload()).getReplicationType()
                        .equals(Queue.ReplicationType.LOG_ENTRY_SYNC)) {
                    log.info("Process log entry sync msg: {}", entry.getPayload());
                    processUpdatesInLogEntrySync();
                } else {
                    log.info("Process snapshot sync msg: {}", entry.getPayload());
                    processUpdatesInSnapshotSync();
                }
            }
        }

        @Override
        public void onError(Throwable throwable) {

        }

        protected boolean processUpdatesInSnapshotSync() {
            return false;
        }

        protected boolean processUpdatesInLogEntrySync() {
            return false;
        }
    }




    static class RoutingQueueMultiNamespaceListener extends LogReplicationRoutingQueueListener {

        /**
         * Special LogReplication listener which a client creates to receive ordered updates for replicated data.
         *
         * @param corfuStore Corfu Store used on the client
         * @param namespace  Namespace of the client's tables
         */
        public RoutingQueueMultiNamespaceListener(CorfuStore corfuStore, @NonNull String namespace) throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
            super(corfuStore, namespace);

        }
        @Override
        protected void onSnapshotSyncStart() {

        }

        @Override
        protected void onSnapshotSyncComplete() {

        }

        @Override
        protected boolean processUpdatesInSnapshotSync(List<Queue.RoutingTableEntryMsg> results) {
            return false;
        }

        @Override
        protected boolean processUpdatesInLogEntrySync(List<Queue.RoutingTableEntryMsg> results) {
            log.info("processUpdatesInLogEntrySync {}", results);
            return false;
        }

        @Override
        protected void performFullSyncAndMerge(TxnContext txnContext) {

        }

        @Override
        protected String getClientName() {
            return null;
        }
    }
}
