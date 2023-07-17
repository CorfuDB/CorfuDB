package org.corfudb.integration;

import com.google.protobuf.ByteString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterConfig;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterManager;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.runtime.CorfuOptions;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LiteRoutingQueueListener;
import org.corfudb.runtime.LogReplication;
import org.corfudb.runtime.Queue;
import org.corfudb.runtime.RoutingQueueSenderClient;
import org.corfudb.runtime.collections.CorfuStore;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.runtime.LogReplicationUtils.LOG_ENTRY_SYNC_QUEUE_NAME_SENDER;
import static org.corfudb.runtime.LogReplicationUtils.LOG_ENTRY_SYNC_QUEUE_TAG_SENDER_PREFIX;
import static org.corfudb.runtime.LogReplicationUtils.REPLICATED_QUEUE_NAME;
import static org.corfudb.runtime.LogReplicationUtils.REPLICATED_QUEUE_TAG;
import static org.corfudb.runtime.LogReplicationUtils.REPLICATION_STATUS_TABLE_NAME;
import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

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
        String clientName = "testClient";
        RoutingQueueSenderClient queueSenderClient = new RoutingQueueSenderClient(clientCorfuStore, clientName);

        // Open queue on sink
        try {
            log.info("Sink Queue name: {}", REPLICATED_QUEUE_NAME);
            Table<Queue.CorfuGuidMsg, Queue.RoutingTableEntryMsg, Queue.CorfuQueueMetadataMsg> replicatedQueueSink
                    = sinkCorfuStores.get(0).openQueue(CORFU_SYSTEM_NAMESPACE, REPLICATED_QUEUE_NAME,
                    Queue.RoutingTableEntryMsg.class, TableOptions.builder().schemaOptions(CorfuOptions.SchemaOptions.newBuilder()
                            .addStreamTag(REPLICATED_QUEUE_TAG).build()).build());

            RoutingQueueListener listener = new RoutingQueueListener(sinkCorfuStores.get(0));
            sinkCorfuStores.get(0).subscribeListenerFromTrimMark(listener, CORFU_SYSTEM_NAMESPACE, REPLICATED_QUEUE_TAG);

            startReplicationServers();
            generateData(clientCorfuStore, queueSenderClient);
            Thread.sleep(5000);

            int sinkQueueSize = replicatedQueueSink.count();
            while (sinkQueueSize != 10) {
                Thread.sleep(5000);
                sinkQueueSize = replicatedQueueSink.count();
                log.info("Sink replicated queue size: {}", sinkQueueSize);
            }
            assertThat(listener.logEntryMsgCnt).isEqualTo(10);
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private void generateData(CorfuStore corfuStore, RoutingQueueSenderClient client) throws Exception {
        String namespace = CORFU_SYSTEM_NAMESPACE;

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

    static class RoutingQueueListener extends LiteRoutingQueueListener {
        public int logEntryMsgCnt;

        public RoutingQueueListener(CorfuStore corfuStore) {
            super(corfuStore);
            logEntryMsgCnt = 0;
        }

        @Override
        protected boolean processUpdatesInSnapshotSync(List<Queue.RoutingTableEntryMsg> updates) {
            return false;
        }

        @Override
        protected boolean processUpdatesInLogEntrySync(List<Queue.RoutingTableEntryMsg> updates) {
            log.info("LiteRoutingQueueListener::processUpdatesInLogEntrySync::{}", updates);
            logEntryMsgCnt++;
            return false;
        }
    }
}
