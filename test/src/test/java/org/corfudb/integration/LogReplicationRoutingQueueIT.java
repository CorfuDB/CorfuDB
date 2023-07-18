package org.corfudb.integration;

import com.google.protobuf.ByteString;
import lombok.extern.slf4j.Slf4j;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.runtime.LogReplicationUtils.LOG_ENTRY_SYNC_QUEUE_NAME_SENDER;
import static org.corfudb.runtime.LogReplicationUtils.REPLICATED_QUEUE_NAME;
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
        // create topology
        super.setUp(1,DefaultClusterManager.TP_SINGLE_SOURCE_MULTI_SINK_ROUTING_QUEUE.getValue() , DefaultClusterManager.TP_SINGLE_SOURCE_MULTI_SINK_ROUTING_QUEUE);
        openLogReplicationStatusTable();
    }

    /**
     * Tests delta sync. The number of SINK clusters is configurable in
     * DefaultClusterManager.TP_SINGLE_SOURCE_SINK_ROUTING_QUEUE definition
     * @throws Exception
     */
    @Test
    public void testLogEntrySync() throws Exception {

        // Register client and setup initial group destinations mapping
        CorfuRuntime clientRuntime = getClientRuntime();
        CorfuStore clientCorfuStore = new CorfuStore(clientRuntime);
        RoutingQueueSenderClient queueSenderClient = new RoutingQueueSenderClient();

        List<Table<Queue.CorfuGuidMsg, Queue.RoutingTableEntryMsg, Queue.CorfuQueueMetadataMsg>> sinkReplicatedQueues = new ArrayList<>();
        List<RoutingQueueListener> listeners = new ArrayList<>();
        openSinkReplicatedQueues(sinkReplicatedQueues, listeners);
        startReplicationServers();
        int numRecords = 10;
        generateData(clientCorfuStore, queueSenderClient, numRecords);
        Thread.sleep(5000);

        for (int i = 0; i < sinkReplicatedQueues.size(); ++i) {
            int expectedCount = i == 0 ? numRecords : numRecords * 2;
            while(sinkReplicatedQueues.get(i).count() != expectedCount) {
                // wait
//                System.out.println("count is {}"+ sinkReplicatedQueues.get(i).count());
            }
            System.out.println("table is " + i);
        }

        for(int i = 0; i < sinkReplicatedQueues.size(); ++i) {
            int expectedCount = i == 0 ? numRecords : numRecords * 2;
            assertThat(listeners.get(i).logEntryMsgCnt).isGreaterThanOrEqualTo(expectedCount);
        }
    }

    private void openSinkReplicatedQueues(
            List<Table<Queue.CorfuGuidMsg, Queue.RoutingTableEntryMsg, Queue.CorfuQueueMetadataMsg>> sinkReplicatedQueues,
            List<RoutingQueueListener> listeners) {

        log.info("Sink Queue name: {}", REPLICATED_QUEUE_NAME);
        for(int i = 0; i < getNumSinkClusters(); ++i) {
            try {
                sinkReplicatedQueues.add(sinkCorfuStores.get(i).openQueue(CORFU_SYSTEM_NAMESPACE, REPLICATED_QUEUE_NAME,
                        Queue.RoutingTableEntryMsg.class, TableOptions.builder().schemaOptions(CorfuOptions.SchemaOptions.newBuilder()
                                .addStreamTag(REPLICATED_QUEUE_TAG).build()).build()));

                RoutingQueueListener listener = new RoutingQueueListener(sinkCorfuStores.get(0));
                sinkCorfuStores.get(i).subscribeListenerFromTrimMark(listener, CORFU_SYSTEM_NAMESPACE, REPLICATED_QUEUE_TAG);
                listeners.add(listener);
            } catch(NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void generateData(CorfuStore corfuStore, RoutingQueueSenderClient client, int numRecords) throws Exception {
        String namespace = CORFU_SYSTEM_NAMESPACE;
        Table<Queue.CorfuGuidMsg, Queue.RoutingTableEntryMsg, Queue.CorfuQueueMetadataMsg> senderQ = corfuStore.openQueue(namespace, LOG_ENTRY_SYNC_QUEUE_NAME_SENDER, Queue.RoutingTableEntryMsg.class,
                TableOptions.fromProtoSchema(Queue.RoutingTableEntryMsg.class));

        String sourceUuid = UUID.nameUUIDFromBytes(("Source" + 0).getBytes()).toString();
        int sinkCounterForUuid = 0;
        int keyCounter = 100;
        for (int sinkNum = 0; sinkNum < getNumSinkClusters(); ++sinkNum) {
            List<String> destinationList;
            if (sinkNum == getNumSinkClusters() - 1) {
                destinationList = Arrays.asList(UUID.nameUUIDFromBytes(("Sink" + sinkCounterForUuid).getBytes()).toString());
            } else {
                destinationList = Arrays.asList(UUID.nameUUIDFromBytes(("Sink" + sinkCounterForUuid).getBytes()).toString(),
                        UUID.nameUUIDFromBytes(("Sink" + (sinkCounterForUuid + 2)).getBytes()).toString());
            }
            for (int i = keyCounter; i < keyCounter + numRecords; i++) {
                ByteBuffer buffer = ByteBuffer.allocate(Integer.SIZE);
                buffer.putInt(i);
                Queue.RoutingTableEntryMsg val =
                        Queue.RoutingTableEntryMsg.newBuilder()
                                .setSourceClusterId(sourceUuid)
                                .addAllDestinations(destinationList)
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
            sinkCounterForUuid += 2;
            keyCounter *= 10;
        }

        assertThat(senderQ.count()).isEqualTo(numRecords * getNumSinkClusters());
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
