package org.corfudb.integration;

import com.google.protobuf.ByteString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterConfig;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterManager;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.runtime.CorfuOptions;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LogReplication;
import org.corfudb.runtime.Queue;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.runtime.LogReplicationUtils.*;
import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

@Slf4j
public class LogReplicationRoutingQueueIT extends CorfuReplicationMultiSourceSinkIT {

    private int numSource = 1;

    @Before
    public void setUp() throws Exception {
        super.setUp(1, 1, DefaultClusterManager.TP_SINGLE_SOURCE_SINK_ROUTING_QUEUE);
        openLogReplicationStatusTable();
    }

    @Test
    public void testLogEntrySync() throws Exception {

        // Open queue on sink
        Table<Queue.CorfuGuidMsg, Queue.RoutingTableEntryMsg, Queue.CorfuQueueMetadataMsg> replicatedQueueSink = null;
        try {
            replicatedQueueSink = sinkCorfuStores.get(0).openQueue(CORFU_SYSTEM_NAMESPACE, String.join("",
                REPLICATED_QUEUE_NAME_PREFIX,
                DefaultClusterConfig.getSourceClusterIds().get(0)),
                Queue.RoutingTableEntryMsg.class, TableOptions.builder().schemaOptions(CorfuOptions.SchemaOptions.newBuilder()
                    .addStreamTag(REPLICATED_QUEUE_TAG).build()).build());
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }

        startReplicationServers();
        generateData();
        Thread.sleep(5000);
        //verifySessionInLogEntrySyncState(0, LogReplicationConfigManager.getDefaultRoutingQueueSubscriber());
    }

    private void generateData() throws Exception {
        String tableName = LOG_ENTRY_SYNC_QUEUE_NAME_SENDER;
        String namespace = CORFU_SYSTEM_NAMESPACE;

        Table<Queue.CorfuGuidMsg, Queue.RoutingTableEntryMsg, Queue.CorfuQueueMetadataMsg> q =
            sourceCorfuStores.get(0).openQueue(namespace, tableName, Queue.RoutingTableEntryMsg.class,
                TableOptions.fromProtoSchema(Queue.RoutingTableEntryMsg.class));

        String streamTagFollowed =
            LOG_ENTRY_SYNC_QUEUE_TAG_SENDER_PREFIX + DefaultClusterConfig.getSinkClusterIds().get(0);
        log.info("Stream UUID: {}", CorfuRuntime.getStreamID(streamTagFollowed));

        for (int i = 0; i < 10; i++) {
            ByteBuffer buffer = ByteBuffer.allocate(Integer.SIZE);
            buffer.putInt(i);
            Queue.RoutingTableEntryMsg val =
                Queue.RoutingTableEntryMsg.newBuilder().setOpaquePayload(ByteString.copyFrom(buffer.array()))
                    .build();

            try (TxnContext txnContext = sourceCorfuStores.get(0).txn(namespace)) {
                txnContext.logUpdateEnqueue(q, val, Arrays.asList(CorfuRuntime.getStreamID(streamTagFollowed)),
                    sourceCorfuStores.get(0));
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
}
