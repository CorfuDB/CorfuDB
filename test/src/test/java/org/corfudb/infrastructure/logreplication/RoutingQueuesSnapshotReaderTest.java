package org.corfudb.infrastructure.logreplication;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.LogReplicationContext;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterConfig;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.LogReplicationPluginConfig;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.BaseSnapshotReader;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.RoutingQueuesSnapshotReader;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationConfigManager;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LogReplication;
import org.corfudb.runtime.LogReplicationUtils;
import org.corfudb.runtime.Queue;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.view.AbstractViewTest;
import org.junit.Before;
import org.junit.Test;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

import static org.corfudb.runtime.LogReplicationUtils.SNAPSHOT_END_MARKER_TABLE_NAME;
import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

@Slf4j
public class RoutingQueuesSnapshotReaderTest extends AbstractViewTest {

    private UUID syncRequestId;
    private BaseSnapshotReader snapshotReader;
    private CorfuRuntime runtime1;
    private CorfuRuntime runtime2;
    private LogReplication.LogReplicationSession session;
    private LogReplicationConfigManager configManager;
    private LogReplicationContext replicationContext;
    private CorfuStore corfuStore;
    String streamTagFollowed;

    @Before
    public void setUp() {
        syncRequestId = UUID.randomUUID();
        runtime1 = getDefaultRuntime();
        runtime2 = getNewRuntime(getDefaultNode()).connect();
        corfuStore = new CorfuStore(runtime2);
        session = DefaultClusterConfig.getRoutingQueueSessions().get(0);

        configManager = new LogReplicationConfigManager(runtime1, session.getSourceClusterId());
        replicationContext = new LogReplicationContext(configManager, 5, session.getSourceClusterId(),
                true, new LogReplicationPluginConfig(""));
        configManager.generateConfig(Collections.singleton(session));
        snapshotReader = new RoutingQueuesSnapshotReader(runtime1, session, replicationContext);
        snapshotReader.reset(runtime1.getAddressSpaceView().getLogTail());

        streamTagFollowed = LogReplicationUtils.SNAPSHOT_SYNC_QUEUE_TAG_SENDER_PREFIX + session.getSinkClusterId();
    }

    @Test
    public void testRead() throws Exception {
        generateData();
        snapshotReader.read(syncRequestId);
    }

    private void generateData() throws Exception {

        String tableName = LogReplicationUtils.SNAPSHOT_SYNC_QUEUE_NAME_SENDER;
        String namespace = CORFU_SYSTEM_NAMESPACE;

        Table<Queue.CorfuGuidMsg, Queue.RoutingTableEntryMsg, Queue.CorfuQueueMetadataMsg> q =
                corfuStore.openQueue(namespace, tableName, Queue.RoutingTableEntryMsg.class,
                    TableOptions.fromProtoSchema(Queue.RoutingTableEntryMsg.class));

        log.info("Stream UUID: {}", CorfuRuntime.getStreamID(streamTagFollowed));

        for (int i = 0; i < 10; i++) {
                ByteBuffer buffer = ByteBuffer.allocate(Integer.SIZE);
                buffer.putInt(i);
            Queue.RoutingTableEntryMsg val =
                    Queue.RoutingTableEntryMsg.newBuilder().setOpaquePayload(ByteString.copyFrom(buffer.array()))
                        .build();

            try (TxnContext txnContext = corfuStore.txn(namespace)) {
                txnContext.logUpdateEnqueue(q, val, Arrays.asList(CorfuRuntime.getStreamID(streamTagFollowed)),
                    corfuStore);
                log.info("Committed at {}", txnContext.commit());
            } catch (Exception e) {
                log.error("Failed to add data to the queue", e);
            }
        }
        log.info("Queue Size = {}.  Stream tags = {}", q.count(), q.getStreamTags());

        Table<Queue.RoutingTableSnapshotEndKeyMsg, Queue.RoutingTableSnapshotEndMarkerMsg, Message> endMarkerTable =
            corfuStore.openTable(CORFU_SYSTEM_NAMESPACE, SNAPSHOT_END_MARKER_TABLE_NAME,
                Queue.RoutingTableSnapshotEndKeyMsg.class, Queue.RoutingTableSnapshotEndMarkerMsg.class, null,
                TableOptions.fromProtoSchema(Queue.RoutingTableSnapshotEndMarkerMsg.class));

        Queue.RoutingTableSnapshotEndKeyMsg snapshotSyncId =
                Queue.RoutingTableSnapshotEndKeyMsg.newBuilder().setSnapshotSyncId(syncRequestId.toString()).build();

        Queue.RoutingTableSnapshotEndMarkerMsg endMarker =
            Queue.RoutingTableSnapshotEndMarkerMsg.newBuilder().setDestination(session.getSinkClusterId()).build();

        try (TxnContext txnContext = corfuStore.txn(namespace)) {
            txnContext.putRecord(endMarkerTable, snapshotSyncId, endMarker, null);
            txnContext.commit();
        } catch (Exception e) {
            log.error("Failed to add End Marker", e);
        }
    }
}
