package org.corfudb.infrastructure.logreplication;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.LogReplicationContext;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterConfig;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.LogReplicationPluginConfig;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.BaseSnapshotReader;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.RoutingQueuesSnapshotReader;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.SnapshotReadMessage;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationConfigManager;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LogReplication;
import org.corfudb.runtime.LogReplicationUtils;
import org.corfudb.runtime.Queue;
import org.corfudb.runtime.collections.CorfuRecord;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.util.serializer.ProtobufSerializer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

import static org.corfudb.runtime.LogReplicationUtils.SNAP_SYNC_TXN_ENVELOPE_TABLE;
import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

@SuppressWarnings("checkstyle:magicnumber")
@Slf4j
public class RoutingQueuesSnapshotReaderTest extends AbstractViewTest {

    private UUID syncRequestId;
    private BaseSnapshotReader snapshotReader;
    private CorfuRuntime lrRuntime;
    private CorfuRuntime clientRuntime;
    private LogReplication.LogReplicationSession session;
    private CorfuStore corfuStore;
    private String streamTagFollowed;
    private final String namespace = CORFU_SYSTEM_NAMESPACE;

    @Before
    public void setUp() {
        syncRequestId = UUID.randomUUID();
        lrRuntime = getDefaultRuntime();
        clientRuntime = getNewRuntime(getDefaultNode()).connect();
        corfuStore = new CorfuStore(clientRuntime);
        session = DefaultClusterConfig.getRoutingQueueSessions().get(0);

        LogReplicationConfigManager configManager = new LogReplicationConfigManager(lrRuntime,
            session.getSourceClusterId());
        LogReplicationContext replicationContext = new LogReplicationContext(configManager, 5,
            session.getSourceClusterId(), true, new LogReplicationPluginConfig(""), lrRuntime);
        configManager.generateConfig(Collections.singleton(session), false);
        snapshotReader = new RoutingQueuesSnapshotReader(session, replicationContext);
        snapshotReader.reset(lrRuntime.getAddressSpaceView().getLogTail());

        streamTagFollowed = TableRegistry.getStreamTagFullStreamName(namespace,
            LogReplicationUtils.SNAPSHOT_SYNC_QUEUE_TAG_SENDER_PREFIX + session.getSinkClusterId() + "_" +
            session.getSubscriber().getClientName());
    }

    /**
     *
     * @throws Exception
     */
    @Test
    public void testRead() throws Exception {
        generateData();
        SnapshotReadMessage snapshotMessage = snapshotReader.read(syncRequestId);
        Assert.assertTrue(snapshotMessage.isEndRead());
    }

    private void generateData() throws Exception {

        corfuStore.openTable(namespace, SNAP_SYNC_TXN_ENVELOPE_TABLE,
                Queue.RoutingQSnapSyncHeaderKeyMsg.class, Queue.RoutingQSnapSyncHeaderMsg.class, null,
                TableOptions.fromProtoSchema(Queue.RoutingQSnapSyncHeaderMsg.class));

        String tableName = LogReplicationUtils.SNAPSHOT_SYNC_QUEUE_NAME_SENDER;

        Table<Queue.CorfuGuidMsg, Queue.RoutingTableEntryMsg, Queue.CorfuQueueMetadataMsg> q =
            corfuStore.openQueue(namespace, tableName, Queue.RoutingTableEntryMsg.class,
                TableOptions.fromProtoSchema(Queue.RoutingTableEntryMsg.class));

        log.info("Stream UUID: {}, name: {}", CorfuRuntime.getStreamID(streamTagFollowed), streamTagFollowed);

        // Corfu log tail which represents the timestamp at which snapshot sync was requested
        long snapshotReqTimestamp = lrRuntime.getAddressSpaceView().getLogTail();
        ((RoutingQueuesSnapshotReader) snapshotReader).setRequestSnapSyncId(snapshotReqTimestamp);

        for (int i = 0; i < 10; i++) {
            ByteBuffer buffer = ByteBuffer.allocate(Integer.SIZE);
            buffer.putInt(i);
            Queue.RoutingTableEntryMsg val =
                Queue.RoutingTableEntryMsg.newBuilder().setOpaquePayload(ByteString.copyFrom(buffer.array()))
                    .build();

            try (TxnContext txnContext = corfuStore.txn(namespace)) {
                txnContext.logUpdateEnqueue(q, val, Arrays.asList(CorfuRuntime.getStreamID(streamTagFollowed)),
                    corfuStore);
                writeMarker(true, snapshotReqTimestamp, txnContext);
                txnContext.commit();
            } catch (Exception e) {
                log.error("Failed to add data to the queue", e);
            }
        }
        log.info("Queue Size = {}.  Stream tags = {}", q.count(), q.getStreamTags());

        // Write the End Marker
        writeMarker(false, snapshotReqTimestamp, null);
    }

    private void writeMarker(boolean start, long timestamp, TxnContext txnContext) {
        Queue.RoutingQSnapSyncHeaderKeyMsg snapshotSyncId =
            Queue.RoutingQSnapSyncHeaderKeyMsg.newBuilder().setDestination(session.getSinkClusterId())
                .setSnapSyncRequestId(timestamp).build();

        Queue.RoutingQSnapSyncHeaderMsg marker;

        Object[] smrArgs = new Object[2];
        smrArgs[0] = snapshotSyncId;

        CorfuRecord<Queue.RoutingQSnapSyncHeaderMsg, Message> entry;

        UUID markerStreamId = CorfuRuntime.getStreamID(TableRegistry.getFullyQualifiedTableName(
                namespace, SNAP_SYNC_TXN_ENVELOPE_TABLE));

        if (start) {
            marker = Queue.RoutingQSnapSyncHeaderMsg.newBuilder().setSnapshotStartTimestamp(txnContext.getTxnSequence())
                .build();
            entry = new CorfuRecord<>(marker, null);
            smrArgs[1] = entry;
            // Start marker is written in the same transaction as the data.  So reuse the transaction which was passed
            txnContext.logUpdate(markerStreamId, new SMREntry("put", smrArgs,
                    corfuStore.getRuntime().getSerializers().getSerializer(ProtobufSerializer.PROTOBUF_SERIALIZER_CODE)),
                Arrays.asList(CorfuRuntime.getStreamID(streamTagFollowed)));
            return;
        }

        // End marker is written in a separate transaction and with different data
        marker = Queue.RoutingQSnapSyncHeaderMsg.newBuilder().setSnapshotStartTimestamp(-timestamp).build();
        entry = new CorfuRecord<>(marker, null);
        smrArgs[1] = entry;
        try (TxnContext txn = corfuStore.txn(namespace)) {
            txn.logUpdate(markerStreamId, new SMREntry("put", smrArgs,
                    corfuStore.getRuntime().getSerializers().getSerializer(ProtobufSerializer.PROTOBUF_SERIALIZER_CODE)),
                Arrays.asList(CorfuRuntime.getStreamID(streamTagFollowed)));
            txn.commit();
        } catch (Exception e) {
            log.error("Failed to add the Marker", e);
        }
    }

    @After
    public void cleanUp() {
        lrRuntime.shutdown();
        clientRuntime.shutdown();
    }
}
