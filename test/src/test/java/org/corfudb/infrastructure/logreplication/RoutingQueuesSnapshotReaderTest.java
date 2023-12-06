package org.corfudb.infrastructure.logreplication;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.LogReplicationContext;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterConfig;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.LogReplicationPluginConfig;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.BaseSnapshotReader;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.ReplicationReaderException;
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
import org.corfudb.util.retry.RetryNeededException;
import org.corfudb.util.serializer.ProtobufSerializer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

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
    private final String namespace = CORFU_SYSTEM_NAMESPACE;
    private LogReplicationConfigManager configManager;
    private LogReplicationContext replicationContext;
    private long snapshotReqTimestamp;

    @Before
    public void setUp() {
        syncRequestId = UUID.randomUUID();
        lrRuntime = getDefaultRuntime();
        clientRuntime = getNewRuntime(getDefaultNode()).connect();
        corfuStore = new CorfuStore(clientRuntime);
        session = DefaultClusterConfig.getRoutingQueueSessions().get(0);

        configManager = new LogReplicationConfigManager(lrRuntime,
            session.getSourceClusterId());
        replicationContext = new LogReplicationContext(configManager, 5,
            session.getSourceClusterId(), true, new LogReplicationPluginConfig(""), lrRuntime);
        configManager.generateConfig(Collections.singleton(session), false);
        snapshotReader = new RoutingQueuesSnapshotReader(session, replicationContext);
        snapshotReader.reset(lrRuntime.getAddressSpaceView().getLogTail());
    }

    /**
     *
     * @throws Exception
     */
    @Test
    public void testReadWithData() throws Exception {
        generateData(session);
        SnapshotReadMessage snapshotMessage = snapshotReader.read(syncRequestId);
        Assert.assertTrue(snapshotMessage.isEndRead());
    }

    @Test
    public void testReadWithoutData() throws Exception {
        // 1. Try to read when there is no data in the send queue.
        Exception e = Assert.assertThrows(ReplicationReaderException.class, ()-> snapshotReader.read(syncRequestId));
        Assert.assertTrue(e.getCause() instanceof RetryNeededException);

        // 2. Write data to a different session and read for that session, ensure the timeout window moves.
        LogReplication.LogReplicationSession anotherSession = DefaultClusterConfig.getRoutingQueueSessions().get(1);
        configManager.generateConfig(Collections.singleton(anotherSession), false);
        BaseSnapshotReader anotherSnapshotReader = new RoutingQueuesSnapshotReader(anotherSession, replicationContext);

        generateData(anotherSession);
        ((RoutingQueuesSnapshotReader) anotherSnapshotReader).setRequestSnapSyncId(snapshotReqTimestamp);
        SnapshotReadMessage differentSnapshotMessage = anotherSnapshotReader.read(syncRequestId);
        Assert.assertTrue(!differentSnapshotMessage.getMessages().isEmpty());
        // Now check if the "session" is going to timeout.
        Assert.assertFalse(((RoutingQueuesSnapshotReader) snapshotReader).receiveWindowTimedOut());

        RoutingQueuesSnapshotReader.DATA_WAIT_TIMEOUT_MS = 1000;
        //3. Sleep for the timeout window to simulate data not being seen for the timeout window.
        TimeUnit.MILLISECONDS.sleep(RoutingQueuesSnapshotReader.DATA_WAIT_TIMEOUT_MS);
        Assert.assertThrows(ReplicationReaderException.class, () -> snapshotReader.read(syncRequestId));
    }

    private void generateData(LogReplication.LogReplicationSession session) throws Exception {

        corfuStore.openTable(namespace, SNAP_SYNC_TXN_ENVELOPE_TABLE,
                Queue.RoutingQSnapSyncHeaderKeyMsg.class, Queue.RoutingQSnapSyncHeaderMsg.class, null,
                TableOptions.fromProtoSchema(Queue.RoutingQSnapSyncHeaderMsg.class));

        String tableName = LogReplicationUtils.SNAPSHOT_SYNC_QUEUE_NAME_SENDER;

        Table<Queue.CorfuGuidMsg, Queue.RoutingTableEntryMsg, Queue.CorfuQueueMetadataMsg> q =
            corfuStore.openQueue(namespace, tableName, Queue.RoutingTableEntryMsg.class,
                TableOptions.fromProtoSchema(Queue.RoutingTableEntryMsg.class));

        String streamTagFollowed = TableRegistry.getStreamTagFullStreamName(namespace,
                LogReplicationUtils.SNAPSHOT_SYNC_QUEUE_TAG_SENDER_PREFIX + session.getSinkClusterId() + "_" +
                        session.getSubscriber().getClientName());

        log.info("Stream UUID: {}, name: {}", CorfuRuntime.getStreamID(streamTagFollowed), streamTagFollowed);

        // Corfu log tail which represents the timestamp at which snapshot sync was requested
        snapshotReqTimestamp = lrRuntime.getAddressSpaceView().getLogTail();
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
                writeMarker(true, txnContext, streamTagFollowed, session);
                txnContext.commit();
            } catch (Exception e) {
                log.error("Failed to add data to the queue", e);
            }
        }
        log.info("Queue Size = {}.  Stream tags = {}", q.count(), q.getStreamTags());

        // Write the End Marker
        writeMarker(false, null, streamTagFollowed, session);
    }

    private void writeMarker(boolean start, TxnContext txnContext, String streamTagFollowed,
                             LogReplication.LogReplicationSession session) {
        Queue.RoutingQSnapSyncHeaderKeyMsg snapshotSyncId =
            Queue.RoutingQSnapSyncHeaderKeyMsg.newBuilder().setDestination(session.getSinkClusterId())
                .setSnapSyncRequestId(snapshotReqTimestamp).build();

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
        marker = Queue.RoutingQSnapSyncHeaderMsg.newBuilder().setSnapshotStartTimestamp(-snapshotReqTimestamp).build();
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
