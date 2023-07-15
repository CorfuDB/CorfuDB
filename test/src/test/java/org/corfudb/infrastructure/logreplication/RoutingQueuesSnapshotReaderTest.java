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
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.util.serializer.ProtobufSerializer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.corfudb.runtime.LogReplicationUtils.SNAP_SYNC_START_END_Q_NAME;
import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;
import static org.junit.jupiter.api.Assertions.fail;

@Slf4j
public class RoutingQueuesSnapshotReaderTest extends AbstractViewTest {

    private UUID syncRequestId;
    private BaseSnapshotReader snapshotReader;
    private CorfuRuntime lrRuntime;
    private CorfuRuntime clientRuntime;
    private LogReplication.LogReplicationSession session;
    private LogReplicationConfigManager configManager;
    private LogReplicationContext replicationContext;
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

        configManager = new LogReplicationConfigManager(lrRuntime, session.getSourceClusterId());
        replicationContext = new LogReplicationContext(configManager, 5, session.getSourceClusterId(),
                true, new LogReplicationPluginConfig(""));
        configManager.generateConfig(Collections.singleton(session));
        snapshotReader = new RoutingQueuesSnapshotReader(lrRuntime, session, replicationContext);
        snapshotReader.reset(lrRuntime.getAddressSpaceView().getLogTail());

        streamTagFollowed = LogReplicationUtils.SNAPSHOT_SYNC_QUEUE_TAG_SENDER_PREFIX + session.getSinkClusterId();
    }

    @Test
    public void testRead() throws Exception {
        generateData(0);
        SnapshotReadMessage snapshotMessage = snapshotReader.read(syncRequestId);
        Assert.assertTrue(snapshotMessage.isEndRead());
    }


    @Test
    public void testConcurrentDataGenerationAndRead() throws Exception {
        List<SnapshotReadMessage> messagesRead = new ArrayList<>();

        scheduleConcurrently(f -> {
            for (int i = 0; i < 3; i++) {
                generateData(i*10);
            }
        });

        scheduleConcurrently(f -> {
            boolean endMarkerFound = false;
            while (!endMarkerFound) {
                SnapshotReadMessage messageRead = snapshotReader.read(syncRequestId);
                messagesRead.add(messageRead);
                endMarkerFound = messageRead.isEndRead();
            }
        });
        executeScheduled(2, PARAMETERS.TIMEOUT_NORMAL);

        Assert.assertTrue(messagesRead.get(messagesRead.size() - 1).isEndRead());
    }

    @Test
    public void testEmptySnapshotRead() {
        generateEmptySnapshot();
        SnapshotReadMessage snapshotMessage = snapshotReader.read(syncRequestId);
        Assert.assertTrue(snapshotMessage.isEndRead());
    }

    private void generateData(int start) throws Exception {

        Table<Queue.RoutingQSnapStartEndKeyMsg, Queue.RoutingQSnapStartEndMarkerMsg, Message> endMarkerTable =
            corfuStore.openTable(namespace, SNAP_SYNC_START_END_Q_NAME,
                Queue.RoutingQSnapStartEndKeyMsg.class, Queue.RoutingQSnapStartEndMarkerMsg.class, null,
                TableOptions.fromProtoSchema(Queue.RoutingQSnapStartEndMarkerMsg.class));

        String tableName = LogReplicationUtils.SNAPSHOT_SYNC_QUEUE_NAME_SENDER;

        Table<Queue.CorfuGuidMsg, Queue.RoutingTableEntryMsg, Queue.CorfuQueueMetadataMsg> q =
            corfuStore.openQueue(namespace, tableName, Queue.RoutingTableEntryMsg.class,
                TableOptions.fromProtoSchema(Queue.RoutingTableEntryMsg.class));

        log.info("Stream UUID: {}", CorfuRuntime.getStreamID(streamTagFollowed));

        for (int i = start; i < (start + 10); i++) {
            ByteBuffer buffer = ByteBuffer.allocate(Integer.SIZE);
            buffer.putInt(i);
            Queue.RoutingTableEntryMsg val =
                Queue.RoutingTableEntryMsg.newBuilder().setOpaquePayload(ByteString.copyFrom(buffer.array()))
                    .build();

            try (TxnContext txnContext = corfuStore.txn(namespace)) {
                txnContext.logUpdateEnqueue(q, val, Arrays.asList(CorfuRuntime.getStreamID(streamTagFollowed)),
                    corfuStore);
                if (i == 0) {
                    writeMarker(true, txnContext.getTxnSequence(), txnContext);
                }
                txnContext.commit();
            } catch (Exception e) {
                log.error("Failed to add data to the queue", e);
                fail("Failed to add data to the queue", e);
            }
        }
        log.info("Queue Size = {}.  Stream tags = {}", q.count(), q.getStreamTags());

        // Write the End Marker
        writeMarker(false, Address.NON_ADDRESS, null);
    }

    private void writeMarker(boolean start, long timestamp, TxnContext txnContext) {
        Queue.RoutingQSnapStartEndKeyMsg snapshotSyncId =
            Queue.RoutingQSnapStartEndKeyMsg.newBuilder().setSnapshotSyncId(syncRequestId.toString()).build();

        Queue.RoutingQSnapStartEndMarkerMsg marker =
            Queue.RoutingQSnapStartEndMarkerMsg.newBuilder().setDestination(session.getSinkClusterId()).build();

        if (start) {
            marker.toBuilder().setSnapshotStartTimestamp(timestamp).build();
        }

        CorfuRecord<Queue.RoutingQSnapStartEndMarkerMsg, Message> record = new CorfuRecord<>(marker, null);

        Object[] smrArgs = new Object[2];
        smrArgs[0] = snapshotSyncId;
        smrArgs[1] = record;

        UUID endMarkerStreamId = CorfuRuntime.getStreamID(TableRegistry.getFullyQualifiedTableName(
            namespace, SNAP_SYNC_START_END_Q_NAME));

        // Start marker is written in the same transaction as the data.  So reuse the transaction which was passed
        if (start) {
            txnContext.logUpdate(endMarkerStreamId, new SMREntry("put", smrArgs,
                    corfuStore.getRuntime().getSerializers().getSerializer(ProtobufSerializer.PROTOBUF_SERIALIZER_CODE)),
                Arrays.asList(CorfuRuntime.getStreamID(streamTagFollowed)));
            return;
        }

        // End marker is written in a separate transaction
        try (TxnContext txn = corfuStore.txn(namespace)) {
            txn.logUpdate(endMarkerStreamId, new SMREntry("put", smrArgs,
                    corfuStore.getRuntime().getSerializers().getSerializer(ProtobufSerializer.PROTOBUF_SERIALIZER_CODE)),
                Arrays.asList(CorfuRuntime.getStreamID(streamTagFollowed)));
            txn.commit();
        } catch (Exception e) {
            log.error("Failed to add the End Marker", e);
            fail("Failed to add the End Marker", e);
        }
    }

    private void generateEmptySnapshot() {
        try (TxnContext txnContext = corfuStore.txn(namespace)) {
            writeMarker(true, txnContext.getTxnSequence(), txnContext);
            txnContext.commit();
        } catch (Exception e) {
            log.error("Failed to write the Start Marker", e);
            fail("Failed to write the Start Marker", e);
        }
        writeMarker(false, Address.NON_ADDRESS, null);
    }

    @After
    public void cleanUp() {
        lrRuntime.shutdown();
        clientRuntime.shutdown();
    }
}
