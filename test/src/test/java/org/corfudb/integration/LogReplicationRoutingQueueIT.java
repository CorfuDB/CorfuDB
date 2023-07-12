package org.corfudb.integration;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterConfig;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterManager;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.CorfuOptions;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.ExampleSchemas;
import org.corfudb.runtime.ExampleSchemas.ClusterUuidMsg;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.corfudb.runtime.LogReplication.ReplicationStatus;
import org.corfudb.runtime.Queue;
import org.corfudb.runtime.Queue.RoutingTableEntryMsg;
import org.corfudb.runtime.collections.CorfuRecord;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.util.serializer.ProtobufSerializer;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.runtime.LogReplicationUtils.LR_STATUS_STREAM_TAG;
import static org.corfudb.runtime.LogReplicationUtils.REPLICATED_QUEUE_NAME_PREFIX;
import static org.corfudb.runtime.LogReplicationUtils.REPLICATED_QUEUE_TAG_PREFIX;
import static org.corfudb.runtime.LogReplicationUtils.REPLICATION_STATUS_TABLE_NAME;
import static org.corfudb.runtime.LogReplicationUtils.SNAPSHOT_END_MARKER_TABLE_NAME;
import static org.corfudb.runtime.LogReplicationUtils.SNAPSHOT_SYNC_QUEUE_NAME_SENDER;
import static org.corfudb.runtime.LogReplicationUtils.SNAPSHOT_SYNC_QUEUE_TAG_SENDER_PREFIX;
import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

@Slf4j
@RunWith(Parameterized.class)
public class LogReplicationRoutingQueueIT extends LogReplicationAbstractIT {

    public LogReplicationRoutingQueueIT(Pair<String, ClusterUuidMsg> pluginAndTopologyType) {
        this.transportType = pluginAndTopologyType.getKey();
        this.topologyType = pluginAndTopologyType.getValue();
    }

    @Parameterized.Parameters
    public static Collection<Pair<String, ClusterUuidMsg>> input() {

        List<String> transportPlugins = Collections.singletonList(
                "GRPC"
        );

        List<ExampleSchemas.ClusterUuidMsg> topologyTypes = Collections.singletonList(
                DefaultClusterManager.TP_RQ_SINGLE_SOURCE_SINK
        );

        List<Pair<String, ExampleSchemas.ClusterUuidMsg>> absolutePathPlugins = new ArrayList<>();

        if(runProcess) {
            transportPlugins.stream().map(File::new).forEach(f ->
                    topologyTypes.forEach(type -> absolutePathPlugins.add(Pair.of(f.getAbsolutePath(), type))));
        } else {

            transportPlugins.forEach(f ->
                    topologyTypes.forEach(type -> absolutePathPlugins.add(Pair.of(f, type))));
        }

        return absolutePathPlugins;
    }

    /**
     * Verifies replication is occurring from source to sink for routing queue model
     * TODO: Currently only tests for snapshot queue entries.
     *
     */
    @Test
    public void testRoutingQueueLogReplicationEndToEnd() throws Exception {
        log.debug("Using plugin :: {}", transportType);

        log.info(">> Setup source and sink Corfu's");
        setupSourceAndSinkCorfu();
        initSingleSourceSinkCluster();

        // Open queue on source
        Table<Queue.CorfuGuidMsg, Queue.RoutingTableEntryMsg, Queue.CorfuQueueMetadataMsg> snapshotQueueSource;
        try {
            snapshotQueueSource = corfuStoreSource.openQueue(CORFU_SYSTEM_NAMESPACE, SNAPSHOT_SYNC_QUEUE_NAME_SENDER,
                    Queue.RoutingTableEntryMsg.class, TableOptions.fromProtoSchema(Queue.RoutingTableEntryMsg.class));
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }

        // Open end marker table on source
        corfuStoreSource.openTable(CORFU_SYSTEM_NAMESPACE, SNAPSHOT_END_MARKER_TABLE_NAME,
                Queue.RoutingTableSnapshotEndKeyMsg.class, Queue.RoutingTableSnapshotEndMarkerMsg.class, null,
                TableOptions.fromProtoSchema(Queue.RoutingTableSnapshotEndMarkerMsg.class));

        // Open queue on sink
        Table<Queue.CorfuGuidMsg, Queue.RoutingTableEntryMsg, Queue.CorfuQueueMetadataMsg> replicatedQueueSink;
        try {
            replicatedQueueSink = corfuStoreSink.openQueue(CORFU_SYSTEM_NAMESPACE,
                    String.join("", REPLICATED_QUEUE_NAME_PREFIX, DefaultClusterConfig.getSourceClusterIds().get(0)),
                    Queue.RoutingTableEntryMsg.class, TableOptions.builder().schemaOptions(CorfuOptions.SchemaOptions.newBuilder()
                            .addStreamTag(REPLICATED_QUEUE_TAG_PREFIX).build()).build());
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }

        // Set of source and sink entries to confirm data consistency after snapshot sync from negotiation
        Set<Table.CorfuQueueRecord> sourceEntries = new HashSet<>();
        Set<Table.CorfuQueueRecord> sinkEntries = new HashSet<>();

        // TODO: Update code to use client to add snapshot entries
        log.info(">> Write data to source CorfuDB before LR is started ...");
        int numEntries = 5;
        for (int i = 0; i < numEntries; i++) {
            try (TxnContext txn = corfuStoreSource.txn(CORFU_SYSTEM_NAMESPACE)) {
                byte[] payload = {(byte) i};
                List<UUID> tags = new ArrayList<>();
                tags.add(CorfuRuntime.getStreamID(String.join("", SNAPSHOT_SYNC_QUEUE_TAG_SENDER_PREFIX, DefaultClusterConfig.getSinkClusterIds().get(0))));
                txn.logUpdateEnqueue(snapshotQueueSource, RoutingTableEntryMsg.newBuilder().setOpaquePayload(ByteString.copyFrom(payload)).build(), tags, corfuStoreSource);
                txn.commit();
            } catch (Exception e) {
                log.info("Exception: ", e);
            }
        }

        // Add records to source entries set
        try (TxnContext txn = corfuStoreSource.txn(CORFU_SYSTEM_NAMESPACE)) {
            sourceEntries.addAll(txn.entryList(snapshotQueueSource));
        } catch (Exception e) {
            log.info("Exception: ", e);
        }

        log.info(">> Create and insert an end marker to signal end of snapshot messages...");
        Queue.RoutingTableSnapshotEndKeyMsg snapshotSyncId =
                Queue.RoutingTableSnapshotEndKeyMsg.newBuilder().setSnapshotSyncId(UUID.randomUUID().toString()).build();
        Queue.RoutingTableSnapshotEndMarkerMsg endMarker =
                Queue.RoutingTableSnapshotEndMarkerMsg.newBuilder().setDestination(DefaultClusterConfig.getSinkClusterIds().get(0)).build();
        CorfuRecord<Queue.RoutingTableSnapshotEndMarkerMsg, Message> record = new CorfuRecord<>(endMarker, null);

        Object[] smrArgs = new Object[2];
        smrArgs[0] = snapshotSyncId;
        smrArgs[1] = record;

        UUID endMarkerStreamId = CorfuRuntime.getStreamID(TableRegistry.getFullyQualifiedTableName(
                CORFU_SYSTEM_NAMESPACE, SNAPSHOT_END_MARKER_TABLE_NAME));

        // Add the marker to endMarkerStream
        try (TxnContext txnContext = corfuStoreSource.txn(CORFU_SYSTEM_NAMESPACE)) {
            txnContext.logUpdate(endMarkerStreamId, new SMREntry("put", smrArgs,
                            corfuStoreSource.getRuntime().getSerializers().getSerializer(ProtobufSerializer.PROTOBUF_SERIALIZER_CODE)),
                    Arrays.asList(CorfuRuntime.getStreamID(String.join("", SNAPSHOT_SYNC_QUEUE_TAG_SENDER_PREFIX, DefaultClusterConfig.getSinkClusterIds().get(0)))));
            txnContext.commit();
        } catch (Exception e) {
            log.error("Failed to add End Marker", e);
        }

        // Confirm data does exist on Source Cluster
        try (TxnContext txn = corfuStoreSource.txn(CORFU_SYSTEM_NAMESPACE)) {
            assertThat(txn.entryList(snapshotQueueSource).size()).isEqualTo(numEntries);
            txn.commit();
        } catch (Exception e) {
            log.info("Exception: ", e);
        }

        // Confirm data does not exist on Sink Cluster
        try (TxnContext txn = corfuStoreSink.txn(CORFU_SYSTEM_NAMESPACE)) {
            Assert.assertTrue(txn.entryList(replicatedQueueSink).isEmpty());
            txn.commit();
        } catch (Exception e) {
            log.info("Exception: ", e);
        }

        // Subscribe to replication status table on Source to capture the status of snapshot sync completion
        corfuStoreSink.openTable(CORFU_SYSTEM_NAMESPACE,
                REPLICATION_STATUS_TABLE_NAME,
                LogReplicationSession.class,
                ReplicationStatus.class,
                null,
                TableOptions.fromProtoSchema(ReplicationStatus.class));

        CountDownLatch statusUpdateLatch = new CountDownLatch(2);
        ReplicationStatusListener sinkStatusListener = new ReplicationStatusListener(statusUpdateLatch, false);
        corfuStoreSink.subscribeListener(sinkStatusListener, CORFU_SYSTEM_NAMESPACE, LR_STATUS_STREAM_TAG);

        // Start LR for both source and sink
        startSourceLogReplicator();
        startSinkLogReplicator();

        log.info(">> Wait ... Snapshot log replication in progress ...");
        statusUpdateLatch.await();

        // Verify data on sink and add records to sink entries set
        while (true) {
            try (TxnContext txn = corfuStoreSink.txn(CORFU_SYSTEM_NAMESPACE)) {
                if (!txn.entryList(replicatedQueueSink).isEmpty()) {
                    sinkEntries.addAll(txn.entryList(replicatedQueueSink));
                    break;
                }
                txn.commit();
            } catch (Exception e) {
                log.info("Exception: ", e);
            }
            TimeUnit.MILLISECONDS.sleep(2000);
        }

        // Check if data on sink queue is consistent with source queue
        Assert.assertEquals(sourceEntries, sinkEntries);
    }
}
