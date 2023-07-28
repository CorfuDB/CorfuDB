package org.corfudb.infrastructure.logreplication.replication.send.logreader;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.Message;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.LogReplicationContext;
import org.corfudb.infrastructure.logreplication.replication.send.IllegalSnapshotEntrySizeException;
import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LogReplication;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.corfudb.runtime.LogReplicationUtils;
import org.corfudb.runtime.Queue;
import org.corfudb.runtime.collections.CorfuRecord;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.runtime.view.stream.OpaqueStream;
import org.corfudb.util.retry.ExponentialBackoffRetry;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.IntervalRetry;
import org.corfudb.util.retry.RetryNeededException;

import java.lang.reflect.InvocationTargetException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

import static org.corfudb.infrastructure.logreplication.config.LogReplicationConfig.DEFAULT_MAX_DATA_MSG_SIZE;
import static org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager.REPLICATION_EVENT_TABLE_NAME;
import static org.corfudb.runtime.LogReplicationUtils.SNAPSHOT_SYNC_QUEUE_NAME_SENDER;
import static org.corfudb.runtime.LogReplicationUtils.SNAP_SYNC_START_END_Q_NAME;
import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

/**
 * Snapshot reader implementation for Routing Queues Replication Model.
 */
@Slf4j
public class RoutingQueuesSnapshotReader extends BaseSnapshotReader {

    private static final long DATA_WAIT_TIMEOUT_MS = 120000;

    @Getter
    private final CorfuStore corfuStore;

    // UUID of the global queue which contains snapshot sync data
    private UUID snapshotSyncQueueId;

    // Stream tag which will be followed to fetch any writes to the snapshot sync queue for this destination
    private String streamTagFollowed;

    private final ExecutorService dataPoller;

    private boolean endMarkerReached = false;

    // Timestamp of the last read entry in the queue.  Every subsequent batch read will continue from this timestamp
    private long lastReadTimestamp;

    // UUID of the replicated queue on the receiver(Sink)
    private final UUID replicatedQueueId;

    // UUID of the stream which contains snapshot end markers
    private final UUID endMarkerStreamId;

    // Boolean indicating if the Snapshot reader is waiting for a start marker from this snapshot sync
    private boolean waitingForStartMarker = true;

    public RoutingQueuesSnapshotReader(CorfuRuntime corfuRuntime, LogReplicationSession session,
                                       LogReplicationContext replicationContext) {
        super(corfuRuntime, session, replicationContext);
        streamTagFollowed = LogReplicationUtils.SNAPSHOT_SYNC_QUEUE_TAG_SENDER_PREFIX + session.getSinkClusterId();

        String snapshotSyncQueueFullyQualifiedName = TableRegistry.getFullyQualifiedTableName(CORFU_SYSTEM_NAMESPACE,
                SNAPSHOT_SYNC_QUEUE_NAME_SENDER);
        snapshotSyncQueueId = CorfuRuntime.getStreamID(snapshotSyncQueueFullyQualifiedName);

        lastReadTimestamp = snapshotTimestamp;

        dataPoller = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("snapshot-sync-data" +
            "-poller-" + session.hashCode()).build());

        String replicatedQueueName = TableRegistry.getFullyQualifiedTableName(CORFU_SYSTEM_NAMESPACE,
                LogReplicationUtils.REPLICATED_QUEUE_NAME);
        replicatedQueueId = CorfuRuntime.getStreamID(replicatedQueueName);

        // Open the marker table so that its entries can be deserialized
        try {
            this.corfuStore = new CorfuStore(replicationContext.getConfigManager().getRuntime());
            corfuStore.openTable(CORFU_SYSTEM_NAMESPACE, SNAP_SYNC_START_END_Q_NAME,
                Queue.RoutingQSnapStartEndKeyMsg.class, Queue.RoutingQSnapStartEndMarkerMsg.class, null,
                TableOptions.fromProtoSchema(Queue.RoutingTableEntryMsg.class));
            corfuStore.openTable(CORFU_SYSTEM_NAMESPACE, REPLICATION_EVENT_TABLE_NAME,
                    LogReplication.ReplicationEventInfoKey.class,
                    LogReplication.ReplicationEvent.class,
                    null,
                    TableOptions.fromProtoSchema(LogReplication.ReplicationEvent.class));
        } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            log.error("Failed to open the End Marker table", e);
            throw new RuntimeException(e);
        }

        String endMarkerTableName = TableRegistry.getFullyQualifiedTableName(CORFU_SYSTEM_NAMESPACE,
                SNAP_SYNC_START_END_Q_NAME);
        endMarkerStreamId = CorfuRuntime.getStreamID(endMarkerTableName);
    }

    // Create an opaque stream and iterator for the stream of interest, starting from lastReadTimestamp+1 to the global
    // log tail
    private void buildOpaqueStreamIterator() {
        String streamOfStreamTag = TableRegistry.getFullStreamTagStreamName(CORFU_SYSTEM_NAMESPACE, streamTagFollowed);
        UUID uuidOfStreamTagFollowed = CorfuRuntime.getStreamID(streamOfStreamTag);

        OpaqueStream opaqueStream = new OpaqueStream(rt.getStreamsView().get(uuidOfStreamTagFollowed));

        // On first snapshot sync, lastReadTs will be 0. Change it to the latest trim mark
        long trimMark = rt.getAddressSpaceView().getTrimMark().getSequence();
        if (lastReadTimestamp == 0 || lastReadTimestamp < trimMark) {
            lastReadTimestamp = trimMark;
        }

        // Seek till the last read timestamp so that duplicate entries are eliminated.
        opaqueStream.seek(lastReadTimestamp + 1);

        // long logTail = rt.getAddressSpaceView().getLogTail();
        long logTail = Address.MAX;

        log.info("Start reading data from {}, log tail = {} streamName={} uuid={}", lastReadTimestamp,
                logTail, streamOfStreamTag, uuidOfStreamTagFollowed);

        currentStreamInfo = new OpaqueStreamIterator(opaqueStream, streamOfStreamTag, logTail);
    }

    /**
     * Fetch the set of streams to replicate.
     */
    @Override
    protected void refreshStreamsToReplicateSet() {
        streams = replicationContext.getConfig(session).getStreamsToReplicate();
    }

    @Override
    public void reset(long ts) {
        streams = replicationContext.getConfig(session).getStreamsToReplicate();
        snapshotTimestamp = ts;
        waitingForStartMarker = true;
        endMarkerReached = false;
        currentStreamInfo = null;
        lastEntry = null;
    }

    /**
     * Reads data from the Snapshot Sync Queue, waiting for a predetermined time for new data to appear if there is
     * none.  This data is sent to the destination.
     * @param syncRequestId
     * @return
     */
    @Override
    public SnapshotReadMessage read(UUID syncRequestId) {
        List<LogReplication.LogReplicationEntryMsg> messages = new ArrayList<>();

        LogReplication.LogReplicationEntryMsg msg;

        String streamNameOfTag = TableRegistry.getFullStreamTagStreamName(CORFU_SYSTEM_NAMESPACE, streamTagFollowed);
        log.info("Start Snapshot Sync replication for stream name={}, id={}", streamNameOfTag,
                CorfuRuntime.getStreamID(streamNameOfTag));

        // Wait for data to be available on the Snapshot Sync Queue if it is empty
        if (currentStreamInfo == null || !currentStreamHasNext()) {
            waitForData();
        }
        msg = read(currentStreamInfo, syncRequestId);
        if (msg != null) {
            messages.add(msg);
        }
        return new SnapshotReadMessage(messages, endMarkerReached);
    }

    public void requestClientForSnapshotData(UUID snapshotRequestId) {
        log.info("RQSnapReader asking sender for full sync data session={}, sync_id={}", session, snapshotRequestId);

        // Write a force sync event to the logReplicationEventTable
        LogReplication.ReplicationEventInfoKey key = LogReplication.ReplicationEventInfoKey.newBuilder()
                .setSession(session)
                .build();

        LogReplication.ReplicationEvent event = LogReplication.ReplicationEvent.newBuilder()
                .setEventId(snapshotRequestId.toString())
                .setType(LogReplication.ReplicationEvent.ReplicationEventType.SERVER_REQUESTED_SNAPSHOT_SYNC)
                .setEventTimestamp(com.google.protobuf.Timestamp.newBuilder().setSeconds(Instant.now().getEpochSecond()).build())
                .build();

        try {
            IRetry.build(IntervalRetry.class, () -> {
                try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                    txn.putRecord(txn.getTable(REPLICATION_EVENT_TABLE_NAME), key, event, null);
                    lastReadTimestamp = txn.commit().getSequence(); // set last read event to what is requested
                } catch (TransactionAbortedException tae) {
                    log.warn("TXAbort while adding enforce snapshot sync event, retrying", tae);
                    throw new RetryNeededException();
                }
                log.debug("Added enforce snapshot sync event to the logReplicationEventTable for session={}", session);
                return null;
            }).run();
        } catch (InterruptedException e) {
            log.error("Unrecoverable exception while adding enforce snapshot sync event", e);
            throw new UnrecoverableCorfuInterruptedError(e);
        }
    }

    // Waits for a default timeout period for data to get written for this destination on the Snapshot Sync queue
    private void waitForData() {
        // Create a task to wait for more data until DATA_WAIT_TIMEOUT_MS
        StreamQueryTask streamQueryTask = new StreamQueryTask();
        Future<Void> queryFuture = dataPoller.submit(streamQueryTask);

        try {
            queryFuture.get(DATA_WAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            throw new RuntimeException("Timed out waiting for data or end marker for Snapshot Sync");
        } catch (Exception e) {
            // Handle all other types of exceptions
            log.error("Caught exception in WaitForData ", e);
        }
    }

    // Check if the Opaque Entry contains an End Marker, denoting the end of snapshot sync data
    private boolean isEndMarker(OpaqueEntry opaqueEntry) {
        if (!opaqueEntry.getEntries().keySet().contains(endMarkerStreamId)) {
            return false;
        }
        log.info("isEndMarker is called from next of snapshot reader");
        // There should be a single SMR update for the end marker
        Preconditions.checkState(opaqueEntry.getEntries().get(endMarkerStreamId).size() == 1);
        SMREntry smrEntry = opaqueEntry.getEntries().get(endMarkerStreamId).get(0);

        // Deserialize the entry to get the marker
        Object[] objs = smrEntry.getSMRArguments();

        ByteBuf valueBuf = Unpooled.wrappedBuffer((byte[]) objs[1]);
        CorfuRecord<Queue.RoutingQSnapStartEndMarkerMsg, Message> record =
            (CorfuRecord<Queue.RoutingQSnapStartEndMarkerMsg, Message>)
                replicationContext.getProtobufSerializer().deserialize(valueBuf, null);

        Preconditions.checkState(Objects.equals(record.getPayload().getDestination(), session.getSinkClusterId()));

        // TODO Pankti: Pass the sync request id and return true only if the marker is for this snapshot sync
        return (record.getPayload().getSnapshotStartTimestamp() == 0);
    }

    private void processStartMarker(OpaqueEntry opaqueEntry) {
        log.info("start is called from next of snapshot reader");
        // If waiting for a start marker and the first entry does not contain a start marker, throw an exception
        if (!opaqueEntry.getEntries().keySet().contains(endMarkerStreamId)) {
            throw new IllegalStateException("Waiting for a Start Marker but None Was Found");
        }

        // There should be a single SMR update for the start marker
        Preconditions.checkState(opaqueEntry.getEntries().get(endMarkerStreamId).size() == 1);
        SMREntry smrEntry = opaqueEntry.getEntries().get(endMarkerStreamId).get(0);

        // Deserialize the entry to get the marker
        Object[] objs = smrEntry.getSMRArguments();

        ByteBuf valueBuf = Unpooled.wrappedBuffer((byte[]) objs[1]);
        CorfuRecord<Queue.RoutingQSnapStartEndMarkerMsg, Message> record =
            (CorfuRecord<Queue.RoutingQSnapStartEndMarkerMsg, Message>)
                replicationContext.getProtobufSerializer().deserialize(valueBuf, null);

        Preconditions.checkState(Objects.equals(record.getPayload().getDestination(), session.getSinkClusterId()));

        // Update the base snapshot timestamp
        snapshotTimestamp = record.getPayload().getSnapshotStartTimestamp();

        // TODO Pankti: Pass the sync request id and check if the marker corresponds to this snapshot sync
    }

    /**
     * Constructs a list of SMR entries to be sent to the destination.  Max size of this list is 'maxDataSizePerMsg'
     * @param stream Iterator over the Opaque Stream
     * @return List of SMR entries
     */
    @Override
    protected SMREntryList next(OpaqueStreamIterator stream) {
        List<SMREntry> smrList = new ArrayList<>();
        int currentMsgSize = 0;
        log.info("Enter next of Snapshot reader");

        try {
            while (currentMsgSize < maxDataSizePerMsg) {
                if (lastEntry != null) {

                    // Maximum 2 tables entries should be found - Snapshot Sync Routing Queue and Start/End Marker table
                    Preconditions.checkState(lastEntry.getEntries().keySet().size() <= 2);

                    // If this OpaqueEntry was for the end marker, no further processing is required.
                    if (isEndMarker(lastEntry)) {
                        log.info("RQSnapReader found end marker {}", lastEntry.getVersion());
                        endMarkerReached = true;
                        waitingForStartMarker = true;
                        break;
                    }

                    // Process the start marker first if waiting for it
                    if (waitingForStartMarker) {
                        processStartMarker(lastEntry);
                        waitingForStartMarker = false;
                        endMarkerReached = false;
                        log.info("RQSnapReader found start marker {}", lastEntry.getVersion());
                    }

                    List<SMREntry> smrEntries = lastEntry.getEntries().get(snapshotSyncQueueId);
                    if (smrEntries != null) {
                        int currentEntrySize = ReaderUtility.calculateSize(smrEntries);

                        if (currentEntrySize > DEFAULT_MAX_DATA_MSG_SIZE) {
                            log.error("The current entry size {} is bigger than the maxDataSizePerMsg {} supported",
                                currentEntrySize, DEFAULT_MAX_DATA_MSG_SIZE);
                            throw new IllegalSnapshotEntrySizeException(" The snapshot entry is bigger than the system supported");
                        } else if (currentEntrySize > maxDataSizePerMsg) {
                            observeBiggerMsg.setValue(observeBiggerMsg.getValue()+1);
                            log.warn("The current entry size {} is bigger than the configured maxDataSizePerMsg {}",
                                currentEntrySize, maxDataSizePerMsg);
                        }

                        // Skip append this entry in this message. Will process it first at the next round.
                        if (currentEntrySize + currentMsgSize > maxDataSizePerMsg && currentMsgSize != 0) {
                            break;
                        }

                        smrList.addAll(smrEntries);
                        currentMsgSize += currentEntrySize;
                        stream.maxVersion = Math.max(stream.maxVersion, lastEntry.getVersion());
                        lastReadTimestamp = stream.maxVersion;
                    }
                    lastEntry = null;
                }

                if (stream.iterator.hasNext()) {
                    lastEntry = (OpaqueEntry) stream.iterator.next();
                }

                if (lastEntry == null) {
                    break;
                }
            }
        } catch (TrimmedException e) {
            log.error("Caught a TrimmedException", e);
            throw e;
        }

        log.info("CurrentMsgSize {}  maxDataSizePerMsg {}", currentMsgSize, maxDataSizePerMsg);
        return new SMREntryList(currentMsgSize, smrList);
    }

    @Override
    protected OpaqueEntry generateOpaqueEntry(long version, UUID streamID, SMREntryList entryList) {
        Map<UUID, List<SMREntry>> map = new HashMap<>();
        map.put(replicatedQueueId, entryList.getSmrEntries());
        return new OpaqueEntry(version, map);
    }

    @Override
    public String toString() {
        return "RoutingQueuesSnapshotReader{" +
                "corfuStore=" + corfuStore +
                ", snapshotSyncQueueId=" + snapshotSyncQueueId +
                ", streamTagFollowed='" + streamTagFollowed + '\'' +
                ", dataPoller=" + dataPoller +
                ", endMarkerReached=" + endMarkerReached +
                ", lastReadTimestamp=" + lastReadTimestamp +
                ", replicatedQueueId=" + replicatedQueueId +
                ", endMarkerStreamId=" + endMarkerStreamId +
                ", waitingForStartMarker=" + waitingForStartMarker +
                ", maxDataSizePerMsg=" + maxDataSizePerMsg +
                ", rt=" + rt +
                ", snapshotTimestamp=" + snapshotTimestamp +
                ", streams=" + streams +
                ", currentStreamInfo=" + currentStreamInfo +
                ", lastEntry=" + lastEntry +
                ", observeBiggerMsg=" + observeBiggerMsg +
                ", session=" + session +
                ", replicationContext=" + replicationContext +
                '}';
    }

    // Internal Utility class which polls for new data to be available on the Sender Routing Queue.  It uses
    // Exponential Backoff mechanism to wait between subsequent retries.
    private class StreamQueryTask implements Callable<Void> {

        @Override
        public Void call() {
            try {
                IRetry.build(ExponentialBackoffRetry.class, () -> {
                    try {
                        buildOpaqueStreamIterator();
                        if (!currentStreamHasNext()) {
                            log.info("Retrying. currentStreamInfo={}", currentStreamInfo.maxVersion);
                            throw new RuntimeException("Retry");
                        }
                    } catch (Exception e) {
                        throw new RetryNeededException();
                    }
                    return null;
                }).run();
            } catch (InterruptedException ie) {
                throw new RuntimeException("Interrupted Exception");
            }
            return null;
        }
    }
}
