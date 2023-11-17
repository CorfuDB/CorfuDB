package org.corfudb.infrastructure.logreplication.replication.send.logreader;

import org.corfudb.infrastructure.logreplication.infrastructure.LogReplicationContext;
import org.corfudb.infrastructure.logreplication.replication.send.IllegalSnapshotEntrySizeException;
import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LogReplication;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.corfudb.runtime.Queue;
import org.corfudb.runtime.collections.CorfuRecord;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStoreEntry;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.runtime.view.stream.OpaqueStream;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.IntervalRetry;
import org.corfudb.util.retry.RetryNeededException;

import static org.corfudb.infrastructure.logreplication.config.LogReplicationConfig.DEFAULT_MAX_DATA_MSG_SIZE;
import static org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager.REPLICATION_EVENT_TABLE_NAME;
import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;
import static org.corfudb.runtime.LogReplicationUtils.REPLICATED_RECV_Q_PREFIX;
import static org.corfudb.runtime.LogReplicationUtils.SNAPSHOT_SYNC_QUEUE_TAG_SENDER_PREFIX;
import static org.corfudb.runtime.LogReplicationUtils.SNAPSHOT_SYNC_QUEUE_NAME_SENDER;
import static org.corfudb.runtime.LogReplicationUtils.SNAP_SYNC_TXN_ENVELOPE_TABLE;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.Message;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;


/**
 * Snapshot reader implementation for Routing Queues Replication Model.
 */
@Slf4j
public class RoutingQueuesSnapshotReader extends BaseSnapshotReader {

    // Timeout value for which the reader waits for new data to arrive from the Client.  Once the timeout is
    // exceeded, the current snapshot sync gets cancelled and a new one is started all over again.
    // TODO: The timeout is currently set to 2s.  This can be revisited later to determine a more appropriate
    //  value.
    @VisibleForTesting
    public static long DATA_WAIT_TIMEOUT_MS = 120000;

    private static long lastDataReceivedEpoch = 0;

    @Getter
    private final CorfuStore corfuStore;

    // UUID of the global queue which contains snapshot sync data
    private final UUID snapshotSyncQueueId;

    // Stream tag which will be followed to fetch any writes to the snapshot sync queue for this destination
    private final String streamTagFollowed;

    private final ExecutorService dataPoller;

    private boolean endMarkerReached = false;

    // Timestamp of the last read entry in the queue.  Every subsequent batch read will continue from this timestamp
    private long lastReadTimestamp;

    // UUID of the replicated queue on the receiver(Sink)
    private final UUID replicatedQueueId;

    // UUID of the stream which contains snapshot end markers
    private final UUID snapSyncHeaderStreamId;

    @VisibleForTesting
    @Setter
    private long requestSnapSyncId = 0;

    private OpaqueStream opaqueStream = null;

    public RoutingQueuesSnapshotReader(LogReplicationSession session,
                                       LogReplicationContext replicationContext) {
        super(session, replicationContext);
        streamTagFollowed = SNAPSHOT_SYNC_QUEUE_TAG_SENDER_PREFIX + session.getSinkClusterId() + "_" +
                session.getSubscriber().getClientName();

        String snapshotSyncQueueFullyQualifiedName = TableRegistry.getFullyQualifiedTableName(CORFU_SYSTEM_NAMESPACE,
                SNAPSHOT_SYNC_QUEUE_NAME_SENDER);
        snapshotSyncQueueId = CorfuRuntime.getStreamID(snapshotSyncQueueFullyQualifiedName);

        lastReadTimestamp = snapshotTimestamp;

        dataPoller = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("snapshot-sync-data" +
            "-poller-" + session.hashCode()).build());

        String replicatedQueueName = TableRegistry.getFullyQualifiedTableName(CORFU_SYSTEM_NAMESPACE,
                REPLICATED_RECV_Q_PREFIX + session.getSourceClusterId() + "_" +
                session.getSubscriber().getClientName());
        replicatedQueueId = CorfuRuntime.getStreamID(replicatedQueueName);

        // Open the marker table so that its entries can be deserialized
        try {
            this.corfuStore = new CorfuStore(replicationContext.getConfigManager().getRuntime());
            corfuStore.openTable(CORFU_SYSTEM_NAMESPACE, SNAP_SYNC_TXN_ENVELOPE_TABLE,
                Queue.RoutingQSnapSyncHeaderKeyMsg.class, Queue.RoutingQSnapSyncHeaderMsg.class, null,
                TableOptions.fromProtoSchema(Queue.RoutingTableEntryMsg.class));
            corfuStore.openTable(CORFU_SYSTEM_NAMESPACE, REPLICATION_EVENT_TABLE_NAME,
                    LogReplication.ReplicationEventInfoKey.class,
                    LogReplication.ReplicationEvent.class,
                    null,
                    TableOptions.fromProtoSchema(LogReplication.ReplicationEvent.class));
        } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            log.error("Failed to open the End Marker table", e);
            throw new ReplicationReaderException(e);
        }

        String endMarkerTableName = TableRegistry.getFullyQualifiedTableName(CORFU_SYSTEM_NAMESPACE,
                SNAP_SYNC_TXN_ENVELOPE_TABLE);
        snapSyncHeaderStreamId = CorfuRuntime.getStreamID(endMarkerTableName);
    }

    // Create an opaque stream and iterator for the stream of interest, starting from lastReadTimestamp+1 to the global
    // log tail
    private void buildOpaqueStreamIterator() {
        String streamOfStreamTag = TableRegistry.getStreamTagFullStreamName(CORFU_SYSTEM_NAMESPACE, streamTagFollowed);
        UUID uuidOfStreamTagFollowed = CorfuRuntime.getStreamID(streamOfStreamTag);

        if (opaqueStream == null) {
            opaqueStream = new OpaqueStream(rt.getStreamsView().get(uuidOfStreamTagFollowed));
        }

        // On first snapshot sync, lastReadTs will be 0. Change it to the latest trim mark
        long trimMark = rt.getAddressSpaceView().getTrimMark().getSequence();
        if (lastReadTimestamp == 0 || lastReadTimestamp < trimMark) {
            lastReadTimestamp = trimMark;
        }

        // Seek till the last read timestamp so that duplicate entries are eliminated.
        opaqueStream.seek(lastReadTimestamp + 1);

        // long logTail = rt.getAddressSpaceView().getLogTail();
        long logTail = Address.MAX;

        log.info("Check for data from {}, log tail = {} streamName={} uuid={}", lastReadTimestamp,
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
        endMarkerReached = false;
        currentStreamInfo = null;
        lastEntry = null;
        requestSnapSyncId = 0;
    }

    /**
     * Reads data from the Snapshot Sync Queue. If data is not found, retry again after a fixed delay.
     * This data is sent to the destination.
     * @param syncRequestId
     * @return
     */
    @Override
    public SnapshotReadMessage read(UUID syncRequestId) {
        List<LogReplication.LogReplicationEntryMsg> messages = new ArrayList<>();

        LogReplication.LogReplicationEntryMsg msg;

        String streamNameOfTag = TableRegistry.getStreamTagFullStreamName(CORFU_SYSTEM_NAMESPACE, streamTagFollowed);
        log.info("Start Snapshot Sync replication for stream name={}, id={}", streamNameOfTag,
                CorfuRuntime.getStreamID(streamNameOfTag));

        // If data is not present in the snapshot sync queue:
        // (1) check if data for any session was received in the last time window. If not, request for snapshot sync
        // data from the client again.
        // (2) If within the timeout window, enqueue a snapshot_continue event. This would mean that the client is
        // providing data for other sessions, and is yet to start generating for the current session.
        if (currentStreamInfo == null || !currentStreamHasNext()) {
            if (!dataFound() && receiveWindowTimedOut()) {
                throw new ReplicationReaderException("Timed out waiting for data or end marker for Snapshot Sync", new TimeoutException());
            } else if (!dataFound()) {
                return new SnapshotReadMessage(messages, endMarkerReached, true);
            }
        }
        msg = read(currentStreamInfo, syncRequestId);
        if (msg != null) {
            messages.add(msg);
        }
        return new SnapshotReadMessage(messages, endMarkerReached);
    }

    /**
     * If LR server for any reason requested multiple snapshot syncs from client then it is possible that
     * the client is providing multiple snapshot syncs in the same stream at the same time.
     * To disambiguate one snapshot sync response from another, stamp each snapshot sync message
     * with the LR Server's request (Destination+logTail at time of request)
     * @param snapshotRequestId
     */
    public void requestClientForSnapshotData(UUID snapshotRequestId) {
        // Write a force sync event to the logReplicationEventTable
        LogReplication.ReplicationEventInfoKey key = LogReplication.ReplicationEventInfoKey.newBuilder()
                .setSession(session)
                .build();
        try {
            IRetry.build(IntervalRetry.class, () -> {
                try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                    this.requestSnapSyncId = txn.getTxnSequence();
                    log.info("RQSnapReader asking sender for snapshot sync data session={}, sync_id={}", session,
                            requestSnapSyncId);
                    LogReplication.ReplicationEvent event = LogReplication.ReplicationEvent.newBuilder()
                            .setEventId(snapshotRequestId.toString())
                            .setType(LogReplication.ReplicationEvent.ReplicationEventType.SERVER_REQUESTED_SNAPSHOT_SYNC)
                            .setEventTimestamp(com.google.protobuf.Timestamp.newBuilder().setSeconds(requestSnapSyncId).build())
                            .build();
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
        lastDataReceivedEpoch = System.currentTimeMillis();
    }

    private boolean dataFound() {
        try {
            buildOpaqueStreamIterator();
            if (currentStreamHasNext()) {
                return true;
            }
        } catch (Exception e) {
            log.error("Unexpected exception caught. ", e);
        }

        log.info("Retrying. currentStreamInfo={}", currentStreamInfo.maxVersion);
        return false;
    }

    @VisibleForTesting
    public boolean receiveWindowTimedOut() {
        // This for unit test. In real scenarios, lastDataReceivedEpoch would not be 0 when we reach here.
        if (lastDataReceivedEpoch == 0) {
            lastDataReceivedEpoch = System.currentTimeMillis();
        }
        return System.currentTimeMillis() - lastDataReceivedEpoch >= DATA_WAIT_TIMEOUT_MS;
    }

    private CorfuStoreEntry<Queue.RoutingQSnapSyncHeaderKeyMsg, Queue.RoutingQSnapSyncHeaderMsg, Message>
    getSnapSyncTxnHeader(OpaqueEntry opaqueEntry) {
        Preconditions.checkState(opaqueEntry.getEntries().get(snapSyncHeaderStreamId).size() == 1);
        SMREntry smrEntry = opaqueEntry.getEntries().get(snapSyncHeaderStreamId).get(0);

        // Deserialize the entry to get the marker
        Object[] objs = smrEntry.getSMRArguments();

        ByteBuf rawBuf = Unpooled.wrappedBuffer((byte[]) objs[0]);
        Queue.RoutingQSnapSyncHeaderKeyMsg key = (Queue.RoutingQSnapSyncHeaderKeyMsg)
                replicationContext.getProtobufSerializer().deserialize(rawBuf, null);
        Preconditions.checkState(Objects.equals(key.getDestination(), session.getSinkClusterId()));

        if (key.getSnapSyncRequestId() != requestSnapSyncId) {
            log.warn("Ignoring an older snapshot sync request {} for destination {}. Looking for {}",
                    key.getSnapSyncRequestId(), key.getDestination(), requestSnapSyncId);
            return null;
        }

        rawBuf = Unpooled.wrappedBuffer((byte[]) objs[1]);
        CorfuRecord<Queue.RoutingQSnapSyncHeaderMsg, Message> entry =
                (CorfuRecord<Queue.RoutingQSnapSyncHeaderMsg, Message>)
                        replicationContext.getProtobufSerializer().deserialize(rawBuf, null);
        return new CorfuStoreEntry<>(key, entry.getPayload(), entry.getMetadata());
    }

    // Check if the Opaque Entry contains an End Marker, denoting the end of snapshot sync data
    private boolean isEndMarker(OpaqueEntry opaqueEntry) {
        CorfuStoreEntry<Queue.RoutingQSnapSyncHeaderKeyMsg, Queue.RoutingQSnapSyncHeaderMsg, Message> entry =
                getSnapSyncTxnHeader(opaqueEntry);
        if (entry == null) {
            return false;
        }

        log.info("isEndMarker is called from next of snapshot reader. snapshot sync request {} {}", entry.getKey(), entry.getPayload());
        // The protocol is the end marker is represented by the Full Sync Request Id but negated.
        return entry.getPayload().getSnapshotStartTimestamp() == -requestSnapSyncId;
    }

    private boolean validateSnapSyncEntry(OpaqueEntry opaqueEntry) {
        CorfuStoreEntry<Queue.RoutingQSnapSyncHeaderKeyMsg, Queue.RoutingQSnapSyncHeaderMsg, Message> entry =
                getSnapSyncTxnHeader(opaqueEntry);
        if (entry == null) {
            return false;
        }

        log.info("Processing snapshot sync request {} {}", entry.getKey(), entry.getPayload());
        // Update the base snapshot timestamp
        snapshotTimestamp = entry.getPayload().getSnapshotStartTimestamp();
        return true;
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
            while (currentMsgSize < maxTransferSize) {
                if (lastEntry != null) {

                    // Maximum 2 tables entries should be found - Snapshot Sync Routing Queue and Start/End Marker table
                    Preconditions.checkState(lastEntry.getEntries().keySet().size() <= 2);

                    // If this OpaqueEntry was for the end marker, no further processing is required.
                    if (isEndMarker(lastEntry)) {
                        log.info("RQSnapReader found end marker {}", lastEntry.getVersion());
                        endMarkerReached = true;
                        break;
                    }

                    if (validateSnapSyncEntry(lastEntry)) { // returns true if entry belongs to current snapshot sync
                        endMarkerReached = false;
                        log.info("RQSnapReader validated entry address {}", lastEntry.getVersion());
                        List<SMREntry> smrEntries = lastEntry.getEntries().get(snapshotSyncQueueId);
                        if (smrEntries != null) {
                            int currentEntrySize = ReaderUtility.calculateSize(smrEntries);

                            if (currentEntrySize > DEFAULT_MAX_DATA_MSG_SIZE) {
                                log.error("The current entry size {} is bigger than the maxDataSizePerMsg {} supported",
                                        currentEntrySize, DEFAULT_MAX_DATA_MSG_SIZE);
                                throw new IllegalSnapshotEntrySizeException(" The snapshot entry is bigger than the system supported");
                            } else if (currentEntrySize > maxTransferSize) {
                                observeBiggerMsg.setValue(observeBiggerMsg.getValue() + 1);
                                log.warn("The current entry size {} is bigger than the configured maxDataSizePerMsg {}",
                                        currentEntrySize, maxTransferSize);
                            }

                            // Skip append this entry in this message. Will process it first at the next round.
                            if (currentEntrySize + currentMsgSize > maxTransferSize && currentMsgSize != 0) {
                                break;
                            }

                            smrList.addAll(smrEntries);
                            currentMsgSize += currentEntrySize;
                            stream.maxVersion = Math.max(stream.maxVersion, lastEntry.getVersion());
                            lastReadTimestamp = stream.maxVersion;
                        }
                        lastEntry = null;
                    }
                } // else this snapshot sync message possibly belongs to an older request made to the client, so ignore

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

        log.info("CurrentMsgSize {}  maxDataSizePerMsg {}", currentMsgSize, maxTransferSize);
        lastDataReceivedEpoch = System.currentTimeMillis();
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
                ", endMarkerStreamId=" + snapSyncHeaderStreamId +
                ", maxDataSizePerMsg=" + maxTransferSize +
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
}
