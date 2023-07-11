package org.corfudb.infrastructure.logreplication.replication.send.logreader;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.Message;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
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
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.runtime.view.stream.OpaqueStream;
import org.corfudb.util.retry.ExponentialBackoffRetry;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.RetryNeededException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.corfudb.runtime.LogReplicationUtils.DEMO_NAMESPACE;
import static org.corfudb.runtime.LogReplicationUtils.SNAPSHOT_END_MARKER_TABLE_NAME;
import static org.corfudb.runtime.LogReplicationUtils.SNAPSHOT_SYNC_QUEUE_NAME_SENDER;
import static org.corfudb.infrastructure.logreplication.config.LogReplicationConfig.DEFAULT_MAX_DATA_MSG_SIZE;

/**
 * Snapshot reader implementation for Routing Queues Replication Model.
 */
@Slf4j
public class RoutingQueuesSnapshotReader extends BaseSnapshotReader {

    private static final long DATA_WAIT_TIMEOUT_MS = 120000;

    // UUID of the global queue which contains snapshot sync data
    private UUID snapshotSyncQueueId;

    // Stream tag which will be followed to fetch any writes to the snapshot sync queue for this destination
    private String streamTagFollowed;

    private final ExecutorService dataPoller;

    private boolean endMarkerReached = false;

    // Timestamp of the last read entry in the queue.  Every subsequent batch read will continue from this timestamp
    private long lastReadTimestamp;

    // UUID of the replicated queue on the receiver(Sink)
    private UUID replicatedQueueId;

    // UUID of the stream which contains snapshot end markers
    private UUID endMarkerStreamId;

    public RoutingQueuesSnapshotReader(CorfuRuntime corfuRuntime, LogReplicationSession session,
                                       LogReplicationContext replicationContext) {
        super(corfuRuntime, session, replicationContext);
        streamTagFollowed = LogReplicationUtils.SNAPSHOT_SYNC_QUEUE_TAG_SENDER_PREFIX + session.getSinkClusterId();

        String snapshotSyncQueueFullyQualifiedName = TableRegistry.getFullyQualifiedTableName(DEMO_NAMESPACE,
                SNAPSHOT_SYNC_QUEUE_NAME_SENDER);
        snapshotSyncQueueId = CorfuRuntime.getStreamID(snapshotSyncQueueFullyQualifiedName);

        lastReadTimestamp = snapshotTimestamp;

        dataPoller = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("snapshot-sync-data" +
            "-poller-" + session.hashCode()).build());

        String replicatedQueueName = TableRegistry.getFullyQualifiedTableName(DEMO_NAMESPACE,
                LogReplicationUtils.REPLICATED_QUEUE_NAME_PREFIX + session.getSourceClusterId());
        replicatedQueueId = CorfuRuntime.getStreamID(replicatedQueueName);

        // Open the marker table so that its entries can be deserialized
        try {
            CorfuStore corfuStore = new CorfuStore(replicationContext.getConfigManager().getRuntime());
            corfuStore.openTable(DEMO_NAMESPACE, SNAPSHOT_END_MARKER_TABLE_NAME,
                Queue.RoutingTableSnapshotEndKeyMsg.class, Queue.RoutingTableSnapshotEndMarkerMsg.class, null,
                TableOptions.fromProtoSchema(Queue.RoutingTableEntryMsg.class));
        } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            log.error("Failed to open the End Marker table", e);
            throw new RuntimeException(e);
        }

        String endMarkerTableName = TableRegistry.getFullyQualifiedTableName(DEMO_NAMESPACE,
            SNAPSHOT_END_MARKER_TABLE_NAME);
        endMarkerStreamId = CorfuRuntime.getStreamID(endMarkerTableName);

        buildOpaqueStreamIterator();
    }

    // Create an opaque stream and iterator for the stream of interest, starting from lastReadTimestamp+1 to the global
    // log tail
    private void buildOpaqueStreamIterator() {
        OpaqueStream opaqueStream =
                new OpaqueStream(rt.getStreamsView().get(CorfuRuntime.getStreamID(streamTagFollowed)));

        // Seek till the last read timestamp so that duplicate entries are eliminated.
        opaqueStream.seek(lastReadTimestamp + 1);

        long logTail = rt.getAddressSpaceView().getLogTail();

        currentStreamInfo = new OpaqueStreamIterator(opaqueStream, streamTagFollowed, logTail);

        log.info("Start reading data from {}, log tail = {}", lastReadTimestamp, logTail);
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

        log.info("Start Snapshot Sync replication for stream name={}, id={}", streamTagFollowed,
                CorfuRuntime.getStreamID(streamTagFollowed));

        // If the current opaque stream has data, read and send it
        if (currentStreamHasNext()) {
            msg = read(currentStreamInfo, syncRequestId);
            if (msg != null) {
                messages.add(msg);
            }
        } else {
            // Wait for more data to be written by the client and send it once it is received
            waitForData();
            msg = read(currentStreamInfo, syncRequestId);
            if (msg != null) {
                messages.add(msg);
            }
        }
        return new SnapshotReadMessage(messages, endMarkerReached);
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
        }
    }

    // Check if the Opaque Entry contains an End Marker, denoting the end of snapshot sync data
    private boolean isEndMarker(OpaqueEntry opaqueEntry) {
        if (!opaqueEntry.getEntries().keySet().contains(endMarkerStreamId)) {
            return false;
        }

        // There should be a single SMR update for the end marker
        Preconditions.checkState(opaqueEntry.getEntries().get(endMarkerStreamId).size() == 1);
        SMREntry smrEntry = opaqueEntry.getEntries().get(endMarkerStreamId).get(0);

        // Deserialize the entry to get the marker
        Object[] objs = smrEntry.getSMRArguments();

        ByteBuf valueBuf = Unpooled.wrappedBuffer((byte[]) objs[1]);
        CorfuRecord<Queue.RoutingTableSnapshotEndMarkerMsg, Message> record =
            (CorfuRecord<Queue.RoutingTableSnapshotEndMarkerMsg, Message>)
                replicationContext.getProtobufSerializer().deserialize(valueBuf, null);

        Preconditions.checkState(Objects.equals(record.getPayload().getDestination(), session.getSinkClusterId()));

        // TODO Pankti: Pass the sync request id and return true only if the marker is for this snapshot sync
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

        try {
            while (currentMsgSize < maxDataSizePerMsg) {
                if (lastEntry != null) {

                    // Only the entries from Routing Queue OR End Marker for Snapshot Sync must be found
                    Preconditions.checkState(lastEntry.getEntries().keySet().size() == 1);

                    // If this OpaqueEntry was for the end marker, no further processing is required.
                    if (isEndMarker(lastEntry)) {
                        endMarkerReached = true;
                        break;
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

        log.trace("CurrentMsgSize {}  maxDataSizePerMsg {}", currentMsgSize, maxDataSizePerMsg);
        return new SMREntryList(currentMsgSize, smrList);
    }

    @Override
    protected OpaqueEntry generateOpaqueEntry(long version, UUID streamID, SMREntryList entryList) {
        Map<UUID, List<SMREntry>> map = new HashMap<>();
        map.put(replicatedQueueId, entryList.getSmrEntries());
        return new OpaqueEntry(version, map);
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
                            log.info("Retrying");
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
