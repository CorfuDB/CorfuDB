package org.corfudb.infrastructure.logreplication.replication.send.logreader;

import com.google.common.base.Preconditions;
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
import org.corfudb.runtime.Queue.RoutingTableEntryMsg;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.view.stream.OpaqueStream;
import org.corfudb.util.retry.ExponentialBackoffRetry;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.RetryNeededException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.corfudb.infrastructure.logreplication.config.LogReplicationConfig.DEFAULT_MAX_DATA_MSG_SIZE;

/**
 * Snapshot reader implementation for Routing Queues Replication Model.
 */
@Slf4j
public class RoutingQueuesSnapshotReader extends BaseSnapshotReader {

    private static final long DATA_WAIT_TIMEOUT_MS = 120000;

    private String streamToReplicate;

    private UUID snapshotSyncQueueId;

    private ExecutorService executorService = Executors.newSingleThreadExecutor();

    private boolean endMarkerReached = false;

    private long lastReadTimestamp;

    public RoutingQueuesSnapshotReader(CorfuRuntime corfuRuntime, LogReplicationSession session,
                                       LogReplicationContext replicationContext) {
        super(corfuRuntime, session, replicationContext);
        streamToReplicate = LogReplicationUtils.SNAPSHOT_SYNC_QUEUE_TAG_SENDER_PREFIX + session.getSinkClusterId();
        snapshotSyncQueueId = CorfuRuntime.getStreamID(LogReplicationUtils.SNAPSHOT_SYNC_QUEUE_NAME_SENDER);
        lastReadTimestamp = snapshotTimestamp;
    }

    @Override
    protected void refreshStreamsToReplicateSet() {
        throw new IllegalStateException("Unexpected workflow encountered.  Stream UUIDs cannot be refreshed for this " +
            "model");
    }

    @Override
    public void reset(long ts) {
        // In addition to setting the snapshot timestamp to ts, write to the table subscribed to by the client
        // requesting for a snapshot sync
    }

    @Override
    public SnapshotReadMessage read(UUID syncRequestId) {
        List<LogReplication.LogReplicationEntryMsg> messages = new ArrayList<>();

        LogReplication.LogReplicationEntryMsg msg;

        log.info("Start Snapshot Sync replication for stream name={}, id={}", streamToReplicate,
            CorfuRuntime.getStreamID(streamToReplicate));

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

    private void waitForData() {
        // Create an opaque stream on snapshot sync stream for this destination and seek upto the timestamp at which
        // snapshot was taken + 1
        OpaqueStream opaqueStream =
            new OpaqueStream(rt.getStreamsView().get(CorfuRuntime.getStreamID(streamToReplicate)));
        opaqueStream.seek(lastReadTimestamp + 1);

        // Create a task to wait for more data until DATA_WAIT_TIMEOUT_MS
        StreamQueryTask streamQueryTask = new StreamQueryTask(opaqueStream);
        Future<Void> queryFuture = executorService.submit(streamQueryTask);

        try {
            queryFuture.get(DATA_WAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            throw new RuntimeException("Timed out waiting for data or end marker for Snapshot Sync");
        } catch (Exception e) {
            // Handle all other types of exceptions
        }
    }

    private boolean isEndMarker(OpaqueEntry opaqueEntry) {

        Preconditions.checkState(opaqueEntry.getEntries().get(snapshotSyncQueueId).size() == 1);
        SMREntry smrEntry = opaqueEntry.getEntries().get(snapshotSyncQueueId).get(0);

        // Deserialize the entry to get the marker
        Object[] objs = smrEntry.getSMRArguments();
        ByteBuf valueBuf = Unpooled.wrappedBuffer((byte[]) objs[1]);
        RoutingTableEntryMsg routingTableEntryMsg =
            (RoutingTableEntryMsg) replicationContext.getProtobufSerializer().deserialize(valueBuf, null);

        return (routingTableEntryMsg.getIsSnapshotEnd());
    }

    @Override
    protected SMREntryList next(OpaqueStreamIterator stream) {
        List<SMREntry> smrList = new ArrayList<>();
        int currentMsgSize = 0;

        try {
            while (currentMsgSize < maxDataSizePerMsg) {
                if (lastEntry != null) {

                    // Only the entries from Routing Queue for Snapshot Sync must be found
                    Preconditions.checkState(lastEntry.getEntries().keySet().size() == 1);
                    Preconditions.checkState(lastEntry.getEntries().keySet().contains(snapshotSyncQueueId));

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

        log.trace("CurrentMsgSize {} lastEntrySize {}  maxDataSizePerMsg {}",
            currentMsgSize, lastEntry == null ? 0 : ReaderUtility.calculateSize(lastEntry.getEntries().get(snapshotSyncQueueId)),
            maxDataSizePerMsg);
        return new SMREntryList(currentMsgSize, smrList);
    }

    @Override
    protected OpaqueEntry generateOpaqueEntry(long version, UUID streamID, SMREntryList entryList) {
        Map<UUID, List<SMREntry>> map = new HashMap<>();
        UUID streamId = UUID.fromString(LogReplicationUtils.REPLICATED_QUEUE_NAME_PREFIX);
        map.put(streamId, entryList.getSmrEntries());
        return new OpaqueEntry(version, map);
    }

    private class StreamQueryTask implements Callable<Void> {

        OpaqueStream opaqueStream;

        StreamQueryTask(OpaqueStream opaqueStream) {
            this.opaqueStream = opaqueStream;
        }

        @Override
        public Void call() {
            try {
                IRetry.build(ExponentialBackoffRetry.class, () -> {
                    try {
                        currentStreamInfo = new OpaqueStreamIterator(opaqueStream, streamToReplicate,
                            rt.getAddressSpaceView().getLogTail());

                        if (!currentStreamHasNext()) {
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
