package org.corfudb.infrastructure.logreplication.replication.receive;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.config.LogReplicationRoutingQueueConfig;
import org.corfudb.infrastructure.logreplication.infrastructure.LogReplicationContext;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationMetadata;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.corfudb.protocols.CorfuProtocolCommon;
import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.service.CorfuProtocolLogReplication;
import org.corfudb.protocols.wireprotocol.StreamAddressRange;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMetadataMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryType;
import org.corfudb.runtime.Queue;
import org.corfudb.runtime.collections.CorfuRecord;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.StreamOptions;
import org.corfudb.runtime.view.stream.OpaqueStream;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.IntervalRetry;
import org.corfudb.util.retry.RetryNeededException;
import org.corfudb.util.serializer.Serializers;

import javax.annotation.concurrent.NotThreadSafe;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.corfudb.infrastructure.logreplication.config.LogReplicationConfig.MERGE_ONLY_STREAMS;
import static org.corfudb.infrastructure.logreplication.config.LogReplicationConfig.REGISTRY_TABLE_ID;
import static org.corfudb.infrastructure.logreplication.config.LogReplicationConfig.PROTOBUF_TABLE_ID;
import static org.corfudb.runtime.LogReplication.ReplicationModel.ROUTING_QUEUES;

/**
 * This class represents the entity responsible for writing streams' snapshots into the sink cluster DB.
 *
 * Snapshot sync is the process of transferring a snapshot of the DB. We create a shadow stream per stream to
 * replicate. A shadow stream aims to accumulate updates temporarily while the (full) snapshot transfer completes.
 * Shadow streams aim to avoid inconsistent states while data is still being transferred from source to sink
 * Once all the data is received, the shadow streams are applied into the actual streams.

 * There is still a window of inconsistency as apply is not atomic when transaction size exceeds the limit, but it is
 * guarded by the isDataConsistent flag such that clients do not observe inconsistent data. In the future, we will
 * support Table Aliasing which will enable atomic flip from shadow to regular streams, avoiding complete inconsistency.
 */
@Slf4j
@NotThreadSafe
public class StreamsSnapshotWriter extends SinkWriter implements SnapshotWriter {

    private static final String CLEAR_SMR_METHOD = "clear";
    private static final String SHADOW_STREAM_SUFFIX = "_SHADOW";
    private static final SMREntry CLEAR_ENTRY = new SMREntry(CLEAR_SMR_METHOD, new Array[0], Serializers.PRIMITIVE);

    // Runtime from LogReplicationSinkManager, for handling shadow streams in StreamsSnapshotWriter
    private final CorfuRuntime rt;

    private long topologyConfigId;

    // The source snapshot timestamp
    private long srcGlobalSnapshot;

    private long recvSeq;

    private Optional<SnapshotSyncStartMarker> snapshotSyncStartMarker;

    // Represents the actual replicated streams from source. This is a subset of all regular streams in
    // regularToShadowStreamId map
    private final Set<UUID> replicatedStreamIds = new HashSet<>();

    private final LogReplicationMetadataManager metadataManager;

    @Getter
    private Phase phase;

    public StreamsSnapshotWriter(CorfuRuntime rt, LogReplicationMetadataManager metadataManager,
                                 LogReplicationSession session, LogReplicationContext replicationContext) {
        super(session, replicationContext);
        this.rt = rt;
        this.metadataManager = metadataManager;
        this.phase = Phase.TRANSFER_PHASE;
        this.snapshotSyncStartMarker = Optional.empty();

        // Serialize the clear entry once to access its constant size on each subsequent use
        serializeClearEntry();
    }

    private void serializeClearEntry() {
        ByteBuf byteBuf = Unpooled.buffer();
        CLEAR_ENTRY.serialize(byteBuf);
    }

    /**
     * Get the shadow stream id of the given regular stream id.
     */
    private UUID getShadowStreamId(UUID regularStreamId) {
        // The shadow stream name should be given by regularStreamId, because Sink side could have not
        // opened the stream before Snapshot Sync and as a result it cannot get the corresponding stream name.
        String shadowStreamName = regularStreamId.toString() + SHADOW_STREAM_SUFFIX;
        return CorfuRuntime.getStreamID(shadowStreamName);
    }

    /**
     * If the metadata has wrong message type or baseSnapshot, throw an exception
     * @param metadata
     */
    private void verifyMetadata(LogReplicationEntryMetadataMsg metadata) {
        if (metadata.getEntryType() != LogReplicationEntryType.SNAPSHOT_MESSAGE ||
                metadata.getSnapshotTimestamp() != srcGlobalSnapshot ||
                metadata.getSnapshotSyncSeqNum() != recvSeq) {
            log.error("Expected snapshot={}, received snapshot={}, expected seq={}, received seq={}",
                    srcGlobalSnapshot, metadata.getSnapshotTimestamp(), metadata.getSnapshotSyncSeqNum(), recvSeq);
            throw new ReplicationWriterException("Snapshot message out of order");
        }
    }

    /**
     * Reset snapshot writer state
     *
     * @param topologyId topology epoch
     * @param snapshot base snapshot timestamp
     */
    public void reset(long topologyId, long snapshot) {
        log.debug("Reset snapshot writer, snapshot={}, topologyConfigId={}", snapshot, topologyId);
        topologyConfigId = topologyId;
        srcGlobalSnapshot = snapshot;
        recvSeq = 0;
        phase = Phase.TRANSFER_PHASE;
        snapshotSyncStartMarker = Optional.empty();
        replicatedStreamIds.clear();
        // Sync with registry table to capture local updates on Sink side
        replicationContext.refreshConfig(session, true);
    }

    /**
     * Process updates to shadow stream (temporal stream)
     *
     * @param smrEntries
     * @param currentSeqNum
     * @param shadowStreamUuid
     */
    private void processUpdatesShadowStream(List<SMREntry> smrEntries, Long currentSeqNum, UUID shadowStreamUuid,
                                            UUID snapshotSyncId) {
        CorfuStoreMetadata.Timestamp timestamp;

        try (TxnContext txn = metadataManager.getTxnContext()) {
            updateLog(txn, smrEntries, shadowStreamUuid);
            metadataManager.updateReplicationMetadataField(txn, session, ReplicationMetadata.LASTSNAPSHOTTRANSFERREDSEQNUMBER_FIELD_NUMBER, currentSeqNum);
            timestamp = txn.commit();
        }

        if (!snapshotSyncStartMarker.isPresent()) {
            try (TxnContext txn = metadataManager.getTxnContext()) {
                metadataManager.setSnapshotSyncStartMarker(txn, session, snapshotSyncId, timestamp);
                snapshotSyncStartMarker = Optional.of(new SnapshotSyncStartMarker(snapshotSyncId, timestamp.getSequence()));
                txn.commit();
            }
        }

        log.debug("Process entries total={}, set sequence number {}", smrEntries.size(), currentSeqNum);
    }

    /**
     * Write a list of SMR entries to the specified stream log.
     *
     * @param smrEntries
     * @param streamId
     */
    private void updateLog(TxnContext txnContext, List<SMREntry> smrEntries, UUID streamId) {
        ReplicationMetadata metadata = metadataManager.queryReplicationMetadata(txnContext, session);
        long persistedTopologyConfigId = metadata.getTopologyConfigId();
        long persistedSnapshotStart = metadata.getLastSnapshotStarted();
        long persistedSequenceNum = metadata.getLastSnapshotTransferredSeqNumber();

        if (topologyConfigId != persistedTopologyConfigId || srcGlobalSnapshot != persistedSnapshotStart) {
            log.warn("Skip processing opaque entry. Current topologyConfigId={}, srcGlobalSnapshot={}, currentSeqNum={}, " +
                            "persistedTopologyConfigId={}, persistedSnapshotStart={}, persistedLastSequenceNum={}", topologyConfigId,
                    srcGlobalSnapshot, recvSeq, persistedTopologyConfigId, persistedSnapshotStart, persistedSequenceNum);
            return;
        }

        metadataManager.updateReplicationMetadataField(txnContext, session, ReplicationMetadata.TOPOLOGYCONFIGID_FIELD_NUMBER, topologyConfigId);
        metadataManager.updateReplicationMetadataField(txnContext, session, ReplicationMetadata.LASTSNAPSHOTSTARTED_FIELD_NUMBER, srcGlobalSnapshot);

        // Since the routing queue model requires the data to maintain the "write" order on the SINK, create a new
        // queue entry to embed the sequence number used by the SINK.
        // Transfer phase was chosen to retain the simplicity of the code.
        if (session.getSubscriber().getModel().equals(ROUTING_QUEUES) && phase.equals(Phase.TRANSFER_PHASE)) {
            createAndWriteQueueRecord(txnContext, smrEntries, metadataManager.getCorfuStore());
        } else {
            for (SMREntry smrEntry : smrEntries) {
                if (session.getSubscriber().getModel().equals(ROUTING_QUEUES)) {
                    txnContext.logUpdate(streamId, smrEntry, Collections.singletonList(replicatedRoutingQueueTag));
                } else {
                    txnContext.logUpdate(streamId, smrEntry, replicationContext.getConfig(session).getDataStreamToTagsMap().get(streamId));
                }
            }
        }
    }

    /**
     * Apply updates to shadow stream (temporarily) to avoid data
     * inconsistency until full snapshot has been transferred.
     * Note: We should not clear the shadow streams when a new snapshot
     * sync starts because this would overwrite(clear) merge-only streams
     * when the shadow stream is applied to the regular stream.  Shadow streams
     * are sought on each replication cycle and are GC'ed by the
     * checkpoint/trim.
     * @param message snapshot log entry
     */
    @Override
    public void apply(LogReplicationEntryMsg message) {

        verifyMetadata(message.getMetadata());

        if (message.getMetadata().getSnapshotSyncSeqNum() != recvSeq ||
                message.getMetadata().getEntryType() != LogReplicationEntryType.SNAPSHOT_MESSAGE) {
            log.error("Received {} Expecting snapshot message sequencer number {} != recvSeq {} or wrong message type {} expecting {}",
                    message.getMetadata(), message.getMetadata().getSnapshotSyncSeqNum(), recvSeq,
                    message.getMetadata().getEntryType(), LogReplicationEntryType.SNAPSHOT_MESSAGE);
            throw new ReplicationWriterException("Message is out of order or wrong type");
        }

        List<OpaqueEntry> opaqueEntryList = CorfuProtocolLogReplication.extractOpaqueEntries(message);

        // For snapshot message, it has only one opaque entry.
        if (opaqueEntryList.size() > 1) {
            log.error(" Get {} instead of one opaque entry in Snapshot Message", opaqueEntryList.size());
            return;
        }

        OpaqueEntry opaqueEntry = opaqueEntryList.get(0);
        if (opaqueEntry.getEntries().keySet().size() != 1) {
            log.error("The opaqueEntry has more than one entry {}", opaqueEntry);
            return;
        }
        UUID regularStreamId = opaqueEntry.getEntries().keySet().stream().findFirst().get();

        if (ignoreEntriesForStream(regularStreamId)) {
            log.warn("Skip applying log entries for stream {} as it is noisy. Source and Sink are likely to be operating in" +
                    " different versions", regularStreamId);
            recvSeq++;
            return;
        }

        // Collect the streams that have evidenced data from source.
        replicatedStreamIds.add(regularStreamId);

        processUpdatesShadowStream(opaqueEntry.getEntries().get(regularStreamId),
            message.getMetadata().getSnapshotSyncSeqNum(),
            getShadowStreamId(regularStreamId),
            CorfuProtocolCommon.getUUID(message.getMetadata().getSyncRequestId()));
        recvSeq++;
    }

    private void clearStream(UUID streamId, TxnContext txnContext) {
        txnContext.logUpdate(streamId, CLEAR_ENTRY, replicationContext.getConfig(session)
                .getDataStreamToTagsMap().get(streamId));
    }

    @Override
    public void apply(List<LogReplicationEntryMsg> messages) {
        for (LogReplicationEntryMsg msg : messages) {
            apply(msg);
        }
    }

    /**
     * Read from the shadow table and write to the original stream
     *
     * @param streamId regular stream id
     * @param snapshot base snapshot timestamp
     */
    private void applyShadowStream(UUID streamId, long snapshot) {
        log.debug("Apply shadow stream for stream {}, snapshot={}", streamId, snapshot);

        log.trace("Current addresses of stream {} :: {}", streamId, rt.getSequencerView().getStreamAddressSpace(
            new StreamAddressRange(streamId, Long.MAX_VALUE, Address.NON_ADDRESS)));

        UUID shadowStreamId = getShadowStreamId(streamId);

        // In order to avoid data loss as part of a plugin failing to successfully
        // stop/resume checkpoint and trim. We will not ignore trims on the shadow stream.
        // However, because in between different snapshot cycles, the shadow stream could
        // have been trimmed (without a checkpoint) we will sync the stream up to the first
        // valid known position during the current cycle (i.e. the first applied entry in
        // the current snapshot cycle, which should not have been trimmed by the time of
        // applying the shadow stream).
        StreamOptions options = StreamOptions.builder()
                .ignoreTrimmed(false)
                .cacheEntries(false)
                .build();

        // This variable reflects the minimum timestamp for all shadow streams in the current snapshot cycle.
        // We seek up to this address, assuming that no trim should occur beyond this snapshot start
        long currentMinShadowStreamTimestamp =
            metadataManager.getReplicationMetadata(session).getCurrentCycleMinShadowStreamTs();
        OpaqueStream shadowOpaqueStream = new OpaqueStream(rt.getStreamsView().get(shadowStreamId, options));
        shadowOpaqueStream.seek(currentMinShadowStreamTimestamp);
        Stream<OpaqueEntry> shadowStream = shadowOpaqueStream.streamUpTo(snapshot);

        Iterator<OpaqueEntry> iterator = shadowStream.iterator();
        List<SMREntry> smrEntries = new ArrayList<>();

        // Clear the stream before updates are applied atomically.
        // This is done for all replicated streams except 2 cases -
        // 1. Merge-only streams
        // 2. Streams which did not evidence data on either source or sink
        // as these streams will get trimmed and 'clear' will be a 'data loss'.
        if (MERGE_ONLY_STREAMS.contains(streamId)) {
            log.debug("Do not clear stream={} (merge stream)", streamId);
        }

        boolean shouldAddClearRecord = !MERGE_ONLY_STREAMS.contains(streamId);

        while (iterator.hasNext()) {
            // append a clear record at the beginning of every non-merge-only stream
            if(shouldAddClearRecord) {
                smrEntries.add(CLEAR_ENTRY);
                shouldAddClearRecord = false;
            }

            OpaqueEntry opaqueEntry = iterator.next();
            smrEntries.addAll(opaqueEntry.getEntries().get(shadowStreamId));
        }

        // if clear record has not been added by now, indicates that shadow stream is empty.
        if (shouldAddClearRecord) {
            log.trace("No data was written to stream {} on source or sink. Do not clear.", streamId);
            return;
        }

        if (streamId.equals(REGISTRY_TABLE_ID)) {
            smrEntries = filterRegistryTableEntries(smrEntries);
        }

        List<SMREntry> buffer = new ArrayList<>();
        long bufferSize = 0;
        int numBatches = 1;

        for (SMREntry smrEntry : smrEntries) {
            // Apply all SMR entries in a single transaction as long as it does not exceed the max write size(25MB).
            // It was observed that special streams(ProtobufDescriptor table), can get a lot of updates, especially
            // due to schema updates during an upgrade.  If the table was not checkpointed and trimmed on the Source,
            // no de-duplication on these updates will occur.  As a result, the transaction size can be large.
            // Although it is within the maxWriteSize limit, deserializing these entries to read the table can cause an
            // OOM on applications running with a small memory footprint.  So for such tables, introduce an
            // additional limit of max number of entries(50 by default) applied in a single transaction.  This
            // algorithm is in line with the limits imposed in Compaction and Restore workflows.
            if (bufferSize + smrEntry.getSerializedSize() > metadataManager.getRuntime().getParameters()
                    .getMaxWriteSize() || maxEntriesLimitReached(streamId, buffer)) {
                try (TxnContext txnContext = metadataManager.getTxnContext()) {
                    updateLog(txnContext, buffer, streamId);
                    CorfuStoreMetadata.Timestamp ts = txnContext.commit();
                    log.debug("Applied shadow stream partially for stream {} on address :: {}.  {} SMR entries written",
                        streamId, ts.getSequence(), buffer.size());
                    buffer.clear();
                    buffer.add(smrEntry);
                    bufferSize = smrEntry.getSerializedSize();
                    numBatches++;
                }
            } else {
                buffer.add(smrEntry);
                bufferSize += smrEntry.getSerializedSize();
            }
        }
        if (!buffer.isEmpty()) {
            try (TxnContext txnContext = metadataManager.getTxnContext()) {
                updateLog(txnContext, buffer, streamId);
                txnContext.commit();
            }
        }
        log.debug("Completed applying updates to stream {}.  {} entries applied across {} transactions.  ", streamId,
            smrEntries.size(), numBatches);
    }

    private boolean maxEntriesLimitReached(UUID streamId, List<SMREntry> buffer) {
        return streamId.equals(PROTOBUF_TABLE_ID) && buffer.size() == replicationContext.getConfig(session)
                .getMaxSnapshotEntriesApplied();
    }

    /**
     * Read from shadowStream and append/apply to the actual stream
     */
    public void applyShadowStreams() {
        log.debug("Apply Shadow Streams, total={}", replicatedStreamIds.size());
        long snapshot = rt.getAddressSpaceView().getLogTail();

        // Registry table needs to be applied first, as there could be tables that haven't been opened in Sink side,
        // such that the config doesn't have the corresponding stream tags.
        applyShadowStream(REGISTRY_TABLE_ID, snapshot);

        // Sync the config with registry table after applying its entries
        replicationContext.refreshConfig(session, true);

        // TODO: Temporary fix for routing queue model, will need more complete fix to getStreamsToReplicate() for
        // this model.
        // Use replicatedStreamIds if model is routing queues
        List<UUID> replicatedStreams = new ArrayList<>();
        if (session.getSubscriber().getModel() == ROUTING_QUEUES) {
            replicatedStreams.addAll(replicatedStreamIds);
        } else {
            replicatedStreams.addAll(replicationContext.getConfig(session)
                    .getStreamsToReplicate().stream()
                    .map(CorfuRuntime::getStreamID).collect(Collectors.toList()));
        }

        for (UUID stream : replicatedStreams) {
            if (stream.equals(REGISTRY_TABLE_ID)) {
                // Skip registry table as it has been applied in advance
                continue;
            }
            applyShadowStream(stream, snapshot);
        }

        // Invalidate client cache after snapshot sync is completed, as shadow streams are
        // no longer useful in the cache
        rt.getAddressSpaceView().invalidateClientCache();
        replicatedStreamIds.clear();
    }

    /**
     * Start Snapshot Sync Apply, i.e., move data from shadow streams to actual streams
     */
    public void startSnapshotSyncApply() {
        phase = Phase.APPLY_PHASE;

        // Get the number of entries to apply
        long sequenceNumber = metadataManager.getReplicationMetadata(session).getLastSnapshotTransferredSeqNumber();

        if (sequenceNumber != Address.NON_ADDRESS) {
            log.debug("Start applying shadow streams, seqNum={}", sequenceNumber);
            applyShadowStreams();

            // For Routing Queue replication model, write a dummy entry indicating the end of Snapshot sync
            // TODO: This is a temporary workaround according to the client behavior.  It must be removed in future.
            if (session.getSubscriber().getModel().equals(ROUTING_QUEUES)) {
                writeLastSnapshotSyncEntry();
            }
        }
    }

    private void writeLastSnapshotSyncEntry() {
        try {
            // For ROUTING_QUEUES model, write a dummy entry with type LAST_SNAPSHOT_SYNC_ENTRY to indicate
            // subscribers to complete snapshot sync.
            IRetry.build(IntervalRetry.class, () -> {
                try {
                    try (TxnContext txnContext = metadataManager.getTxnContext()) {
                        String replicatedQueueName = ((LogReplicationRoutingQueueConfig) replicationContext
                                .getConfig(session)).getSinkQueueName();
                        UUID streamId = CorfuRuntime.getStreamID(replicatedQueueName);
                        UUID replicatedQueueTag = ((LogReplicationRoutingQueueConfig) replicationContext
                                .getConfig(session)).getSinkQueueStreamTag();


                        long dummyEntryId = 0;
                        // Embed this key into a protobuf.
                        Queue.CorfuGuidMsg keyOfQueueEntry =
                            Queue.CorfuGuidMsg.newBuilder().setInstanceId(dummyEntryId).build();
                        Queue.RoutingTableEntryMsg dummyQueueMsg = Queue.RoutingTableEntryMsg.newBuilder()
                                .setSourceClusterId(session.getSourceClusterId())
                                .addAllDestinations(Collections.singleton(session.getSinkClusterId()))
                                .setReplicationType(Queue.ReplicationType.LAST_SNAPSHOT_SYNC_ENTRY).build();

                        CorfuRecord<Queue.RoutingTableEntryMsg, Queue.CorfuQueueMetadataMsg> dummyEntry =
                                new CorfuRecord<>(dummyQueueMsg,
                                    Queue.CorfuQueueMetadataMsg.newBuilder().setTxSequence(Address.MAX).build());
                        Object[] smrArgs = new Object[2];
                        smrArgs[0] = keyOfQueueEntry;
                        smrArgs[1] = dummyEntry;

                        SMREntry smrEntry = new SMREntry("put", smrArgs, replicationContext.getProtobufSerializer());
                        txnContext.logUpdate(streamId, smrEntry, Collections.singletonList(replicatedQueueTag));
                        txnContext.commit();
                    }
                } catch (Exception e) {
                    log.error("Error while attempting to connect to cluster manager. Retry.", e);
                    throw new RetryNeededException();
                }
                return null;
            }).run();
        } catch (InterruptedException e) {
            log.error("Unrecoverable Error when writing dummy entry to log entry queue", e);
            throw new ReplicationWriterException(e);
        }
    }

    /**
     * Clear streams which are not merge-only and have had local updates when
     * the role was not SINK and no data was received for them from the
     * SOURCE.
     *
     * Note: streams could be locally written while this node had no assigned role.
     */
    public void clearLocalStreams() {
        // Iterate over all streams to replicate (as obtained from configuration) and accumulate
        // those for which no data came from Source and were not merge-only, to
        // make a single call to the sequencer for log tails and discover
        // those with local writes, to be cleared.

        // Note: we cannot clear any stream which has not evidenced updates
        // either on Source or Sink because we would be enforcing an update
        // without opening the stream, hence, leading to "apparent" data loss as
        // checkpoint won't run on these streams
        Set<UUID> streamsToQuery = new HashSet<>();
        for (String replicatedStream :
            replicationContext.getConfig(session).getStreamsToReplicate()) {
            UUID id = CorfuRuntime.getStreamID(replicatedStream);
            if (replicatedStreamIds.contains(id) || MERGE_ONLY_STREAMS.contains(id)) {
                continue;
            }
            streamsToQuery.add(id);
        }

        log.debug("Total of {} streams were replicated from Source, sequencer query for {} streams, streamsToQuery={}",
            replicatedStreamIds.size(), streamsToQuery.size(), streamsToQuery);
        TokenResponse tokenResponse = rt.getSequencerView().query(
            streamsToQuery.toArray(new UUID[0]));
        Set<UUID> streamsWithLocalWrites = new HashSet<>();
        streamsToQuery.forEach(streamId -> {
            if (tokenResponse.getStreamTail(streamId) != Address.NON_EXIST) {
                streamsWithLocalWrites.add(streamId);
            }
        });

        if (!streamsWithLocalWrites.isEmpty()) {
            log.debug("Clear streams with local writes, total={}, streams={}",
                streamsWithLocalWrites.size(), streamsWithLocalWrites);
        } else {
            log.debug("No local written streams were found, nothing to clear.");
        }
        clearStreams(streamsWithLocalWrites);
    }

    private void clearStreams(Set<UUID> streamsToClear) {
        try {
            IRetry.build(IntervalRetry.class, () -> {
                try (TxnContext txnContext = metadataManager.getTxnContext()) {
                    metadataManager.updateReplicationMetadataField(txnContext, session,
                            ReplicationMetadata.TOPOLOGYCONFIGID_FIELD_NUMBER, topologyConfigId);
                    streamsToClear.forEach(streamId -> {
                        clearStream(streamId, txnContext);
                    });
                    CorfuStoreMetadata.Timestamp ts = txnContext.commit();
                    log.trace("Clear {} streams committed at :: {}", streamsToClear.size(), ts.getSequence());
                } catch (TransactionAbortedException tae) {
                    log.error("Error while attempting to clear locally written streams.", tae);
                    throw new RetryNeededException();
                }
                return null;
            }).run();
        } catch (InterruptedException e) {
            log.error("Unrecoverable exception when attempting to clear locally written streams.", e);
            throw new UnrecoverableCorfuInterruptedError(e);
        }
    }

    enum Phase {
        TRANSFER_PHASE,
        APPLY_PHASE
    }

    /**
     * This class represents a unique identification of the start of a snapshot cycle.
     * It is represented by the unique identifier of the snapshot sync cycle and
     * the min (first timestamp) to a shadow stream in this snapshot cycle.
     *
     * This is used to validate Trim Exceptions that can lead to data loss.
     */
    static class SnapshotSyncStartMarker {
        private final UUID snapshotId;
        private final long minShadowStreamTimestamp;

        public SnapshotSyncStartMarker(UUID snapshotId, long minShadowStreamTimestamp) {
            this.snapshotId = snapshotId;
            this.minShadowStreamTimestamp = minShadowStreamTimestamp;
        }
    }
}
