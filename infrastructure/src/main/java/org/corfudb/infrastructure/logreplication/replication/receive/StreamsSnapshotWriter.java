package org.corfudb.infrastructure.logreplication.replication.receive;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.infrastructure.logreplication.infrastructure.ReplicationSession;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager.LogReplicationMetadataType;
import org.corfudb.protocols.CorfuProtocolCommon;
import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.service.CorfuProtocolLogReplication;
import org.corfudb.protocols.wireprotocol.StreamAddressRange;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMetadataMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryType;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.corfudb.infrastructure.logreplication.LogReplicationConfig.MERGE_ONLY_STREAMS;
import static org.corfudb.infrastructure.logreplication.LogReplicationConfig.REGISTRY_TABLE_ID;

/**
 * This class represents the entity responsible of writing streams' snapshots into the sink cluster DB.
 *
 * Snapshot sync is the process of transferring a snapshot of the DB, for this reason, data is temporarily applied
 * to shadow streams in an effort to avoid inconsistent states. Once all the data is received, the shadow streams
 * are applied into the actual streams.
 */

@Slf4j
@NotThreadSafe
public class StreamsSnapshotWriter extends SinkWriter implements SnapshotWriter {

    private static final String CLEAR_SMR_METHOD = "clear";
    private static final String SHADOW_STREAM_SUFFIX = "_SHADOW";
    private static final SMREntry CLEAR_ENTRY = new SMREntry(CLEAR_SMR_METHOD, new Array[0], Serializers.PRIMITIVE);

    // Mapping from regular stream Id to stream Name
    private final HashMap<UUID, String> streamViewMap;

    // Mapping from regular stream Id to shadow stream Id
    private final HashMap<UUID, UUID> regularToShadowStreamId;

    private long topologyConfigId;
    private long srcGlobalSnapshot; // The source snapshot timestamp
    private long recvSeq;
    private Optional<SnapshotSyncStartMarker> snapshotSyncStartMarker;
    private final Map<UUID, List<UUID>> dataStreamToTagsMap;

    @Getter
    private final LogReplicationMetadataManager logReplicationMetadataManager;

    // Represents the actual replicated streams from source. This is a subset of all regular streams in
    // regularToShadowStreamId map
    private final Set<UUID> replicatedStreamIds = new HashSet<>();

    @Getter
    private Phase phase;

    public StreamsSnapshotWriter(CorfuRuntime rt, LogReplicationConfig config,
                                 LogReplicationMetadataManager logReplicationMetadataManager,
                                 ReplicationSession replicationSession) {
        super(rt);
        this.logReplicationMetadataManager = logReplicationMetadataManager;
        this.streamViewMap = new HashMap<>();
        this.regularToShadowStreamId = new HashMap<>();
        this.phase = Phase.TRANSFER_PHASE;
        this.snapshotSyncStartMarker = Optional.empty();
        this.dataStreamToTagsMap = config.getDataStreamToTagsMap();

        initializeShadowStreams(config, replicationSession);

        // Serialize the clear entry once to access its constant size on each
        // subsequent use
        serializeClearEntry();
    }

    /**
     * Create shadow streams.
     *
     * We create a shadow stream per stream to replicate. A shadow stream aims to accumulate updates
     * temporarily while the (full) snapshot sync completes. Shadow streams aim to avoid inconsistent
     * states while data is still being transferred from source to sink.
     *
     * We currently, wait for snapshot sync to complete before applying data in shadow streams
     * to the actual streams, this means that there is still a window of inconsistency as apply is not atomic,
     * but at least inconsistency is at a point where there is guarantee of all data being available on the receiver.
     * In the future, we will support Table Aliasing which will enable atomic flip from shadow to regular streams, avoiding
     * complete inconsistency.
     */
    private void initializeShadowStreams(LogReplicationConfig config, ReplicationSession replicationSession) {
        // For every stream create a shadow stream which name is unique based
        // on the original stream and a suffix.
        for (String streamName : config.getReplicationSubscriberToStreamsMap().get(replicationSession.getSubscriber())) {
            String shadowStreamName = streamName + SHADOW_STREAM_SUFFIX;
            UUID streamId = CorfuRuntime.getStreamID(streamName);
            UUID shadowStreamId = CorfuRuntime.getStreamID(shadowStreamName);
            regularToShadowStreamId.put(streamId, shadowStreamId);
            regularToShadowStreamId.put(shadowStreamId, streamId);
            streamViewMap.put(streamId, streamName);

            log.trace("Shadow stream=[{}] for regular stream=[{}] name=({})", shadowStreamId, streamId, streamName);
        }

        log.info("Stream tag map for streaming on Sink total={}, streams={}", dataStreamToTagsMap.size(),
                dataStreamToTagsMap);
    }

    private void serializeClearEntry() {
        ByteBuf byteBuf = Unpooled.buffer();
        CLEAR_ENTRY.serialize(byteBuf);
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

        try (TxnContext txn = logReplicationMetadataManager.getTxnContext()) {
            updateLog(txn, smrEntries, shadowStreamUuid);
            logReplicationMetadataManager.appendUpdate(txn,
                    LogReplicationMetadataType.LAST_SNAPSHOT_TRANSFERRED_SEQUENCE_NUMBER, currentSeqNum);
            timestamp = txn.commit();
        }

        if (!snapshotSyncStartMarker.isPresent()) {
            try (TxnContext txn = logReplicationMetadataManager.getTxnContext()) {
                logReplicationMetadataManager.setSnapshotSyncStartMarker(txn, snapshotSyncId, timestamp);
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
        Map<LogReplicationMetadataType, Long> metadataMap = logReplicationMetadataManager.queryMetadata(txnContext, LogReplicationMetadataType.TOPOLOGY_CONFIG_ID,
                LogReplicationMetadataType.LAST_SNAPSHOT_STARTED, LogReplicationMetadataType.LAST_SNAPSHOT_TRANSFERRED_SEQUENCE_NUMBER);
        long persistedTopologyConfigId = metadataMap.get(LogReplicationMetadataType.TOPOLOGY_CONFIG_ID);
        long persistedSnapshotStart = metadataMap.get(LogReplicationMetadataType.LAST_SNAPSHOT_STARTED);
        long persistedSequenceNum = metadataMap.get(LogReplicationMetadataType.LAST_SNAPSHOT_TRANSFERRED_SEQUENCE_NUMBER);

        if (topologyConfigId != persistedTopologyConfigId || srcGlobalSnapshot != persistedSnapshotStart) {
            log.warn("Skip processing opaque entry. Current topologyConfigId={}, srcGlobalSnapshot={}, currentSeqNum={}, " +
                            "persistedTopologyConfigId={}, persistedSnapshotStart={}, persistedLastSequenceNum={}", topologyConfigId,
                    srcGlobalSnapshot, recvSeq, persistedTopologyConfigId, persistedSnapshotStart, persistedSequenceNum);
            return;
        }

        logReplicationMetadataManager.appendUpdate(txnContext, LogReplicationMetadataType.TOPOLOGY_CONFIG_ID, topologyConfigId);
        logReplicationMetadataManager.appendUpdate(txnContext, LogReplicationMetadataType.LAST_SNAPSHOT_STARTED, srcGlobalSnapshot);

        for (SMREntry smrEntry : smrEntries) {
            txnContext.logUpdate(streamId, smrEntry, dataStreamToTagsMap.get(streamId));
        }
    }

    /**
     * Apply updates to shadow stream (temporarily) to avoid data
     * inconsistency until full snapshot has been transferred.
     * Note: We should not clear the shadow streams when a new snapshot
     * sync starts because this would overwrite(clear) merge-only streams
     * when the shadow stream is applied to the regular stream.  Shadow streams
     * are seeked on each replication cycle and are GC'ed by the
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

        // When an upgrade has been performed and Source's streams to replicate
        // set has streams not present in the Sink's set, they will be
        // dropped for version compatibility consideration
        if (!regularToShadowStreamId.containsKey(regularStreamId)) {
            log.warn("Stream {} sent from Source is not expected in Sink. LR could be" +
                    " undergoing a rolling upgrade", regularStreamId);
            recvSeq++;
            return;
        }

        // Collect the streams that have evidenced data from source.
        replicatedStreamIds.add(regularStreamId);

        processUpdatesShadowStream(opaqueEntry.getEntries().get(regularStreamId),
            message.getMetadata().getSnapshotSyncSeqNum(),
            regularToShadowStreamId.get(regularStreamId),
            CorfuProtocolCommon.getUUID(message.getMetadata().getSyncRequestId()));
        recvSeq++;
    }

    private void clearStream(UUID streamId, TxnContext txnContext) {
        SMREntry entry = new SMREntry(CLEAR_SMR_METHOD, new Array[0], Serializers.PRIMITIVE);
        txnContext.logUpdate(streamId, entry, dataStreamToTagsMap.get(streamId));
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
        log.debug("Apply shadow stream for stream {}, snapshot={}", streamId,
            snapshot);
        log.debug("Current addresses of stream {} :: {}", streamId,
            rt.getSequencerView().getStreamAddressSpace(
                new StreamAddressRange(streamId, Long.MAX_VALUE,
                    Address.NON_ADDRESS)));
        UUID shadowStreamId = regularToShadowStreamId.get(streamId);

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
        long currentMinShadowStreamTimestamp = logReplicationMetadataManager.getMinSnapshotSyncShadowStreamTs();
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
        } else if (!replicatedStreamIds.contains(streamId)) {
            log.trace("No data was written to stream {} on source or sink." +
                "  Do not clear.", streamId);
            return;
        } else {
            smrEntries.add(CLEAR_ENTRY);
        }

        while (iterator.hasNext()) {
            OpaqueEntry opaqueEntry = iterator.next();
            smrEntries.addAll(opaqueEntry.getEntries().get(shadowStreamId));
        }

        if (streamId.equals(REGISTRY_TABLE_ID)) {
            // Only keep entries that currently not exist in registry table
            smrEntries = fetchNewEntries(new ArrayList<>(smrEntries));
        }

        List<SMREntry> buffer = new ArrayList<>();
        long bufferSize = 0;
        int numBatches = 1;

        for (SMREntry smrEntry : smrEntries) {
            if (bufferSize + smrEntry.getSerializedSize() >
                logReplicationMetadataManager.getRuntime().getParameters()
                    .getMaxWriteSize()) {
                try (TxnContext txnContext = logReplicationMetadataManager.getTxnContext()) {
                    updateLog(txnContext, buffer, streamId);
                    CorfuStoreMetadata.Timestamp ts = txnContext.commit();
                    log.debug("Applied shadow stream partially for stream {} " +
                        "on address :: {}.  {} SMR entries written", streamId,
                        ts.getSequence(), buffer.size());
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
            try (TxnContext txnContext = logReplicationMetadataManager.getTxnContext()) {
                updateLog(txnContext, buffer, streamId);
                txnContext.commit();
            }
        }
        log.debug("Completed applying updates to stream {}.  {} " +
            "entries applied across {} transactions.  ", streamId,
            smrEntries.size(), numBatches);
    }

    /**
     * Read from shadowStream and append/apply to the actual stream
     */
    public void applyShadowStreams() {
        log.debug("Apply Shadow Streams, total={}", streamViewMap.size());

        long snapshot = rt.getAddressSpaceView().getLogTail();
        for (UUID regularStreamId : streamViewMap.keySet()) {
            applyShadowStream(regularStreamId, snapshot);
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
        long seqNum = logReplicationMetadataManager.queryMetadata(LogReplicationMetadataType.LAST_SNAPSHOT_TRANSFERRED_SEQUENCE_NUMBER);

        // Only if there is data to be applied
        if (seqNum != Address.NON_ADDRESS) {
            log.debug("Start applying shadow streams, seqNum={}", seqNum);
            applyShadowStreams();
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
        Set<UUID> streamsToQuery = streamViewMap.keySet().stream()
                .filter(id -> !replicatedStreamIds.contains(id) && !MERGE_ONLY_STREAMS.contains(id))
                .collect(Collectors.toCollection(HashSet::new));

        log.debug("Total of {} streams were replicated from Source out of {}," +
            " sequencer query for {}, streamsToQuery={}",
            replicatedStreamIds.size(), streamViewMap.size(),
            streamsToQuery.size(), streamsToQuery);
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
                try (TxnContext txnContext = logReplicationMetadataManager.getTxnContext()) {
                    logReplicationMetadataManager.appendUpdate(txnContext, LogReplicationMetadataType.TOPOLOGY_CONFIG_ID, topologyConfigId);
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
