package org.corfudb.infrastructure.logreplication.replication.receive;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager.LogReplicationMetadataType;
import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMetadataMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryType;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.StreamOptions;
import org.corfudb.runtime.view.stream.OpaqueStream;
import org.corfudb.util.serializer.Serializers;

import javax.annotation.concurrent.NotThreadSafe;
import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

import static org.corfudb.protocols.CorfuProtocolCommon.getUUID;
import static org.corfudb.protocols.service.CorfuProtocolLogReplication.extractOpaqueEntries;

/**
 * This class represents the entity responsible of writing streams' snapshots into the standby cluster DB.
 *
 * Snapshot sync is the process of transferring a snapshot of the DB, for this reason, data is temporarily applied
 * to shadow streams in an effort to avoid inconsistent states. Once all the data is received, the shadow streams
 * are applied into the actual streams.
 */

@Slf4j
@NotThreadSafe
public class StreamsSnapshotWriter implements SnapshotWriter {

    private final static String SHADOW_STREAM_SUFFIX = "_SHADOW";

    // Mapping from regular stream Id to stream Name
    private final HashMap<UUID, String> streamViewMap;
    private CorfuRuntime rt;

    private long topologyConfigId;
    private long srcGlobalSnapshot; // The source snapshot timestamp
    private long recvSeq;
    private Optional<SnapshotSyncStartMarker> snapshotSyncStartMarker;

    @Getter
    private LogReplicationMetadataManager logReplicationMetadataManager;
    // Mapping from regular stream Id to shadow stream Id
    private HashMap<UUID, UUID> regularToShadowStreamId;

    @Getter
    private Phase phase;

    public StreamsSnapshotWriter(CorfuRuntime rt, LogReplicationConfig config, LogReplicationMetadataManager logReplicationMetadataManager) {
        this.rt = rt;
        this.logReplicationMetadataManager = logReplicationMetadataManager;
        this.streamViewMap = new HashMap<>();
        this.regularToShadowStreamId = new HashMap<>();
        this.phase = Phase.TRANSFER_PHASE;
        this.snapshotSyncStartMarker = Optional.empty();

        initializeShadowStreams(config);
    }

    /**
     * Create shadow streams.
     *
     * We create a shadow stream per stream to replicate. A shadow stream aims to accumulate updates
     * temporarily while the (full) snapshot sync completes. Shadow streams aim to avoid inconsistent
     * states while data is still being transferred from active to standby.
     *
     * We currently, wait for snapshot sync to complete before applying data in shadow streams
     * to the actual streams, this means that there is still a window of inconsistency as apply is not atomic,
     * but at least inconsistency is at a point where there is guarantee of all data being available on the receiver.
     * In the future, we will support Table Aliasing which will enable atomic flip from shadow to regular streams, avoiding
     * complete inconsistency.
     */
    private void initializeShadowStreams(LogReplicationConfig config) {
        // For every stream create a shadow stream which name is unique based
        // on the original stream and a suffix.
        for (String streamName : config.getStreamsToReplicate()) {
            String shadowStreamName = streamName + SHADOW_STREAM_SUFFIX;
            UUID streamId = CorfuRuntime.getStreamID(streamName);
            UUID shadowStreamId = CorfuRuntime.getStreamID(shadowStreamName);
            regularToShadowStreamId.put(streamId, shadowStreamId);
            regularToShadowStreamId.put(shadowStreamId, streamId);
            streamViewMap.put(streamId, streamName);

            log.trace("Shadow stream=[{}] for regular stream=[{}] name=({})", shadowStreamId, streamId, streamName);
        }
    }

    /**
     * Clear all tables registered
     *
     * TODO: replace with stream API
     */
    private void clearTables() {

        long persistedTopologyConfigId;
        long persistedSnapshotStart;
        long persistedTransferredSequenceNum;

        try (TxnContext txnContext = logReplicationMetadataManager.getTxnContext()) {
            Map<LogReplicationMetadataType, Long> metadataMap = logReplicationMetadataManager.queryMetadata(txnContext,
                    LogReplicationMetadataType.TOPOLOGY_CONFIG_ID, LogReplicationMetadataType.LAST_SNAPSHOT_STARTED,
                    LogReplicationMetadataType.LAST_SNAPSHOT_TRANSFERRED_SEQUENCE_NUMBER);
            persistedTopologyConfigId = metadataMap.get(LogReplicationMetadataType.TOPOLOGY_CONFIG_ID);
            persistedSnapshotStart = metadataMap.get(LogReplicationMetadataType.LAST_SNAPSHOT_STARTED);
            persistedTransferredSequenceNum = metadataMap.get(LogReplicationMetadataType.LAST_SNAPSHOT_TRANSFERRED_SEQUENCE_NUMBER);
            txnContext.commit();
        }

        // For transfer phase start
        if (topologyConfigId != persistedTopologyConfigId || srcGlobalSnapshot != persistedSnapshotStart ||
                (persistedTransferredSequenceNum + 1) != recvSeq) {
            log.warn("Skip clearing shadow streams. Current topologyConfigId={}, srcGlobalSnapshot={}, currentSeqNum={}, " +
                    "persistedTopologyConfigId={}, persistedSnapshotStart={}, persistedLastTransferredSequenceNum={}", topologyConfigId,
                    srcGlobalSnapshot, recvSeq, persistedTopologyConfigId, persistedSnapshotStart, persistedTransferredSequenceNum);
            return;
        }

        if (phase == Phase.APPLY_PHASE) {
            log.debug("Clear regular streams, count={}", streamViewMap.size());
        } else {
            log.debug("Clear shadow streams, count={}", streamViewMap.size());
        }

        try (TxnContext txnContext = logReplicationMetadataManager.getTxnContext()) {
            logReplicationMetadataManager.appendUpdate(txnContext, LogReplicationMetadataType.TOPOLOGY_CONFIG_ID, topologyConfigId);

            for (UUID streamID : streamViewMap.keySet()) {
                UUID streamToClear = streamID;
                if (phase == Phase.TRANSFER_PHASE) {
                    streamToClear = regularToShadowStreamId.get(streamID);
                }

                SMREntry entry = new SMREntry("clear", new Array[0], Serializers.PRIMITIVE);
                txnContext.logUpdate(streamToClear, entry);
            }
            txnContext.commit();
        }
    }

    /**
     * If the metadata has wrong message type or baseSnapshot, throw an exception
     * @param metadata
     */
    private void verifyMetadata(LogReplicationEntryMetadataMsg metadata) throws ReplicationWriterException {
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
        clearTables();
        snapshotSyncStartMarker = Optional.empty();
    }


    /**
     * Write a list of SMR entries to the specified stream log.
     *
     * @param smrEntries
     * @param shadowStreamUuid
     */
    private void processOpaqueEntry(List<SMREntry> smrEntries, UUID shadowStreamUuid) {
        try (TxnContext txnContext = logReplicationMetadataManager.getTxnContext()) {
            processOpaqueEntry(txnContext, smrEntries, shadowStreamUuid);
            txnContext.commit();
        }

        log.debug("Process entries count={}", smrEntries.size());
    }

    /**
     * Write a list of SMR entries to the specified stream log.
     *
     * @param smrEntries
     * @param currentSeqNum
     * @param shadowStreamUuid
     */
    private void processOpaqueEntry(List<SMREntry> smrEntries, Long currentSeqNum, UUID shadowStreamUuid, UUID snapshotSyncId) {
        CorfuStoreMetadata.Timestamp timestamp;

        try (TxnContext txn = logReplicationMetadataManager.getTxnContext()) {
            processOpaqueEntryShadowStream(txn, smrEntries, currentSeqNum, shadowStreamUuid);
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

    private void processOpaqueEntryShadowStream(TxnContext txnContext, List<SMREntry> smrEntries, Long currentSeqNum, UUID shadowStreamUuid) {
        processOpaqueEntry(txnContext, smrEntries, shadowStreamUuid);
        logReplicationMetadataManager.appendUpdate(txnContext, LogReplicationMetadataType.LAST_SNAPSHOT_TRANSFERRED_SEQUENCE_NUMBER, currentSeqNum);
    }

    /**
     * Write a list of SMR entries to the specified stream log.
     *
     * @param smrEntries
     * @param shadowStreamUuid
     */
    private void processOpaqueEntry(TxnContext txnContext, List<SMREntry> smrEntries, UUID shadowStreamUuid) {
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
            txnContext.logUpdate(shadowStreamUuid, smrEntry);
        }
    }

    /**
     * Apply updates to shadow stream (temporarily) to avoid data
     * inconsistency until full snapshot has been transferred.
     *
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

        List<OpaqueEntry> opaqueEntryList = extractOpaqueEntries(message);

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
        UUID uuid = opaqueEntry.getEntries().keySet().stream().findFirst().get();
        processOpaqueEntry(opaqueEntry.getEntries().get(uuid), message.getMetadata().getSnapshotSyncSeqNum(),
                regularToShadowStreamId.get(uuid), getUUID(message.getMetadata().getSyncRequestId()));
        recvSeq++;

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
        Stream shadowStream = shadowOpaqueStream.streamUpTo(snapshot);

        Iterator<OpaqueEntry> iterator = shadowStream.iterator();
        while (iterator.hasNext()) {
            OpaqueEntry opaqueEntry = iterator.next();
            processOpaqueEntry(opaqueEntry.getEntries().get(shadowStreamId), streamId);
        }
    }

    /**
     * Read from shadowStream and append/apply to the actual stream
     */
    public void applyShadowStreams() {
        long snapshot = rt.getAddressSpaceView().getLogTail();
        clearTables();
        log.debug("Apply Shadow Streams, total={}", streamViewMap.size());
        for (UUID uuid : streamViewMap.keySet()) {
            applyShadowStream(uuid, snapshot);
        }
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
