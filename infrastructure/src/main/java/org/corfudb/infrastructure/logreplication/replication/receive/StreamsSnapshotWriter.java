package org.corfudb.infrastructure.logreplication.replication.receive;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntryMetadata;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager.LogReplicationMetadataType;
import org.corfudb.protocols.wireprotocol.logreplication.MessageType;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.collections.TxBuilder;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.StreamOptions;
import org.corfudb.runtime.view.stream.OpaqueStream;
import org.corfudb.util.serializer.Serializers;

import javax.annotation.concurrent.NotThreadSafe;
import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

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
    private long shadowStreamStartAddress;
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
        CorfuStoreMetadata.Timestamp timestamp = logReplicationMetadataManager.getTimestamp();
        long persistedTopologyConfigId = logReplicationMetadataManager.query(timestamp,
                LogReplicationMetadataType.TOPOLOGY_CONFIG_ID);
        long persistedSnapshotStart = logReplicationMetadataManager.query(timestamp,
                LogReplicationMetadataType.LAST_SNAPSHOT_STARTED);
        long persistedTransferredSequenceNum = logReplicationMetadataManager.query(timestamp,
                LogReplicationMetadataType.LAST_SNAPSHOT_TRANSFERRED_SEQUENCE_NUMBER);

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

        TxBuilder txBuilder = logReplicationMetadataManager.getTxBuilder();
        logReplicationMetadataManager.appendUpdate(txBuilder, LogReplicationMetadataType.TOPOLOGY_CONFIG_ID, topologyConfigId);

        for (UUID streamID : streamViewMap.keySet()) {
            UUID streamToClear = streamID;
            if (phase == Phase.TRANSFER_PHASE) {
                streamToClear = regularToShadowStreamId.get(streamID);
            }

            SMREntry entry = new SMREntry("clear", new Array[0], Serializers.PRIMITIVE);
            txBuilder.logUpdate(streamToClear, entry);
        }

        txBuilder.commit(timestamp);
    }

    /**
     * If the metadata has wrong message type or baseSnapshot, throw an exception
     * @param metadata
     */
    private void verifyMetadata(LogReplicationEntryMetadata metadata) throws ReplicationWriterException {
        if (metadata.getMessageMetadataType() != MessageType.SNAPSHOT_MESSAGE ||
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
     * @param topologyConfigId topology epoch
     * @param snapshot base snapshot timestamp
     */
    public void reset(long topologyConfigId, long snapshot) {
        log.debug("Reset snapshot writer, snapshot={}, topologyConfigId={}", snapshot, topologyConfigId);
        this.topologyConfigId = topologyConfigId;
        srcGlobalSnapshot = snapshot;
        recvSeq = 0;
        phase = Phase.TRANSFER_PHASE;
        clearTables();
        snapshotSyncStartMarker = Optional.empty();
        shadowStreamStartAddress = rt.getAddressSpaceView().getLogTail();
    }


    /**
     * Write a list of SMR entries to the specified stream log.
     *
     * @param smrEntries
     * @param shadowStreamUuid
     */
    private void processOpaqueEntry(List<SMREntry> smrEntries, UUID shadowStreamUuid) {
        TxBuilder txBuilder = logReplicationMetadataManager.getTxBuilder();
        CorfuStoreMetadata.Timestamp timestamp = processOpaqueEntry(txBuilder, smrEntries, shadowStreamUuid);

        try {
            txBuilder.commit(timestamp);
        } catch (Exception e) {
            log.warn("Caught an exception ", e);
            throw e;
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
        TxBuilder txBuilder = logReplicationMetadataManager.getTxBuilder();
        CorfuStoreMetadata.Timestamp timestamp = processOpaqueEntryShadowStream(txBuilder, smrEntries, currentSeqNum, shadowStreamUuid);

        try {
            if (!snapshotSyncStartMarker.isPresent()) {
                logReplicationMetadataManager.setSnapshotSyncStartMarker(snapshotSyncId, timestamp, txBuilder);
                snapshotSyncStartMarker = Optional.of(new SnapshotSyncStartMarker(snapshotSyncId, timestamp.getSequence()));
            }
            txBuilder.commit(timestamp);
        } catch (Exception e) {
            log.warn("Caught an exception ", e);
            throw e;
        }
        log.debug("Process entries total={}, set sequence number {}", smrEntries.size(), currentSeqNum);
    }

    private CorfuStoreMetadata.Timestamp processOpaqueEntryShadowStream(TxBuilder txBuilder, List<SMREntry> smrEntries, Long currentSeqNum, UUID shadowStreamUuid) {
        CorfuStoreMetadata.Timestamp timestamp = processOpaqueEntry(txBuilder, smrEntries, shadowStreamUuid);
        logReplicationMetadataManager.appendUpdate(txBuilder, LogReplicationMetadataType.LAST_SNAPSHOT_TRANSFERRED_SEQUENCE_NUMBER, currentSeqNum);
        return timestamp;
    }

        /**
         * Write a list of SMR entries to the specified stream log.
         * @param smrEntries
         * @param shadowStreamUuid
         */
    private CorfuStoreMetadata.Timestamp processOpaqueEntry(TxBuilder txBuilder, List<SMREntry> smrEntries, UUID shadowStreamUuid) {
        CorfuStoreMetadata.Timestamp timestamp = logReplicationMetadataManager.getTimestamp();
        long persistedTopologyConfigId = logReplicationMetadataManager.query(timestamp,
                LogReplicationMetadataType.TOPOLOGY_CONFIG_ID);
        long persistedSnapshotStart = logReplicationMetadataManager.query(timestamp,
                LogReplicationMetadataType.LAST_SNAPSHOT_STARTED);
        long persistedSequenceNum = logReplicationMetadataManager.query(timestamp,
                LogReplicationMetadataType.LAST_SNAPSHOT_TRANSFERRED_SEQUENCE_NUMBER);

        if (topologyConfigId != persistedTopologyConfigId || srcGlobalSnapshot != persistedSnapshotStart) {
            log.warn("Skip processing opaque entry. Current topologyConfigId={} srcGlobalSnapshot={}, currentSeqNum={}, " +
                            "persistedTopologyConfigId={}, persistedSnapshotStart={}, persistedLastSequenceNum={}", topologyConfigId,
                    srcGlobalSnapshot, recvSeq, persistedTopologyConfigId, persistedSnapshotStart, persistedSequenceNum);
            return CorfuStoreMetadata.Timestamp.getDefaultInstance();
        }

        logReplicationMetadataManager.appendUpdate(txBuilder, LogReplicationMetadataType.TOPOLOGY_CONFIG_ID, topologyConfigId);
        logReplicationMetadataManager.appendUpdate(txBuilder, LogReplicationMetadataType.LAST_SNAPSHOT_STARTED, srcGlobalSnapshot);

        for (SMREntry smrEntry : smrEntries) {
            txBuilder.logUpdate(shadowStreamUuid, smrEntry);
        }

        return timestamp;
    }

    /**
     * Apply updates to shadow stream (temporarily) to avoid data
     * inconsistency until full snapshot has been transferred.
     *
     * @param message snapshot log entry
     */
    @Override
    public void apply(LogReplicationEntry message) {

        verifyMetadata(message.getMetadata());

        if (message.getMetadata().getSnapshotSyncSeqNum() != recvSeq ||
                message.getMetadata().getMessageMetadataType() != MessageType.SNAPSHOT_MESSAGE) {
            log.error("Received {} Expecting snapshot message sequencer number {} != recvSeq {} or wrong message type {} expecting {}",
                    message.getMetadata(), message.getMetadata().getSnapshotSyncSeqNum(), recvSeq,
                    message.getMetadata().getMessageMetadataType(), MessageType.SNAPSHOT_MESSAGE);
            throw new ReplicationWriterException("Message is out of order or wrong type");
        }

        // For snapshot message, it has only one opaque entry.
        if (message.getOpaqueEntryList().size() > 1) {
            log.error(" Get {} instead of one opaque entry in Snapshot Message", message.getOpaqueEntryList().size());
            return;
        }

        OpaqueEntry opaqueEntry = message.getOpaqueEntryList().get(0);
        if (opaqueEntry.getEntries().keySet().size() != 1) {
            log.error("The opaqueEntry has more than one entry {}", opaqueEntry);
            return;
        }
        UUID uuid = opaqueEntry.getEntries().keySet().stream().findFirst().get();
        processOpaqueEntry(opaqueEntry.getEntries().get(uuid), message.getMetadata().getSnapshotSyncSeqNum(),
                regularToShadowStreamId.get(uuid), message.getMetadata().getSyncRequestId());
        recvSeq++;

    }

    @Override
    public void apply(List<LogReplicationEntry> messages) {
        for (LogReplicationEntry msg : messages) {
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
        log.trace("Apply shadow stream for stream {}, snapshot={}", streamId, snapshot);
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
        OpaqueStream shadowOpaqueStream = new OpaqueStream(rt, rt.getStreamsView().get(shadowStreamId, options));
        shadowOpaqueStream.seek(currentMinShadowStreamTimestamp);
        Stream shadowStream = shadowOpaqueStream.streamUpTo(snapshot);

        Iterator<OpaqueEntry> iterator = shadowStream.iterator();
        while (iterator.hasNext()) {
            OpaqueEntry opaqueEntry = iterator.next();
            if (opaqueEntry.getVersion() >= shadowStreamStartAddress) {
                processOpaqueEntry(opaqueEntry.getEntries().get(shadowStreamId), streamId);
            } else {
                log.warn("Skipping shadow stream opaque entry {} because it does not fall" +
                        "in the valid range >= {} for this cycle", opaqueEntry.getVersion(), shadowStreamStartAddress);
            }
        }
    }

    /**
     * Read from shadowStream and append/apply to the actual stream
     */
    public void applyShadowStreams() {
        long snapshot = rt.getAddressSpaceView().getLogTail();
        clearTables();
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
        long seqNum = logReplicationMetadataManager.query(null,
                LogReplicationMetadataType.LAST_SNAPSHOT_TRANSFERRED_SEQUENCE_NUMBER);

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
