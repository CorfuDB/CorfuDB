package org.corfudb.infrastructure.logreplication.replication.receive;

import io.netty.buffer.Unpooled;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntryMetadata;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.LogReplicationMetadataVal;
import org.corfudb.protocols.wireprotocol.logreplication.MessageType;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata.Timestamp;
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
import java.util.function.Consumer;
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
    private HashMap<UUID, UUID> uuidMap;
    private Phase phase;

    public StreamsSnapshotWriter(CorfuRuntime rt, LogReplicationConfig config, LogReplicationMetadataManager logReplicationMetadataManager) {
        this.rt = rt;
        this.logReplicationMetadataManager = logReplicationMetadataManager;
        this.streamViewMap = new HashMap<>();
        this.uuidMap = new HashMap<>();
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
            uuidMap.put(streamId, shadowStreamId);
            uuidMap.put(shadowStreamId, streamId);
            streamViewMap.put(streamId, streamName);

            log.trace("Shadow stream=[{}] for regular stream=[{}] name=({})", shadowStreamId, streamId, streamName);
        }
    }

    /**
     * Clear all tables registered
     *
     * TODO: replace with stream API
     */
    void clearTables() {
        Timestamp timestamp = logReplicationMetadataManager.getTimestamp();
        LogReplicationMetadataVal metadataVal = logReplicationMetadataManager.queryPersistedMetadata();

        //for transfer phase start
        if (topologyConfigId != metadataVal.getTopologyConfigId() || srcGlobalSnapshot != metadataVal.getSnapshotStartTimestamp() ||
                recvSeq != (metadataVal.getSnapshotMessageReceivedSeqNum() + 1)) {
            log.warn("Skip clearTable as the metadata topologyConfigId {}, srcGlobalSnapshot {}, snapshotMsgRecvSeqNum {} are inconsistent with the persisted metadata {}",
                    topologyConfigId, srcGlobalSnapshot, recvSeq, LogReplicationMetadataManager.getPersistedMetadataStr(metadataVal));
            return;
        }

        LogReplicationTxBuilder txBuilder = LogReplicationTxBuilder.getLogReplicationTxBuilder(getLogReplicationMetadataManager());
        txBuilder.appendUpdate(LogReplicationMetadataManager.LogReplicationMetadataName.TOPOLOGY_CONFIG_ID, topologyConfigId);


        for (UUID streamID : streamViewMap.keySet()) {
            UUID usedStreamID = streamID;
            if (phase == Phase.TRANSFER_PHASE) {
                usedStreamID = uuidMap.get(streamID);
            }

            SMREntry entry = new SMREntry("clear", new Array[0], Serializers.PRIMITIVE);
            txBuilder.logUpdate(usedStreamID, entry);
        }

        txBuilder.commit();
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
     * Reset snapshot writer state.
     * @param snapshot
     */
    public void reset(long siteConfigID, long snapshot) {
        this.topologyConfigId = siteConfigID;
        srcGlobalSnapshot = snapshot;
        recvSeq = 0;

        // Clear shadow streams and remember the start address
        clearTables();
        snapshotSyncStartMarker = Optional.empty();
        shadowStreamStartAddress = rt.getAddressSpaceView().getLogTail();
    }


    /**
     * Write a list of SMR entries to the specified stream log.
     * @param smrEntries
     * @param currentSeqNum
     * @param shadowStreamUuid
     */
    private void processOpaqueEntry(List<SMREntry> smrEntries, Long currentSeqNum, UUID shadowStreamUuid) {
        LogReplicationTxBuilder txBuilder = LogReplicationTxBuilder.getLogReplicationTxBuilder(getLogReplicationMetadataManager());
        processOpaqueEntry(txBuilder, smrEntries, currentSeqNum, shadowStreamUuid);

        try {
            txBuilder.commit();
        } catch (Exception e) {
            log.warn("Caught an exception ", e);
            throw e;
        }
        log.debug("Process the entries {} and set sequence number {}", smrEntries, currentSeqNum);
    }

    /**
     * Write a list of SMR entries to the specified stream log.
     * @param smrEntries
     * @param currentSeqNum
     * @param shadowStreamUuid
     */
    private void processOpaqueEntry(LogReplicationTxBuilder txBuilder, List<SMREntry> smrEntries, Long currentSeqNum, UUID shadowStreamUuid) {
        //Timestamp timestamp = logReplicationMetadataManager.getTimestamp();
        LogReplicationMetadataVal metadataVal = logReplicationMetadataManager.queryPersistedMetadata();

        if (topologyConfigId != metadataVal.getTopologyConfigId() || srcGlobalSnapshot != metadataVal.getSnapshotStartTimestamp() ||
                currentSeqNum != (metadataVal.getSnapshotMessageReceivedSeqNum() + 1)) {
            log.warn("Skip processing opaque entry. Current topologyConfigId={} srcGlobalSnapshot={}, currentSeqNum={}, " +
                            "peristed metadata {}", topologyConfigId,
                    srcGlobalSnapshot, recvSeq, LogReplicationMetadataManager.getPersistedMetadataStr(metadataVal));
            return;
        }

        txBuilder.appendUpdate(LogReplicationMetadataManager.LogReplicationMetadataName.LAST_SNAPSHOT_MSG_RECEIVED_SEQ_NUM, currentSeqNum);
        for (SMREntry smrEntry : smrEntries) {
            txBuilder.logUpdate(shadowStreamUuid, smrEntry);
        }
   }

    /**
     * Write a list of SMR entries to the specified stream log.
     * @param smrEntries
     * @param currentSeqNum
     * @param shadowStreamUuid
     */
    private void processOpaqueEntry(List<SMREntry> smrEntries, Long currentSeqNum, UUID shadowStreamUuid, UUID snapshotSyncId) {
        LogReplicationTxBuilder txBuilder = LogReplicationTxBuilder.getLogReplicationTxBuilder(getLogReplicationMetadataManager());
        processOpaqueEntry(txBuilder, smrEntries, currentSeqNum, shadowStreamUuid);

        try {
            if (!snapshotSyncStartMarker.isPresent()) {
                logReplicationMetadataManager.setSnapshotSyncStartMarker(snapshotSyncId, txBuilder.getTimestamp(), txBuilder);
                snapshotSyncStartMarker = Optional.of(new SnapshotSyncStartMarker(snapshotSyncId, txBuilder.getTimestamp().getSequence()));
            }
            txBuilder.commit();
        } catch (Exception e) {
            log.warn("Caught an exception ", e);
            throw e;
        }
        log.debug("Process the entries {} and set sequence number {}", smrEntries, currentSeqNum);
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
        log.trace("Apply message {}", message.getMetadata());

        if (message.getMetadata().getSnapshotSyncSeqNum() != recvSeq ||
                message.getMetadata().getMessageMetadataType() != MessageType.SNAPSHOT_MESSAGE) {
            log.error("Expecting recvSeq {} and  message type {} but got {}",
                    recvSeq, MessageType.SNAPSHOT_MESSAGE, message.getMetadata());
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
                uuidMap.get(uuid), message.getMetadata().getSyncRequestId());
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
     * @param seqNum sequence number to apply
     * @param snapshot base snapshot timestamp
     */
    private long applyShadowStream(UUID streamId, long seqNum, long snapshot) {
        UUID shadowStreamId = uuidMap.get(streamId);

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
                processOpaqueEntry(opaqueEntry.getEntries().get(shadowStreamId), seqNum, streamId);
                seqNum = seqNum + 1;
            }
        }

        return seqNum;
    }

    /**
     * Read from shadowStream and append/apply to the actual stream
     */
    public void applyShadowStreams(long seqNum) {
        long snapshot = rt.getAddressSpaceView().getLogTail();
        clearTables();
        for (UUID uuid : streamViewMap.keySet()) {
            seqNum = applyShadowStream(uuid, seqNum, snapshot);
        }
        phase = Phase.TRANSFER_PHASE;
    }

    /**
     * Snapshot data has been transferred from primary node to the standby node
     * @param entry
     */
    public void snapshotTransferDone(LogReplicationEntry entry) {
        phase = Phase.APPLY_PHASE;
        // Verify that the snapshot apply hasn't started yet and set it as started and set the seqNumber
        long ts = entry.getMetadata().getSnapshotTimestamp();
        long seqNum = 0;
        topologyConfigId = entry.getMetadata().getTopologyConfigId();

        // Update the metadata
        logReplicationMetadataManager.setLastSnapTransferDoneTimestamp(topologyConfigId, ts);

        //get the number of entries to apply
        seqNum = logReplicationMetadataManager.query(LogReplicationMetadataManager.LogReplicationMetadataName.LAST_SNAPSHOT_MSG_RECEIVED_SEQ_NUM);

        // There is no snapshot data to apply
        if (seqNum == Address.NON_ADDRESS) {
            log.info("There is no date to apply for snapshot sync");
            return;
        }
        // Only if there is data to be applied
        if (seqNum != Address.NON_ADDRESS) {
            applyShadowStreams(seqNum + 1);
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
