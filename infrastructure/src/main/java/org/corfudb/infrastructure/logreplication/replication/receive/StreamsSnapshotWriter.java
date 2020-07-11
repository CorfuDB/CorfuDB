package org.corfudb.infrastructure.logreplication.replication.receive;

import io.netty.buffer.Unpooled;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntryMetadata;
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
import java.util.UUID;
import java.util.stream.Stream;

/**
 * Writing a snapshot fullsync data
 * Open streams interested and append all entries
 */

@Slf4j
@NotThreadSafe
public class StreamsSnapshotWriter implements SnapshotWriter {

    private static final UUID NIL_UUID = new UUID(0,0);

    HashMap<UUID, String> streamViewMap; // It contains all the streams registered for write to.
    CorfuRuntime rt;

    private long topologyConfigId;
    private long srcGlobalSnapshot; // The source snapshot timestamp
    private long recvSeq;
    private long shadowStreamStartAddress;
    private LogReplicationConfig config;
    private UUID lastRequestId = NIL_UUID;     // Records the identifier of the last Snapshot Sync Request Unique Identifier

    @Getter
    private LogReplicationMetadataManager logReplicationMetadataManager;
    HashMap<UUID, UUID> uuidMap;
    Phase phase;


    // The sequence number of the message, it has received.
    // It is expecting the message in order of the sequence.

    public StreamsSnapshotWriter(CorfuRuntime rt, LogReplicationConfig config, LogReplicationMetadataManager logReplicationMetadataManager) {
        this.rt = rt;
        this.config = config;
        this.logReplicationMetadataManager = logReplicationMetadataManager;
        this.streamViewMap = new HashMap<>();
        this.uuidMap = new HashMap<>();
        this.phase = Phase.TRANSFER_PHASE;
    }

    /**
     * Clear all tables registered
     *
     * TODO: replace with stream API
     */
    private void clearTables() {
        CorfuStoreMetadata.Timestamp timestamp = logReplicationMetadataManager.getTimestamp();
        long persistedTopologyConfigId = logReplicationMetadataManager.query(timestamp, LogReplicationMetadataManager.LogReplicationMetadataType.TOPOLOGY_CONFIG_ID);
        long persistedSnapshotStart = logReplicationMetadataManager.query(timestamp, LogReplicationMetadataManager.LogReplicationMetadataType.LAST_SNAPSHOT_STARTED);
        long persistedSequenceNum = logReplicationMetadataManager.query(timestamp, LogReplicationMetadataManager.LogReplicationMetadataType.LAST_SNAPSHOT_SEQ_NUM);

        // For transfer phase start
        if (topologyConfigId != persistedTopologyConfigId || srcGlobalSnapshot != persistedSnapshotStart ||
                (persistedSequenceNum + 1)!= recvSeq) {
            log.warn("Skip clearing shadow tables. Current topologyConfigId={} srcGlobalSnapshot={}, currentSeqNum={}, " +
                    "persistedTopologyConfigId={}, persistedSnapshotStart={}, persistedLastSequenceNum={}", topologyConfigId,
                    srcGlobalSnapshot, recvSeq, persistedTopologyConfigId, persistedSnapshotStart, persistedSequenceNum);
            return;
        }

        TxBuilder txBuilder = logReplicationMetadataManager.getTxBuilder();
        logReplicationMetadataManager.appendUpdate(txBuilder, LogReplicationMetadataManager.LogReplicationMetadataType.TOPOLOGY_CONFIG_ID, topologyConfigId);

        for (UUID streamID : streamViewMap.keySet()) {
            UUID usedStreamID = streamID;
            if (phase == Phase.TRANSFER_PHASE) {
                usedStreamID = uuidMap.get(streamID);
            }

            SMREntry entry = new SMREntry("clear", new Array[0], Serializers.PRIMITIVE);
            txBuilder.logUpdate(usedStreamID, entry);
        }

        txBuilder.commit(timestamp);
    }

    /**
     * If the metadata has wrong message type or baseSnapshot, throw an exception
     * @param metadata
     */
    private void verifyMetadata(LogReplicationEntryMetadata metadata) throws ReplicationWriterException {
        if (metadata.getMessageMetadataType() != MessageType.SNAPSHOT_MESSAGE ||
                metadata.getSnapshotTimestamp() != srcGlobalSnapshot) {
            log.error("snapshot expected {} != recv snapshot {}, metadata {}",
                    srcGlobalSnapshot, metadata.getSnapshotTimestamp(), metadata);
            throw new ReplicationWriterException("Message is out of order");
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

        //clear shadow streams and remember the start address
        clearTables();
        shadowStreamStartAddress = rt.getAddressSpaceView().getLogTail();
    }


    /**
     * Write a list of SMR entries to the specified stream log.
     * @param smrEntries
     * @param currentSeqNum
     * @param dstUUID
     */
    private void processOpaqueEntry(List<SMREntry> smrEntries, Long currentSeqNum, UUID dstUUID) {
        CorfuStoreMetadata.Timestamp timestamp = logReplicationMetadataManager.getTimestamp();
        long persistedTopologyConfigId = logReplicationMetadataManager.query(timestamp, LogReplicationMetadataManager.LogReplicationMetadataType.TOPOLOGY_CONFIG_ID);
        long persistedSnapshotStart = logReplicationMetadataManager.query(timestamp, LogReplicationMetadataManager.LogReplicationMetadataType.LAST_SNAPSHOT_STARTED);
        long persistedSequenceNum = logReplicationMetadataManager.query(timestamp, LogReplicationMetadataManager.LogReplicationMetadataType.LAST_SNAPSHOT_SEQ_NUM);

        if (topologyConfigId != persistedTopologyConfigId || srcGlobalSnapshot != persistedSnapshotStart || currentSeqNum != (persistedSequenceNum + 1)) {
            log.warn("Skip processing opaque entry. Current topologyConfigId={} srcGlobalSnapshot={}, currentSeqNum={}, " +
                            "persistedTopologyConfigId={}, persistedSnapshotStart={}, persistedLastSequenceNum={}", topologyConfigId,
                    srcGlobalSnapshot, recvSeq, persistedTopologyConfigId, persistedSnapshotStart, persistedSequenceNum);
            return;
        }

        TxBuilder txBuilder = logReplicationMetadataManager.getTxBuilder();
        logReplicationMetadataManager.appendUpdate(txBuilder, LogReplicationMetadataManager.LogReplicationMetadataType.TOPOLOGY_CONFIG_ID, topologyConfigId);
        logReplicationMetadataManager.appendUpdate(txBuilder, LogReplicationMetadataManager.LogReplicationMetadataType.LAST_SNAPSHOT_STARTED, srcGlobalSnapshot);
        logReplicationMetadataManager.appendUpdate(txBuilder, LogReplicationMetadataManager.LogReplicationMetadataType.LAST_SNAPSHOT_SEQ_NUM, currentSeqNum);
        for (SMREntry smrEntry : smrEntries) {
            txBuilder.logUpdate(dstUUID, smrEntry);
        }

        try {
            txBuilder.commit(timestamp);
        } catch (Exception e) {
            log.warn("Caught an exception ", e);
            throw e;
        }
        log.debug("Process the entries {} and set sequence number {}", smrEntries, currentSeqNum);
    }

    @Override
    public void apply(LogReplicationEntry message) {

        // For every new snapshot sync request/cycle create a shadow stream which name is unique based
        // on the stream which it shadows and the snapshot sync
        // This is a quick way of avoiding TrimmedExceptions for snapshot syncs in between checkpoint/trim cycles.
        // Note: shadow streams are not checkpointed, so data will just be removed.
        // This is a temporal solution as metadata will be kept around for growing number of shadow streams.
        if(lastRequestId == NIL_UUID || !lastRequestId.equals(message.getMetadata().getSyncRequestId())) {
            createShadowTables(message.getMetadata().getSyncRequestId());
            lastRequestId = message.getMetadata().getSyncRequestId();
        }

        verifyMetadata(message.getMetadata());

        if (message.getMetadata().getSnapshotSyncSeqNum() != recvSeq ||
                message.getMetadata().getMessageMetadataType() != MessageType.SNAPSHOT_MESSAGE) {
            log.error("Expecting sequencer {} != recvSeq {} or wrong message type {} expecting {}",
                    message.getMetadata().getSnapshotSyncSeqNum(), recvSeq,
                    message.getMetadata().getMessageMetadataType(), MessageType.SNAPSHOT_MESSAGE);
            throw new ReplicationWriterException("Message is out of order or wrong type");
        }

        byte[] payload = message.getPayload();
        OpaqueEntry opaqueEntry = OpaqueEntry.deserialize(Unpooled.wrappedBuffer(payload));

        if (opaqueEntry.getEntries().keySet().size() != 1) {
            log.error("The opaqueEntry has more than one entry {}", opaqueEntry);
            return;
        }

        UUID uuid = opaqueEntry.getEntries().keySet().stream().findFirst().get();
        processOpaqueEntry(opaqueEntry.getEntries().get(uuid), message.getMetadata().getSnapshotSyncSeqNum(), uuidMap.get(uuid));
        recvSeq++;
    }

    /**
     * Create a shadow table per stream to replicate.
     *
     * A shadow stream is the combination of the stream it shadows and the snapshot sync request Id,
     * in order to make them unique between snapshot syncs.
     *
     * @param newSnapshotRequestId
     */
    private void createShadowTables(UUID newSnapshotRequestId) {
        for (String streamName : config.getStreamsToReplicate()) {
            UUID streamId = CorfuRuntime.getStreamID(streamName);
            // Remove mapping from previous shadow stream to avoid memory leak
            uuidMap.values().removeIf(regularStream -> regularStream.equals(streamId));
            UUID shadowStreamId = new UUID(streamId.getMostSignificantBits(), newSnapshotRequestId.getLeastSignificantBits());
            uuidMap.put(streamId, shadowStreamId);
            uuidMap.put(shadowStreamId, streamId);
            streamViewMap.put(streamId, streamName);
        }
    }

    @Override
    public void apply(List<LogReplicationEntry> messages) throws Exception {
        for (LogReplicationEntry msg : messages) {
            apply(msg);
        }
    }

    /**
     * Read from the shadow table and write to the real table
     * @param uuid: the real table uuid
     */
    private long applyShadowStream(UUID uuid, Long seqNum, long snapshot) {
        UUID shadowUUID = uuidMap.get(uuid);
        StreamOptions options = StreamOptions.builder()
                .ignoreTrimmed(false)
                .cacheEntries(false)
                .build();

        //Can we do a seek after open to ignore all entries that are earlier
        Stream shadowStream = (new OpaqueStream(rt, rt.getStreamsView().get(shadowUUID, options))).streamUpTo(snapshot);

        Iterator<OpaqueEntry> iterator = shadowStream.iterator();
        while (iterator.hasNext()) {
            OpaqueEntry opaqueEntry = iterator.next();
            if (opaqueEntry.getVersion() > shadowStreamStartAddress) {
                processOpaqueEntry(opaqueEntry.getEntries().get(shadowUUID), seqNum, uuid);
                seqNum = seqNum + 1;
            }
        }

        return seqNum;
    }


    /**
     * Read from shadowStream and append/apply to the actual stream
     */
    public void applyShadowStreams(long seqNum) {
        phase = Phase.APPLY_PHASE;
        long snapshot = rt.getAddressSpaceView().getLogTail();
        clearTables();
        for (UUID uuid : streamViewMap.keySet()) {
            seqNum = applyShadowStream(uuid, seqNum, snapshot);
        }
    }

    /**
     * Snapshot data has been transferred from primary node to the standby node
     * @param entry
     */
    public void snapshotTransferDone(LogReplicationEntry entry) {
        phase = Phase.APPLY_PHASE;
        //verify that the snapshot Apply hasn't started yet and set it as started and set the seqNumber
        long ts = entry.getMetadata().getSnapshotTimestamp();
        long seqNum = 0;
        topologyConfigId = entry.getMetadata().getTopologyConfigId();

        //update the metadata
        logReplicationMetadataManager.setLastSnapTransferDoneTimestamp(topologyConfigId, ts);

        //get the number of entries to apply
        seqNum = logReplicationMetadataManager.query(null, LogReplicationMetadataManager.LogReplicationMetadataType.LAST_SNAPSHOT_SEQ_NUM);

        // There is no snapshot data to apply
        if (seqNum == Address.NON_ADDRESS)
            return;

        applyShadowStreams(seqNum + 1);
    }
    
    enum Phase {
        TRANSFER_PHASE,
        APPLY_PHASE
    };
}
