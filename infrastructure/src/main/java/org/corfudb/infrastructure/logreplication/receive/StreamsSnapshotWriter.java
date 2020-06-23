package org.corfudb.infrastructure.logreplication.receive;

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
    // The suffix used to the corresponding shadow tables.
    final static String SHADOW_STREAM_NAME_SUFFIX = "_shadow";

    // It contains all the streams registered for write to.
    // Mapping uuid to table name.
    HashMap<UUID, String> streamViewMap;

    // It contains all the shadow streams
    HashMap<UUID, String> shadowMap;

    CorfuRuntime rt;

    /*
     * Record the siteConfigID when the snapshot full sync start.
     * If the siteConfigID has changed during the snapshot full sync,
     * this snapshot full sync should be aborted and restart a new one.
     */
    long siteConfigID;

    /**
     * The current snapshot full sync's snapshot timestamp.
     */
    private long srcGlobalSnapshot;

    /*
     * Used by Snapshot Full Sync Phase I: writing to shadow streams.
     * The expecting message's snapshotSeqNum. If the message is out of order, will buffer it.
     * The message will be processed according the snapshotSeqNum.
     */
    private long recvSeq;


    /*
     * Used by Snapshot Full Sync Phase II: read from shadow stream and apply data to the real streams.
     * It records the operation number.
     */
    private long appliedSeq;


    /*
     * Before writing to the shadowStream, record the current tail.
     * While open a shadow stream and apply entries to
     * the real stream, we can seek the shadowStream to this address first to skip
     * reading irrelevant entries.
     */
    private long shadowStreamStartAddress;

    /**
     * Used to query/update metadata
     */
    @Getter
    private LogReplicationMetadataAccessor logReplicationMetadataAccessor;

    /**
     * Mapping of the real table's uuid to the corresponding shadow table's uuid.
     */
    HashMap<UUID, UUID> uuidMap;

    /**
     * Record the Snapshot Full Sync phase: transfer phase or apply phase.
     */
    Phase phase;

    public StreamsSnapshotWriter(CorfuRuntime rt, LogReplicationConfig config, LogReplicationMetadataAccessor logReplicationMetadataAccessor) {
        this.rt = rt;
        this.logReplicationMetadataAccessor = logReplicationMetadataAccessor;
        streamViewMap = new HashMap<>();
        uuidMap = new HashMap<>();
        shadowMap = new HashMap<>();
        phase = Phase.TransferPhase;

        for (String stream : config.getStreamsToReplicate()) {
            String shadowStream = stream + SHADOW_STREAM_NAME_SUFFIX;
            UUID streamID = CorfuRuntime.getStreamID(stream);
            UUID shadowID = CorfuRuntime.getStreamID(shadowStream);
            uuidMap.put(streamID, shadowID);
            uuidMap.put(shadowID, streamID);
            streamViewMap.put(streamID, stream);
            shadowMap.put(shadowID, shadowStream);
        }
    }

    /**
     * When the receiver gets a SNAPSHOT_START message, it will first clear the shadow tables.
     */
    void clearShadowTables() {
        CorfuStoreMetadata.Timestamp timestamp = logReplicationMetadataAccessor.getTimestamp();
        long persistSiteConfigID = logReplicationMetadataAccessor.query(timestamp, LogReplicationMetadataAccessor.LogReplicationMetadataType.SiteConfigID);
        long persistSnapStart = logReplicationMetadataAccessor.query(timestamp, LogReplicationMetadataAccessor.LogReplicationMetadataType.LastSnapshotStarted);
        long persistSnapTransferred = logReplicationMetadataAccessor.query(timestamp, LogReplicationMetadataAccessor.LogReplicationMetadataType.LastSnapshotTransferred);
        long persitSeqNum = logReplicationMetadataAccessor.query(timestamp, LogReplicationMetadataAccessor.LogReplicationMetadataType.LastSnapshotSeqNum);

        // Verify that the replication metadata shows that it is in the correct state.
        if (siteConfigID != persistSiteConfigID || srcGlobalSnapshot != persistSnapStart || srcGlobalSnapshot <= persistSnapTransferred ||
                persitSeqNum != Address.NON_ADDRESS) {
                log.warn("Skip current processing as the persistent metadata {} shows the current operation is out of date " +
                        "current siteConfigID {} srcGlobalSnapshot {} ", logReplicationMetadataAccessor.getLogReplicationStatus(),
                        siteConfigID, srcGlobalSnapshot);
            return;
        }


        TxBuilder txBuilder = logReplicationMetadataAccessor.getTxBuilder();
        logReplicationMetadataAccessor.appendUpdate(txBuilder, LogReplicationMetadataAccessor.LogReplicationMetadataType.SiteConfigID, siteConfigID);

        // Clear all the tables.
        for (UUID streamID : streamViewMap.keySet()) {
            UUID usedStreamID = uuidMap.get(streamID);
            SMREntry entry = new SMREntry("clear", new Array[0], Serializers.PRIMITIVE);
            txBuilder.logUpdate(usedStreamID, entry);
        }

        txBuilder.commit(timestamp);
    }


    /**
     * Clear all real tables registered for replication and start SNAPSHOT FULL Sync phase II:
     * applying data to the real tables.
     */
    void clearTables() {
        CorfuStoreMetadata.Timestamp timestamp = logReplicationMetadataAccessor.getTimestamp();
        long persistSiteConfigID = logReplicationMetadataAccessor.query(timestamp, LogReplicationMetadataAccessor.LogReplicationMetadataType.SiteConfigID);
        long persistSnapStart = logReplicationMetadataAccessor.query(timestamp, LogReplicationMetadataAccessor.LogReplicationMetadataType.LastSnapshotStarted);
        long persistSnapTransferred = logReplicationMetadataAccessor.query(timestamp,
                LogReplicationMetadataAccessor.LogReplicationMetadataType.LastSnapshotTransferred);
        long appliedSeqNum = logReplicationMetadataAccessor.query(timestamp, LogReplicationMetadataAccessor.LogReplicationMetadataType.LastSnapshotAppliedSeqNum);

        //If the metadata shows it is in the apply start phase and other metadata is consistent.
        if (siteConfigID != persistSiteConfigID || srcGlobalSnapshot != persistSnapStart ||
                persistSnapTransferred != srcGlobalSnapshot || appliedSeqNum != Address.NON_ADDRESS) {
            log.warn("Skip current processing as the persistent metadata {} shows the current operation is out of date " +
                            "current siteConfigID {} srcGlobalSnapshot {}", logReplicationMetadataAccessor.getLogReplicationStatus(),
                    siteConfigID, srcGlobalSnapshot);
            return;
        }

        TxBuilder txBuilder = logReplicationMetadataAccessor.getTxBuilder();
        logReplicationMetadataAccessor.appendUpdate(txBuilder, LogReplicationMetadataAccessor.LogReplicationMetadataType.SiteConfigID, siteConfigID);

        for (UUID streamID : streamViewMap.keySet()) {
            SMREntry entry = new SMREntry("clear", new Array[0], Serializers.PRIMITIVE);
            txBuilder.logUpdate(streamID, entry);
        }

        txBuilder.commit(timestamp);
    }

    /**
     * If the metadata has wrong message type or baseSnapshot, throw an exception
     * @param metadata
     * @return
     */
    void verifyMetadata(LogReplicationEntryMetadata metadata) throws ReplicationWriterException {
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
        this.siteConfigID = siteConfigID;
        srcGlobalSnapshot = snapshot;
        recvSeq = 0;
        appliedSeq = 0;

        //clear shadow streams and remember the start address
        clearShadowTables();
        shadowStreamStartAddress = rt.getAddressSpaceView().getLogTail();
    }


    /**
     * Used by Full Snapshot Full Sync Phase:
     * write a list of SMR entries to the specified shadow stream log.
     * @param smrEntries
     * @param currentSeqNum
     * @param dstUUID
     */
    void processOpaqueEntry(List<SMREntry> smrEntries, Long currentSeqNum, UUID dstUUID) {
        CorfuStoreMetadata.Timestamp timestamp = logReplicationMetadataAccessor.getTimestamp();
        long persistConfigID = logReplicationMetadataAccessor.query(timestamp, LogReplicationMetadataAccessor.LogReplicationMetadataType.SiteConfigID);
        long persistSnapStart = logReplicationMetadataAccessor.query(timestamp, LogReplicationMetadataAccessor.LogReplicationMetadataType.LastSnapshotStarted);
        long persitSeqNum = logReplicationMetadataAccessor.query(timestamp, LogReplicationMetadataAccessor.LogReplicationMetadataType.LastSnapshotSeqNum);

        // Verify the message is in the correct order according to the metadata. If it is out of sync with the metadata, skip it.
        if (siteConfigID != persistConfigID || srcGlobalSnapshot != persistSnapStart || currentSeqNum != (persitSeqNum + 1)) {
            log.warn("Skip processing current entry with siteConfigID {} srcGlobalSnapshot {} currentSeqNum {} " +
                    " as it is not the expected entry according to the log replication metadata status {}",
            siteConfigID, srcGlobalSnapshot, currentSeqNum, logReplicationMetadataAccessor.getLogReplicationStatus());
            return;
        }

        TxBuilder txBuilder = logReplicationMetadataAccessor.getTxBuilder();

        // Update the siteConfigID with the same value to fence off other metadata updates.
        logReplicationMetadataAccessor.appendUpdate(txBuilder, LogReplicationMetadataAccessor.LogReplicationMetadataType.SiteConfigID, siteConfigID);

        // Update the snapshotSeqNumb
        logReplicationMetadataAccessor.appendUpdate(txBuilder, LogReplicationMetadataAccessor.LogReplicationMetadataType.LastSnapshotSeqNum, currentSeqNum);

        // Append the log entries.
        for (SMREntry smrEntry : smrEntries) {
            txBuilder.logUpdate(dstUUID, smrEntry);
        }

        try {
            txBuilder.commit(timestamp);
        } catch (Exception e) {
            log.warn("Caught an exception ", e);
            throw e;
        }
        log.debug("Process the entries {}  and set sequence number {} ", smrEntries, currentSeqNum);
    }

    /**
     * Used by Full Snapshot Sync Phase II:
     * Write a list of SMR entries to the specified real stream log.
     * @param smrEntries
     * @param currentSeqNum
     * @param dstUUID
     */
    void processShadowOpaqueEntry(List<SMREntry> smrEntries, Long currentSeqNum, UUID dstUUID) {
        CorfuStoreMetadata.Timestamp timestamp = logReplicationMetadataAccessor.getTimestamp();
        long persistConfigID = logReplicationMetadataAccessor.query(timestamp, LogReplicationMetadataAccessor.LogReplicationMetadataType.SiteConfigID);
        long persistSnapStart = logReplicationMetadataAccessor.query(timestamp, LogReplicationMetadataAccessor.LogReplicationMetadataType.LastSnapshotStarted);
        long persitSeqNum = logReplicationMetadataAccessor.query(timestamp, LogReplicationMetadataAccessor.LogReplicationMetadataType.LastSnapshotAppliedSeqNum);

        // Verify that the operation is in the correct order. If it is out of sync with the metadata, skip it.
        if (siteConfigID != persistConfigID || srcGlobalSnapshot != persistSnapStart || currentSeqNum != (persitSeqNum + 1)) {
            log.warn("Skip current processing as the persistent metadata {} has applied more recent message {} than current siteConfigID {} " +
                    "srcGlobalSnapshot {} currentAppliedSeqNum {}", logReplicationMetadataAccessor.getLogReplicationStatus(),
                    siteConfigID, srcGlobalSnapshot, currentSeqNum);
            return;
        }

        TxBuilder txBuilder = logReplicationMetadataAccessor.getTxBuilder();

        // Update the siteConfigID with the same value to fence off any metadata updates based on the same timestamp.
        logReplicationMetadataAccessor.appendUpdate(txBuilder, LogReplicationMetadataAccessor.LogReplicationMetadataType.SiteConfigID, siteConfigID);

        // Update the appliedSeqNumber.
        logReplicationMetadataAccessor.appendUpdate(txBuilder, LogReplicationMetadataAccessor.LogReplicationMetadataType.LastSnapshotAppliedSeqNum, currentSeqNum);

        // Append log entries.
        for (SMREntry smrEntry : smrEntries) {
            txBuilder.logUpdate(dstUUID, smrEntry);
        }

        try {
            txBuilder.commit(timestamp);
        } catch (Exception e) {
            log.warn("Caught an exception ", e);
            throw e;
        }
        log.debug("Process the entries {}  and set applied sequence number {} ", smrEntries, currentSeqNum);
    }

    /**
     * Apply a snapshot sync message.
     * @param message
     */
    @Override
    public void apply(LogReplicationEntry message) {
        verifyMetadata(message.getMetadata());

        // If it is out of order or it is wrong type message, skip it.
        if (message.getMetadata().getSnapshotSyncSeqNum() != recvSeq ||
                message.getMetadata().getMessageMetadataType() != MessageType.SNAPSHOT_MESSAGE) {
            log.error("Expecting sequencer {} != recvSeq {} or wrong message type {} expecting {}",
                    message.getMetadata().getSnapshotSyncSeqNum(), recvSeq,
                    message.getMetadata().getMessageMetadataType(), MessageType.SNAPSHOT_MESSAGE);
            throw new ReplicationWriterException("Message is out of order or wrong type");
        }

        byte[] payload = message.getPayload();
        OpaqueEntry opaqueEntry = OpaqueEntry.deserialize(Unpooled.wrappedBuffer(payload));

        // The opaqueEntry should have one key as it has only one stream.
        if (opaqueEntry.getEntries().keySet().size() != 1) {
            log.error("The opaqueEntry has more than one entry {}", opaqueEntry);
            return;
        }

        UUID uuid = opaqueEntry.getEntries().keySet().stream().findFirst().get();

        // Process the opaqueEntries.
        processOpaqueEntry(opaqueEntry.getEntries().get(uuid), recvSeq, uuidMap.get(uuid));
        recvSeq++;
    }

    /**
     * Apply a list of messages.
     * @param messages
     * @throws Exception
     */
    @Override
    public void apply(List<LogReplicationEntry> messages) throws Exception {
        for (LogReplicationEntry msg : messages) {
            apply(msg);
        }
    }

    /**
     * Snapshot data has been transferred from primary node to the standby node
     * @param entry
     */
    public void setSnapshotTransferDone(LogReplicationEntry entry) {
        phase = Phase.ApplyPhase;
        //verify that the snapshot Apply hasn't started yet and set it as started and set the seqNumber
        long ts = entry.getMetadata().getSnapshotTimestamp();
        siteConfigID = entry.getMetadata().getSiteConfigID();

        //update the metadata
        logReplicationMetadataAccessor.setLastSnapTransferDoneTimestamp(siteConfigID, ts);
    }

    /**
     * Read from the shadow table and write to the real table
     * @param uuid: the real table uuid
     */
    public long applyShadowStream(UUID uuid, Long seqNum, long snapshot) {
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
                processShadowOpaqueEntry(opaqueEntry.getEntries().get(shadowUUID), seqNum, uuid);
                seqNum = seqNum + 1;
            }
        }

        return seqNum;
    }

    /**
     * read from shadowStreams and append to the real streams.
     */
    public void snapshotTransferDone(LogReplicationEntry message) {
        // Set metadata and phase
        setSnapshotTransferDone(message);

        //get the number of entries to apply
        long seqNum = logReplicationMetadataAccessor.query(null, LogReplicationMetadataAccessor.LogReplicationMetadataType.LastSnapshotSeqNum);

        // There is snapshot data to apply
        if (seqNum != Address.NON_ADDRESS) {
            phase = Phase.ApplyPhase;
            long snapshot = rt.getAddressSpaceView().getLogTail();
            clearTables();

            for (UUID uuid : streamViewMap.keySet()) {
                appliedSeq = applyShadowStream(uuid, appliedSeq, snapshot);
            }
        }

        // Apply phase is done, update the metadata
        logReplicationMetadataAccessor.setSnapshotApplied(message);
    }

    enum Phase {
        TransferPhase,
        ApplyPhase
    };
}
