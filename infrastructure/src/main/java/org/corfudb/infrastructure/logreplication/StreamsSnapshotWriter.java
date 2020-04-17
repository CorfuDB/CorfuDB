package org.corfudb.infrastructure.logreplication;

import io.netty.buffer.Unpooled;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntryMetadata;
import org.corfudb.protocols.wireprotocol.logreplication.MessageType;

import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.transactions.TransactionalContext;
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
    private final static int MAX_NUM_TX_RETRY = 4;
    final static String SHADOW_STREAM_NAME_SUFFIX = "_shadow";
    HashMap<UUID, String> streamViewMap; // It contains all the streams registered for write to.
    HashMap<UUID, String> shadowMap;
    CorfuRuntime rt;
    private long srcGlobalSnapshot; // The source snapshot timestamp
    private long recvSeq;
    @Getter
    private PersistedWriterMetadata persistedWriterMetadata;
    HashMap<UUID, UUID> uuidMap;
    Phase phase;
    // The sequence number of the message, it has received.
    // It is expecting the message in order of the sequence.

    public StreamsSnapshotWriter(CorfuRuntime rt, LogReplicationConfig config, PersistedWriterMetadata persistedWriterMetadata) {
        this.rt = rt;
        this.persistedWriterMetadata = persistedWriterMetadata;
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
     * clear all tables registered
     * TODO: replace with stream API
     */
    void clearTables() {
        boolean doRetry = true;
        int numRetry = 0;

        while (doRetry && numRetry++ < MAX_NUM_TX_RETRY) {
            try {
                rt.getObjectsView().TXBegin();
                for (UUID streamID : streamViewMap.keySet()) {
                    UUID usedStreamID = streamID;
                    if (phase == Phase.TransferPhase) {
                        usedStreamID = uuidMap.get(streamID);
                    }
                    SMREntry entry = new SMREntry("clear", new Array[0], Serializers.PRIMITIVE);
                    TransactionalContext.getCurrentContext().logUpdate(usedStreamID, entry);
                    log.info("Clear tables for streams {} ", usedStreamID);
                }

                doRetry = false;
            } catch (TransactionAbortedException e) {
                log.warn("Caught an exception {} will retry {}", e, numRetry);
            } finally {
                rt.getObjectsView().TXEnd();
            }
        }
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
    public void reset(long snapshot) {
       srcGlobalSnapshot = snapshot;
       recvSeq = 0;
       clearTables();
    }

    /**
     * Convert an OpaqueEntry to an MultiObjectSMREntry and write to log.
     */


    /**
     * Write a list of SMR entries to the specified stream log.
     * @param smrEntries
     * @param currentSeqNum
     * @param dstUUID
     */
    void processOpaqueEntry(List<SMREntry> smrEntries, Long currentSeqNum, UUID dstUUID) {
        int numRetry = 0;
        boolean doRetry = true;
        long persistentSeqNum;

        while (doRetry && numRetry++ < MAX_NUM_TX_RETRY){
            try {
                rt.getObjectsView().TXBegin();
                System.out.print("\nTxBeing seq " + currentSeqNum);
                // read persistentMetadata's snapshot seq number
                persistentSeqNum = persistedWriterMetadata.getLastSnapSeqNum();

                if (currentSeqNum == (persistentSeqNum + 1)) {
                    for (SMREntry smrEntry : smrEntries) {
                        TransactionalContext.getCurrentContext().logUpdate(dstUUID, smrEntry);
                    }
                    System.out.print("\nphase " + phase + " dst_uuid " + dstUUID + " name " +
                            (shadowMap.containsKey(dstUUID)? shadowMap.get(dstUUID) : streamViewMap.get(dstUUID)));
                    persistedWriterMetadata.setLastSnapSeqNum(currentSeqNum);
                    log.debug("Process the entries {}  and set sequence number {} ", smrEntries, currentSeqNum);
                } else {
                    System.out.print("\nSkip the entry as the sequence number " + currentSeqNum + " is not equal to " + persistentSeqNum +  " + 1 ");
                    log.warn("\nSkip the entry as the sequence number is not equal to {} + 1", persistentSeqNum);
                }
                System.out.print("\nThe sequence number " + currentSeqNum + " persistent " + persistentSeqNum +  " + 1 ");
                // We have succeed update successful, don't need retry any more.
                doRetry = false;
            } catch (TransactionAbortedException e) {
                System.out.print("Caughte an exception " + e);
                log.warn("Caught an exception {}, will retry {}.", e, numRetry);
             } finally {
                System.out.print("\nTxEnd seq " + currentSeqNum);
                rt.getObjectsView().TXEnd();
            }
        }
    }

    @Override
    public void apply(LogReplicationEntry message) {
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
    public long applyShadowStream(UUID uuid, Long seqNum, long snapshot) {
        UUID shadowUUID = uuidMap.get(uuid);
        System.out.print("\napplyshadowStream seqNum " + seqNum + " uuid " + uuid + " name " + streamViewMap.get(uuid));
        StreamOptions options = StreamOptions.builder()
                .ignoreTrimmed(false)
                .cacheEntries(false)
                .build();

        Stream shadowStream = (new OpaqueStream(rt, rt.getStreamsView().get(shadowUUID, options))).streamUpTo(snapshot);
        Iterator<OpaqueEntry> iterator = shadowStream.iterator();
        System.out.print("\napply to stream " + uuid + " name " + streamViewMap.get(uuid) + " shadow stream " + shadowMap.get(shadowUUID)) ;
        while (iterator.hasNext()) {
            OpaqueEntry opaqueEntry = iterator.next();
            processOpaqueEntry(opaqueEntry.getEntries().get(shadowUUID), seqNum, uuid);
            seqNum = seqNum + 1;
        }

        System.out.print("\nshadowStream seqNum " + seqNum);
        return seqNum;
    }

    public  void snapshotTransferDone(LogReplicationEntry entry) {
        phase = Phase.ApplyPhase;
        //verify that the snapshot Apply hasn't started yet and set it as started and set the seqNumber
        long ts = entry.getMetadata().getSnapshotTimestamp();
        long seqNum = 0;
        try {
            rt.getObjectsView().TXBegin();
            if (persistedWriterMetadata.getLastSnapStartTimestamp() == ts && persistedWriterMetadata.getLastSnapTransferDoneTimestamp() <= ts) {
                persistedWriterMetadata.setLastSnapTransferDoneTimestamp(ts);
                seqNum = persistedWriterMetadata.getLastSnapSeqNum() + 1;
                clearTables();
            }
        } catch (Exception e) {
            log.warn("caught an exception ", e);
        } finally {
            rt.getObjectsView().TXEnd();
        }

        if (seqNum == 0)
            return;

        applyShadowStreams(seqNum);
    }

    /**
     * read from shadowStream and append to the
     */
    public void applyShadowStreams(Long seqNum) {
        System.out.print("***start applyShadowStreams with seqNum " + seqNum);
        phase = Phase.ApplyPhase;
        long snapshot = rt.getAddressSpaceView().getLogTail();
        for (UUID uuid : streamViewMap.keySet()) {
            seqNum = applyShadowStream(uuid, seqNum, snapshot);
        }
    }

    enum Phase {
        TransferPhase,
        ApplyPhase
    };
}
