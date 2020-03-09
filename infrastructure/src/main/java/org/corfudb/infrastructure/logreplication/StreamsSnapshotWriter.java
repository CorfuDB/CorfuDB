package org.corfudb.infrastructure.logreplication;

import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntryMetadata;
import org.corfudb.protocols.wireprotocol.logreplication.MessageType;

import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.stream.IStreamView;
import org.corfudb.util.serializer.Serializers;

import javax.annotation.concurrent.NotThreadSafe;
import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Writing a snapshot fullsync data
 * Open streams interested and append all entries
 */

@Slf4j
@NotThreadSafe
public class StreamsSnapshotWriter implements SnapshotWriter {
    private final static int MAX_NUM_TX_RETRY = 4;
    HashMap<UUID, IStreamView> streamViewMap; // It contains all the streams registered for write to.
    CorfuRuntime rt;
    private long srcGlobalSnapshot; // The source snapshot timestamp
    private Set<UUID> streamsDone;
    private long recvSeq;
    private PersistedWriterMetadata persistedWriterMetadata;

    // The sequence number of the message, it has received.
    // It is expecting the message in order of the sequence.

    public StreamsSnapshotWriter(CorfuRuntime rt, LogReplicationConfig config, PersistedWriterMetadata persistedWriterMetadata) {
        this.rt = rt;
        this.persistedWriterMetadata = persistedWriterMetadata;

        streamViewMap = new HashMap<>();

        for (String stream : config.getStreamsToReplicate()) {
            UUID streamID = CorfuRuntime.getStreamID(stream);
            IStreamView sv = rt.getStreamsView().getUnsafe(streamID);
            streamViewMap.put(streamID, sv);
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
                    SMREntry entry = new SMREntry("clear", new Array[0], Serializers.PRIMITIVE);
                    TransactionalContext.getCurrentContext().logUpdate(streamID, entry);
                }

                doRetry = false;
                log.info("Clear tables for streams {} ", streamViewMap.keySet());
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
       streamsDone = new HashSet<>();
       clearTables();
    }

    /**
     * Convert an OpaqueEntry to an MultiObjectSMREntry and write to log.
     * @param opaqueEntry
     */
    void processOpaqueEntry(LogReplicationEntry entry, OpaqueEntry opaqueEntry) {
        int numRetry = 0;
        boolean doRetry = true;
        long currentSeqNum = entry.getMetadata().getSnapshotSyncSeqNum();
        long persistentSeqNum = Address.NON_ADDRESS;

        while (doRetry && numRetry++ < MAX_NUM_TX_RETRY){
            try {
                rt.getObjectsView().TXBegin();
                // read persistentMetadata's snapshot seq number
                persistentSeqNum = persistedWriterMetadata.getLastSnapSeqNum();

                if (currentSeqNum == (persistentSeqNum + 1)) {
                    for (UUID uuid : opaqueEntry.getEntries().keySet()) {
                        for (SMREntry smrEntry : opaqueEntry.getEntries().get(uuid)) {
                            //streamViewMap.get(uuid).append(smrEntry);
                            TransactionalContext.getCurrentContext().logUpdate(uuid, smrEntry);
                        }
                    }
                    persistedWriterMetadata.setLastSnapSeqNum(currentSeqNum);
                    log.debug("Process the entry {} and set sequence number {} ",
                            entry.getMetadata(), currentSeqNum);

                } else {
                    log.warn("Skip the entry {} as the sequence number is not equal to {} + 1",
                            entry.getMetadata(), persistentSeqNum);
                }

                // We have succeed update successful, don't need retry any more.
                doRetry = false;
            } catch (TransactionAbortedException e) {
                log.warn("Caught an exception {}, will retry {}.", e, numRetry);
             } finally {
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

        processOpaqueEntry(message, opaqueEntry);
        recvSeq++;
    }

    @Override
    public void apply(List<LogReplicationEntry> messages) throws Exception {
        for (LogReplicationEntry msg : messages) {
            apply(msg);
        }
    }
}
