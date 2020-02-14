package org.corfudb.logreplication.receive;

import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.logreplication.message.MessageMetadata;

import org.corfudb.logreplication.message.MessageType;
import org.corfudb.logreplication.fsm.LogReplicationConfig;
import org.corfudb.logreplication.message.DataMessage;

import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.CorfuRuntime;
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
    HashMap<UUID, IStreamView> streamViewMap; // It contains all the streams registered for write to.
    CorfuRuntime rt;
    private long srcGlobalSnapshot; // The source snapshot timestamp
    private Set<UUID> streamsDone;
    private long recvSeq;

    // The sequence number of the message, it has received.
    // It is expecting the message in order of the sequence.

    public StreamsSnapshotWriter(CorfuRuntime rt, LogReplicationConfig config) {
        this.rt = rt;
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
        for (IStreamView sv : streamViewMap.values()) {
            //maxi: just put dummy args and serilizer, may ask Maithem for correctness.
            SMREntry entry = new SMREntry("clear", new Array[0], Serializers.PRIMITIVE);
            sv.append(entry);
        }
    }

    /**
     * If the metadata has wrong message type or baseSnapshot, throw an exception
     * @param metadata
     * @return
     */
    void verifyMetadata(MessageMetadata metadata) throws ReplicationWriterException {
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
    void processOpaqueEntry(OpaqueEntry opaqueEntry) {
        for (UUID uuid : opaqueEntry.getEntries().keySet()) {
            for (SMREntry smrEntry : opaqueEntry.getEntries().get(uuid)) {
                streamViewMap.get(uuid).append(smrEntry);
            }
        }
    }

    @Override
    public void apply(DataMessage message) {
        verifyMetadata(message.getMetadata());
        if (message.getMetadata().getSnapshotSyncSeqNum() != recvSeq) {
            log.error("Expecting sequencer {} != recvSeq {}",
                    message.getMetadata().getSnapshotSyncSeqNum(), recvSeq);
            throw new ReplicationWriterException("Message is out of order");
        }

        OpaqueEntry opaqueEntry = OpaqueEntry.deserialize(Unpooled.wrappedBuffer(message.getData()));
        if (opaqueEntry.getEntries().keySet().size() != 1) {
            log.error("The opaqueEntry has more than one entry {}", opaqueEntry);
            return;
        }

        processOpaqueEntry(opaqueEntry);
        recvSeq++;
    }

    @Override
    public void apply(List<DataMessage> messages) throws Exception {
        for (DataMessage msg : messages) {
            apply(msg);
        }
    }
}
