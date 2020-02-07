package org.corfudb.logreplication.receiver;

import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.logreplication.MessageMetadata;
import org.corfudb.logreplication.MessageType;
import org.corfudb.logreplication.transmitter.TxMessage;
import org.corfudb.protocols.logprotocol.MultiObjectSMREntry;
import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import com.google.common.reflect.TypeToken;
import org.corfudb.runtime.object.StreamViewSMRAdapter;
import org.corfudb.runtime.view.stream.IStreamView;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.HashMap;
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

    private long recvSeq;
    // The sequence number of the message, it has received.
    // It is expecting the message in order of the sequence.

    StreamsSnapshotWriter(CorfuRuntime rt, Set<String> streams) {
        this.rt = rt;
        for (String stream : streams) {
            UUID streamID = CorfuRuntime.getStreamID(stream);
            IStreamView sv = rt.getStreamsView().getUnsafe(streamID);
            streamViewMap.put(streamID, sv);
        }
    }

    /**
     * clear all tables registered
     */
    void clearTables() {
        for (UUID streamID : streamViewMap.keySet()) {
            CorfuTable<String, String> corfuTable = rt.getObjectsView()
                    .build()
                    .setTypeToken(new TypeToken<CorfuTable<String, String>>() {
                    })
                    .setStreamID(streamID)
                    .open();
            corfuTable.clear();
            corfuTable.close();
        }
    }

    /**
     * If the metadata has wrong message type or baseSnapshot, throw an exception
     * @param metadata
     * @return
     */
    void verifyMetadata(MessageMetadata metadata) throws Exception {
        if (metadata.getMessageMetadataType() != MessageType.SNAPSHOT_MESSAGE ||
                metadata.getSnapshotTimestamp() != srcGlobalSnapshot) {
            log.error("snapshot expected {} != recv snapshot {}, metadata {}",
                    srcGlobalSnapshot, metadata.getSnapshotTimestamp(), metadata);
            throw new Exception("Message is out of order");
        }
    }

    /**
     *
     */
    void reset(long snapshot) {
       srcGlobalSnapshot = snapshot;
       recvSeq = 0;
    }

    /**
     * Convert an OpaqueEntry to an MultiObjectSMREntry and write to log.
     * @param opaqueEntry
     */
    void processOpaqueEntry(OpaqueEntry opaqueEntry) {
        MultiObjectSMREntry multiObjectSMREntry = new MultiObjectSMREntry();
        for (UUID uuid : opaqueEntry.getEntries().keySet()) {
            for (SMREntry smrEntry : opaqueEntry.getEntries().get(uuid)) {
                multiObjectSMREntry.addTo(uuid, smrEntry);
                streamViewMap.get(uuid).append(multiObjectSMREntry);
            }
        }
    }

    @Override
    public void apply(TxMessage message) throws Exception {
        verifyMetadata(message.getMetadata());
        if (message.getMetadata().getSnapshotSyncSeqNum() != recvSeq) {
            log.error("Expecting sequencer {} != recvSeq {}",
                    message.getMetadata().getSnapshotSyncSeqNum(), recvSeq);
            throw new Exception("Message is out of order");
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
    public void apply(List<TxMessage> messages) throws Exception {
        for (TxMessage msg : messages) {
            apply(msg);
        }
    }
}
