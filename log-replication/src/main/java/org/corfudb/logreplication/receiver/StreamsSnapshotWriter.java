package org.corfudb.logreplication.receiver;

import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.logreplication.MessageMetadata;
import org.corfudb.logreplication.transmitter.TxMessage;
import org.corfudb.protocols.logprotocol.MultiObjectSMREntry;
import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import com.google.common.reflect.TypeToken;
import org.corfudb.runtime.view.stream.IStreamView;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Writing a fullsync
 * Open streams interested and append all entries
 */

@Slf4j
@NotThreadSafe
public class StreamsSnapshotWriter implements SnapshotWriter {
    private Set<String> streams;
    HashMap<UUID, IStreamView> streamViewMap;
    CorfuRuntime rt;
    private long srcGlobalSnapshot;
    private long recvSeq;

    /**
     * open all streams interested
     */
    void openStreams() {
        for (String stream : streams) {
            UUID streamID = CorfuRuntime.getStreamID(stream);
            IStreamView sv = rt.getStreamsView().getUnsafe(streamID);
            streamViewMap.put(streamID, sv);
        }
    }

    StreamsSnapshotWriter(CorfuRuntime rt, Set<String> streams) {
        this.rt = rt;
        this.streams = streams;
        openStreams();
    }

    /**
     * clear all tables interested
     */
    void clearTables() {
        for (String stream : streams) {
            UUID streamID = CorfuRuntime.getStreamID(stream);
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
     * if the metadata has wrong message type or baseSnapshot, throw an exception
     * @param metadata
     * @return
     */
    void verifyMetadata(MessageMetadata metadata) throws Exception {
        if (metadata.getSnapshotTimestamp() != srcGlobalSnapshot) {
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
        if (message.getMetadata().getFullSyncSeqNum() != recvSeq) {
            log.error("Expecting sequencer {} != recvSeq {}",
                    message.getMetadata().getFullSyncSeqNum(), recvSeq);
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
