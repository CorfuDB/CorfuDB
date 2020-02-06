package org.corfudb.logreplication.receiver;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.logreplication.MessageMetadata;
import org.corfudb.logreplication.transmitter.TxMessage;
import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import com.google.common.reflect.TypeToken;
import org.corfudb.runtime.object.StreamViewSMRAdapter;
import org.corfudb.runtime.view.stream.IStreamView;

import javax.annotation.concurrent.NotThreadSafe;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.UUID;

/**
 * Writing a fullsync
 * Open streams interested and append all entries
 */

@Slf4j
@NotThreadSafe
public class StreamsSnapshotWriter implements SnapshotWriter {
    private List<UUID> streamUUIDs;
    private Set<String> streams;
    HashMap<UUID, StreamViewSMRAdapter> streamViewMap;
    CorfuRuntime rt;
    long proccessedMsgTs;
    final private int QUEUE_SIZE = 20;
    private PriorityQueue<TxMessage> msgQ;
    private long srcGlobalSnapshot;
    private long recvSeq;

    StreamsSnapshotWriter(CorfuRuntime rt, Set<String> streams) {
        this.rt = rt;
        this.streams = streams;
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
     * open all streams interested
     */
    void openStreams() {
        for (UUID streamID : streamUUIDs) {
            StreamViewSMRAdapter svAdapter = new StreamViewSMRAdapter(rt, rt.getStreamsView().getUnsafe(streamID));
            streamViewMap.put(streamID, svAdapter);
        }
    }

    /**
     * if the metadata has wrong message type or baseSnapshot, throw an exception
     * @param metadata
     * @return
     */
    void verifyMetadata(MessageMetadata metadata) throws Exception {
        if (metadata.getFullSyncSeqNum() != recvSeq ||
                metadata.getSnapshotTimestamp() != srcGlobalSnapshot) {
            log.error("Expecting sequencer {} != recvSeq {} or snapshot expected {} != recv snapshot {}, metadata {}",
                    metadata.getFullSyncSeqNum(), recvSeq, srcGlobalSnapshot, metadata.getSnapshotTimestamp(), metadata);
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

    @Override
    public void apply(TxMessage message) throws Exception {
        verifyMetadata(message.getMetadata());
        OpaqueEntry entry = OpaqueEntry.deserialize(Unpooled.wrappedBuffer(message.getData()));
        for (UUID uuid : entry.getEntries().keySet()) {
            for(SMREntry smrEntry : entry.getEntries().get(uuid)) {
                streamViewMap.get(uuid).append(smrEntry, null, null);
            }
        }
    }

    @Override
    public void apply(List<TxMessage> messages) throws Exception {
        for (TxMessage msg : messages) {
            apply(msg);
        }
    }
}
