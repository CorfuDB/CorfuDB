package org.corfudb.logreplication.transmitter;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.logreplication.MessageType;
import org.corfudb.logreplication.fsm.LogReplicationConfig;
import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.stream.OpaqueStream;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;


@Slf4j
@NotThreadSafe
/**
 *  A class that represents the default implementation of a Snapshot Reader for Log Replication functionality.
 *
 *  This implementation provides log entries at the stream level (no coalesced state).
 *
 */
public class StreamsSnapshotReader implements SnapshotReader {
    //Todo: will change the max_batch_size while Maithem finish the new API
    private final int MAX_BATCH_SIZE = 1;
    private final MessageType MSG_TYPE = MessageType.SNAPSHOT_MESSAGE;
    private long globalSnapshot;
    private Set<String> streams;
    private PriorityQueue<String> streamsToSent;
    private CorfuRuntime rt;
    private long preMsgTs;
    private long currentMsgTs;
    private LogReplicationConfig config;
    private StreamInfo currentStreamInfo;
    private long sequence;

    /**
     * Init runtime and streams to read
     */
    public StreamsSnapshotReader(CorfuRuntime rt, LogReplicationConfig config) {
        this.rt = rt;
        this.config = config;
    }

    /**
     * Verify that the OpaqueEntry has the correct information.
     * @param stream
     * @param entry
     * @return
     */
    boolean verify(StreamInfo stream, OpaqueEntry entry) {
        Set<UUID> keySet = entry.getEntries().keySet();

        if (keySet.size() != 1 || !keySet.contains(stream.uuid)) {
            log.error("OpaqueEntry is wrong ", entry);
            return false;
        }
        return true;
    }

    /**
     * Given a streamID and list of smrEntries, generate an OpaqueEntry
     * @param streamID
     * @param smrEntries
     * @return
     */
    OpaqueEntry generateOpaqueEntry(UUID streamID, List smrEntries) {
        Map<UUID, List<SMREntry>> map = new HashMap<>();
        map.put(streamID, smrEntries);
        return new OpaqueEntry(currentMsgTs, map);
    }

    TxMessage generateMessage(StreamInfo stream, List<SMREntry> entries) {
        ByteBuf buf = Unpooled.buffer();
        OpaqueEntry.serialize(buf, generateOpaqueEntry(stream.uuid, entries));
        currentMsgTs = stream.maxVersion;
        if (!stream.iterator.hasNext()) {
            //mark the end of the current stream.
            currentMsgTs = globalSnapshot;
        }
        TxMessage txMsg = new TxMessage(MSG_TYPE, currentMsgTs, preMsgTs, globalSnapshot, sequence, buf.array());
        preMsgTs = currentMsgTs;
        sequence++;
        log.debug("Generate TxMsg {}", txMsg.getMetadata());
        return  txMsg;
    }

    List<SMREntry> next(StreamInfo stream, int numEntries) {
        //if it is the end of the stream, set an end of stream mark, the current
        List<SMREntry> list = new ArrayList<>();
        try {
            while (stream.iterator.hasNext() && list.size() < numEntries) {
                OpaqueEntry entry = (OpaqueEntry) stream.iterator.next();
                verify(stream, entry);
                stream.maxVersion = Math.max(stream.maxVersion, entry.getVersion());
                list.addAll(entry.getEntries().get(stream.uuid));
            }
        } catch (TrimmedException e) {
            log.error("Catch an TrimmedException exception ", e);
        }
        return list;
    }

    /**
     * Poll the current stream and get a batch of SMR entries and
     * generate one message
     * @param stream bookkeeping of the current stream information.
     * @return
     */
    TxMessage next(StreamInfo stream) {
        List<SMREntry> entries = next(stream, MAX_BATCH_SIZE);
        TxMessage txMsg = generateMessage(stream, entries);
        log.info("Successfully pass a stream {} for globalSnapshot {}", stream.name, globalSnapshot);
        return txMsg;
    }

    @Override
    public SnapshotReadMessage read() {
        if (currentStreamInfo == null || !currentStreamInfo.iterator.hasNext()) {
            if (streamsToSent.isEmpty()) {
                return new SnapshotReadMessage(null, true);
            }
            currentStreamInfo = new StreamInfo(streamsToSent.poll(), rt, globalSnapshot);
        }

        List msgs = new ArrayList<TxMessage>();
        msgs.add(next(currentStreamInfo));
        return new SnapshotReadMessage(msgs, streamsToSent.isEmpty()&&!currentStreamInfo.iterator.hasNext());
    }

    @Override
    public void reset(long snapshotTimestamp) {
        streamsToSent = new PriorityQueue<>(streams);
        preMsgTs = Address.NON_ADDRESS;
        currentMsgTs = Address.NON_ADDRESS;
        globalSnapshot = snapshotTimestamp; //rt.getAddressSpaceView().getLogTail();
        sequence = 0;
    }

    public static class StreamInfo {
        String name;
        UUID uuid;
        Stream stream;
        Iterator iterator;
        long maxVersion;

        StreamInfo(String name, CorfuRuntime rt, long snapshot) {
            this.name = name;
            uuid = CorfuRuntime.getStreamID(name);
            stream = (new OpaqueStream(rt, rt.getStreamsView().get(uuid))).streamUpTo(snapshot);
            iterator = stream.iterator();
            maxVersion = 0;
         }
    }
}
