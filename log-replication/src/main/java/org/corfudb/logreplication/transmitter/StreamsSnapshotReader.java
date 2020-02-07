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
 *  Default snapshot reader implementation
 *
 *  This implementation provides reads at the stream level (no coalesced state).
 *  It generates TxMessages which will be transmitted by the SnapshotListener (provided by the application).
 */
public class StreamsSnapshotReader implements SnapshotReader {
    private final int MAX_NUM_SMR_ENTRY = 50;
    private long globalSnapshot;
    private Set<String> streams;
    private PriorityQueue<String> streamsToSend;
    private CorfuRuntime rt;
    private long preMsgTs;
    private long currentMsgTs;
    private LogReplicationConfig config;
    private OpaqueStreamIterator currentStreamInfo;
    private long sequence;

    /**
     * Init runtime and streams to read
     */
    public StreamsSnapshotReader(CorfuRuntime rt, LogReplicationConfig config) {
        this.rt = rt;
        this.config = config;
        streams = config.getStreamsToReplicate();
    }

    /**
     * Verify that the OpaqueEntry has the correct information.
     * @param stream
     * @param entry
     * @return
     */
    boolean verify(OpaqueStreamIterator stream, OpaqueEntry entry) {
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

    /**
     * Given a list of entries with the same stream, will generate an OpaqueEntry and
     * use the opaqueentry to generate a TxMessage.
     * @param stream
     * @param entries
     * @return
     */
    TxMessage generateMessage(OpaqueStreamIterator stream, List<SMREntry> entries) {
        ByteBuf buf = Unpooled.buffer();
        OpaqueEntry.serialize(buf, generateOpaqueEntry(stream.uuid, entries));
        currentMsgTs = stream.maxVersion;
        if (!stream.iterator.hasNext()) {
            //mark the end of the current stream.
            currentMsgTs = globalSnapshot;
        }
        TxMessage txMsg = new TxMessage(MessageType.SNAPSHOT_MESSAGE, currentMsgTs, preMsgTs, globalSnapshot, sequence, buf.array());
        preMsgTs = currentMsgTs;
        sequence++;
        log.debug("Generate TxMsg {}", txMsg.getMetadata());
        return  txMsg;
    }

    /**
     * Read numEntries from the current stream.
     * @param stream
     * @param numEntries
     * @return
     */
    List<SMREntry> next(OpaqueStreamIterator stream, int numEntries) {
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
            throw e;
        }
        return list;
    }

    /**
     * Poll the current stream and get a batch of SMR entries and
     * generate one message
     * @param stream bookkeeping of the current stream information.
     * @return
     */
    TxMessage read(OpaqueStreamIterator stream) {
        List<SMREntry> entries = next(stream, MAX_NUM_SMR_ENTRY);
        TxMessage txMsg = generateMessage(stream, entries);
        log.info("Successfully pass a stream {} for globalSnapshot {}", stream.name, globalSnapshot);
        return txMsg;
    }

    /**
     * If currentStreamInfo is null that is the case for the first call of read, it will init currentStreamInfo.
     * If the currentStreamInfo ends, poll the next stream.
     * Otherwise, continue to process the current stream.
     * @return
     */
    @Override
    public SnapshotReadMessage read() {
        // If the currentStreamInfo still has entry to process, it will reuse the currentStreamInfo
        // and process the remaining entries.

        if (currentStreamInfo == null || !currentStreamInfo.iterator.hasNext()) {
            // If it is null, it means the start of snapshot fullsync, we should init the first stream
            // If the currentStream end, we need to poll the next stream.

            while (!streamsToSend.isEmpty()) {
                // Setup a new stream
                currentStreamInfo = new OpaqueStreamIterator(streamsToSend.poll(), rt, globalSnapshot);

                // If the new stream has entries to be proccessed, go to the next step
                if (currentStreamInfo.iterator.hasNext()) {
                    break;
                }

                // Skip process this stream as it has no entries to process, will poll the next one.
                log.info("Snapshot reader will skip reading stream {} as there are no entries to send",
                            currentStreamInfo.uuid);
            }


            if (streamsToSend.isEmpty() && !currentStreamInfo.iterator.hasNext()) {
                //there is no stream to be sent, return an end of full sync message.
                log.info("");
                return new SnapshotReadMessage(null, true);
            }
        }

        List msgs = new ArrayList<TxMessage>();
        msgs.add(read(currentStreamInfo));

        if (!currentStreamInfo.iterator.hasNext()) {
            log.debug("Snapshot reader finish reading stream {}", currentStreamInfo.uuid);
        }

        if (streamsToSend.isEmpty()) {
            log.info("Snapshot reader finish reading all streams {}", streams);
        }
        return new SnapshotReadMessage(msgs, streamsToSend.isEmpty()&&!currentStreamInfo.iterator.hasNext());
    }

    @Override
    public void reset(long snapshotTimestamp) {
        streamsToSend = new PriorityQueue<>(streams);
        preMsgTs = Address.NON_ADDRESS;
        currentMsgTs = Address.NON_ADDRESS;
        globalSnapshot = snapshotTimestamp; //rt.getAddressSpaceView().getLogTail();
        currentStreamInfo = null;
        sequence = 0;
    }

    /**
     * Used to bookkeeping the stream information for the current processing stream
     */
    public static class OpaqueStreamIterator {
        String name;
        UUID uuid;
        Iterator iterator;
        long maxVersion; //the max address of the log entries processed for this stream.

        OpaqueStreamIterator(String name, CorfuRuntime rt, long snapshot) {
            this.name = name;
            uuid = CorfuRuntime.getStreamID(name);
            Stream stream = (new OpaqueStream(rt, rt.getStreamsView().get(uuid))).streamUpTo(snapshot);
            iterator = stream.iterator();
            maxVersion = 0;
         }
    }
}
