package org.corfudb.logreplication.transmitter;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.logreplication.MessageType;
import org.corfudb.logreplication.fsm.LogReplicationConfig;
import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.ObjectsView;
import org.corfudb.runtime.view.StreamOptions;
import org.corfudb.runtime.view.stream.OpaqueStream;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

@Slf4j
@NotThreadSafe
public class StreamsLogEntryReader implements LogEntryReader {
    private CorfuRuntime rt;
    private final MessageType MSG_TYPE = MessageType.LOG_ENTRY_MESSAGE;
    private OpaqueStream txStream;
    private long globalBaseSnapshot;
    private long preMsgTs;
    private long currentMsgTs;
    private long sequence;
    private Set<String> streams;
    private Set<UUID> streamUUIDs;

    public StreamsLogEntryReader(CorfuRuntime runtime, LogReplicationConfig config) {
        this.rt = runtime;
        streams = config.getStreamsToReplicate();
        initStream();
    }

    void initStream() {
        for (String s : streams) {
            streamUUIDs.add(CorfuRuntime.getStreamID(s));
        }
        StreamOptions options = StreamOptions.builder()
                .cacheEntries(false)
                .build();
        long tail = 0;
        txStream = new OpaqueStream(rt, rt.getStreamsView().get(ObjectsView.TRANSACTION_STREAM_ID));
    }


    TxMessage generateMessage(OpaqueEntry entry) {
        ByteBuf buf = Unpooled.buffer();
        OpaqueEntry.serialize(buf, entry);

        currentMsgTs = entry.getVersion();
        TxMessage txMessage = new TxMessage(MSG_TYPE, currentMsgTs, preMsgTs, globalBaseSnapshot, sequence, buf.array());
        preMsgTs = currentMsgTs;
        sequence++;
        return  txMessage;
    }

    boolean shouldProcess(OpaqueEntry entry) throws Exception {
        Set<UUID> tmpUUIDs = entry.getEntries().keySet();

        //If the entry's stream set is a subset of interested streams, it is the entry we should process
        if (streamUUIDs.containsAll(tmpUUIDs))
            return true;

        //If the entry's stream set has no overlap with the interested streams, it should be skipped.
        tmpUUIDs.retainAll(streamUUIDs);
        if (tmpUUIDs.isEmpty())
            return false;

        //If the entry's stream set contains both interested streams and other streams, it is not
        //the expected behavior
        log.error("There are noisy streams {} in the entry, expected streams set {}",
                    entry.getEntries().keySet(), streamUUIDs);
        throw new Exception("There are noisy streams");

    }

    void nextMsgs() throws Exception {
        //txStream.seek(preMsgTs + 1);  we may no need to call seek every time
        long tail = rt.getAddressSpaceView().getLogTail();
        Stream stream = txStream.streamUpTo(tail); //this can throw trimmed exception

        while(stream.iterator().hasNext()) {
            OpaqueEntry opaqueEntry = (OpaqueEntry)stream.iterator().next();
            if (!shouldProcess(opaqueEntry)) {
                continue;
            }
            TxMessage txMessage = generateMessage(opaqueEntry);
            //callback to send message
        }
    }

    public void setGlobalBaseSnapshot(long snapshot) {
        globalBaseSnapshot = snapshot;
        preMsgTs = snapshot;
        txStream.seek(snapshot + 1);
        sequence = 0;
    }

    public void sync() throws Exception {
        try {
            nextMsgs();
        } catch (Exception e) {
            log.warn("Sync caught an exception ", e);
            throw(e);
        }
    }
}
