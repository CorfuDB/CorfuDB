package org.corfudb.logreplication.transmitter;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.logreplication.MessageType;
import org.corfudb.logreplication.fsm.LogReplicationConfig;
import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.view.ObjectsView;
import org.corfudb.runtime.view.stream.OpaqueStream;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

@Slf4j
@NotThreadSafe
/**
 * Reading transaction log changes after a snapshot transfer for a specific set of streams.
 */
public class StreamsLogEntryReader implements LogEntryReader {
    private CorfuRuntime rt;
    private final MessageType MSG_TYPE = MessageType.LOG_ENTRY_MESSAGE;
    private OpaqueStream txStream; //The opaquestream wrapper for the transaction stream.
    private long globalBaseSnapshot; //The base snapshot the log entry reader starts to poll transaction logs
    private Set<UUID> streamUUIDs; //The set of uuids for the corresponding streams.
    private long preMsgTs; //the timestamp of the transaction log that is the previous message
    private long currentMsgTs; //the timestamp of the transaction log that is the current message
    private long sequence; //the sequence number of the message based on the globalBaseSnapshot
    private PersistedReaderMetadata persistedMetadata;

    public StreamsLogEntryReader(CorfuRuntime runtime, LogReplicationConfig config) {
        this.rt = runtime;
        Set<String> streams = config.getStreamsToReplicate();

        for (String s : streams) {
            streamUUIDs.add(CorfuRuntime.getStreamID(s));
        }

        //create an opaque stream for transaction stream
        txStream = new OpaqueStream(rt, rt.getStreamsView().get(ObjectsView.TRANSACTION_STREAM_ID));
        persistedMetadata = new PersistedReaderMetadata(rt, config.getSiteID(), config.getRemoteSiteID());
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

    boolean shouldProcess(OpaqueEntry entry) throws ReplicationReaderException {
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
        throw new ReplicationReaderException("There are noisy streams in the transaction log entry");

    }

    public void setGlobalBaseSnapshot(long snapshot) {
        globalBaseSnapshot = snapshot;
        preMsgTs = snapshot;
        txStream.seek(snapshot + 1);
        sequence = 0;
    }

    public void read() throws TrimmedException, ReplicationReaderException {
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

    public void ackFromWriter(long timestamp) {
        persistedMetadata.setLastAckedTimestamp(timestamp);
    }
}
