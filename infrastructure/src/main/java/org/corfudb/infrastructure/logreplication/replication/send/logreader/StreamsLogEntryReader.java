package org.corfudb.infrastructure.logreplication.replication.send.logreader;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.infrastructure.logreplication.replication.send.IllegalTransactionStreamsException;
import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;
import org.corfudb.protocols.wireprotocol.logreplication.MessageType;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.view.ObjectsView;
import org.corfudb.runtime.view.stream.OpaqueStream;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;

@Slf4j
@NotThreadSafe
/**
 * Reading transaction log changes after a snapshot transfer for a specific set of streams.
 */
public class StreamsLogEntryReader implements LogEntryReader {

    private CorfuRuntime rt;
    private final MessageType MSG_TYPE = MessageType.LOG_ENTRY_MESSAGE;
    // the set of uuids for the corresponding streams.
    private Set<UUID> streamUUIDs;

    // the opaquestream wrapper for the transaction stream.
    private TxOpaqueStream txStream;
   

    // the base snapshot the log entry logreader starts to poll transaction logs
    private long globalBaseSnapshot;
    // timestamp of the transaction log that is the previous message
    private long preMsgTs;
    // the timestamp of the transaction log that is the current message
    private long currentMsgTs;
    // the sequence number of the message based on the globalBaseSnapshot
    private long sequence;

    private long topologyConfigId;

    public StreamsLogEntryReader(CorfuRuntime runtime, LogReplicationConfig config) {
        this.rt = runtime;
        this.rt.parseConfigurationString(runtime.getLayoutServers().get(0)).connect();
        Set<String> streams = config.getStreamsToReplicate();
        streamUUIDs = new HashSet<>();
        for (String s : streams) {
            streamUUIDs.add(CorfuRuntime.getStreamID(s));
        }

        //create an opaque stream for transaction stream
        txStream = new TxOpaqueStream(rt);
    }

    LogReplicationEntry generateMessage(OpaqueEntry entry, UUID logEntryRequestId) {
        ByteBuf buf = Unpooled.buffer();
        OpaqueEntry.serialize(buf, entry);
        currentMsgTs = entry.getVersion();
        LogReplicationEntry txMessage = new LogReplicationEntry(MSG_TYPE, topologyConfigId, logEntryRequestId,
                currentMsgTs, preMsgTs, globalBaseSnapshot, sequence, buf.array());
        preMsgTs = currentMsgTs;
        sequence++;
        return  txMessage;
    }

    boolean shouldProcess(OpaqueEntry entry) throws ReplicationReaderException {
        Set<UUID> tmpUUIDs = entry.getEntries().keySet();

        //If the entry's stream set is a subset of interested streams, it is the entry we should process
        if (streamUUIDs.containsAll(tmpUUIDs)) {
            return true;
        }

        //If the entry's stream set has no overlap with the interested streams, it should be skipped.
        tmpUUIDs.retainAll(streamUUIDs);
        if (tmpUUIDs.isEmpty()) {
            return false;
        }

        //If the entry's stream set contains both interested streams and other streams, it is not
        //the expected behavior
        log.error("There are noisy streams {} in the entry, expected streams set {}",
                    entry.getEntries().keySet(), streamUUIDs);

        throw new IllegalTransactionStreamsException("There are noisy streams in the transaction log entry");
    }

    public void setGlobalBaseSnapshot(long snapshot, long ackTimestamp) {
        globalBaseSnapshot = snapshot;
        preMsgTs = Math.max(snapshot, ackTimestamp);
        log.info("snapshot {} ackTimestamp {} preMsgTs {} seek {}", snapshot, ackTimestamp, preMsgTs, preMsgTs + 1);
        txStream.seek(preMsgTs + 1);
        sequence = 0;
    }

    @Override
    public LogReplicationEntry read(UUID logEntryRequestId) throws TrimmedException, IllegalTransactionStreamsException {
        try {
            while (txStream.hasNext()) {
                OpaqueEntry opaqueEntry = txStream.next();
                if (!shouldProcess(opaqueEntry)) {
                    continue;
                }
                LogReplicationEntry txMessage = generateMessage(opaqueEntry, logEntryRequestId);
                return txMessage;
            }
        } catch (Exception e) {
            log.warn("Caught an exception {}", e);
            throw e;
        }

        return null;
    }

    @Override
    public void reset(long lastSentBaseSnapshotTimestamp, long lastAckedTimestamp) {
        setGlobalBaseSnapshot(lastSentBaseSnapshotTimestamp, lastAckedTimestamp);
    }

    /**
     * The class used to track the transaction opaque stream
     */
    public static class TxOpaqueStream {
        CorfuRuntime rt;
        OpaqueStream txStream;
        Iterator iterator;
        
        public TxOpaqueStream(CorfuRuntime rt) {
            //create an opaque stream for transaction stream
            this.rt = rt;
            txStream = new OpaqueStream(rt, rt.getStreamsView().get(ObjectsView.TRANSACTION_STREAM_ID));
            streamUpTo();
        }

        /**
         * Set the iterator with entries from current seekAddress till snapshot
         * @param snapshot
         */
        void streamUpTo(long snapshot) {
            log.trace("StreamUpTo {}", snapshot);
            iterator = txStream.streamUpTo(snapshot).iterator();
        }

        /**
         * Set the iterator with entries from current seekAddress till end of the log tail
         */
        void streamUpTo() {
            streamUpTo(rt.getAddressSpaceView().getLogTail());
        }
            
        /**
         * Tell if the transaction stream has the next entry
         */
        boolean hasNext() {
            if (!iterator.hasNext()) {
                streamUpTo();
            }
            return iterator.hasNext();
        }

        /**
         * Get the next entry from the transaction stream.
         */
        OpaqueEntry next() {
            if (!hasNext())
                return null;

            OpaqueEntry opaqueEntry = (OpaqueEntry)iterator.next();
            log.trace("Address {} OpaqueEntry {}", opaqueEntry.getVersion(), opaqueEntry);
            return opaqueEntry;
        }

        /**
         * Set stream head as firstAddress, set the iterator from 
         * firstAddress till the current tail of the log
         * @param firstAddress
         */
        public void seek(long firstAddress) {
            log.trace("seek head {}", firstAddress);
            txStream.seek(firstAddress);
            streamUpTo();
        }
    }

    @Override
    public void setTopologyConfigId(long topologyConfigId) {
        this.topologyConfigId = topologyConfigId;
    }
}
