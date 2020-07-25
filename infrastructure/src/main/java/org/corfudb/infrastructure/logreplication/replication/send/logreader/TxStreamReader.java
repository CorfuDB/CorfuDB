package org.corfudb.infrastructure.logreplication.replication.send.logreader;

import com.google.common.annotations.VisibleForTesting;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.infrastructure.logreplication.replication.send.IllegalLogDataSizeException;
import org.corfudb.infrastructure.logreplication.replication.send.IllegalTransactionStreamsException;
import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;
import org.corfudb.protocols.wireprotocol.logreplication.MessageType;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.view.ObjectsView;
import org.corfudb.runtime.view.stream.OpaqueStream;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;

@Slf4j
@NotThreadSafe
/**
 * Reading transaction log changes after a snapshot transfer for a specific set of streams.
 */
public class TxStreamReader extends StreamsReader {

    private final MessageType MSG_TYPE = MessageType.LOG_ENTRY_MESSAGE;

    // the set of uuids for the corresponding streams.
    private Set<UUID> streamUUIDs;

    // the opaquestream wrapper for the transaction stream.
    private TxOpaqueStream txOpaqueStream;

    // the base snapshot the log entry logreader starts to poll transaction logs
    private long globalBaseSnapshot;

    // timestamp of the transaction log that is the previous message
    private long preMsgTs;

    // the timestamp of the transaction log that is the current message
    private long currentMsgTs;

    // the sequence number of the message based on the globalBaseSnapshot
    private long sequence;

    private OpaqueEntry lastOpaqueEntry = null;

    @VisibleForTesting
    public TxStreamReader() { }

    public TxStreamReader(CorfuRuntime runtime, LogReplicationConfig config) {
        this.rt = runtime;
        this.rt.parseConfigurationString(runtime.getLayoutServers().get(0)).connect();
        this.maxDataSizePerMsg = config.getMaxDataSizePerMsg();
        streams = config.getStreamsToReplicate();

        streamUUIDs = new HashSet<>();
        for (String s : streams) {
            streamUUIDs.add(CorfuRuntime.getStreamID(s));
        }

        //create an opaque stream for transaction stream
        txOpaqueStream = new TxOpaqueStream(rt);
    }

    private LogReplicationEntry generateMessageWithOpaqueEntryList(List<OpaqueEntry> opaqueEntryList, UUID logEntryRequestId) {
        if (opaqueEntryList.size() == 0) {
            return null;
        }

        // Set the last timestamp as the max timestamp
        currentMsgTs = opaqueEntryList.get(opaqueEntryList.size() - 1).getVersion();
        LogReplicationEntry txMessage = new LogReplicationEntry(MSG_TYPE, topologyConfigId, logEntryRequestId,
                currentMsgTs, preMsgTs, globalBaseSnapshot, sequence, opaqueEntryList);
        preMsgTs = currentMsgTs;
        sequence++;
        log.info("Generate a log entry message {} with {} transactions ", txMessage.getMetadata(), opaqueEntryList.size());
        return txMessage;
    }

    // Check if it has the correct streams.
    private boolean shouldProcess(OpaqueEntry entry) {
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

        // If the entry's stream set contains both interested streams and other streams, it is not
        // the expected behavior
        log.error("There are noisy streams {} in the entry, expected streams set {}",
                entry.getEntries().keySet(), streamUUIDs);

        hasNoiseData = true;
        return false;
    }

    public void setGlobalBaseSnapshot(long snapshot, long ackTimestamp) {
        globalBaseSnapshot = snapshot;
        preMsgTs = Math.max(snapshot, ackTimestamp);
        log.info("snapshot {} ackTimestamp {} preMsgTs {} seek {}", snapshot, ackTimestamp, preMsgTs, preMsgTs + 1);
        txOpaqueStream.seek(preMsgTs + 1);
        sequence = 0;
    }

    public LogReplicationEntry read(UUID logEntryRequestId) throws TrimmedException, IllegalTransactionStreamsException, IllegalLogDataSizeException {
        checkValidStreams();

        List<OpaqueEntry> opaqueEntryList = new ArrayList<>();
        int currentMsgSize = 0;
        int currentEntrySize = 0;

        try {
            while (currentMsgSize < maxDataSizePerMsg && !hasNoiseData &&
                    (lastOpaqueEntry != null || txOpaqueStream.hasNext())) {
                if (lastOpaqueEntry != null && shouldProcess(lastOpaqueEntry)) {
                    currentEntrySize = calculateOpaqueEntrySize(lastOpaqueEntry);
                    if (!shouldAppend(currentMsgSize, currentEntrySize)) {
                        break;
                    }

                    // Add the lastOpaqueEntry to the current message.
                    opaqueEntryList.add(lastOpaqueEntry);
                    currentMsgSize += currentEntrySize;
                    lastOpaqueEntry = null;
                }

                lastOpaqueEntry = txOpaqueStream.next();
            }

            if (hasNoiseData) {
                throw new IllegalTransactionStreamsException("The transaction log has illegal streams");
            }

            log.info("Generate LogEntryDataMessage size {} with {} entries for maxDataSizePerMsg {}. lastEnry size {}",
                    currentMsgSize, opaqueEntryList.size(), maxDataSizePerMsg, lastOpaqueEntry == null? 0 : currentEntrySize);

            LogReplicationEntry txMessage = generateMessageWithOpaqueEntryList(opaqueEntryList, logEntryRequestId);
            return txMessage;

        } catch (Exception e) {
            log.warn("Caught an exception {}", e);
            throw e;
        }
    }

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
}
