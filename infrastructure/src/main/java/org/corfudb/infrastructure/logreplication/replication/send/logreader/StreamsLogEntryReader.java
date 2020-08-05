package org.corfudb.infrastructure.logreplication.replication.send.logreader;

import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;
import org.corfudb.protocols.wireprotocol.logreplication.MessageType;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.view.ObjectsView;
import org.corfudb.runtime.view.stream.OpaqueStream;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.corfudb.infrastructure.logreplication.LogReplicationConfig.MAX_DATA_MSG_SIZE_SUPPORTED;

@Slf4j
@NotThreadSafe
/**
 * Reading transaction log changes after a snapshot transfer for a specific set of streams.
 */
public class StreamsLogEntryReader implements LogEntryReader {

    private CorfuRuntime rt;

    private final MessageType MSG_TYPE = MessageType.LOG_ENTRY_MESSAGE;

    // Set of UUIDs for the corresponding streams
    private Set<UUID> streamUUIDs;

    // Opaque Stream wrapper for the transaction stream
    private TxOpaqueStream txOpaqueStream;

    // Snapshot Timestamp on which the log entry reader is based on
    private long globalBaseSnapshot;

    // Timestamp of the transaction log that is the previous message
    private long preMsgTs;

    // the timestamp of the transaction log that is the current message
    private long currentMsgTs;

    // the sequence number of the message based on the globalBaseSnapshot
    private long sequence;

    private long topologyConfigId;

    private final int maxDataSizePerMsg;

    @Getter
    @VisibleForTesting
    private OpaqueEntry lastOpaqueEntry = null;

    private boolean messageExceededSize = false;

    public StreamsLogEntryReader(CorfuRuntime runtime, LogReplicationConfig config) {
        this.rt = runtime;
        this.rt.parseConfigurationString(runtime.getLayoutServers().get(0)).connect();
        this.maxDataSizePerMsg = config.getMaxDataSizePerMsg();

        Set<String> streams = config.getStreamsToReplicate();

        streamUUIDs = new HashSet<>();
        for (String s : streams) {
            streamUUIDs.add(CorfuRuntime.getStreamID(s));
        }

        log.info("On Streams Log Entry Reader start, streams to replicate total={}, stream_names={}. stream_ids={}", streamUUIDs.size(), streams, streamUUIDs);

        //create an opaque stream for transaction stream
        txOpaqueStream = new TxOpaqueStream(rt);
    }

    private LogReplicationEntry generateMessageWithOpaqueEntryList(List<OpaqueEntry> opaqueEntryList, UUID logEntryRequestId) {
        // Set the last timestamp as the max timestamp
        currentMsgTs = opaqueEntryList.get(opaqueEntryList.size() - 1).getVersion();
        LogReplicationEntry txMessage = new LogReplicationEntry(MSG_TYPE, topologyConfigId, logEntryRequestId,
                currentMsgTs, preMsgTs, globalBaseSnapshot, sequence, opaqueEntryList);
        preMsgTs = currentMsgTs;
        sequence++;
        log.trace("Generate a log entry message {} with {} transactions ", txMessage.getMetadata(), opaqueEntryList.size());
        return txMessage;
    }

    /**
     * Verify the transaction entry is valid, i.e., if the entry contains any
     * of the streams to be replicated.
     *
     * Notice that a transaction stream entry can be fully or partially replicated,
     * i.e., if only a subset of streams in the transaction entry are part of the streams
     * to replicate, the transaction entry will be partially replicated,
     * avoiding replication of the other streams present in the transaction.
     *
     * @param entry transaction stream opaque entry
     *
     * @return true, if the transaction entry has any valid stream to replicate.
     *         false, otherwise.
     */
    private boolean isValidTransactionEntry(@NonNull OpaqueEntry entry) {
        Set<UUID> txEntryStreamIds = entry.getEntries().keySet();

        txEntryStreamIds.retainAll(streamUUIDs);

        // If none of the streams in the transaction entry are specified to be replicated, this is an invalid entry, skip
        if (txEntryStreamIds.isEmpty()) {
            return false;
        }

        return true;
    }

    private boolean checkValidSize(int currentMsgSize, int currentEntrySize) {
        // For interested entry, if its size is too big we should skip and report error
        if (currentEntrySize > maxDataSizePerMsg) {
            log.error("The current entry size {} is bigger than the maxDataSizePerMsg {} supported.",
                    currentEntrySize, MAX_DATA_MSG_SIZE_SUPPORTED);
            // If a message cannot be sent due to its size exceeding the maximum boundary, the replication will be stopped.
            messageExceededSize = true;
            return false;
        }

        // If it exceeds the maximum size of this message, skip appending this entry,
        // it will be processed with the next message;
        if (currentEntrySize + currentMsgSize > maxDataSizePerMsg) {
            return false;
        }

        return true;
    }

    public void setGlobalBaseSnapshot(long snapshot, long ackTimestamp) {
        globalBaseSnapshot = snapshot;
        preMsgTs = Math.max(snapshot, ackTimestamp);
        log.info("snapshot {} ackTimestamp {} preMsgTs {} seek {}", snapshot, ackTimestamp, preMsgTs, preMsgTs + 1);
        txOpaqueStream.seek(preMsgTs + 1);
        sequence = 0;
    }

    @Override
    public LogReplicationEntry read(UUID logEntryRequestId) throws TrimmedException {
        List<OpaqueEntry> opaqueEntryList = new ArrayList<>();
        int currentEntrySize = 0;
        int currentMsgSize = 0;

        if (lastOpaqueEntry == null) {
            lastOpaqueEntry = txOpaqueStream.next();
        }

        try {
            while (currentMsgSize < maxDataSizePerMsg && lastOpaqueEntry != null) {
                if (isValidTransactionEntry(lastOpaqueEntry)) {

                    // If the currentEntry is too big to append the current message, will skip it and
                    // append it to the next message as the first entry.
                    currentEntrySize = ReaderUtility.calculateOpaqueEntrySize(lastOpaqueEntry);

                    if (!checkValidSize(currentMsgSize, currentEntrySize)) {
                        break;
                    }

                    // Add the lastOpaqueEntry to the current message.
                    opaqueEntryList.add(lastOpaqueEntry);
                    currentMsgSize += currentEntrySize;
                }

                lastOpaqueEntry = txOpaqueStream.next();
            }

            log.trace("Generate LogEntryDataMessage size {} with {} entries for maxDataSizePerMsg {}. lastEnry size {}",
                    currentMsgSize, opaqueEntryList.size(), maxDataSizePerMsg, lastOpaqueEntry == null ? 0 : currentEntrySize);

            if (opaqueEntryList.isEmpty()) {
                return null;
            }

            LogReplicationEntry txMessage = generateMessageWithOpaqueEntryList(opaqueEntryList, logEntryRequestId);
            return txMessage;

        } catch (Exception e) {
            log.warn("Caught an exception while reading transaction stream {}", e);
            throw e;
        }
    }


    @Override
    public void reset(long lastSentBaseSnapshotTimestamp, long lastAckedTimestamp) {
        messageExceededSize = false;
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

    @Override
    public boolean hasMessageExceededSize() {
        return messageExceededSize;
    }
}
