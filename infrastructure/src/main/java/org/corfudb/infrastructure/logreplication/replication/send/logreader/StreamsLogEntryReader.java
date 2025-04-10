package org.corfudb.infrastructure.logreplication.replication.send.logreader;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.TextFormat;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MeterRegistryProvider;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMetadataMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryType;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.ObjectsView;
import org.corfudb.runtime.view.stream.OpaqueStream;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.google.protobuf.UnsafeByteOperations.unsafeWrap;
import static org.corfudb.protocols.CorfuProtocolCommon.getUuidMsg;
import static org.corfudb.protocols.service.CorfuProtocolLogReplication.generatePayload;
import static org.corfudb.protocols.service.CorfuProtocolLogReplication.getLrEntryMsg;

@Slf4j
@NotThreadSafe
/**
 * Reading transaction log changes after a snapshot transfer for a specific set of streams. The set of streams to replicate
 * will be synced by the config at the start of a log entry sync and when a new stream to replicate is discovered.
 */
public class StreamsLogEntryReader implements LogEntryReader {

    private final LogReplicationEntryType MSG_TYPE = LogReplicationEntryType.LOG_ENTRY_MESSAGE;

    private final LogReplicationConfig config;

    // Set of UUIDs for the corresponding streams
    private Set<UUID> streamUUIDs;

    // Opaque Stream wrapper for the transaction stream
    private TxOpaqueStream txOpaqueStream;

    // Snapshot Timestamp on which the log entry reader is based on
    private long globalBaseSnapshot;

    // Timestamp of the transaction log that is the previous message
    private long preMsgTs;

    // the sequence number of the message based on the globalBaseSnapshot
    private long sequence;

    private long topologyConfigId;

    private final int maxDataSizePerMsg;

    private final Optional<DistributionSummary> messageSizeDistributionSummary;
    private final Optional<Counter> deltaCounter;
    private final Optional<Counter> validDeltaCounter;
    private final Optional<Counter> opaqueEntryCounter;
    @Getter
    @VisibleForTesting
    private OpaqueEntry lastOpaqueEntry = null;
    private boolean lastOpaqueEntryValid = true;
    private StreamIteratorMetadata currentProcessedEntryMetadata;

    public StreamsLogEntryReader(CorfuRuntime runtime, LogReplicationConfig config) {
        runtime.parseConfigurationString(runtime.getLayoutServers().get(0)).connect();
        this.config = config;
        this.maxDataSizePerMsg = config.getMaxDataSizePerMsg();
        this.currentProcessedEntryMetadata = new StreamIteratorMetadata(Address.NON_ADDRESS, false);
        this.messageSizeDistributionSummary = configureMessageSizeDistributionSummary();
        this.deltaCounter = configureDeltaCounter();
        this.validDeltaCounter = configureValidDeltaCounter();
        this.opaqueEntryCounter = configureOpaqueEntryCounter();

        // Get UUIDs for streams to replicate
        refreshStreamUUIDs();
        log.info("Total of {} streams to replicate at initialization.", this.streamUUIDs.size());
        //create an opaque stream for transaction stream
        txOpaqueStream = new TxOpaqueStream(runtime);
    }

    /**
     * Get streams to replicate from config and convert them into stream ids. This method will be invoked at
     * constructor and when LogReplicationConfig is synced with registry table.
     */
    private void refreshStreamUUIDs() {
        Set<String> streams = config.getStreamsToReplicate();

        streamUUIDs = new HashSet<>();
        for (String s : streams) {
            streamUUIDs.add(CorfuRuntime.getStreamID(s));
        }
    }

    private LogReplicationEntryMsg generateMessageWithOpaqueEntryList(
            List<OpaqueEntry> opaqueEntryList, UUID logEntryRequestId) {
        // Set the last timestamp as the max timestamp
        long currentMsgTs = opaqueEntryList.get(opaqueEntryList.size() - 1).getVersion();
        LogReplicationEntryMetadataMsg metadata = LogReplicationEntryMetadataMsg.newBuilder()
                .setEntryType(MSG_TYPE)
                .setTopologyConfigID(topologyConfigId)
                .setSyncRequestId(getUuidMsg(logEntryRequestId))
                .setTimestamp(currentMsgTs)
                .setPreviousTimestamp(preMsgTs)
                .setSnapshotTimestamp(globalBaseSnapshot)
                .setSnapshotSyncSeqNum(sequence)
                .build();

        LogReplicationEntryMsg txMessage = getLrEntryMsg(unsafeWrap(generatePayload(opaqueEntryList)), metadata);

        preMsgTs = currentMsgTs;
        sequence++;
        log.trace("Generate a log entry message {} with {} transactions ",
                TextFormat.shortDebugString(txMessage.getMetadata()), opaqueEntryList.size());
        return txMessage;
    }

    /**
     * Verify the transaction entry is valid, i.e., if the entry contains any
     * of the streams to be replicated.
     * <p>
     * Notice that a transaction stream entry can be fully or partially replicated,
     * i.e., if only a subset of streams in the transaction entry are part of the streams
     * to replicate, the transaction entry will be partially replicated,
     * avoiding replication of the other streams present in the transaction.
     *
     * @param entry transaction stream opaque entry
     * @return true, if the transaction entry has any valid stream to replicate.
     * false, otherwise.
     */
    private boolean isValidTransactionEntry(@NonNull OpaqueEntry entry) {
        Set<UUID> txEntryStreamIds = new HashSet<>(entry.getEntries().keySet());

        // Sanity Check: discard if transaction stream opaque entry is empty (no streams are present)
        if (txEntryStreamIds.isEmpty()) {
            log.debug("TX Stream entry[{}] :: EMPTY [ignored]", entry.getVersion());
            return false;
        }

        // It is possible that tables corresponding to some streams to replicate were not opened when LogReplicationConfig
        // was initialized. So these streams will be missing from the list of streams to replicate. Check the registry
        // table and add them to the list in that case.
        if (!streamUUIDs.containsAll(txEntryStreamIds)) {
            log.info("There could be additional streams to replicate in tx stream. Checking with registry table.");
            config.syncWithRegistry();
            refreshStreamUUIDs();
            // TODO: Add log message here for the newly found streams when we support incremental refresh.
        }
        // If none of the streams in the transaction entry are specified to be replicated, this is an invalid entry, skip
        if (Collections.disjoint(streamUUIDs, txEntryStreamIds)) {
            log.trace("TX Stream entry[{}] :: contains none of the streams of interest, streams={} [ignored]", entry.getVersion(), txEntryStreamIds);
            return false;
        } else {
            Set<UUID> ignoredTxStreams = txEntryStreamIds.stream().filter(id -> !streamUUIDs.contains(id))
                    .collect(Collectors.toSet());
            txEntryStreamIds.removeAll(ignoredTxStreams);
            log.debug("TX Stream entry[{}] :: replicate[{}]={}, ignore[{}]={} [valid]", entry.getVersion(),
                    txEntryStreamIds.size(), txEntryStreamIds, ignoredTxStreams.size(), ignoredTxStreams);
            return true;
        }
    }

    private boolean sizeExceeded(int currentMsgSize, int currentEntrySize) {
        // Check the size of current entry
        if (currentEntrySize > maxDataSizePerMsg) {
            log.warn("The current entry size {} is bigger than the maxDataSizePerMsg {} supported.  Streams in this " +
                    "transaction are {}", currentEntrySize, maxDataSizePerMsg, lastOpaqueEntry.getEntries().keySet());
            return true;
        }

        // If it exceeds the maximum size of this message, skip appending this entry,
        // it will be processed with the next message;
        return currentEntrySize + currentMsgSize > maxDataSizePerMsg;
    }

    public void setGlobalBaseSnapshot(long snapshot, long ackTimestamp) {
        globalBaseSnapshot = snapshot;
        preMsgTs = Math.max(snapshot, ackTimestamp);
        log.info("snapshot {} ackTimestamp {} preMsgTs {} seek {}", snapshot, ackTimestamp, preMsgTs, preMsgTs + 1);
        txOpaqueStream.seek(preMsgTs + 1);
        sequence = 0;
    }

    @Override
    public LogReplicationEntryMsg read(UUID logEntryRequestId) throws TrimmedException {
        List<OpaqueEntry> opaqueEntryList = new ArrayList<>();
        int currentEntrySize = 0;
        int currentMsgSize = 0;

        try {
            while (currentMsgSize < maxDataSizePerMsg) {
                if (lastOpaqueEntry != null) {

                    if (lastOpaqueEntryValid) {

                        lastOpaqueEntry = filterTransactionEntry(lastOpaqueEntry);

                        // If the currentEntry is too big to append the current message, will skip it and
                        // append it to the next message as the first entry.
                        currentEntrySize = ReaderUtility.calculateOpaqueEntrySize(lastOpaqueEntry);

                        // If adding this entry to the current batch can exceed maxDataSizePerMsg, it must be added
                        // to the next batch.  However, if it is the first entry(single transaction bigger than
                        // maxDataSizePerMsg), it cannot be skipped.  In this case, create a batch with this entry
                        // and send it.
                        if (sizeExceeded(currentMsgSize, currentEntrySize) && !opaqueEntryList.isEmpty()) {
                            break;
                        }

                        // Add the lastOpaqueEntry to the current message.
                        opaqueEntryList.add(lastOpaqueEntry);
                        currentMsgSize += currentEntrySize;
                    }

                    lastOpaqueEntry = null;
                }
                if (!txOpaqueStream.hasNext()) {
                    break;
                }

                lastOpaqueEntry = txOpaqueStream.next();
                deltaCounter.ifPresent(Counter::increment);
                lastOpaqueEntryValid = isValidTransactionEntry(lastOpaqueEntry);
                if (lastOpaqueEntryValid) {
                    validDeltaCounter.ifPresent(Counter::increment);
                }
                currentProcessedEntryMetadata = new StreamIteratorMetadata(txOpaqueStream.txStream.pos(), lastOpaqueEntryValid);
            }

            log.trace("Generate LogEntryDataMessage size {} with {} entries for maxDataSizePerMsg {}. lastEntry size {}",
                currentMsgSize, opaqueEntryList.size(), maxDataSizePerMsg, lastOpaqueEntry == null ? 0 : currentEntrySize);
            final double currentMsgSizeSnapshot = currentMsgSize;

            messageSizeDistributionSummary.ifPresent(distribution -> distribution.record(currentMsgSizeSnapshot));

            opaqueEntryCounter.ifPresent(counter -> counter.increment(opaqueEntryList.size()));

            if (opaqueEntryList.isEmpty()) {
                return null;
            }

            LogReplicationEntryMsg txMessage = generateMessageWithOpaqueEntryList(
                    opaqueEntryList, logEntryRequestId);
            return txMessage;

        } catch (Exception e) {
            log.warn("Caught an exception while reading transaction stream {}", e);
            throw e;
        }
    }

    /**
     * Filter out streams that are not intended for replication
     *
     * @param opaqueEntry opaque entry to parse.
     * @return filtered opaque entry
     */
    private OpaqueEntry filterTransactionEntry(OpaqueEntry opaqueEntry) {
        Map<UUID, List<SMREntry>> filteredTxEntryMap = opaqueEntry.getEntries().entrySet().stream()
                .filter(entry -> streamUUIDs.contains(entry.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        return new OpaqueEntry(opaqueEntry.getVersion(), filteredTxEntryMap);
    }

    private Optional<DistributionSummary> configureMessageSizeDistributionSummary() {
        return MeterRegistryProvider.getInstance().map(registry ->
                DistributionSummary.builder("logreplication.message.size.bytes")
                        .baseUnit("bytes")
                        .tags("replication.type", "logentry")
                        .register(registry));
    }

    private Optional<Counter> configureDeltaCounter() {
        return MeterRegistryProvider.getInstance().map(registry ->
                registry.counter("logreplication.opaque.count_total"));
    }

    private Optional<Counter> configureValidDeltaCounter() {
        return MeterRegistryProvider.getInstance().map(registry ->
                registry.counter("logreplication.opaque.count_valid"));
    }

    private Optional<Counter> configureOpaqueEntryCounter() {
        return MeterRegistryProvider.getInstance().map(registry ->
                registry.counter("logreplication.opaque.count_per_message"));
    }

    @Override
    public void reset(long lastSentBaseSnapshotTimestamp, long lastAckedTimestamp) {
        // Sync with registry when entering into IN_LOG_ENTRY_SYNC state
        config.syncWithRegistry();
        refreshStreamUUIDs();
        this.currentProcessedEntryMetadata = new StreamIteratorMetadata(Address.NON_ADDRESS, false);
        setGlobalBaseSnapshot(lastSentBaseSnapshotTimestamp, lastAckedTimestamp);
        lastOpaqueEntry = null;
    }

    @Override
    public StreamIteratorMetadata getCurrentProcessedEntryMetadata() {
        return currentProcessedEntryMetadata;
    }

    /**
     * The class used to track the transaction opaque stream
     */
    public static class TxOpaqueStream {
        private CorfuRuntime rt;
        private OpaqueStream txStream;
        private Iterator iterator;

        public TxOpaqueStream(CorfuRuntime rt) {
            //create an opaque stream for transaction stream
            this.rt = rt;
            txStream = new OpaqueStream(rt.getStreamsView().get(ObjectsView.getLogReplicatorStreamId()));
            streamUpTo();
        }

        /**
         * Set the iterator with entries from current seekAddress till snapshot
         *
         * @param snapshot
         */
        private void streamUpTo(long snapshot) {
            log.trace("StreamUpTo {}", snapshot);
            iterator = txStream.streamUpTo(snapshot).iterator();
        }

        /**
         * Set the iterator with entries from current seekAddress till end of the log tail
         */
        private void streamUpTo() {
            streamUpTo(rt.getAddressSpaceView().getLogTail());
        }

        /**
         * Tell if the transaction stream has the next entry
         */
        private boolean hasNext() {
            if (!iterator.hasNext()) {
                streamUpTo();
            }
            return iterator.hasNext();
        }

        /**
         * Get the next entry from the transaction stream.
         */
        private OpaqueEntry next() {
            if (!hasNext())
                return null;

            OpaqueEntry opaqueEntry = (OpaqueEntry) iterator.next();
            log.trace("Address {} OpaqueEntry {}", opaqueEntry.getVersion(), opaqueEntry);
            return opaqueEntry;
        }

        /**
         * Set stream head as firstAddress, set the iterator from
         * firstAddress till the current tail of the log
         *
         * @param firstAddress
         */
        public void seek(long firstAddress) {
            log.trace("seek head {}", firstAddress);
            txStream.seek(firstAddress);
            streamUpTo();
        }
    }

    public static class StreamIteratorMetadata {
        private long timestamp;
        private boolean streamsToReplicatePresent;

        public StreamIteratorMetadata(long timestamp, boolean streamsToReplicatePresent) {
            this.timestamp = timestamp;
            this.streamsToReplicatePresent = streamsToReplicatePresent;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public boolean isStreamsToReplicatePresent() {
            return streamsToReplicatePresent;
        }
    }

    @Override
    public void setTopologyConfigId(long topologyConfigId) {
        this.topologyConfigId = topologyConfigId;
    }
}
