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
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMetadataMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryType;
import org.corfudb.runtime.collections.CorfuRecord;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.ObjectsView;
import org.corfudb.runtime.view.TableRegistry;
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
import static org.corfudb.infrastructure.logreplication.LogReplicationConfig.MAX_DATA_MSG_SIZE_SUPPORTED;
import static org.corfudb.protocols.CorfuProtocolCommon.getUuidMsg;
import static org.corfudb.protocols.service.CorfuProtocolLogReplication.generatePayload;
import static org.corfudb.protocols.service.CorfuProtocolLogReplication.getLrEntryMsg;

@Slf4j
@NotThreadSafe
/**
 * Reading transaction log changes after a snapshot transfer for a specific set of streams.
 */
public class StreamsLogEntryReader implements LogEntryReader {

    private final LogReplicationEntryType MSG_TYPE = LogReplicationEntryType.LOG_ENTRY_MESSAGE;

    private final Set<UUID> streamUUIDs;

    private final Set<UUID> confirmedNoisyStreams;

    private final CorfuRuntime runtime;

    private final LogReplicationConfig config;

    // Opaque Stream wrapper for the transaction stream
    private final TxOpaqueStream txOpaqueStream;

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

    private boolean messageExceededSize = false;

    private StreamIteratorMetadata currentProcessedEntryMetadata;

    public StreamsLogEntryReader(CorfuRuntime runtime, LogReplicationConfig config) {
        runtime.parseConfigurationString(runtime.getLayoutServers().get(0)).connect();
        this.runtime = runtime;
        this.config = config;
        this.maxDataSizePerMsg = config.getMaxDataSizePerMsg();
        this.currentProcessedEntryMetadata = new StreamIteratorMetadata(Address.NON_ADDRESS, false);
        this.messageSizeDistributionSummary = configureMessageSizeDistributionSummary();
        this.deltaCounter = configureDeltaCounter();
        this.validDeltaCounter = configureValidDeltaCounter();
        this.opaqueEntryCounter = configureOpaqueEntryCounter();
        this.streamUUIDs = new HashSet<>(config.getStreamsInfo().getStreamIds());
        this.confirmedNoisyStreams = new HashSet<>();

        log.debug("Streams to replicate total={}, stream_ids={}", streamUUIDs.size(), streamUUIDs);

        //create an opaque stream for transaction stream
        txOpaqueStream = new TxOpaqueStream(runtime);
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

    private void checkStreams(Set<UUID> txEntryStreamIds) {
        // Get the set of stream ids that we encounter for the first time. i.e. Neither recorded
        // in LogReplicationConfig nor marked as noisy.
        Set<UUID> checkIdSet = txEntryStreamIds.stream()
                .filter(id -> !streamUUIDs.contains(id) && !confirmedNoisyStreams.contains(id))
                .collect(Collectors.toSet());

        if (checkIdSet.isEmpty()) {
            return;
        }

        // Get registry table for checking is_federated flag.
        CorfuTable<CorfuStoreMetadata.TableName,
                CorfuRecord<CorfuStoreMetadata.TableDescriptors, CorfuStoreMetadata.TableMetadata>>
                registryTable = runtime.getTableRegistry().getRegistryTable();

        Set<CorfuStoreMetadata.TableName> checkNameSet = registryTable.keySet().stream()
                .filter(tableName -> {
                    UUID streamID = CorfuRuntime.getStreamID(TableRegistry.getFullyQualifiedTableName(tableName));
                    return checkIdSet.contains(streamID);
                }).collect(Collectors.toSet());

        Set<UUID> discoveredNewStreams = new HashSet<>();
        for (CorfuStoreMetadata.TableName tableName : checkNameSet) {
            CorfuRecord<CorfuStoreMetadata.TableDescriptors, CorfuStoreMetadata.TableMetadata> tableRecord = registryTable.get(tableName);

            UUID discoveredStreamID = CorfuRuntime.getStreamID(TableRegistry.getFullyQualifiedTableName(tableName));
            if (tableRecord.getMetadata().getTableOptions().getIsFederated()) {
                streamUUIDs.add(discoveredStreamID);
                discoveredNewStreams.add(discoveredStreamID);
            } else {
                confirmedNoisyStreams.add(discoveredStreamID);
            }
        }

        if (!discoveredNewStreams.isEmpty()) {
            config.getStreamsInfo().addStreams(discoveredNewStreams);
        }
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

        // For all stream ids present in the transaction stream, inspect those observed for the first
        // time, and query through table registry whether they are intended to be replicated or not
        // i.e., whether 'is_federated' flag is set or not.
        checkStreams(txEntryStreamIds);
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
        return currentEntrySize + currentMsgSize <= maxDataSizePerMsg;
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

                        if (!checkValidSize(currentMsgSize, currentEntrySize)) {
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
        messageExceededSize = false;
        setGlobalBaseSnapshot(lastSentBaseSnapshotTimestamp, lastAckedTimestamp);
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

    @Override
    public boolean hasMessageExceededSize() {
        return messageExceededSize;
    }
}
