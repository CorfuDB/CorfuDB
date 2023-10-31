package org.corfudb.infrastructure.logreplication.replication.send.logreader;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.TextFormat;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MeterRegistryProvider;
import org.corfudb.infrastructure.logreplication.exceptions.GroupDestinationChangeException;
import org.corfudb.infrastructure.logreplication.exceptions.MessageSizeExceededException;
import org.corfudb.infrastructure.logreplication.infrastructure.LogReplicationContext;
import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LogReplication;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.stream.OpaqueStream;
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

/**
 * Base class for Log Entry Reader. It provides common functionality for log entry readers in different
 * replication models.
 */
@Slf4j
public abstract class BaseLogEntryReader extends LogEntryReader {

    private static final LogReplication.LogReplicationEntryType MSG_TYPE =
            LogReplication.LogReplicationEntryType.LOG_ENTRY_MESSAGE;

    // Opaque Stream wrapper for the stream to track for a given replication model
    private ModelBasedOpaqueStream modelBasedOpaqueStream;

    // Snapshot Timestamp on which the log entry reader is based on
    private long globalBaseSnapshot;

    // Timestamp of the transaction log that is the previous message
    private long preMsgTs;

    // the sequence number of the message based on the globalBaseSnapshot
    private long sequence;

    private final long maxDataSizePerMsg;

    private final Optional<DistributionSummary> messageSizeDistributionSummary;
    private final Optional<Counter> deltaCounter;
    private final Optional<Counter> validDeltaCounter;
    private final Optional<Counter> opaqueEntryCounter;
    @Getter
    @VisibleForTesting
    private OpaqueEntry lastOpaqueEntry = null;

    private boolean lastOpaqueEntryValid = true;

    private StreamIteratorMetadata currentProcessedEntryMetadata;

    protected LogReplication.LogReplicationSession session;

    protected LogReplicationContext replicationContext;
    
    private final String sessionName;

    public BaseLogEntryReader(LogReplication.LogReplicationSession replicationSession,
                              LogReplicationContext replicationContext) {
        CorfuRuntime runtime = replicationContext.getCorfuRuntime();
        this.maxDataSizePerMsg = replicationContext.getConfig(replicationSession).getMaxMsgSize();
        this.sessionName = replicationContext.getSessionName(replicationSession);

        this.currentProcessedEntryMetadata = new StreamIteratorMetadata(Address.NON_ADDRESS, false);
        this.messageSizeDistributionSummary = configureMessageSizeDistributionSummary();
        this.deltaCounter = configureDeltaCounter();
        this.validDeltaCounter = configureValidDeltaCounter();
        this.opaqueEntryCounter = configureOpaqueEntryCounter();
        this.session = replicationSession;
        this.replicationContext = replicationContext;

        log.info("[{}]:: Total of {} streams to replicate at initialization. Streams to replicate={}",
                sessionName, getStreamUUIDs().size(),
                replicationContext.getConfig(session).getStreamsToReplicate());

        //create an opaque stream for transaction stream
        modelBasedOpaqueStream = new ModelBasedOpaqueStream(runtime);
    }

    private LogReplication.LogReplicationEntryMsg generateMessageWithOpaqueEntryList(
        List<OpaqueEntry> opaqueEntryList, UUID logEntryRequestId) {

        // Set the last timestamp as the max timestamp
        long currentMsgTs = opaqueEntryList.get(opaqueEntryList.size() - 1).getVersion();
        LogReplication.LogReplicationEntryMetadataMsg metadata = LogReplication.LogReplicationEntryMetadataMsg.newBuilder()
            .setEntryType(MSG_TYPE)
            .setTopologyConfigID(topologyConfigId)
            .setSyncRequestId(getUuidMsg(logEntryRequestId))
            .setTimestamp(currentMsgTs)
            .setPreviousTimestamp(preMsgTs)
            .setSnapshotTimestamp(globalBaseSnapshot)
            .setSnapshotSyncSeqNum(sequence)
            .build();

        LogReplication.LogReplicationEntryMsg txMessage = getLrEntryMsg(unsafeWrap(generatePayload(opaqueEntryList)), metadata);

        preMsgTs = currentMsgTs;
        sequence++;
        log.trace("[{}]:: Generate a log entry message {} with {} transactions ", sessionName,
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
    boolean isValidTransactionEntry(@NonNull OpaqueEntry entry) {
        Set<UUID> txEntryStreamIds = new HashSet<>(entry.getEntries().keySet());

        // Sanity Check: discard if transaction stream opaque entry is empty (no streams are present)
        if (txEntryStreamIds.isEmpty()) {
            log.debug("[{}]:: TX Stream entry[{}] :: EMPTY [ignored]", sessionName, entry.getVersion());
            return false;
        }

        // It is possible that tables corresponding to some streams to replicate were not opened when LogReplicationConfig
        // was initialized. So these streams will be missing from the list of streams to replicate. Check the registry
        // table and add them to the list in that case.
        if (!getStreamUUIDs().containsAll(txEntryStreamIds)) {
            log.info("[{}]:: There could be additional streams to replicate in tx stream. Checking with registry table.", sessionName);
            replicationContext.refreshConfig(session, false);
            // TODO: Add log message here for the newly found streams when we support incremental refresh.
        }
        // If none of the streams in the transaction entry are specified to be replicated, this is an invalid entry, skip
        if (Collections.disjoint(getStreamUUIDs(), txEntryStreamIds)) {
            log.trace("[{}]:: TX Stream entry[{}] :: contains none of the streams of interest, streams={} [ignored]",
                    sessionName, entry.getVersion(), txEntryStreamIds);
            return false;
        } else {
            Set<UUID> ignoredTxStreams = txEntryStreamIds.stream().filter(id -> !getStreamUUIDs().contains(id))
                .collect(Collectors.toSet());
            txEntryStreamIds.removeAll(ignoredTxStreams);
            log.debug("[{}]:: TX Stream entry[{}] :: replicate[{}]={}, ignore[{}]={} [valid]", sessionName, entry.getVersion(),
                txEntryStreamIds.size(), txEntryStreamIds, ignoredTxStreams.size(), ignoredTxStreams);
            return true;
        }
    }

    public void setGlobalBaseSnapshot(long snapshot, long ackTimestamp) {
        globalBaseSnapshot = snapshot;
        preMsgTs = Math.max(snapshot, ackTimestamp);
        log.info("[{}]:: snapshot {} ackTimestamp {} preMsgTs {} seek {}", sessionName, snapshot, ackTimestamp, preMsgTs, preMsgTs + 1);
        modelBasedOpaqueStream.seek(preMsgTs + 1);
        sequence = 0;
    }

    @Override
    public LogReplication.LogReplicationEntryMsg read(UUID logEntryRequestId) throws TrimmedException,
            MessageSizeExceededException, GroupDestinationChangeException {
        List<OpaqueEntry> opaqueEntryList = new ArrayList<>();
        int currentEntrySize = 0;
        int currentMsgSize = 0;

        try {
            while (currentMsgSize < maxDataSizePerMsg) {
                if (lastOpaqueEntry != null) {

                    if (lastOpaqueEntryValid) {

                        lastOpaqueEntry = filterTransactionEntry(lastOpaqueEntry);
                        currentEntrySize = ReaderUtility.calculateOpaqueEntrySize(lastOpaqueEntry);

                        // If a message cannot be sent due to its size exceeding the maximum boundary,
                        // throw an exception and the replication will be stopped.
                        if (currentEntrySize > maxDataSizePerMsg) {
                            throw new MessageSizeExceededException();
                        }

                        // If it cannot fit into this message, skip appending this entry and it will
                        // be processed with the next message.
                        if (currentEntrySize + currentMsgSize > maxDataSizePerMsg) {
                            break;
                        }

                        // Add the lastOpaqueEntry to the current message.
                        opaqueEntryList.add(lastOpaqueEntry);
                        currentMsgSize += currentEntrySize;
                    }

                    lastOpaqueEntry = null;
                }
                if (!modelBasedOpaqueStream.hasNext()) {
                    break;
                }

                lastOpaqueEntry = modelBasedOpaqueStream.next();
                deltaCounter.ifPresent(Counter::increment);
                lastOpaqueEntryValid = isValidTransactionEntry(lastOpaqueEntry);
                if (lastOpaqueEntryValid) {
                    validDeltaCounter.ifPresent(Counter::increment);
                }
                currentProcessedEntryMetadata = new StreamIteratorMetadata(modelBasedOpaqueStream.opaqueStream.pos(),
                        lastOpaqueEntryValid);
            }

            log.trace("[{}]:: Generate LogEntryDataMessage size {} with {} entries for maxDataSizePerMsg {}. lastEntry size {}",
                    sessionName, currentMsgSize, opaqueEntryList.size(), maxDataSizePerMsg, lastOpaqueEntry == null ? 0 : currentEntrySize);
            final double currentMsgSizeSnapshot = currentMsgSize;

            messageSizeDistributionSummary.ifPresent(distribution -> distribution.record(currentMsgSizeSnapshot));

            opaqueEntryCounter.ifPresent(counter -> counter.increment(opaqueEntryList.size()));

            if (opaqueEntryList.isEmpty()) {
                return null;
            }

            LogReplication.LogReplicationEntryMsg txMessage = generateMessageWithOpaqueEntryList(
                opaqueEntryList, logEntryRequestId);
            return txMessage;

        } catch (Exception e) {
            log.warn("[{}]:: Caught an exception while reading transaction stream", sessionName, e);
            throw e;
        }
    }

    /**
     * Filter out streams that are not intended for replication
     *
     * @param opaqueEntry opaque entry to parse.
     * @return filtered opaque entry
     */
    protected OpaqueEntry filterTransactionEntry(OpaqueEntry opaqueEntry) {
        Map<UUID, List<SMREntry>> filteredTxEntryMap = opaqueEntry.getEntries().entrySet().stream()
            .filter(entry -> getStreamUUIDs().contains(entry.getKey()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        return new OpaqueEntry(opaqueEntry.getVersion(), filteredTxEntryMap);
    }

    /**
     * Helper method for getting UUIDs of streams to replicate from config manager.
     *
     * @return UUIDs of streams to replicate
     */
    Set<UUID> getStreamUUIDs() {
        return replicationContext.getConfig(session).getDataStreamToTagsMap().keySet();
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
        replicationContext.refreshConfig(session, true);
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
    private class ModelBasedOpaqueStream {
        private CorfuRuntime rt;
        private OpaqueStream opaqueStream;
        private Iterator iterator;

        public ModelBasedOpaqueStream(CorfuRuntime rt) {
            this.rt = rt;
            opaqueStream = new OpaqueStream(rt.getStreamsView().get(replicationContext.getConfigManager()
                    .getLogEntrySyncOpaqueStream(session)));
            streamUpTo();
        }

        /**
         * Set the iterator with entries from current seekAddress till snapshot
         *
         * @param snapshot
         */
        private void streamUpTo(long snapshot) {
            log.trace("[{}]:: StreamUpTo {}", sessionName, snapshot);
            iterator = opaqueStream.streamUpTo(snapshot).iterator();
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
            if (!hasNext()) {
                return null;
            }

            OpaqueEntry opaqueEntry = (OpaqueEntry) iterator.next();
            log.trace("[{}]:: Address {} OpaqueEntry {}", sessionName, opaqueEntry.getVersion(), opaqueEntry);
            return opaqueEntry;
        }

        /**
         * Set stream head as firstAddress, set the iterator from
         * firstAddress till the current tail of the log
         *
         * @param firstAddress
         */
        public void seek(long firstAddress) {
            log.trace("[{}]:: seek head {}", sessionName, firstAddress);
            opaqueStream.seek(firstAddress);
            streamUpTo();
        }
    }

}
