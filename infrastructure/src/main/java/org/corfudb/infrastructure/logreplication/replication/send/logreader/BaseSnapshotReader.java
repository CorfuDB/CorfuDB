package org.corfudb.infrastructure.logreplication.replication.send.logreader;

import com.google.protobuf.TextFormat;
import io.micrometer.core.instrument.DistributionSummary;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MeterRegistryProvider;
import org.corfudb.common.util.Memory;
import org.corfudb.common.util.ObservableValue;
import org.corfudb.infrastructure.logreplication.infrastructure.LogReplicationContext;
import org.corfudb.infrastructure.logreplication.replication.send.IllegalSnapshotEntrySizeException;
import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LogReplication;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.StreamOptions;
import org.corfudb.runtime.view.stream.OpaqueStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import static com.google.protobuf.UnsafeByteOperations.unsafeWrap;
import static org.corfudb.infrastructure.logreplication.config.LogReplicationConfig.DEFAULT_MAX_DATA_MSG_SIZE;
import static org.corfudb.protocols.CorfuProtocolCommon.getUuidMsg;
import static org.corfudb.protocols.service.CorfuProtocolLogReplication.generatePayload;
import static org.corfudb.protocols.service.CorfuProtocolLogReplication.getLrEntryMsg;

/**
 * Base class for snapshot readers. It provides common functionality for snapshot readers in different
 * replication models.
 */
@Slf4j
public abstract class BaseSnapshotReader extends SnapshotReader {
    /**
     * The max size of data for SMR entries in a replication message.
     */
    protected final long maxTransferSize;
    private final Optional<DistributionSummary> messageSizeDistributionSummary;
    protected final CorfuRuntime rt;
    protected long snapshotTimestamp;
    protected Set<String> streams;
    private PriorityQueue<String> streamsToSend;
    private long preMsgTs;
    private long currentMsgTs;
    protected OpaqueStreamIterator currentStreamInfo;
    private long sequence;
    protected OpaqueEntry lastEntry = null;

    @Getter
    protected ObservableValue<Integer> observeBiggerMsg = new ObservableValue(0);

    protected final LogReplication.LogReplicationSession session;
    protected final LogReplicationContext replicationContext;

    /**
     * Init runtime and streams to read
     */
    public BaseSnapshotReader(LogReplication.LogReplicationSession session,
                              LogReplicationContext replicationContext) {
        this.rt = replicationContext.getCorfuRuntime();
        this.session = session;
        this.replicationContext = replicationContext;
        this.maxTransferSize = replicationContext.getConfig(session).getMaxTransferSize();
        this.messageSizeDistributionSummary = configureMessageSizeDistributionSummary();
        refreshStreamsToReplicateSet();
        log.info("Total of {} streams to replicate at initialization. Streams to replicate={}, Session={}",
                this.streams.size(), replicationContext.getConfig(session).getStreamsToReplicate(),
                TextFormat.shortDebugString(session));
    }

    protected abstract void refreshStreamsToReplicateSet();

    /**
     * Given a streamID and list of smrEntries, generate an OpaqueEntry
     * @param streamID
     * @param entryList
     * @return
     */
    protected OpaqueEntry generateOpaqueEntry(long version, UUID streamID, SMREntryList entryList) {
        Map<UUID, List<SMREntry>> map = new HashMap<>();
        map.put(streamID, entryList.getSmrEntries());
        return new OpaqueEntry(version, map);
    }

    /**
     * Given a list of entries with the same stream, will generate an OpaqueEntry and
     * use the opaque entry to generate a TxMessage.
     * @param stream
     * @param entryList
     * @return
     */
    private LogReplication.LogReplicationEntryMsg generateMessage(OpaqueStreamIterator stream, SMREntryList entryList,
                                                                  UUID snapshotRequestId) {
        currentMsgTs = stream.maxVersion;
        OpaqueEntry opaqueEntry = generateOpaqueEntry(currentMsgTs, stream.uuid, entryList);
        if (!stream.iterator.hasNext()) {
            //mark the end of the current stream.
            currentMsgTs = snapshotTimestamp;
        }
        LogReplication.LogReplicationEntryMetadataMsg metadata = LogReplication.LogReplicationEntryMetadataMsg.newBuilder()
            .setEntryType(LogReplication.LogReplicationEntryType.SNAPSHOT_MESSAGE)
            .setTopologyConfigID(topologyConfigId)
            .setSyncRequestId(getUuidMsg(snapshotRequestId))
            .setTimestamp(currentMsgTs)
            .setPreviousTimestamp(preMsgTs)
            .setSnapshotTimestamp(snapshotTimestamp)
            .setSnapshotSyncSeqNum(sequence)
            .build();

        LogReplication.LogReplicationEntryMsg txMsg = getLrEntryMsg(unsafeWrap(generatePayload(opaqueEntry)), metadata);

        preMsgTs = currentMsgTs;
        sequence++;

        log.trace("txMsg {} deepsize sizeInBytes {} entryList.sizeInByres {}  with numEntries {} deepSize sizeInBytes {}",
            TextFormat.printToString(txMsg.getMetadata()), Memory.sizeOf.deepSizeOf(txMsg), entryList.getSizeInBytes(),
            entryList.getSmrEntries().size(), Memory.sizeOf.deepSizeOf(entryList.smrEntries));
        return txMsg;
    }

    /**
     * Read log data from the current stream until the sum of all SMR entries sizeInBytes reaches the maxDataSizePerMsg.
     * @param stream
     * @return
     */
    protected SMREntryList next(OpaqueStreamIterator stream) {
        List<SMREntry> smrList = new ArrayList<>();
        int currentMsgSize = 0;

        try {
            while (currentMsgSize < maxTransferSize) {
                if (lastEntry != null) {
                    List<SMREntry> smrEntries = lastEntry.getEntries().get(stream.uuid);
                    if (smrEntries != null) {
                        int currentEntrySize = ReaderUtility.calculateSize(smrEntries);
                        if (currentEntrySize > DEFAULT_MAX_DATA_MSG_SIZE) {
                            log.error("The current entry size {} is bigger than the max size {} supported",
                                currentEntrySize, DEFAULT_MAX_DATA_MSG_SIZE);
                            throw new IllegalSnapshotEntrySizeException(" The snapshot entry is bigger than the system supported");
                        } else if (currentEntrySize > maxTransferSize) {
                            // TODO: As of now, there is no plan to allow applications to change the max uncompressed
                            //  tx size. (CorfuRuntime.MAX_UNCOMPRESSED_WRITE_SIZE).  So the transfer size(85 MB)
                            //  will be higher than DEFAULT_MAX_DATA_MSG_SIZE(64 MB).
                            // However, if this behavior changes in future, it is possible that
                            // currentEntrySize <= DEFAULT_MAX_DATA_MSG_SIZE but currentEntrySize > maxTransferSize.
                            // In that case, split the transaction (right now currentEntrySize contains the size of all
                            // SMR entries in the transaction).
                            observeBiggerMsg.setValue(observeBiggerMsg.getValue()+1);
                            log.warn("The current entry size {} is bigger than the configured transfer size {}",
                                currentEntrySize, maxTransferSize);
                        }

                        // Skip append this entry in this message. Will process it first at the next round.
                        if (currentEntrySize + currentMsgSize > maxTransferSize && currentMsgSize != 0) {
                            break;
                        }

                        smrList.addAll(smrEntries);
                        currentMsgSize += currentEntrySize;
                        stream.maxVersion = Math.max(stream.maxVersion, lastEntry.getVersion());
                    }
                    lastEntry = null;
                }

                if (stream.iterator.hasNext()) {
                    lastEntry = (OpaqueEntry) stream.iterator.next();
                }

                if (lastEntry == null) {
                    break;
                }
            }
        } catch (TrimmedException e) {
            log.error("Caught a TrimmedException", e);
            throw e;
        }

        log.trace("CurrentMsgSize {} lastEntrySize {}  maxTransferSize {}",
            currentMsgSize, lastEntry == null ? 0 : ReaderUtility.calculateSize(lastEntry.getEntries().get(stream.uuid)),
                    maxTransferSize);
        return new SMREntryList(currentMsgSize, smrList);
    }

    /**
     * Poll the current stream and get a batch of SMR entries and
     * generate one message
     * @param stream bookkeeping of the current stream information.
     * @return
     */
    protected LogReplication.LogReplicationEntryMsg read(OpaqueStreamIterator stream, UUID syncRequestId) {
        SMREntryList entryList = next(stream);
        LogReplication.LogReplicationEntryMsg txMsg = generateMessage(stream, entryList, syncRequestId);
        log.info("Successfully generate a snapshot message for stream {} with snapshotTimestamp={}, numEntries={}, " +
                "entriesBytes={}, streamId={}", stream.name, snapshotTimestamp,
            entryList.getSmrEntries().size(), entryList.getSizeInBytes(), stream.uuid);
        messageSizeDistributionSummary
            .ifPresent(distribution -> distribution.record(entryList.getSizeInBytes()));
        return txMsg;
    }

    /**
     * If currentStreamInfo is null that is the case for the first call of read, it will init currentStreamInfo.
     * If the currentStreamInfo ends, poll the next stream.
     * Otherwise, continue to process the current stream and generate one message only.
     * @return
     */
    @Override
    public @NonNull SnapshotReadMessage read(UUID syncRequestId) {
        List<LogReplication.LogReplicationEntryMsg> messages = new ArrayList<>();

        boolean endSnapshotSync = false;
        LogReplication.LogReplicationEntryMsg msg;

        // If the currentStreamInfo still has entry to process, it will reuse the currentStreamInfo
        // and process the remaining entries.
        if (currentStreamInfo == null) {
            while (!streamsToSend.isEmpty()) {
                // Setup a new stream
                String streamToReplicate = streamsToSend.poll();
                currentStreamInfo = new OpaqueStreamIterator(streamToReplicate, rt, snapshotTimestamp);
                log.info("Start Snapshot Sync replication for stream name={}, id={}", streamToReplicate,
                    CorfuRuntime.getStreamID(streamToReplicate));

                // If the new stream has entries to be processed, go to the next step
                if (currentStreamInfo.iterator.hasNext()) {
                    break;
                } else {
                    // Skip process this stream as it has no entries to process, will poll the next one.
                    log.info("Snapshot reader will skip reading stream {} as there are no entries to send",
                        currentStreamInfo.uuid);
                }
            }
        }

        if (currentStreamHasNext()) {
            msg = read(currentStreamInfo, syncRequestId);
            if (msg != null) {
                messages.add(msg);
            }
        }

        if (!currentStreamHasNext()) {
            log.debug("Snapshot log reader finished reading stream id={}, name={}", currentStreamInfo.uuid, currentStreamInfo.name);
            currentStreamInfo = null;

            if (streamsToSend.isEmpty()) {
                log.info("Snapshot log reader finished reading ALL streams, total={}", streams.size());
                endSnapshotSync = true;
            }
        }

        return new SnapshotReadMessage(messages, endSnapshotSync);
    }

    protected boolean currentStreamHasNext() {
        log.trace("Iterator hasNext = {}", currentStreamInfo.iterator.hasNext());
        return currentStreamInfo.iterator.hasNext() || lastEntry != null;
    }

    @Override
    public void reset(long ts) {
        // As the config should reflect the latest configuration read from registry table, it will be synced with the
        // latest registry table content instead of the given ts, while the streams to replicate will be read up to ts.
        replicationContext.refreshConfig(session, true);
        streams = replicationContext.getConfig(session).getStreamsToReplicate();
        streamsToSend = new PriorityQueue<>(streams);
        preMsgTs = Address.NON_ADDRESS;
        currentMsgTs = Address.NON_ADDRESS;
        snapshotTimestamp = ts;
        currentStreamInfo = null;
        sequence = 0;
        lastEntry = null;
    }

    /**
     * Used to bookkeeping the stream information for the current processing stream
     */
    public static class OpaqueStreamIterator {
        private String name;
        private UUID uuid;
        Iterator iterator;
        protected long maxVersion; // the max address of the log entries processed for this stream.

        OpaqueStreamIterator(String name, CorfuRuntime rt, long snapshot) {
            this.name = name;
            uuid = CorfuRuntime.getStreamID(name);
            StreamOptions options = StreamOptions.builder()
                .ignoreTrimmed(false)
                .cacheEntries(false)
                .build();
            Stream stream = (new OpaqueStream(rt.getStreamsView().get(uuid, options))).streamUpTo(snapshot);
            iterator = stream.iterator();
            maxVersion = 0;
        }

        OpaqueStreamIterator(OpaqueStream opaqueStream, String name, long snapshot) {
            this.name = name;
            uuid = CorfuRuntime.getStreamID(name);
            iterator = opaqueStream.streamUpTo(snapshot).iterator();
        }
    }

    private Optional<DistributionSummary> configureMessageSizeDistributionSummary() {
        return MeterRegistryProvider.getInstance().map(registry ->
            DistributionSummary.builder("logreplication.message.size.bytes")
                .baseUnit("bytes")
                .tags("replication.type", "snapshot")
                .register(registry));
    }

    /**
     * Record a list of SMR entries
     */
    public static class SMREntryList {

        // The total sizeInBytes of smrEntries in bytes.
        @Getter
        private final int sizeInBytes;

        @Getter
        private final List<SMREntry> smrEntries;

        public SMREntryList(int size, List<SMREntry> smrEntries) {
            this.sizeInBytes = size;
            this.smrEntries = smrEntries;
        }
    }
}
