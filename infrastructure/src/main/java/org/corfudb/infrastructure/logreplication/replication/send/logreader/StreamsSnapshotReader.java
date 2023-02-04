package org.corfudb.infrastructure.logreplication.replication.send.logreader;

import com.google.protobuf.TextFormat;
import io.micrometer.core.instrument.DistributionSummary;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MeterRegistryProvider;
import org.corfudb.common.util.Memory;
import org.corfudb.common.util.ObservableValue;
import org.corfudb.infrastructure.logreplication.infrastructure.LogReplicationContext;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.corfudb.infrastructure.logreplication.replication.send.IllegalSnapshotEntrySizeException;
import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.CorfuRuntime;

import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
/**
 *  Default snapshot reader implementation
 *
 *  The streams to replicate are read from registry table and will be refreshed at the start of a snapshot sync.
 *
 *  This implementation provides reads at the stream level (no coalesced state).
 *  It generates TxMessages which will be transmitted by the DataSender (provided by the application).
 */
public class StreamsSnapshotReader implements SnapshotReader {

    /**
     * The max size of data for SMR entries in data message.
     */
    private final int maxDataSizePerMsg;
    private final Optional<DistributionSummary> messageSizeDistributionSummary;
    private final CorfuRuntime rt;
    private long snapshotTimestamp;
    private Set<String> streams;
    private PriorityQueue<String> streamsToSend;
    private long preMsgTs;
    private long currentMsgTs;
    private OpaqueStreamIterator currentStreamInfo;
    private long sequence;
    private OpaqueEntry lastEntry = null;

    @Getter
    private ObservableValue<Integer> observeBiggerMsg = new ObservableValue(0);

    private final LogReplicationSession session;
    private final LogReplicationContext replicationContext;

    /**
     * Init runtime and streams to read
     */
    public StreamsSnapshotReader(CorfuRuntime runtime, LogReplicationSession session,
                                 LogReplicationContext replicationContext) {
        this.rt = runtime;
        this.session = session;
        this.replicationContext = replicationContext;
        this.rt.parseConfigurationString(runtime.getLayoutServers().get(0)).connect();
        this.maxDataSizePerMsg = replicationContext.getConfigManager().getConfig().getMaxDataSizePerMsg();
        this.streams = replicationContext.getConfigManager().getConfig().getStreamsToReplicate();
        this.messageSizeDistributionSummary = configureMessageSizeDistributionSummary();
    }

    /**
     * Given a streamID and list of smrEntries, generate an OpaqueEntry
     * @param streamID
     * @param entryList
     * @return
     */
    private OpaqueEntry generateOpaqueEntry(long version, UUID streamID, SMREntryList entryList) {
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
    private LogReplicationEntryMsg generateMessage(
            OpaqueStreamIterator stream,
            SMREntryList entryList,
            UUID snapshotRequestId) {
        currentMsgTs = stream.maxVersion;
        OpaqueEntry opaqueEntry = generateOpaqueEntry(currentMsgTs, stream.uuid, entryList);
        if (!stream.iterator.hasNext()) {
            //mark the end of the current stream.
            currentMsgTs = snapshotTimestamp;
        }
        LogReplication.LogReplicationEntryMetadataMsg metadata = LogReplication.LogReplicationEntryMetadataMsg.newBuilder()
                .setEntryType(LogReplicationEntryType.SNAPSHOT_MESSAGE)
                .setTopologyConfigID(topologyConfigId)
                .setSyncRequestId(getUuidMsg(snapshotRequestId))
                .setTimestamp(currentMsgTs)
                .setPreviousTimestamp(preMsgTs)
                .setSnapshotTimestamp(snapshotTimestamp)
                .setSnapshotSyncSeqNum(sequence)
                .build();

        LogReplicationEntryMsg txMsg = getLrEntryMsg(unsafeWrap(generatePayload(opaqueEntry)), metadata);

        preMsgTs = currentMsgTs;
        sequence++;

        log.trace("txMsg {} deepsize sizeInBytes {} entryList.sizeInByres {}  with numEntries {} deepSize sizeInBytes {}",
                TextFormat.printToString(txMsg.getMetadata()), Memory.sizeOf.deepSizeOf(txMsg), entryList.getSizeInBytes(),
                entryList.getSmrEntries().size(), Memory.sizeOf.deepSizeOf(entryList.smrEntries));
        return txMsg;
    }

    /**
     * Read log data from the current stream until the sum of all SMR entries's sizeInBytes reaches the maxDataSizePerMsg.
     * @param stream
     * @return
     */
    private SMREntryList next(OpaqueStreamIterator stream) {
        List<SMREntry> smrList = new ArrayList<>();
        int currentMsgSize = 0;

        try {
            while (currentMsgSize < maxDataSizePerMsg) {
                if (lastEntry != null) {
                    List<SMREntry> smrEntries = lastEntry.getEntries().get(stream.uuid);
                    if (smrEntries != null) {
                        int currentEntrySize = ReaderUtility.calculateSize(smrEntries);

                        if (currentEntrySize > MAX_DATA_MSG_SIZE_SUPPORTED) {
                            log.error("The current entry size {} is bigger than the maxDataSizePerMsg {} supported",
                                    currentEntrySize, MAX_DATA_MSG_SIZE_SUPPORTED);
                            throw new IllegalSnapshotEntrySizeException(" The snapshot entry is bigger than the system supported");
                        } else if (currentEntrySize > maxDataSizePerMsg) {
                            observeBiggerMsg.setValue(observeBiggerMsg.getValue()+1);
                            log.warn("The current entry size {} is bigger than the configured maxDataSizePerMsg {}",
                                    currentEntrySize, maxDataSizePerMsg);
                        }

                        // Skip append this entry in this message. Will process it first at the next round.
                        if (currentEntrySize + currentMsgSize > maxDataSizePerMsg && currentMsgSize != 0) {
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

        log.trace("CurrentMsgSize {} lastEntrySize {}  maxDataSizePerMsg {}",
                currentMsgSize, lastEntry == null ? 0 : ReaderUtility.calculateSize(lastEntry.getEntries().get(stream.uuid)), maxDataSizePerMsg);
        return new SMREntryList(currentMsgSize, smrList);
    }

    /**
     * Poll the current stream and get a batch of SMR entries and
     * generate one message
     * @param stream bookkeeping of the current stream information.
     * @return
     */
    private LogReplicationEntryMsg read(OpaqueStreamIterator stream, UUID syncRequestId) {
        SMREntryList entryList = next(stream);
        LogReplicationEntryMsg txMsg = generateMessage(stream, entryList, syncRequestId);
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
    public SnapshotReadMessage read(UUID syncRequestId) {
        List<LogReplicationEntryMsg> messages = new ArrayList<>();

        boolean endSnapshotSync = false;
        LogReplicationEntryMsg msg;

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

    public StreamsSnapshotReader(CorfuRuntime runtime, LogReplicationSession session,
                                 LogReplicationContext replicationContext) {
        super(runtime, session, replicationContext);
    }

    @Override
    public void reset(long ts) {
        // As the config should reflect the latest configuration read from registry table, it will be synced with the
        // latest registry table content instead of the given ts, while the streams to replicate will be read up to ts.
        replicationContext.refresh();
        streams = replicationContext.getConfig().getStreamsToReplicate();
        streamsToSend = new PriorityQueue<>(streams);
        preMsgTs = Address.NON_ADDRESS;
        currentMsgTs = Address.NON_ADDRESS;
        snapshotTimestamp = ts;
        currentStreamInfo = null;
        sequence = 0;
        lastEntry = null;
    }

}
