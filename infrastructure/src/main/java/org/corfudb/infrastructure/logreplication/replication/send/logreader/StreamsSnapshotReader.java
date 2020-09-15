package org.corfudb.infrastructure.logreplication.replication.send.logreader;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.util.ObservableValue;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.infrastructure.logreplication.replication.send.IllegalSnapshotEntrySizeException;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;
import org.corfudb.protocols.wireprotocol.logreplication.MessageType;
import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.StreamOptions;
import org.corfudb.runtime.view.stream.OpaqueStream;
import org.corfudb.util.MetricsUtils;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import static org.corfudb.infrastructure.logreplication.LogReplicationConfig.MAX_DATA_MSG_SIZE_SUPPORTED;

@Slf4j
@NotThreadSafe
/**
 *  Default snapshot reader implementation
 *
 *  This implementation provides reads at the stream level (no coalesced state).
 *  It generates TxMessages which will be transmitted by the DataSender (provided by the application).
 */
public class StreamsSnapshotReader implements SnapshotReader {

    /**
     * The max size of data for SMR entries in data message.
     */
    private final int maxDataSizePerMsg;

    private long snapshotTimestamp;
    private Set<String> streams;
    private PriorityQueue<String> streamsToSend;
    private CorfuRuntime rt;
    private long preMsgTs;
    private long currentMsgTs;
    private OpaqueStreamIterator currentStreamInfo;
    private long sequence;
    private OpaqueEntry lastEntry = null;

    @Getter
    private ObservableValue<Integer> observeBiggerMsg = new ObservableValue(0);

    @Setter
    private long topologyConfigId;

    /**
     * Init runtime and streams to read
     */
    public StreamsSnapshotReader(CorfuRuntime runtime, LogReplicationConfig config) {
        this.rt = runtime;
        this.rt.parseConfigurationString(runtime.getLayoutServers().get(0)).connect();
        this.maxDataSizePerMsg = config.getMaxDataSizePerMsg();
        this.streams = config.getStreamsToReplicate();
    }

    /**
     * Given a streamID and list of smrEntries, generate an OpaqueEntry
     * @param streamID
     * @param entryList
     * @return
     */
    private OpaqueEntry generateOpaqueEntry(long version, UUID streamID,  SMREntryList entryList) {
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
    private LogReplicationEntry generateMessage(OpaqueStreamIterator stream, SMREntryList entryList, UUID snapshotRequestId) {
        currentMsgTs = stream.maxVersion;
        OpaqueEntry opaqueEntry = generateOpaqueEntry(currentMsgTs, stream.uuid, entryList);
        if (!stream.iterator.hasNext()) {
            //mark the end of the current stream.
            currentMsgTs = snapshotTimestamp;
        }

        LogReplicationEntry txMsg = new LogReplicationEntry(MessageType.SNAPSHOT_MESSAGE, topologyConfigId, snapshotRequestId, currentMsgTs,
                preMsgTs, snapshotTimestamp, sequence, opaqueEntry);

        preMsgTs = currentMsgTs;
        sequence++;

        log.trace("txMsg {} deepsize sizeInBytes {} entryList.sizeInByres {}  with numEntries {} deepSize sizeInBytes {}",
                txMsg.getMetadata(), MetricsUtils.sizeOf.deepSizeOf(txMsg), entryList.getSizeInBytes(), entryList.getSmrEntries().size(), MetricsUtils.sizeOf.deepSizeOf(entryList.smrEntries));

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
    private LogReplicationEntry read(OpaqueStreamIterator stream, UUID syncRequestId) {
        SMREntryList entryList = next(stream);
        LogReplicationEntry txMsg = generateMessage(stream, entryList, syncRequestId);
        log.info("Successfully generate a snapshot message for stream {} with snapshotTimestamp={}, numEntries={}, " +
                        "entriesBytes={}, streamId={}", stream.name, snapshotTimestamp,
                entryList.getSmrEntries().size(), entryList.getSizeInBytes(), stream.uuid);
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
        List<LogReplicationEntry> messages = new ArrayList<>();

        boolean endSnapshotSync = false;
        LogReplicationEntry msg;

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

    private boolean currentStreamHasNext() {
        return currentStreamInfo.iterator.hasNext() || lastEntry != null;
    }

    @Override
    public void reset(long ts) {
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
        private Iterator iterator;
        private long maxVersion; // the max address of the log entries processed for this stream.

        OpaqueStreamIterator(String name, CorfuRuntime rt, long snapshot) {
            this.name = name;
            uuid = CorfuRuntime.getStreamID(name);
            StreamOptions options = StreamOptions.builder()
                    .ignoreTrimmed(false)
                    .cacheEntries(false)
                    .build();
            Stream stream = (new OpaqueStream(rt, rt.getStreamsView().get(uuid, options))).streamUpTo(snapshot);
            iterator = stream.iterator();
            maxVersion = 0;
         }
    }

    @Override
    public void setTopologyConfigId(long topologyConfigId) {
        this.topologyConfigId = topologyConfigId;
    }

    /**
     * Record a list of SMR entries
     */
    static private class SMREntryList {

        // The total sizeInBytes of smrEntries in bytes.
        @Getter
        private int sizeInBytes;

        @Getter
        private List<SMREntry> smrEntries;

        public SMREntryList (int size, List<SMREntry> smrEntries) {
            this.sizeInBytes = size;
            this.smrEntries = smrEntries;
        }
    }
}
