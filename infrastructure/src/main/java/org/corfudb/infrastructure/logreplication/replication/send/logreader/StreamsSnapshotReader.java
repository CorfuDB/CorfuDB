package org.corfudb.infrastructure.logreplication.replication.send.logreader;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
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

@Slf4j
@NotThreadSafe
/**
 *  Default snapshot logreader implementation
 *
 *  This implementation provides reads at the stream level (no coalesced state).
 *  It generates TxMessages which will be transmitted by the DataSender (provided by the application).
 */
public class StreamsSnapshotReader implements SnapshotReader {

    /**
     * percentage of metadata of the message
     */
    private final int DATA_MSG_OVERHEAD_FRACTION = 10;

    /**
     * The max size of snapshot data msg.
     */
    private final int maxDataMsgSize;

    /**
     * The max size of data for SMR entries in data message.
     */
    private final int maxDataSize;

    private long snapshotTimestamp;
    private Set<String> streams;
    private PriorityQueue<String> streamsToSend;
    private CorfuRuntime rt;
    private long preMsgTs;
    private long currentMsgTs;
    private OpaqueStreamIterator currentStreamInfo;
    private long sequence;
    private List<SMREntry> lastEntry = null;

    @Setter
    private long topologyConfigId;

    /**
     * Init runtime and streams to read
     */
    public StreamsSnapshotReader(CorfuRuntime runtime, LogReplicationConfig config) {
        this.rt = runtime;
        this.rt.parseConfigurationString(runtime.getLayoutServers().get(0)).connect();
        this.maxDataMsgSize = config.getMaxDataMsgSize();
        this.maxDataSize = maxDataMsgSize *(100 - DATA_MSG_OVERHEAD_FRACTION)/100;
        streams = config.getStreamsToReplicate();
        log.info("The maxDataNessageSize {} maxDataSize {} ", maxDataMsgSize, maxDataSize);
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

        ByteBuf buf = Unpooled.buffer(entryList.size);
        OpaqueEntry.serialize(buf, opaqueEntry);

        // Make a new buf backed by data only.
        ByteBuf newBuf = Unpooled.wrappedBuffer(buf.array(), 0, buf.writerIndex());

        LogReplicationEntry txMsg = new LogReplicationEntry(MessageType.SNAPSHOT_MESSAGE, topologyConfigId, snapshotRequestId, currentMsgTs,
                preMsgTs, snapshotTimestamp, sequence, newBuf.array());

        preMsgTs = currentMsgTs;
        sequence++;

        log.trace("txMsg {} deepsize size {} buf size {} buf.writerIndex {} entryList.size {} numEntries {} deepSize size {}",
                txMsg.getMetadata(), MetricsUtils.sizeOf.deepSizeOf(txMsg), MetricsUtils.sizeOf.deepSizeOf(buf),
                buf.writerIndex(), entryList.getSize(), entryList.getSmrEntries().size(), MetricsUtils.sizeOf.deepSizeOf(entryList.smrEntries));

        return txMsg;
    }

    /**
     * Given a list of SMREntries, calculate the total size.
     * @param smrEntries
     * @return
     */
    private int calculateSize(List<SMREntry> smrEntries) {
        int size = 0;
        for (SMREntry entry : smrEntries) {
            size += entry.getSerializedSize();
        }

        log.trace("current entry size {}", size);
        return size;
    }

    /**
     * Read log data from the current stream until the sum of all SMR entries's size reaches the maxDataSize.
     * @param stream
     * @return
     */
    private SMREntryList next(OpaqueStreamIterator stream) {
        List<SMREntry> list = new ArrayList<>();
        int currentSize = 0;

        // If there is a remaining element, put it on the list first
        if (lastEntry != null) {
            list.addAll(lastEntry);
            currentSize += calculateSize(lastEntry);
            lastEntry = null;
        }

        try {
            while (stream.iterator.hasNext() && currentSize < maxDataSize) {
                OpaqueEntry entry = (OpaqueEntry) stream.iterator.next();
                stream.maxVersion = Math.max(stream.maxVersion, entry.getVersion());
                List<SMREntry> currentSMREntries = entry.getEntries().get(stream.uuid);
                int currentEntrySize = calculateSize(currentSMREntries);

                // If the current element could not be included in the message
                // remember it has the iterator has moved it.
                if (currentSize + currentEntrySize > maxDataSize) {
                    lastEntry = currentSMREntries;
                    break;
                }

                // Add all entries and update the size
                list.addAll(entry.getEntries().get(stream.uuid));
                currentSize += currentEntrySize;
            }
        } catch (TrimmedException e) {
            log.error("Catch an TrimmedException exception ", e);
            throw e;
        }

        log.trace("CurrentSize {} lastEntrySize {}  maxDataMsgSize {}",
                currentSize, lastEntry == null ? 0 : calculateSize(lastEntry), maxDataMsgSize);
        return new SMREntryList(currentSize, list);
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
        log.info("Successfully pass stream {} for snapshotTimestamp {}", stream.name, snapshotTimestamp);
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
        LogReplicationEntry msg = null;

        // If the currentStreamInfo still has entry to process, it will reuse the currentStreamInfo
        // and process the remaining entries.
        if (currentStreamInfo == null) {
            while (!streamsToSend.isEmpty()) {
                // Setup a new stream
                currentStreamInfo = new OpaqueStreamIterator(streamsToSend.poll(), rt, snapshotTimestamp);

                // If the new stream has entries to be processed, go to the next step
                if (currentStreamInfo.iterator.hasNext()) {
                    break;
                } else {
                    // Skip process this stream as it has no entries to process, will poll the next one.
                    log.info("Snapshot logreader will skip reading stream {} as there are no entries to send",
                            currentStreamInfo.uuid);
                }
            }
        }

        if (currentStreamInfo.iterator.hasNext()) {
            msg = read(currentStreamInfo, syncRequestId);
            if (msg != null) {
                messages.add(msg);
            }
        }

        if (!currentStreamInfo.iterator.hasNext()) {
            log.debug("Snapshot log reader finished reading stream {}", currentStreamInfo.uuid);
            currentStreamInfo = null;

            if (streamsToSend.isEmpty()) {
                log.info("Snapshot log reader finished reading all streams {}", streams);
                endSnapshotSync = true;
            }
        }

        return new SnapshotReadMessage(messages, endSnapshotSync);
    }

    @Override
    public void reset(long snapshotTimestamp) {
        streamsToSend = new PriorityQueue<>(streams);
        preMsgTs = Address.NON_ADDRESS;
        currentMsgTs = Address.NON_ADDRESS;
        this.snapshotTimestamp = snapshotTimestamp; //rt.getAddressSpaceView().getLogTail();
        currentStreamInfo = null;
        sequence = 0;
        lastEntry = null;
    }

    /**
     * Used to bookkeeping the stream information for the current processing stream
     */
    public static class OpaqueStreamIterator {
        String name;
        UUID uuid;
        Iterator iterator;
        long maxVersion; //the max address of the log entries processed for this stream.

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

    static class SMREntryList {

        @Getter
        private int size;

        @Getter
        private List<SMREntry> smrEntries;

        public SMREntryList (int size, List<SMREntry> smrEntries) {
            this.size = size;
            this.smrEntries = smrEntries;
        }
    }
}
