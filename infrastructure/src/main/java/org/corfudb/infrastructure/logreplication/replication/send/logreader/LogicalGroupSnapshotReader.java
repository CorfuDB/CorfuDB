package org.corfudb.infrastructure.logreplication.replication.send.logreader;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.config.LogReplicationLogicalGroupConfig;
import org.corfudb.infrastructure.logreplication.infrastructure.LogReplicationContext;
import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LogReplication;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.corfudb.runtime.view.Address;
import org.corfudb.util.serializer.Serializers;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.google.protobuf.UnsafeByteOperations.unsafeWrap;
import static org.corfudb.protocols.CorfuProtocolCommon.getUuidMsg;
import static org.corfudb.protocols.service.CorfuProtocolLogReplication.generatePayload;
import static org.corfudb.protocols.service.CorfuProtocolLogReplication.getLrEntryMsg;

/**
 * Snapshot reader implementation for Logical Grouping Replication Model.
 */
@Slf4j
public class LogicalGroupSnapshotReader extends BaseSnapshotReader {

    private Set<String> streamsToClear;

    private static final String CLEAR_SMR_METHOD = "clear";

    private static final SMREntry CLEAR_ENTRY = new SMREntry(CLEAR_SMR_METHOD, new Array[0], Serializers.PRIMITIVE);

    public LogicalGroupSnapshotReader(CorfuRuntime runtime, LogReplicationSession session,
                                      LogReplicationContext replicationContext) {
        super(runtime, session, replicationContext);
    }

    /**
     * If currentStreamInfo is null that is the case for the first call of read, it will init currentStreamInfo.
     * If the currentStreamInfo ends, poll the next stream.
     * Otherwise, continue to process the current stream and generate one message only.
     * @return
     */
    @Override
    public SnapshotReadMessage read(UUID syncRequestId) {
        List<LogReplication.LogReplicationEntryMsg> messages = new ArrayList<>();

        boolean endSnapshotSync = false;
        LogReplication.LogReplicationEntryMsg msg;

        // If the currentStreamInfo still has entry to process, it will reuse the currentStreamInfo
        // and process the remaining entries.
        if (currentStreamInfo == null) {
            while (!streamsToSend.isEmpty()) {
                // Set up a new stream
                String streamName = streamsToSend.poll();
                if (!streamsToClear.contains(streamName)) {
                    currentStreamInfo = new OpaqueStreamIterator(streamName, rt, snapshotTimestamp);
                    log.info("Start Snapshot Sync replication for stream name={}, id={}", streamName,
                            CorfuRuntime.getStreamID(streamName));

                    // If the new stream has entries to be processed, go to the next step
                    if (currentStreamInfo.iterator.hasNext()) {
                        break;
                    } else {
                        // Skip process this stream as it has no entries to process, will poll the next one.
                        log.info("Snapshot reader will skip reading stream {} as there are no entries to send",
                                currentStreamInfo.uuid);
                    }
                } else {
                    log.info("Sending CLEAR_ENTRY for stream name={}, id={}", streamName,
                            CorfuRuntime.getStreamID(streamName));
                    currentStreamInfo = null;
                    messages.add(generateClearMessage(streamName, syncRequestId));
                    break;
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
            if (currentStreamInfo != null) {
                log.debug("Snapshot log reader finished reading stream id={}, name={}", currentStreamInfo.uuid, currentStreamInfo.name);
            }
            currentStreamInfo = null;

            if (streamsToSend.isEmpty()) {
                log.info("Snapshot log reader finished reading ALL streams, total={}", streams.size());
                endSnapshotSync = true;
            }
        }

        return new SnapshotReadMessage(messages, endSnapshotSync);
    }

    LogReplication.LogReplicationEntryMsg generateClearMessage(String streamName, UUID snapshotRequestId) {
        //mark the end of the current stream as there is only 1 CLEAR_ENTRY to send.
        currentMsgTs = snapshotTimestamp;
        LogReplication.LogReplicationEntryMetadataMsg metadata = LogReplication.LogReplicationEntryMetadataMsg.newBuilder()
                .setEntryType(LogReplication.LogReplicationEntryType.SNAPSHOT_MESSAGE)
                .setTopologyConfigID(topologyConfigId)
                .setSyncRequestId(getUuidMsg(snapshotRequestId))
                .setTimestamp(currentMsgTs)
                .setPreviousTimestamp(preMsgTs)
                .setSnapshotTimestamp(snapshotTimestamp)
                .setSnapshotSyncSeqNum(sequence)
                .build();

        Map<UUID, List<SMREntry>> map = new HashMap<>();
        map.put(CorfuRuntime.getStreamID(streamName), Collections.singletonList(CLEAR_ENTRY));
        OpaqueEntry opaqueEntry = new OpaqueEntry(0, map);

        LogReplication.LogReplicationEntryMsg txMsg = getLrEntryMsg(unsafeWrap(generatePayload(opaqueEntry)), metadata);

        preMsgTs = currentMsgTs;
        sequence++;

        return txMsg;
    }

    @Override
    protected void refreshStreamsToReplicateSet() {
        streams = replicationContext.getConfig(session).getStreamsToReplicate();
    }

    @Override
    public void reset(long ts) {
        // As the config should reflect the latest configuration read from registry table, it will be synced with the
        // latest registry table content instead of the given ts, while the streams to replicate will be read up to ts.
        replicationContext.refresh();
        preMsgTs = Address.NON_ADDRESS;
        currentMsgTs = Address.NON_ADDRESS;
        snapshotTimestamp = ts;
        currentStreamInfo = null;
        sequence = 0;
        lastEntry = null;
        streams = replicationContext.getConfig(session).getStreamsToReplicate();
        streamsToSend = new PriorityQueue<>(streams);
        streamsToClear = ((LogReplicationLogicalGroupConfig) replicationContext.getConfig(session))
                .getRemovedGroupToStreams().values().stream()
                .flatMap(Set::stream)
                .collect(Collectors.toSet());
        streamsToSend.addAll(streamsToClear);
    }
}
