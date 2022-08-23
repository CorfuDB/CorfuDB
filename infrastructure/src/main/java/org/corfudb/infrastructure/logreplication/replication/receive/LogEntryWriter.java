package org.corfudb.infrastructure.logreplication.replication.receive;

import com.google.protobuf.TextFormat;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.service.CorfuProtocolLogReplication;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMetadataMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryType;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.view.Address;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager.LogReplicationMetadataType;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.corfudb.infrastructure.logreplication.LogReplicationConfig.REGISTRY_TABLE_ID;


/**
 * Process TxMessage that contains transaction logs for registered streams.
 */
@NotThreadSafe
@Slf4j
public class LogEntryWriter extends SinkWriter {
    private final LogReplicationMetadataManager logReplicationMetadataManager;
    private final HashMap<UUID, String> streamMap; //the set of streams that log entry writer will work on.
    private final Map<UUID, List<UUID>> dataStreamToTagsMap;
    private long srcGlobalSnapshot; //the source snapshot that the transaction logs are based
    private long lastMsgTs; //the timestamp of the last message processed.

    public LogEntryWriter(CorfuRuntime rt, LogReplicationConfig config, LogReplicationMetadataManager logReplicationMetadataManager) {
        super(rt);
        this.logReplicationMetadataManager = logReplicationMetadataManager;
        this.srcGlobalSnapshot = Address.NON_ADDRESS;
        this.lastMsgTs = Address.NON_ADDRESS;
        this.streamMap = new HashMap<>();
        this.dataStreamToTagsMap = config.getDataStreamToTagsMap();

        config.getStreamsToReplicate().stream().forEach(stream -> streamMap.put(CorfuRuntime.getStreamID(stream), stream));
    }

    /**
     * Verify the metadata is the correct data type.
     * @param metadata
     * @throws ReplicationWriterException
     */
    private void verifyMetadata(LogReplicationEntryMetadataMsg metadata) throws ReplicationWriterException {
        if (metadata.getEntryType() != LogReplicationEntryType.LOG_ENTRY_MESSAGE) {
            log.error("Wrong message metadata {}, expecting type {} snapshot {}",
                    TextFormat.shortDebugString(metadata),
                    LogReplicationEntryType.LOG_ENTRY_MESSAGE, srcGlobalSnapshot);
            throw new ReplicationWriterException("wrong type of message");
        }
    }

    /**
     * Convert message data to an MultiObjectSMREntry and write to log.
     * @param txMessage
     * @return true when the msg is appended to the log
     */
    private boolean processMsg(LogReplicationEntryMsg txMessage) {
        List<OpaqueEntry> opaqueEntryList = CorfuProtocolLogReplication.extractOpaqueEntries(txMessage);

        try (TxnContext txnContext = logReplicationMetadataManager.getTxnContext()) {

            Map<LogReplicationMetadataType, Long> metadataMap = logReplicationMetadataManager.queryMetadata(
                    txnContext, LogReplicationMetadataType.TOPOLOGY_CONFIG_ID, LogReplicationMetadataType.LAST_SNAPSHOT_STARTED,
                    LogReplicationMetadataType.LAST_SNAPSHOT_APPLIED, LogReplicationMetadataType.LAST_LOG_ENTRY_PROCESSED);
            long persistedTopologyConfigId = metadataMap.get(LogReplicationMetadataType.TOPOLOGY_CONFIG_ID);
            long persistedSnapshotStart = metadataMap.get(LogReplicationMetadataType.LAST_SNAPSHOT_STARTED);
            long persistedSnapshotDone = metadataMap.get(LogReplicationMetadataType.LAST_SNAPSHOT_APPLIED);
            long persistedLogTs = metadataMap.get(LogReplicationMetadataType.LAST_LOG_ENTRY_PROCESSED);

            long topologyConfigId = txMessage.getMetadata().getTopologyConfigID();
            long baseSnapshotTs = txMessage.getMetadata().getSnapshotTimestamp();
            long entryTs = txMessage.getMetadata().getTimestamp();
            long prevTs = txMessage.getMetadata().getPreviousTimestamp();

            lastMsgTs = Math.max(persistedLogTs, lastMsgTs);

            if (topologyConfigId != persistedTopologyConfigId || baseSnapshotTs != persistedSnapshotStart ||
                    baseSnapshotTs != persistedSnapshotDone || prevTs != persistedLogTs) {
                log.warn("Message metadata mismatch. Skip applying message {}, persistedTopologyConfigId={}, persistedSnapshotStart={}, " +
                                "persistedSnapshotDone={}, persistedLogTs={}", txMessage.getMetadata(), persistedTopologyConfigId,
                        persistedSnapshotStart, persistedSnapshotDone, persistedLogTs);
                return false;
            }

            // Skip Opaque entries with timestamp that are not larger than persistedTs
            OpaqueEntry[] newOpaqueEntryList = opaqueEntryList.stream().filter(x -> x.getVersion() > persistedLogTs).toArray(OpaqueEntry[]::new);

            logReplicationMetadataManager.appendUpdate(txnContext, LogReplicationMetadataType.TOPOLOGY_CONFIG_ID, topologyConfigId);
            logReplicationMetadataManager.appendUpdate(txnContext, LogReplicationMetadataType.LAST_LOG_ENTRY_PROCESSED, entryTs);

            for (OpaqueEntry opaqueEntry : newOpaqueEntryList) {
                for (UUID streamId : opaqueEntry.getEntries().keySet()) {
                    if (!streamMap.containsKey(streamId)) {
                        log.warn("Skip applying log entries for stream {} as it is noisy. LR could be undergoing a rolling upgrade", streamId);
                        continue;
                    }

                    List<SMREntry> smrEntries = opaqueEntry.getEntries().get(streamId);
                    if (streamId.equals(REGISTRY_TABLE_ID)) {
                        // Only retain tables that are not present in the registry table
                        smrEntries = fetchNewEntries(new ArrayList<>(smrEntries));
                    }

                    for (SMREntry smrEntry : smrEntries) {
                        // If stream tags exist for the current stream, it means its intended for streaming on the Sink (receiver)
                        txnContext.logUpdate(streamId, smrEntry, dataStreamToTagsMap.get(streamId));
                    }
                }
            }
            txnContext.commit();

            lastMsgTs = Math.max(entryTs, lastMsgTs);
            return true;
        }
    }

    /**
     * Apply message at the destination Corfu Cluster
     *
     * @param msg
     * @return true when the msg is appended to the log
     * @throws ReplicationWriterException
     */
    public boolean apply(LogReplicationEntryMsg msg) throws ReplicationWriterException {

        log.debug("Apply log entry {}", msg.getMetadata().getTimestamp());

        verifyMetadata(msg.getMetadata());

        // Ignore the out of date messages
        if (msg.getMetadata().getSnapshotTimestamp() < srcGlobalSnapshot) {
            log.warn("Ignore Log Entry. Received message with snapshot {} is smaller than current snapshot {}",
                    msg.getMetadata().getSnapshotTimestamp(), srcGlobalSnapshot);
            return false;
        }

        // A new Delta sync is triggered, setup the new srcGlobalSnapshot and msgQ
        if (msg.getMetadata().getSnapshotTimestamp() > srcGlobalSnapshot) {
            log.warn("A new log entry sync is triggered with higher snapshot, previous snapshot " +
                            "is {} and setup the new srcGlobalSnapshot & lastMsgTs as {}",
                    srcGlobalSnapshot, msg.getMetadata().getSnapshotTimestamp());
            srcGlobalSnapshot = msg.getMetadata().getSnapshotTimestamp();
            lastMsgTs = srcGlobalSnapshot;
        }

        //If the entry is the expecting entry, process it and process
        //the messages in the queue.
        if (msg.getMetadata().getPreviousTimestamp() == lastMsgTs) {
            return processMsg(msg);
        }

        log.warn("Log entry {} was not processed, prevTs={}, lastMsgTs={}, srcGlobalSnapshot={}", msg.getMetadata().getTimestamp(),
                msg.getMetadata().getPreviousTimestamp(), lastMsgTs, srcGlobalSnapshot);
        return false;
    }

    /**
     * Set the base snapshot on which the last full sync was based on and ackTimestamp
     * that is the last log entry it has played.
     * This is called while the writer enter the log entry sync state.
     *
     * @param snapshot the base snapshot on which the last full sync was based on.
     * @param ackTimestamp
     */
    public void reset(long snapshot, long ackTimestamp) {
        srcGlobalSnapshot = snapshot;
        lastMsgTs = ackTimestamp;
    }
}
