package org.corfudb.infrastructure.logreplication.replication.receive;

import com.google.protobuf.TextFormat;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMetadataMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryType;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.stream.IStreamView;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager.LogReplicationMetadataType;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.corfudb.protocols.service.CorfuProtocolLogReplication.extractOpaqueEntries;


/**
 * Process TxMessage that contains transaction logs for registered streams.
 */
@NotThreadSafe
@Slf4j
public class LogEntryWriter {
    private final LogReplicationMetadataManager logReplicationMetadataManager;
    private final HashMap<UUID, String> streamMap; //the set of streams that log entry writer will work on.
    HashMap<UUID, IStreamView> streamViewMap; //map the stream uuid to the stream view.
    CorfuRuntime rt;
    private long srcGlobalSnapshot; //the source snapshot that the transaction logs are based
    private long lastMsgTs; //the timestamp of the last message processed.

    public LogEntryWriter(CorfuRuntime rt, LogReplicationConfig config,
                          LogReplicationMetadataManager logReplicationMetadataManager) {
        this.rt = rt;
        this.logReplicationMetadataManager = logReplicationMetadataManager;

        Set<String> streams = config.getStreamsToReplicate();
        streamMap = new HashMap<>();

        for (String s : streams) {
            streamMap.put(CorfuRuntime.getStreamID(s), s);
        }

        srcGlobalSnapshot = Address.NON_ADDRESS;
        lastMsgTs = Address.NON_ADDRESS;

        streamViewMap = new HashMap<>();

        for (UUID uuid : streamMap.keySet()) {
            streamViewMap.put(uuid, rt.getStreamsView().getUnsafe(uuid));
        }
    }


    /**
     * Verify the metadata is the correct data type.
     * @param metadata
     * @throws ReplicationWriterException
     */
    private void verifyMetadata(LogReplicationEntryMetadataMsg metadata) throws ReplicationWriterException {
        if (metadata.getEntryType() != LogReplicationEntryType.LOG_ENTRY_MESSAGE) {
            log.error("Wrong message metadata {}, expecting  type {} snapshot {}",
                    TextFormat.shortDebugString(metadata),
                    LogReplicationEntryType.LOG_ENTRY_MESSAGE, srcGlobalSnapshot);
            throw new ReplicationWriterException("wrong type of message");
        }
    }

    /**
     * Convert message data to an MultiObjectSMREntry and write to log.
     * @param txMessage
     */
    private void processMsg(LogReplicationEntryMsg txMessage) {
        List<OpaqueEntry> opaqueEntryList = extractOpaqueEntries(txMessage);

        try (TxnContext txnContext = logReplicationMetadataManager.getTxnContext()) {

            Map<LogReplicationMetadataType, Long> metadataMap = logReplicationMetadataManager.queryMetadata(txnContext, LogReplicationMetadataType.TOPOLOGY_CONFIG_ID, LogReplicationMetadataType.LAST_SNAPSHOT_STARTED,
                    LogReplicationMetadataType.LAST_SNAPSHOT_APPLIED, LogReplicationMetadataType.LAST_LOG_ENTRY_PROCESSED);
            long persistedTopologyConfigId = metadataMap.get(LogReplicationMetadataType.TOPOLOGY_CONFIG_ID);
            long persistedSnapshotStart = metadataMap.get(LogReplicationMetadataManager.LogReplicationMetadataType.LAST_SNAPSHOT_STARTED);
            long persistedSnapshotDone = metadataMap.get(LogReplicationMetadataManager.LogReplicationMetadataType.LAST_SNAPSHOT_APPLIED);
            long persistedLogTs = metadataMap.get(LogReplicationMetadataManager.LogReplicationMetadataType.LAST_LOG_ENTRY_PROCESSED);

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
                return;
            }

            // Skip Opaque entries with timestamp that are not larger than persistedTs
            OpaqueEntry[] newOpaqueEntryList = opaqueEntryList.stream().filter(x -> x.getVersion() > persistedLogTs).toArray(OpaqueEntry[]::new);

            // Check that all opaque entries contain the correct streams
            for (OpaqueEntry opaqueEntry : newOpaqueEntryList) {
                if (!streamMap.keySet().containsAll(opaqueEntry.getEntries().keySet())) {
                    log.error("txMessage contains noisy streams {}, expecting {}", opaqueEntry.getEntries().keySet(), streamMap);
                    throw new ReplicationWriterException("Wrong streams set");
                }
            }

            logReplicationMetadataManager.appendUpdate(txnContext, LogReplicationMetadataManager.LogReplicationMetadataType.TOPOLOGY_CONFIG_ID, topologyConfigId);
            logReplicationMetadataManager.appendUpdate(txnContext, LogReplicationMetadataManager.LogReplicationMetadataType.LAST_LOG_ENTRY_PROCESSED, entryTs);

            for (OpaqueEntry opaqueEntry : newOpaqueEntryList) {
                for (UUID uuid : opaqueEntry.getEntries().keySet()) {
                    for (SMREntry smrEntry : opaqueEntry.getEntries().get(uuid)) {
                        txnContext.logUpdate(uuid, smrEntry);
                    }
                }
            }

            txnContext.commit();

            lastMsgTs = Math.max(entryTs, lastMsgTs);
        }
    }

    /**
     * Apply message generate by log entry reader and will apply at the destination corfu cluster.
     * @param msg
     * @return last processed message timestamp
     * @throws ReplicationWriterException
     */
    public long apply(LogReplicationEntryMsg msg) throws ReplicationWriterException {

        log.debug("Apply log entry {}", msg.getMetadata().getTimestamp());

        verifyMetadata(msg.getMetadata());

        // Ignore the out of date messages
        if (msg.getMetadata().getSnapshotTimestamp() < srcGlobalSnapshot) {
            log.warn("Ignore Log Entry. Received message with snapshot {} is smaller than current snapshot {}",
                    msg.getMetadata().getSnapshotTimestamp(), srcGlobalSnapshot);
            return Address.NON_ADDRESS;
        }

        // A new Delta sync is triggered, setup the new srcGlobalSnapshot and msgQ
        if (msg.getMetadata().getSnapshotTimestamp() > srcGlobalSnapshot) {
            srcGlobalSnapshot = msg.getMetadata().getSnapshotTimestamp();
            lastMsgTs = srcGlobalSnapshot;
        }

        // we will skip the entries has been processed.
        if (msg.getMetadata().getTimestamp() <= lastMsgTs) {
            log.warn("Ignore Log Entry. Received message with snapshot {} is smaller than lastMsgTs {}.",
                    msg.getMetadata().getSnapshotTimestamp(), lastMsgTs);
            return Address.NON_ADDRESS;
        }

        //If the entry is the expecting entry, process it and process
        //the messages in the queue.
        if (msg.getMetadata().getPreviousTimestamp() == lastMsgTs) {
            processMsg(msg);
            return lastMsgTs;
        }

        log.warn("Log entry {} was not processed, prevTs={}, lastMsgTs={}, srcGlobalSnapshot={}", msg.getMetadata().getTimestamp(),
                msg.getMetadata().getPreviousTimestamp(), lastMsgTs, srcGlobalSnapshot);
        return Address.NON_ADDRESS;
    }

    /**
     * Set the base snapshot that last full sync based on and ackTimestamp
     * that is the last log entry it has played.
     * This is called while the writer enter the log entry sync state.
     *
     * @param snapshot
     * @param ackTimestamp
     */
    public void reset(long snapshot, long ackTimestamp) {
        srcGlobalSnapshot = snapshot;
        lastMsgTs = ackTimestamp;
    }
}
