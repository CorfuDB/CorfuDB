package org.corfudb.infrastructure.logreplication.replication.receive;

import lombok.extern.slf4j.Slf4j;

import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntryMetadata;
import org.corfudb.protocols.wireprotocol.logreplication.MessageType;

import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.collections.TxBuilder;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.stream.IStreamView;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.UUID;


/**
 * Process TxMessage that contains transaction logs for registered streams.
 */
@NotThreadSafe
@Slf4j
public class LogEntryWriter {
    private HashMap<UUID, String> streamMap; //the set of streams that log entry writer will work on.
    HashMap<UUID, IStreamView> streamViewMap; //map the stream uuid to the stream view.
    CorfuRuntime rt;
    private long srcGlobalSnapshot; //the source snapshot that the transaction logs are based
    private long lastMsgTs; //the timestamp of the last message processed.
    private LogReplicationMetadataManager logReplicationMetadataManager;

    public LogEntryWriter(CorfuRuntime rt, LogReplicationConfig config, LogReplicationMetadataManager logReplicationMetadataManager) {
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
    private void verifyMetadata(LogReplicationEntryMetadata metadata) throws ReplicationWriterException {
        if (metadata.getMessageMetadataType() != MessageType.LOG_ENTRY_MESSAGE) {
            log.error("Wrong message metadata {}, expecting  type {} snapshot {}", metadata,
                    MessageType.LOG_ENTRY_MESSAGE, srcGlobalSnapshot);
            throw new ReplicationWriterException("wrong type of message");
        }
    }

    /**
     * Convert message data to an MultiObjectSMREntry and write to log.
     * @param txMessage
     */
    private void processMsg(LogReplicationEntry txMessage) {
        List<OpaqueEntry> opaqueEntryList = txMessage.getOpaqueEntryList();

        CorfuStoreMetadata.Timestamp timestamp = logReplicationMetadataManager.getTimestamp();
        long persistedTopologyConfigId = logReplicationMetadataManager.query(timestamp, LogReplicationMetadataManager.LogReplicationMetadataType.TOPOLOGY_CONFIG_ID);
        long persistedSnapshotStart = logReplicationMetadataManager.query(timestamp, LogReplicationMetadataManager.LogReplicationMetadataType.LAST_SNAPSHOT_STARTED);
        long persistedSnapshotDone = logReplicationMetadataManager.query(timestamp, LogReplicationMetadataManager.LogReplicationMetadataType.LAST_SNAPSHOT_APPLIED);
        long persistedLogTs = logReplicationMetadataManager.query(timestamp, LogReplicationMetadataManager.LogReplicationMetadataType.LAST_LOG_ENTRY_PROCESSED);

        long topologyConfigId = txMessage.getMetadata().getTopologyConfigId();
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
        OpaqueEntry[] newOpaqueEntryList = opaqueEntryList.stream().filter(x->x.getVersion() > persistedLogTs).toArray(OpaqueEntry[]::new);

        // Check that all opaque entries contain the correct streams
        for (OpaqueEntry opaqueEntry : newOpaqueEntryList) {
            if (!streamMap.keySet().containsAll(opaqueEntry.getEntries().keySet())) {
                log.error("txMessage contains noisy streams {}, expecting {}", opaqueEntry.getEntries().keySet(), streamMap);
                throw new ReplicationWriterException("Wrong streams set");
            }
        }

        TxBuilder txBuilder = logReplicationMetadataManager.getTxBuilder();

        logReplicationMetadataManager.appendUpdate(txBuilder, LogReplicationMetadataManager.LogReplicationMetadataType.TOPOLOGY_CONFIG_ID, topologyConfigId);
        logReplicationMetadataManager.appendUpdate(txBuilder, LogReplicationMetadataManager.LogReplicationMetadataType.LAST_LOG_ENTRY_PROCESSED, entryTs);

        for (OpaqueEntry opaqueEntry : newOpaqueEntryList) {
            for (UUID uuid : opaqueEntry.getEntries().keySet()) {
                for (SMREntry smrEntry : opaqueEntry.getEntries().get(uuid)) {
                    txBuilder.logUpdate(uuid, smrEntry);
                }
            }
        }

        txBuilder.commit(timestamp);
        lastMsgTs = Math.max(entryTs, lastMsgTs);
    }

    /**
     * Apply message generate by log entry reader and will apply at the destination corfu cluster.
     * @param msg
     * @return last processed message timestamp
     * @throws ReplicationWriterException
     */
    public long apply(LogReplicationEntry msg) throws ReplicationWriterException {

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
