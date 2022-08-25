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
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager.LogReplicationMetadataType;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.IntervalRetry;
import org.corfudb.util.retry.RetryNeededException;

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

    // The set of streams that log entry writer will work on.
    private final Map<UUID, String> streamMap;

    private final Map<UUID, List<UUID>> dataStreamToTagsMap;

    // The source snapshot that the transaction logs are based
    private long srcGlobalSnapshot;

    // Timestamp of the last message processed.
    private long lastMsgTs;

    public LogEntryWriter(CorfuRuntime rt, LogReplicationConfig config, LogReplicationMetadataManager logReplicationMetadataManager) {
        super(rt);
        this.logReplicationMetadataManager = logReplicationMetadataManager;
        this.srcGlobalSnapshot = logReplicationMetadataManager.getLastAppliedSnapshotTimestamp();
        this.lastMsgTs = logReplicationMetadataManager.getLastProcessedLogEntryBatchTimestamp();
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
            log.error("Wrong message metadata {}, expecting type {} snapshot {}", TextFormat.shortDebugString(metadata),
                LogReplicationEntryType.LOG_ENTRY_MESSAGE, srcGlobalSnapshot);
            throw new ReplicationWriterException("wrong type of message");
        }
    }

    /**
     * Extract OpaqueEntries from the message and write them to the log.  OpaqueEntries which were already applied
     * are skipped
     *
     * @param txMessage
     * @return true when all OpaqueEntries in the msg are appended to the log
     */
    private boolean applyMsg(LogReplicationEntryMsg txMessage) {

        List<OpaqueEntry> opaqueEntryList = CorfuProtocolLogReplication.extractOpaqueEntries(txMessage);

        for (OpaqueEntry opaqueEntry : opaqueEntryList) {
            try {
                IRetry.build(IntervalRetry.class, () -> {
                    try (TxnContext txnContext = logReplicationMetadataManager.getTxnContext()) {

                        // NOTE: The topology config id should be queried and validated for every opaque entry because the
                        // Sink could have received concurrent topology config id changes.  Here we are leveraging a
                        // single read to fetch multiple metadata types.  This will be cleanly handled when the
                        // Metadata table's schema is changed to use the remote session as the key instead of
                        // metadata type.
                        Map<LogReplicationMetadataType, Long> metadataMap =
                            logReplicationMetadataManager.queryMetadata(txnContext,
                                LogReplicationMetadataType.TOPOLOGY_CONFIG_ID,
                                LogReplicationMetadataType.LAST_SNAPSHOT_STARTED,
                                LogReplicationMetadataType.LAST_SNAPSHOT_APPLIED,
                                LogReplicationMetadataType.LAST_LOG_ENTRY_BATCH_PROCESSED,
                                LogReplicationMetadataType.LAST_LOG_ENTRY_APPLIED);

                        long persistedTopologyConfigId =
                            metadataMap.get(LogReplicationMetadataType.TOPOLOGY_CONFIG_ID);
                        long persistedSnapshotStart =
                            metadataMap.get(LogReplicationMetadataType.LAST_SNAPSHOT_STARTED);
                        long persistedSnapshotDone =
                            metadataMap.get(LogReplicationMetadataType.LAST_SNAPSHOT_APPLIED);
                        long persistedBatchTs =
                            metadataMap.get(LogReplicationMetadataType.LAST_LOG_ENTRY_BATCH_PROCESSED);
                        long persistedOpaqueEntryTs =
                            metadataMap.get(LogReplicationMetadataType.LAST_LOG_ENTRY_APPLIED);

                        long topologyConfigId = txMessage.getMetadata().getTopologyConfigID();
                        long baseSnapshotTs = txMessage.getMetadata().getSnapshotTimestamp();
                        long prevTs = txMessage.getMetadata().getPreviousTimestamp();

                        // Validate the message metadata with the local metadata table
                        if (topologyConfigId != persistedTopologyConfigId || baseSnapshotTs != persistedSnapshotStart ||
                            baseSnapshotTs != persistedSnapshotDone || prevTs != persistedBatchTs) {
                            log.warn("Message metadata mismatch. Skip applying message {}, persistedTopologyConfigId={}," +
                                    "persistedSnapshotStart={}, persistedSnapshotDone={}, persistedBatchTs={}",
                                txMessage.getMetadata(), persistedTopologyConfigId, persistedSnapshotStart,
                                persistedSnapshotDone, persistedBatchTs);
                            throw new IllegalArgumentException("Cannot apply log entry message due to metadata mismatch");
                        }

                        // Skip Opaque entries with timestamp that are not larger than persistedOpaqueEntryTs
                        if (opaqueEntry.getVersion() <= persistedOpaqueEntryTs) {
                            log.trace("Skipping entry {} as it is less than the last applied opaque entry {}",
                                opaqueEntry.getVersion(), persistedOpaqueEntryTs);
                            return null;
                        }

                        // LAST_LOG_ENTRY_APPLIED has the timestamp of the last OpaqueEntry applied from a
                        // batch of opaque entries received.
                        logReplicationMetadataManager.appendUpdate(txnContext,
                            LogReplicationMetadataType.LAST_LOG_ENTRY_APPLIED, opaqueEntry.getVersion());

                        // CorfuStore uses WriteAfterWriteTransaction type so even though the topology is read
                        // and validated for each OpaqueEntry, this read will not be used to detect a concurrent
                        // topology update on the Sink.  So force an update to this key by using the touch() api.
                        // NOTE: This will be addressed once the schema of the metadata table is updated to have
                        // all types in a single key (remote session)
                        logReplicationMetadataManager.touch(txnContext, LogReplicationMetadataType.TOPOLOGY_CONFIG_ID);

                        // If this is the last OpaqueEntry in the message/batch, update LAST_LOG_ENTRY_BATCH_PROCESSED
                        // with its timestamp
                        if (opaqueEntry.getVersion() == txMessage.getMetadata().getTimestamp()) {
                            logReplicationMetadataManager.appendUpdate(txnContext,
                                LogReplicationMetadataType.LAST_LOG_ENTRY_BATCH_PROCESSED,
                                txMessage.getMetadata().getTimestamp());
                        }

                        for (UUID streamId : opaqueEntry.getEntries().keySet()) {
                            if (!streamMap.containsKey(streamId)) {
                                log.warn("Skip applying log entries for stream {} as it is noisy. The Source and" +
                                    "Sink sites could be on different versions", streamId);
                                continue;
                            }

                            List<SMREntry> smrEntries = opaqueEntry.getEntries().get(streamId);
                            if (streamId.equals(REGISTRY_TABLE_ID)) {
                                // Only retain tables that are not present in the registry table
                                smrEntries = fetchNewEntries(new ArrayList<>(smrEntries));
                            }

                            for (SMREntry smrEntry : smrEntries) {
                                // If stream tags exist for the current stream, it means its intended for streaming
                                // on the Sink (receiver)
                                txnContext.logUpdate(streamId, smrEntry, dataStreamToTagsMap.get(streamId));
                            }
                        }
                        txnContext.commit();
                    } catch (TransactionAbortedException tae) {
                        throw new RetryNeededException();
                    }
                    return null;
                }).run();
            } catch (IllegalArgumentException e) {
                log.error("Metadata mismatch detected in entry with sequence " + opaqueEntry.getVersion(), e);
                return false;
            } catch (InterruptedException e) {
                log.error("Could not apply entry with sequence " + opaqueEntry.getVersion());
                return false;
            }
        }
        // lastMsgTs always tracks the timestamp of the last Opaque Entry in a LogReplicationEntryMsg which was
        // successfully applied.  So update it accordingly.
        lastMsgTs = txMessage.getMetadata().getTimestamp();
        return true;
    }

    /**
     * Apply message at the destination Corfu Cluster
     *
     * @param msg
     * @return true when all transactions(Opaque Entries) in the msg are appended to the log
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
            log.warn("A new log entry sync is triggered with higher snapshot, previous snapshot is {} and setup the " +
                "new srcGlobalSnapshot & lastMsgTs as {}", srcGlobalSnapshot, msg.getMetadata().getSnapshotTimestamp());
            srcGlobalSnapshot = msg.getMetadata().getSnapshotTimestamp();
            lastMsgTs = srcGlobalSnapshot;
        }

        // If the entry is the expected entry, process it and process the messages in the queue.
        if (msg.getMetadata().getPreviousTimestamp() == lastMsgTs) {
            return applyMsg(msg);
        }

        log.warn("Transactions from {} to {} were not processed.  Last timestamp processed = {}, srcGlobalSnapshot={}",
            msg.getMetadata().getPreviousTimestamp(), msg.getMetadata().getTimestamp(), lastMsgTs, srcGlobalSnapshot);
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
