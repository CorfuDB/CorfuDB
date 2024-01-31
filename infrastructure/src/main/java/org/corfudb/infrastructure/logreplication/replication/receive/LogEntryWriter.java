package org.corfudb.infrastructure.logreplication.replication.receive;

import com.google.protobuf.TextFormat;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.LogReplicationContext;
import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.service.CorfuProtocolLogReplication;
import org.corfudb.runtime.LogReplication;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMetadataMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryType;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.IntervalRetry;
import org.corfudb.util.retry.RetryNeededException;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationMetadata;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.corfudb.infrastructure.logreplication.config.LogReplicationConfig.REGISTRY_TABLE_ID;

/**
 * Process TxMessage that contains transaction logs for registered streams.
 */
@NotThreadSafe
@Slf4j
public class LogEntryWriter extends SinkWriter {
    // The source snapshot that the transaction logs are based
    private long srcGlobalSnapshot;

    // Timestamp of the last message processed.
    private long lastMsgTs;

    @Getter
    private final LogReplicationSession session;

    private final LogReplicationMetadataManager metadataManager;

    public LogEntryWriter(LogReplicationMetadataManager metadataManager,
                          LogReplicationSession session, LogReplicationContext replicationContext) {
        super(session, replicationContext);

        ReplicationMetadata metadata = metadataManager.getReplicationMetadata(session);

        this.srcGlobalSnapshot = metadata.getLastSnapshotApplied();
        this.lastMsgTs = metadata.getLastLogEntryBatchProcessed();
        this.metadataManager = metadataManager;
        this.session = session;
    }

    /**
     * Verify the metadata is the correct data type.
     * @param metadata
     * @throws ReplicationWriterException
     */
    private void verifyMetadata(LogReplicationEntryMetadataMsg metadata) throws ReplicationWriterException {
        if (metadata.getEntryType() != LogReplicationEntryType.LOG_ENTRY_MESSAGE) {
            log.error("[{}]:: Wrong message metadata {}, expecting type {} snapshot {}", getSessionName(), TextFormat.shortDebugString(metadata),
                LogReplicationEntryType.LOG_ENTRY_MESSAGE, srcGlobalSnapshot);
            throw new ReplicationWriterException("wrong type of message");
        }
    }

    /**
     * Extract OpaqueEntries from the message and write them to the log. OpaqueEntries which were already applied
     * are skipped
     *
     * @param txMessage
     * @return true when all OpaqueEntries in the msg are appended to the log
     */
    private boolean applyMsg(LogReplicationEntryMsg txMessage) {
        // Log entry sync could have slow writes. That is, there could be a relatively long duration between last
        // snapshot/log entry sync and the current log entry sync, during which Sink side could have new tables opened.
        // So the config needs to be synced here to capture those updates.
        replicationContext.refreshConfig(session, true);

        // Boolean value that indicate if the config should sync with registry table or not. Note that primitive boolean
        // value cannot be used here as its value needs to be changed in the lambda function below.
        AtomicBoolean registryTableUpdated = new AtomicBoolean(false);
        List<OpaqueEntry> opaqueEntryList = CorfuProtocolLogReplication.extractOpaqueEntries(txMessage);

        for (OpaqueEntry opaqueEntry : opaqueEntryList) {
            try {
                IRetry.build(IntervalRetry.class, () -> {
                    try (TxnContext txnContext = metadataManager.getTxnContext()) {

                        // NOTE: The topology config id should be queried and validated for every opaque entry because the
                        // Sink could have received concurrent topology config id changes. Here we are leveraging a
                        // single read to fetch multiple metadata types. This will be cleanly handled when the
                        // Metadata table's schema is changed to use the remote session as the key instead of
                        // metadata type.
                        ReplicationMetadata metadata = metadataManager.queryReplicationMetadata(txnContext, session);

                        long persistedTopologyConfigId = metadata.getTopologyConfigId();
                        long persistedSnapshotStart = metadata.getLastSnapshotStarted();
                        long persistedSnapshotDone = metadata.getLastSnapshotApplied();
                        long persistedBatchTs = metadata.getLastLogEntryBatchProcessed();
                        long persistedOpaqueEntryTs = metadata.getLastLogEntryApplied();

                        long topologyConfigId = txMessage.getMetadata().getTopologyConfigID();
                        long baseSnapshotTs = txMessage.getMetadata().getSnapshotTimestamp();
                        long prevTs = txMessage.getMetadata().getPreviousTimestamp();

                        // Validate the message metadata with the local metadata table
                        if (topologyConfigId != persistedTopologyConfigId || baseSnapshotTs != persistedSnapshotStart ||
                            baseSnapshotTs != persistedSnapshotDone || prevTs != persistedBatchTs) {
                            log.warn("[{}]:: Message metadata mismatch. Skip applying message {}, persistedTopologyConfigId={}," +
                                    "persistedSnapshotStart={}, persistedSnapshotDone={}, persistedBatchTs={}", getSessionName(),
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

                        ReplicationMetadata.Builder updatedMetadata = metadata.toBuilder()
                                .setTopologyConfigId(topologyConfigId)
                                .setLastLogEntryApplied(opaqueEntry.getVersion());

                        // If this is the last OpaqueEntry in the message/batch, update LAST_LOG_ENTRY_BATCH_PROCESSED
                        // with its timestamp
                        if (opaqueEntry.getVersion() == txMessage.getMetadata().getTimestamp()) {
                            updatedMetadata.setLastLogEntryBatchProcessed(txMessage.getMetadata().getTimestamp());
                        }

                        metadataManager.updateReplicationMetadata(txnContext, session, updatedMetadata.build());

                        for (UUID streamId : opaqueEntry.getEntries().keySet()) {
                            if (ignoreEntriesForStream(streamId)) {
                                log.warn("[{}]:: Skip applying log entries for stream {} as it is noisy. The Source and" +
                                    "Sink sites could be on different versions", getSessionName(), streamId);
                                continue;
                            }

                            List<SMREntry> smrEntries = opaqueEntry.getEntries().get(streamId);
                            if (streamId.equals(REGISTRY_TABLE_ID)) {
                                // If registry table entries are being handled, indicate the config to sync with
                                // registry table after this transaction.
                                smrEntries = filterRegistryTableEntries(new ArrayList<>(smrEntries));
                                if (!smrEntries.isEmpty()) {
                                    log.info("[{}]:: Registry Table entries during log entry sync = {}", getSessionName(),
                                            smrEntries.size());
                                    registryTableUpdated.set(true);
                                }
                            }

                            if (session.getSubscriber().getModel().equals(LogReplication.ReplicationModel.ROUTING_QUEUES)) {
                                createAndWriteQueueRecord(txnContext, smrEntries, metadataManager.getCorfuStore());
                            } else {
                                for (SMREntry smrEntry : smrEntries) {
                                    // If stream tags exist for the current stream, it means it's intended for streaming
                                    // on the Sink (receiver)
                                    txnContext.logUpdate(streamId, smrEntry,
                                            replicationContext.getConfig(session).getDataStreamToTagsMap().get(streamId));
                                }
                            }
                        }
                        txnContext.commit();

                        // Sync with registry table if registry table entries are handled in last transaction, in order
                        // to update the config with those new entries.
                        if (registryTableUpdated.get()) {
                            // As config is not updated automatically with registry table updates, it is possible that
                            // config did not get the local updates and is diverged from registry. This can be avoided
                            // by ‘touching’ the applied stream(s) registry entry(ies) within the transaction, causing
                            // an abort if concurrent updates to the registry occur. We are currently not implementing
                            // this, as (1) it incurs in additional RPC calls for all updates and (2) LR will filter out
                            // these streams on the next batch.
                            replicationContext.refreshConfig(session, true);
                            registryTableUpdated.set(false);
                        }
                    } catch (TransactionAbortedException tae) {
                        throw new RetryNeededException();
                    }
                    return null;
                }).run();
            } catch (IllegalArgumentException e) {
                log.error("[{}]:: Metadata mismatch detected in entry with sequence {}", getSessionName(), opaqueEntry.getVersion(), e);
                return false;
            } catch (InterruptedException e) {
                log.error("[{}]:: Could not apply entry with sequence {}", getSessionName(), opaqueEntry.getVersion(), e);
                return false;
            }
        }
        // lastMsgTs always tracks the timestamp of the last Opaque Entry in a LogReplicationEntryMsg which was
        // successfully applied.  So update it accordingly.
        lastMsgTs = txMessage.getMetadata().getTimestamp();
        return true;
    }

    /**
     * Apply message at the destination Corfu cluster
     *
     * @param msg
     * @return true when all transactions(Opaque Entries) in the msg are appended to the log
     * @throws ReplicationWriterException
     */
    public boolean apply(LogReplicationEntryMsg msg) throws ReplicationWriterException {

        log.debug("[{}]:: Apply log entry {}", getSessionName(), msg.getMetadata().getTimestamp());

        verifyMetadata(msg.getMetadata());

        // Ignore the out of date messages
        if (msg.getMetadata().getSnapshotTimestamp() < srcGlobalSnapshot) {
            log.warn("[{}]:: Ignore Log Entry. Received message with snapshot {} is smaller than current snapshot {}",
                    getSessionName(), msg.getMetadata().getSnapshotTimestamp(), srcGlobalSnapshot);
            return false;
        }

        // A new Delta sync is triggered, setup the new srcGlobalSnapshot and msgQ
        if (msg.getMetadata().getSnapshotTimestamp() > srcGlobalSnapshot) {
            log.warn("[{}]:: A new log entry sync is triggered with higher snapshot, previous snapshot is {} and setup the " +
                "new srcGlobalSnapshot & lastMsgTs as {}", getSessionName(), srcGlobalSnapshot, msg.getMetadata().getSnapshotTimestamp());
            srcGlobalSnapshot = msg.getMetadata().getSnapshotTimestamp();
            lastMsgTs = srcGlobalSnapshot;
        }

        // If the entry is the expected entry, process it and process the messages in the queue.
        if (msg.getMetadata().getPreviousTimestamp() == lastMsgTs) {
            return applyMsg(msg);
        }

        log.warn("[{}]:: Transactions from {} to {} were not processed.  Last timestamp processed = {}, srcGlobalSnapshot={}",
                getSessionName(), msg.getMetadata().getPreviousTimestamp(), msg.getMetadata().getTimestamp(), lastMsgTs, srcGlobalSnapshot);
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
        // Sync with registry table when LogEntryWriter is reset, which will happen when Snapshot sync is completed, and
        // when LogReplicationSinkManager is initialized and reset.
        replicationContext.refreshConfig(session, true);
        srcGlobalSnapshot = snapshot;
        lastMsgTs = ackTimestamp;
    }
}
