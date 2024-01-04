package org.corfudb.runtime;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStreamEntries;
import org.corfudb.runtime.collections.CorfuStreamEntry;
import org.corfudb.runtime.collections.StreamListenerResumeOrFullSync;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.LogReplicationClientException;
import org.corfudb.runtime.Queue.ReplicationType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.corfudb.runtime.LogReplicationUtils.REPLICATED_QUEUE_TAG;
import static org.corfudb.runtime.Queue.ReplicationType.LAST_SNAPSHOT_SYNC_ENTRY;
import static org.corfudb.runtime.Queue.ReplicationType.LOG_ENTRY_SYNC;
import static org.corfudb.runtime.Queue.ReplicationType.SNAPSHOT_SYNC;
import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

// TODO: As the name suggests, this is a simplistic listener which delivers replicated data as received on LR Sink
//  cluster to the application(client).  Most error handling is yet to be implemented.
@Slf4j
public abstract class LiteRoutingQueueListener extends StreamListenerResumeOrFullSync {

    private final CorfuStore corfuStore;

    final Table<Queue.CorfuGuidMsg, Queue.RoutingTableEntryMsg, Queue.CorfuQueueMetadataMsg> recvQ;

    @Getter
    public String sourceSiteId;

    @Getter
    private String clientName;

    private ReplicationType currentReplicationType = LOG_ENTRY_SYNC;

    public LiteRoutingQueueListener(CorfuStore corfuStore, String sourceSiteId, String clientName) {
        super(corfuStore, CORFU_SYSTEM_NAMESPACE, REPLICATED_QUEUE_TAG,
                Arrays.asList(LogReplicationUtils.REPLICATED_RECV_Q_PREFIX + sourceSiteId + "_" + clientName));
        this.corfuStore = corfuStore;
        this.sourceSiteId = sourceSiteId;
        this.clientName = clientName;
        Table<Queue.CorfuGuidMsg, Queue.RoutingTableEntryMsg, Queue.CorfuQueueMetadataMsg> recvQLcl = null;
        int numRetries = 8;
        while (numRetries-- > 0) {
            try {
                try {
                    recvQLcl = corfuStore.getTable(CORFU_SYSTEM_NAMESPACE,
                            LogReplicationUtils.REPLICATED_RECV_Q_PREFIX + sourceSiteId + "_" + clientName);
                } catch (NoSuchElementException | IllegalArgumentException e) {
                    recvQLcl = corfuStore.openQueue(CORFU_SYSTEM_NAMESPACE,
                            LogReplicationUtils.REPLICATED_RECV_Q_PREFIX + sourceSiteId + "_" + clientName,
                            Queue.RoutingTableEntryMsg.class,
                            TableOptions.builder().schemaOptions(
                                            CorfuOptions.SchemaOptions.newBuilder()
                                                    .addStreamTag(REPLICATED_QUEUE_TAG)
                                                    .build())
                                    .build());
                }
                break;
            } catch (Exception e) {
                log.error("Failed to open replicated queue due to exception!", e);
            }
        }
        this.recvQ = recvQLcl;
    }

    @Override
    public void onNext(CorfuStreamEntries results) {
        log.debug("LRQListener received {} updates at address {}!", results.getEntries().size(), results.getTimestamp());
        List<CorfuStreamEntry> entries = results.getEntries().entrySet().stream().map(Map.Entry::getValue)
                .findFirst().get();

        List<Table.CorfuQueueRecord> allEntries = new ArrayList<>(entries.size());
        List<Queue.RoutingTableEntryMsg> allRQMsgs = new ArrayList<>();

        // Capture the first message in this batch and compute the current replication type.  It will later be used
        // for validation that all entries in a transaction belong to the same replication type.
        // For snapshot sync, the first message will be CLEAR so get the next message, if any.
        // For log entry sync, get the first message.
        CorfuStreamEntry firstUpdateEntry = null;
        if (entries.get(0).getOperation() == CorfuStreamEntry.OperationType.CLEAR && entries.size() > 1) {
            firstUpdateEntry = entries.get(1);
        } else if (entries.get(0).getOperation() == CorfuStreamEntry.OperationType.UPDATE) {
            firstUpdateEntry = entries.get(0);
        }

        if (firstUpdateEntry != null) {
            Queue.RoutingTableEntryMsg firstMsg = (Queue.RoutingTableEntryMsg) firstUpdateEntry.getPayload();
            currentReplicationType = firstMsg.getReplicationType();
        }

        for (CorfuStreamEntry entry : entries) {
            // The Source always 'adds' replicated data to the queue.  So ignore all operations where type != UPDATE.
            // Non-update entries are written by LR (CLEAR) and this listener itself (DELETE after successful
            // processing).
            if (entry.getOperation() != CorfuStreamEntry.OperationType.UPDATE) {
                continue;
            }
            Queue.CorfuGuidMsg key = (Queue.CorfuGuidMsg) entry.getKey();
            Queue.RoutingTableEntryMsg msg = (Queue.RoutingTableEntryMsg) entry.getPayload();

            if (msg.getReplicationType() != currentReplicationType) {
                throw new LogReplicationClientException("Not expecting mixed event types "+ currentReplicationType +" vs "
                    + msg.getReplicationType());
            }

            allRQMsgs.add(msg);
            Queue.CorfuQueueMetadataMsg metadataMsg = (Queue.CorfuQueueMetadataMsg) entry.getMetadata();
            allEntries.add(new Table.CorfuQueueRecord(key, metadataMsg, msg));
        }
        if (allEntries.isEmpty()) {
            return;
        }
        int numEntriesProcessed;
        numEntriesProcessed = processBatch(currentReplicationType, allRQMsgs);

        if (numEntriesProcessed == allRQMsgs.size()) {
            deleteQueueEntries(allEntries);
            log.debug("Deleted {} messages from {}", allEntries.size(), recvQ.getFullyQualifiedTableName());
        }
    }

    @Override
    public CorfuStoreMetadata.Timestamp performFullSync() {
        CorfuStoreMetadata.Timestamp ts;
        List<Table.CorfuQueueRecord> allMsgs;

        try (TxnContext txnContext = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            allMsgs = txnContext.entryList(recvQ);
            ts = txnContext.commit();
        }

        List<Queue.RoutingTableEntryMsg> batchOneOfAKind = new ArrayList<>();
        int entriesProcessed = 0;
        for (Table.CorfuQueueRecord q : allMsgs) {
            Queue.RoutingTableEntryMsg msg = (Queue.RoutingTableEntryMsg) q.getEntry();

            if (msg.getReplicationType() != currentReplicationType) {

                // On startup, currentReplicationType = LOG_ENTRY_SYNC.  So if the queue contains messages from
                // snapshot sync or snapshot sync end, this method will get invoked with an empty batch.  Add a
                // check to skip processing in such a case.
                if (!batchOneOfAKind.isEmpty()) {
                    log.info("LiteRecvQ::performFullSync delivering {} messages of type {}", batchOneOfAKind.size(),
                        currentReplicationType);
                    entriesProcessed += processBatch(currentReplicationType, batchOneOfAKind);

                    // We have encountered a change in type, reset our batch..
                    batchOneOfAKind = new ArrayList<>();
                }

                currentReplicationType = msg.getReplicationType();
            }
            // Batch up message of the same kind so we can deliver in a batch
            batchOneOfAKind.add(msg);
        }

        // Process any remaining entries
        if (!batchOneOfAKind.isEmpty()) {
            entriesProcessed += processBatch(currentReplicationType, batchOneOfAKind);
        }

        if (entriesProcessed == allMsgs.size()) {
            log.info("LiteQRecv:performFullSync deleting {} messages", entriesProcessed);
            deleteQueueEntries(allMsgs);
        }
        int finalQSize = recvQ.count();
        log.info("LiteQRecv:performFullSync completed at ts {} final Q size {}", ts, finalQSize);
        return ts;
    }

    private int processBatch(ReplicationType currentReplicationType, List<Queue.RoutingTableEntryMsg> batch) {
        int entriesProcessed = 0;

        switch (currentReplicationType) {
            case LOG_ENTRY_SYNC:
                if (processUpdatesInLogEntrySync(batch)) {
                    entriesProcessed = batch.size();
                }
                break;
            case SNAPSHOT_SYNC:
                if (processUpdatesInSnapshotSync(batch)) {
                    entriesProcessed = batch.size();
                }
                break;
            case LAST_SNAPSHOT_SYNC_ENTRY:
                onSnapshotSyncComplete();
                entriesProcessed = batch.size();
                break;
            default:
                throw new IllegalStateException("Unexpected replication type encountered " + currentReplicationType);
        }
        return entriesProcessed;
    }

    private void deleteQueueEntries(List<Table.CorfuQueueRecord> queueEntries) {
        try (TxnContext txnContext = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            queueEntries.forEach(q -> txnContext.delete(recvQ, q.getRecordId()));
            txnContext.commit();
        }
    }

    protected abstract boolean processUpdatesInSnapshotSync(List<Queue.RoutingTableEntryMsg> updates);

    protected abstract boolean processUpdatesInLogEntrySync(List<Queue.RoutingTableEntryMsg> updates);

    protected abstract void onSnapshotSyncComplete();
}
