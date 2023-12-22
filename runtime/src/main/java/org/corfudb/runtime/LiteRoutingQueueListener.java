package org.corfudb.runtime;

import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStreamEntries;
import org.corfudb.runtime.collections.CorfuStreamEntry;
import org.corfudb.runtime.collections.StreamListenerResumeOrFullSync;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.view.CorfuGuid;
import org.corfudb.runtime.Queue.ReplicationType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;

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

        for (CorfuStreamEntry entry : entries) {
            // The Source always 'adds' replicated data to the queue.  So the op type must be UPDATE
            Preconditions.checkState(entry.getOperation() == CorfuStreamEntry.OperationType.UPDATE);
            Queue.CorfuGuidMsg key = (Queue.CorfuGuidMsg) entry.getKey();
            Queue.RoutingTableEntryMsg msg = (Queue.RoutingTableEntryMsg) entry.getPayload();
            allRQMsgs.add(msg);

            Queue.CorfuQueueMetadataMsg metadataMsg = (Queue.CorfuQueueMetadataMsg) entry.getMetadata();
            allEntries.add(new Table.CorfuQueueRecord(key, metadataMsg, msg));
            currentReplicationType = msg.getReplicationType();
        }
        if (allEntries.isEmpty()) {
            return;
        }
        int numEntriesProcessed;
        long now = System.currentTimeMillis();
        long whenQRecordWasCreated = CorfuGuid.getTimestampFromGuid(allEntries.get(0).getRecordId().getInstanceId(),
            now);

        log.info("Delivering {} {} updates took {}ms end to end", allEntries.size(), currentReplicationType,
            now - whenQRecordWasCreated);
        numEntriesProcessed = processBatch(currentReplicationType, allRQMsgs);

        if (numEntriesProcessed == allRQMsgs.size()) {
            try (TxnContext txnContext = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                List<UUID> noStreamTags = Collections.emptyList();
                allEntries.forEach(q -> txnContext.logUpdateDelete(recvQ, q.getRecordId(), noStreamTags, corfuStore));
                txnContext.commit();
            }
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

                log.info("LiteRecvQ::performFullSync delivering {} messages of type {}", batchOneOfAKind.size(),
                    currentReplicationType);
                entriesProcessed += processBatch(currentReplicationType, batchOneOfAKind);

                // We have encountered a change in type, reset our batch..
                batchOneOfAKind = new ArrayList<>();
                currentReplicationType = msg.getReplicationType();

                // If the current message is of type LAST_SNAPSHOT_SYNC_ENTRY, invoke the completion callback
                if (msg.getReplicationType() == LAST_SNAPSHOT_SYNC_ENTRY) {
                    onSnapshotSyncComplete();
                }
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
            try (TxnContext txnContext = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                allMsgs.forEach(q -> txnContext.delete(recvQ, q.getRecordId()));
                txnContext.commit();
            }
        }
        int finalQSize = recvQ.count();
        log.info("LiteQRecv:performFullSync completed at ts {} final Q size {}", ts, finalQSize);
        return ts;
    }

    private int processBatch(ReplicationType currentReplicationType, List<Queue.RoutingTableEntryMsg> batch) {
        int entriesProcessed = 0;

        // On startup, currentReplicationType = LOG_ENTRY_SYNC.  So if the queue contains messages from
        // snapshot sync or snapshot sync end, this method will get invoked with an empty batch.  Add a
        // check to return in such a case.
        if (batch.isEmpty()) {
            return entriesProcessed;
        }

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
                // Just increment entriesProcessed.  onSnapshotSyncComplete() callback was already made
                entriesProcessed++;
                break;
            default:
                throw new IllegalStateException("Unexpected replication type encountered " + currentReplicationType);
        }
        return entriesProcessed;
    }

    protected abstract boolean processUpdatesInSnapshotSync(List<Queue.RoutingTableEntryMsg> updates);

    protected abstract boolean processUpdatesInLogEntrySync(List<Queue.RoutingTableEntryMsg> updates);

    protected abstract void onSnapshotSyncComplete();
}
