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
import org.corfudb.runtime.view.CorfuGuid;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.corfudb.runtime.LogReplicationUtils.REPLICATED_QUEUE_TAG;
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

    public LiteRoutingQueueListener(CorfuStore corfuStore, String sourceSiteId, String clientName) {
        super(corfuStore, CORFU_SYSTEM_NAMESPACE, REPLICATED_QUEUE_TAG,
                Arrays.asList(LogReplicationUtils.REPLICATED_RECV_Q_PREFIX + sourceSiteId + "_" + clientName));
        this.corfuStore = corfuStore;
        this.sourceSiteId = sourceSiteId;
        this.clientName = clientName;
        Table<Queue.CorfuGuidMsg, Queue.RoutingTableEntryMsg, Queue.CorfuQueueMetadataMsg> recvQ_lcl = null;
        int numRetries = 8;
        while (numRetries-- > 0) {
            try {
                try {
                    recvQ_lcl = corfuStore.getTable(CORFU_SYSTEM_NAMESPACE,
                            LogReplicationUtils.REPLICATED_RECV_Q_PREFIX + sourceSiteId + "_" + clientName);
                } catch(NoSuchElementException | IllegalArgumentException e) {
                    recvQ_lcl = corfuStore.openQueue(CORFU_SYSTEM_NAMESPACE,
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
        this.recvQ = recvQ_lcl;
    }

    @Override
    public void onNext(CorfuStreamEntries results) {
        log.debug("LRQListener received {} updates at address {}!",
                results.getEntries().size(), results.getTimestamp());
        List<CorfuStreamEntry> entries = results.getEntries().entrySet().stream()
                .map(Map.Entry::getValue).findFirst().get();
        List<Table.CorfuQueueRecord> allEntries = new ArrayList<>(entries.size());
        Queue.ReplicationType currentType = Queue.ReplicationType.LAST_FULL_SYNC_ENTRY;
        for (CorfuStreamEntry entry : entries) {
            if (!entry.getOperation().equals(CorfuStreamEntry.OperationType.UPDATE)) {
                continue;
            }
            Queue.CorfuGuidMsg key = (Queue.CorfuGuidMsg) entry.getKey();
            Queue.RoutingTableEntryMsg msg = (Queue.RoutingTableEntryMsg) entry.getPayload();
            Queue.CorfuQueueMetadataMsg metadataMsg = (Queue.CorfuQueueMetadataMsg) entry.getMetadata();
            allEntries.add(new Table.CorfuQueueRecord(key, metadataMsg, msg));
            if (currentType.equals(Queue.ReplicationType.LAST_FULL_SYNC_ENTRY)) {
                currentType = msg.getReplicationType();
            } else if (!currentType.equals(msg.getReplicationType())) {
                throw new LogReplicationClientException("Not expecting mixed event types "+ currentType +" vs "
                    +msg.getReplicationType());
            }
        }
        if (allEntries.isEmpty()) {
            return;
        }
        boolean entriesProcessed;
        long now = System.currentTimeMillis();
        long whenQrecordWasCreated = CorfuGuid.getTimestampFromGuid(allEntries.get(0).getRecordId().getInstanceId(), now);
        if (currentType.equals(Queue.ReplicationType.LOG_ENTRY_SYNC)) {
            log.info("LRQRecvListener delivering {} LOG_ENTRY updates took {}ms end to end",
                    allEntries.size(), now - whenQrecordWasCreated);
            entriesProcessed = processUpdatesInLogEntrySync(allEntries.stream().sorted().map(q ->
                    (Queue.RoutingTableEntryMsg)q.getEntry()).collect(Collectors.toList()));
        } else {
            log.info("LRQRecvListener delivering {} FULLSYNC updates took {}ms end to end",
                    allEntries.size(), now - whenQrecordWasCreated);
            entriesProcessed = processUpdatesInSnapshotSync(allEntries.stream().sorted().map(q ->
                    (Queue.RoutingTableEntryMsg)q.getEntry()).collect(Collectors.toList()));
        }

        if (entriesProcessed) {
            try (TxnContext txnContext = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                List<UUID> noStreamTags = Collections.emptyList();
                allEntries.forEach(q -> txnContext.logUpdateDelete(recvQ, q.getRecordId(), noStreamTags, corfuStore)
                );
                txnContext.commit();
            }
            log.debug("Deleted {} messages from {}", allEntries.size(), recvQ.getFullyQualifiedTableName());
        }
    }
    @Override
    public CorfuStoreMetadata.Timestamp performFullSync() {
        CorfuStoreMetadata.Timestamp ts = null;
        List<Table.CorfuQueueRecord> allMsgs;
        try (TxnContext txnContext = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            allMsgs = txnContext.entryList(recvQ);
            ts = txnContext.commit();
        }

        List<Queue.RoutingTableEntryMsg> batchOneOfAKind = new ArrayList<>();
        // TODO: Fix the enum once the receiver sets the type for full sync as well.
        Queue.ReplicationType currentType = Queue.ReplicationType.LAST_FULL_SYNC_ENTRY;
        int entriesProcessed = 0;
        for (Table.CorfuQueueRecord q : allMsgs) {
            Queue.RoutingTableEntryMsg msg = (Queue.RoutingTableEntryMsg) q.getEntry();
            if (!msg.getReplicationType().equals(currentType)) {
                if (currentType.equals(Queue.ReplicationType.LAST_FULL_SYNC_ENTRY)) {
                    currentType = msg.getReplicationType();
                    batchOneOfAKind.add(msg);
                    continue;
                }
                if (currentType.equals(Queue.ReplicationType.LOG_ENTRY_SYNC)) {
                    log.info("LiteRecvQ::performFullSync {} LOG_ENTRY messages", batchOneOfAKind.size());
                    if (processUpdatesInLogEntrySync(batchOneOfAKind)) {
                        entriesProcessed = entriesProcessed + batchOneOfAKind.size();
                    }
                } else {
                    log.info("LiteRecvQ::performFullSync {} SNAPSHOT messages", batchOneOfAKind.size());
                    if (processUpdatesInSnapshotSync(batchOneOfAKind)) {
                        entriesProcessed = entriesProcessed + batchOneOfAKind.size();
                    }
                } // else we have encountered a change in type, reset our batch..
                batchOneOfAKind = new ArrayList<>();
                currentType = msg.getReplicationType();
            } // else batch up message of the same kind so we can deliver in a batch
            batchOneOfAKind.add(msg);
        }
        if (!batchOneOfAKind.isEmpty()) {
            if (currentType.equals(Queue.ReplicationType.LOG_ENTRY_SYNC)) {
                log.info("LiteRecvQ::performFullSync {} LOG_ENTRY messages", batchOneOfAKind.size());
                if (processUpdatesInLogEntrySync(batchOneOfAKind)) {
                    entriesProcessed = entriesProcessed + batchOneOfAKind.size();
                }
            } else {
                log.info("LiteRecvQ::performFullSync {} SNAPSHOT messages", batchOneOfAKind.size());
                if (processUpdatesInSnapshotSync(batchOneOfAKind)) {
                    entriesProcessed = entriesProcessed + batchOneOfAKind.size();
                }
            }
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

    protected abstract boolean processUpdatesInSnapshotSync(List<Queue.RoutingTableEntryMsg> updates);

    protected abstract boolean processUpdatesInLogEntrySync(List<Queue.RoutingTableEntryMsg> updates);
}
