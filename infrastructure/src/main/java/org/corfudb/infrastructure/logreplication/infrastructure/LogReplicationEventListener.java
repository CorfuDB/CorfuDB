package org.corfudb.infrastructure.logreplication.infrastructure;

import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.DiscoveryServiceEvent.DiscoveryServiceEventType;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationEventInfoKey;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationEvent;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationEvent.ReplicationEventType;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStoreEntry;
import org.corfudb.runtime.collections.CorfuStreamEntries;
import org.corfudb.runtime.collections.CorfuStreamEntry;
import org.corfudb.runtime.collections.StreamListener;
import org.corfudb.runtime.collections.TableSchema;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

@Slf4j
public final class LogReplicationEventListener implements StreamListener {

    private final CorfuReplicationDiscoveryService discoveryService;
    private final CorfuStore corfuStore;
    private AtomicBoolean listenerStarted;

    public LogReplicationEventListener(CorfuReplicationDiscoveryService discoveryService, CorfuRuntime runtime) {
        this.discoveryService = discoveryService;
        this.corfuStore = new CorfuStore(runtime);
        this.listenerStarted = new AtomicBoolean(false);
    }

    public void start() {
        if (listenerStarted.compareAndSet(false, true)) {
            log.info("LogReplication start listener for table {}", LogReplicationMetadataManager.REPLICATION_EVENT_TABLE_NAME);
            try {
                // Read the event table to process any events written when LR was not running (Processed events are deleted from te table)
                processPendingRequests();

                // Subscription can fail if the table was not opened, opened with an incorrect tag or the address at
                // which subscription is attempted has been trimmed.  None of these are likely in this case as this is an
                // internal table opened on MetadataManager init(completed before) and subscription is done at the log tail.
                // However, if there is a failure, simply log it and continue such that normal replication flow is not
                // interrupted.
                corfuStore.subscribeListener(this, LogReplicationMetadataManager.NAMESPACE,
                        LogReplicationMetadataManager.LR_STREAM_TAG, Collections.singletonList(
                                LogReplicationMetadataManager.REPLICATION_EVENT_TABLE_NAME));
            } catch (Exception e) {
                log.error("Failed to subscribe to the ReplicationEvent Table", e);
                listenerStarted.set(false);
            }
        }
    }

    public void stop() {
        corfuStore.unsubscribeListener(this);
        this.listenerStarted.set(false);
    }

    @Override
    public void onNext(CorfuStreamEntries results) {

        log.info("onNext[{}] :: processing updates for tables {}", results.getTimestamp(),
            results.getEntries().keySet().stream().map(TableSchema::getTableName).collect(Collectors.toList()));

        // Generate a discovery event and put it into the discovery service event queue.
        for (List<CorfuStreamEntry> entryList : results.getEntries().values()) {
            for (CorfuStreamEntry entry : entryList) {
                if (entry.getOperation() == CorfuStreamEntry.OperationType.CLEAR ||
                        entry.getOperation() == CorfuStreamEntry.OperationType.DELETE) {
                    log.warn("LR EventListener ignoring a {} operation", entry.getOperation());
                    continue;
                }
                ReplicationEventInfoKey key = (ReplicationEventInfoKey) entry.getKey();
                ReplicationEvent event = (ReplicationEvent) entry.getPayload();
                log.info("Received event :: id={}, type={}, session={}, ts={}", event.getEventId(), event.getType(),
                    key.getSession(), event.getEventTimestamp());
                if (event.getType().equals(ReplicationEventType.FORCE_SNAPSHOT_SYNC) ||
                        event.getType().equals(ReplicationEventType.UPGRADE_COMPLETION_FORCE_SNAPSHOT_SYNC)) {
                    triggerForcedSnapshotSync(key, event);
                } else {
                    log.warn("Received invalid event :: id={}, type={}, cluster_id={} ts={}", event.getEventId(),
                        event.getType(), event.getClusterId(), event.getEventTimestamp());
                }
            }
        }
    }

    /**
     * Read the event table to process any events written when LR was not running.
     */
    private void processPendingRequests() {
        List<CorfuStoreEntry<ReplicationEventInfoKey, ReplicationEvent, Message>> pendingEvents =
            discoveryService.getSessionManager().getMetadataManager().getReplicationEvents();

        for (CorfuStoreEntry event : pendingEvents) {
            triggerForcedSnapshotSync((ReplicationEventInfoKey)event.getKey(), (ReplicationEvent)event.getPayload());
        }
    }

    private void triggerForcedSnapshotSync(ReplicationEventInfoKey key, ReplicationEvent event) {
        if (event.getType().equals(ReplicationEventType.UPGRADE_COMPLETION_FORCE_SNAPSHOT_SYNC)) {
            for (LogReplicationSession session : discoveryService.getSessionManager().getSessions()) {
                log.info("Adding event for forced snapshot sync request for session {}, sync_id={}",
                        session, event.getEventId());
                discoveryService.input(new DiscoveryServiceEvent(DiscoveryServiceEventType.ENFORCE_SNAPSHOT_SYNC,
                        session, event.getEventId()));
            }
        } else {
            discoveryService.input(new DiscoveryServiceEvent(DiscoveryServiceEventType.ENFORCE_SNAPSHOT_SYNC,
                    key.getSession(), event.getEventId()));
        }
    }

    @Override
    public void onError(Throwable throwable) {
        log.error("onError with a throwable ", throwable);
        listenerStarted.set(false);
        // resume the subscription
        start();
    }
}
