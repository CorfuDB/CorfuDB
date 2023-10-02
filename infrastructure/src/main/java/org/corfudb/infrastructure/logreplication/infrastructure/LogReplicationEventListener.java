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
import java.util.stream.Collectors;

@Slf4j
public final class LogReplicationEventListener implements StreamListener {

    private final CorfuReplicationDiscoveryService discoveryService;
    private final CorfuStore corfuStore;

    public LogReplicationEventListener(CorfuReplicationDiscoveryService discoveryService, CorfuRuntime runtime) {
        this.discoveryService = discoveryService;
        this.corfuStore = new CorfuStore(runtime);
    }

    public void start() {
        log.info("LogReplication start listener for table {}", LogReplicationMetadataManager.REPLICATION_EVENT_TABLE_NAME);
        try {
            // Read the event table to process any events written when LR was not running.
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
        }
    }

    public void stop() {
        corfuStore.unsubscribeListener(this);
    }

    @Override
    public void onNext(CorfuStreamEntries results) {

        log.info("onNext[{}] :: processing updates for tables {}", results.getTimestamp(),
            results.getEntries().keySet().stream().map(TableSchema::getTableName).collect(Collectors.toList()));

        // Generate a discovery event and put it into the discovery service event queue.
        for (List<CorfuStreamEntry> entryList : results.getEntries().values()) {
            for (CorfuStreamEntry entry : entryList) {
                if (entry.getOperation() == CorfuStreamEntry.OperationType.CLEAR) {
                    log.warn("LREventListener ignoring a CLEAR operation");
                    continue;
                }
                ReplicationEventInfoKey key = (ReplicationEventInfoKey) entry.getKey();
                ReplicationEvent event = (ReplicationEvent) entry.getPayload();
                log.info("Received event :: id={}, type={}, session={}, ts={}", event.getEventId(), event.getType(),
                    key.getSession(), event.getEventTimestamp());
                if (event.getType().equals(ReplicationEventType.FORCE_SNAPSHOT_SYNC)) {
                    discoveryService.input(new DiscoveryServiceEvent(DiscoveryServiceEventType.ENFORCE_SNAPSHOT_SYNC,
                        key.getSession(), event.getEventId()));
                } else if (event.getType().equals(ReplicationEventType.UPGRADE_COMPLETION_FORCE_SNAPSHOT_SYNC)) {
                    // Note: This block will not get executed in the first version of LRv2 because in this version,
                    // LR does not start until all nodes in the cluster are on the same version.  So the event to
                    // trigger a forced snapshot sync will not be received as an update on the listener.
                    // Instead, it will be read on startup in processPendingRequests().
                    // In later versions of LRv2, the Log Replication process will run even when nodes are on
                    // different versions.  At that time, this event will be processed as an update from this
                    // block.
                    discoveryService.input(new DiscoveryServiceEvent(DiscoveryServiceEventType.ENFORCE_SNAPSHOT_SYNC,
                            key.getSession(), event.getEventId()));
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

        // TODO v2: Currently, this method runs on LR startup and processes events of type
        //  UPGRADE_COMPLETION_FORCE_SNAPSHOT_SYNC only.  If in future, there is a requirement to process all types of
        //  events handled by onNext(), this logic can be abstracted out to a common method which can be shared with
        //  onNext()
        for (CorfuStoreEntry event : pendingEvents) {
            if (((ReplicationEvent)event.getPayload()).getType().equals(
                    ReplicationEventType.UPGRADE_COMPLETION_FORCE_SNAPSHOT_SYNC)) {
                triggerForcedSnapshotSyncForAllSessions((ReplicationEvent)event.getPayload());
            }
        }
    }

    private void triggerForcedSnapshotSyncForAllSessions(ReplicationEvent event) {
        for (LogReplicationSession session : discoveryService.getSessionManager().getSessions()) {
            log.info("Adding event for forced snapshot sync request for session {}, sync_id={}",
                    session, event.getEventId());
            discoveryService.input(new DiscoveryServiceEvent(DiscoveryServiceEventType.ENFORCE_SNAPSHOT_SYNC,
                    session, event.getEventId()));
        }
    }

    @Override
    public void onError(Throwable throwable) {
        log.error("onError with a throwable ", throwable);
    }
}
