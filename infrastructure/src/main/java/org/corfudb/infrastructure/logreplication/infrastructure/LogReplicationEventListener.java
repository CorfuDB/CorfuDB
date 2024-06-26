package org.corfudb.infrastructure.logreplication.infrastructure;

import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationEvent;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.collections.CorfuStoreEntry;
import org.corfudb.runtime.collections.CorfuStreamEntries;
import org.corfudb.runtime.collections.CorfuStreamEntry;
import org.corfudb.runtime.collections.StreamListenerResumeOrFullSync;

import java.util.Collections;
import java.util.List;

import static org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager.LR_STREAM_TAG;
import static org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager.NAMESPACE;
import static org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager.REPLICATION_EVENT_TABLE_NAME;

@Slf4j
public final class LogReplicationEventListener extends StreamListenerResumeOrFullSync {
    private final CorfuReplicationDiscoveryService discoveryService;

    public  LogReplicationEventListener(CorfuReplicationDiscoveryService discoveryService) {
        super(discoveryService.getLogReplicationMetadataManager().getCorfuStore(), NAMESPACE, LR_STREAM_TAG,
                Collections.singletonList(REPLICATION_EVENT_TABLE_NAME));
        this.discoveryService = discoveryService;
    }

    public void start() {
        // perform full sync before subscribing to the table
        CorfuStoreMetadata.Timestamp timestamp = performFullSync();
        discoveryService.getLogReplicationMetadataManager().subscribeReplicationEventTable(this, timestamp);
    }

    @Override
    public CorfuStoreMetadata.Timestamp performFullSync() {
        log.info("Performing full sync of event table");
        Pair<List<CorfuStoreEntry<LogReplicationMetadata.ReplicationEventKey, ReplicationEvent, Message>>, CorfuStoreMetadata.Timestamp> eventsAndSubscriptionTs =
                discoveryService.getLogReplicationMetadataManager().getoutstandingEvents();
        for(CorfuStoreEntry<LogReplicationMetadata.ReplicationEventKey, ReplicationEvent, Message> event: eventsAndSubscriptionTs.getLeft()) {
            // The event table only contains force snapshot sync requests.
            // Incase there are multiple snapshot sync requests already enqueued by now, process any one of them.
            // Upon successfully processing an event, clear the event table. This ensures that LR doesn't perform
            // snapshot sync unnecessarily in a loop.
            processEvent(event.getPayload());
            break;
        }

        return eventsAndSubscriptionTs.getRight();
    }

    public void stop() {
        discoveryService.getLogReplicationMetadataManager().unsubscribeReplicationEventTable(this);
    }

    @Override
    public void onNext(CorfuStreamEntries results) {

        // If the current node is not a leader, ignore the notifications.
        synchronized (discoveryService) {
            if (!discoveryService.getIsLeader().get()) {
                log.info("The onNext call with {} will be skipped as the current node as it is not the leader.", results);
                return;
            }

            log.info("LogReplicationEventListener onNext {} will be processed at node {} in the cluster {}",
                    results, discoveryService.getLocalNodeDescriptor(), discoveryService.getLocalClusterDescriptor());

            // If the current node is the leader, it generates a discovery event and put it into the discovery service event queue.
            for (List<CorfuStreamEntry> entryList : results.getEntries().values()) {
                for (CorfuStreamEntry entry : entryList) {

                    if (entry.getOperation() == CorfuStreamEntry.OperationType.CLEAR ||
                            entry.getOperation() == CorfuStreamEntry.OperationType.DELETE) {
                        log.warn("LR EventListener ignoring a {} operation", entry.getOperation());
                        continue;
                    }

                    ReplicationEvent event = (ReplicationEvent) entry.getPayload();
                    log.info("ReplicationEventListener received an event with id {}, type {}, cluster id {}",
                            event.getEventId(), event.getType(), event.getClusterId());
                    processEvent(event);
                }
            }
        }
    }

    private void processEvent(ReplicationEvent event) {
        if (event.getType().equals(ReplicationEvent.ReplicationEventType.FORCE_SNAPSHOT_SYNC)) {
            discoveryService.input(new DiscoveryServiceEvent(
                    DiscoveryServiceEvent.DiscoveryServiceEventType.ENFORCE_SNAPSHOT_SYNC,
                    event.getClusterId(),
                    event.getEventId()));
        }
    }
}
