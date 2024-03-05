package org.corfudb.infrastructure.logreplication.infrastructure;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationEvent;
import org.corfudb.runtime.collections.CorfuStreamEntries;
import org.corfudb.runtime.collections.CorfuStreamEntry;
import org.corfudb.runtime.collections.StreamListener;

import java.util.List;

@Slf4j
public final class LogReplicationEventListener implements StreamListener {
    private final CorfuReplicationDiscoveryService discoveryService;

    public  LogReplicationEventListener(CorfuReplicationDiscoveryService discoveryService) {
        this.discoveryService = discoveryService;
    }

    public void start() {
        discoveryService.getLogReplicationMetadataManager().subscribeReplicationEventTable(this);
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
                    ReplicationEvent event = (ReplicationEvent) entry.getPayload();
                    log.info("ReplicationEventListener received an event with id {}, type {}, cluster id {}",
                            event.getEventId(), event.getType(), event.getClusterId());
                    if (event.getType().equals(ReplicationEvent.ReplicationEventType.FORCE_SNAPSHOT_SYNC)) {
                        discoveryService.input(new DiscoveryServiceEvent(
                                DiscoveryServiceEvent.DiscoveryServiceEventType.ENFORCE_SNAPSHOT_SYNC,
                                event.getClusterId(),
                                event.getEventId()));
                    }
                }
            }
        }
    }

    @Override
    public void onError(Throwable throwable) {
        log.error("onError with a throwable ", throwable);
    }
}
