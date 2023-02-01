package org.corfudb.infrastructure.logreplication.infrastructure;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.DiscoveryServiceEvent.DiscoveryServiceEventType;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationEvent;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuStore;
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
    private final ClusterDescriptor localCluster;

    public LogReplicationEventListener(CorfuReplicationDiscoveryService discoveryService, CorfuRuntime runtime,
                                       ClusterDescriptor localCluster) {
        this.discoveryService = discoveryService;
        this.corfuStore = new CorfuStore(runtime);
        this.localCluster = localCluster;
    }

    public void start() {
        log.info("LogReplication start listener for table {}", LogReplicationMetadataManager.REPLICATION_EVENT_TABLE_NAME);
        try {
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

        // If the current node is not a leader, ignore the notifications.
        synchronized (discoveryService) {
            if (!discoveryService.getIsLeader().get()) {
                log.info("onNext[{}] :: skipped as current node is not the leader", results.getTimestamp());
                return;
            }

            log.info("onNext[{}] :: processing updates for tables {}", results.getTimestamp(),
                    results.getEntries().keySet().stream().map(TableSchema::getTableName).collect(Collectors.toList()));

            // If the current node is the leader, it generates a discovery event and put it into the discovery service event queue.
            for (List<CorfuStreamEntry> entryList : results.getEntries().values()) {
                for (CorfuStreamEntry entry : entryList) {
                    ReplicationEvent event = (ReplicationEvent) entry.getPayload();
                    log.info("Received event :: id={}, type={}, cluster_id={}", event.getEventId(), event.getType(),
                            event.getClusterId());
                    if (event.getType().equals(ReplicationEvent.ReplicationEventType.FORCE_SNAPSHOT_SYNC)) {
                        discoveryService.input(new DiscoveryServiceEvent(DiscoveryServiceEventType.ENFORCE_SNAPSHOT_SYNC,
                                localCluster.getClusterId(), event.getClusterId(), event.getEventId()));
                    } else {
                        log.warn("Received invalid event :: id={}, type={}, cluster_id={}", event.getEventId(), event.getType(),
                                event.getClusterId());                    }
                }
            }
        }
    }

    @Override
    public void onError(Throwable throwable) {
        log.error("onError with a throwable ", throwable);
    }
}
