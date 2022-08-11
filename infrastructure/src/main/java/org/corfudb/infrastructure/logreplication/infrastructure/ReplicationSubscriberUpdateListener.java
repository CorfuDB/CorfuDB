package org.corfudb.infrastructure.logreplication.infrastructure;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.infrastructure.logreplication.infrastructure.DiscoveryServiceEvent.DiscoveryServiceEventType;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStreamEntries;
import org.corfudb.runtime.collections.CorfuStreamEntry;
import org.corfudb.runtime.collections.StreamListener;
import org.corfudb.runtime.collections.TableSchema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;
import static org.corfudb.runtime.view.TableRegistry.REGISTRY_TABLE_NAME;

@Slf4j
public class ReplicationSubscriberUpdateListener implements StreamListener {

    private CorfuReplicationDiscoveryService discoveryService;
    private LogReplicationConfig replicationConfig;
    private CorfuStore corfuStore;

    public ReplicationSubscriberUpdateListener(CorfuReplicationDiscoveryService discoveryService,
                                               LogReplicationConfig replicationConfig, CorfuRuntime runtime) {
        this.discoveryService = discoveryService;
        this.replicationConfig = replicationConfig;
        this.corfuStore = new CorfuStore(runtime);
    }

    public void start() {
        log.info("Start listening on the Registry Table");
        try {
            corfuStore.subscribeListener(this, CORFU_SYSTEM_NAMESPACE, "",
                Collections.singletonList(REGISTRY_TABLE_NAME));
        } catch (Exception e) {
            log.error("Failed to subscribe to the Registry table", e);
        }
    }

    public void stop() {
        corfuStore.unsubscribeListener(this);
    }

    @Override
    public void onNext(CorfuStreamEntries results) {
        List<CorfuStreamEntry> filteredEntries = new ArrayList<>();
        for (TableSchema key : results.getEntries().keySet()) {
            if (Objects.equals(key.getTableName(), REGISTRY_TABLE_NAME)) {
                filteredEntries.addAll(results.getEntries().get(key));
            }
        }
        for (CorfuStreamEntry entry : filteredEntries) {
            // TODO pankti: Extract replication client and model fields from the metadata
            String replicationClient = null;
            LogReplicationConfig.ReplicationModel replicationModel = null;
            ReplicationSubscriber subscriber = new ReplicationSubscriber(replicationModel, replicationClient);
            if (!replicationConfig.getReplicationSubscriberToStreamsMap().keySet().contains(subscriber)) {
                discoveryService.input(new DiscoveryServiceEvent(DiscoveryServiceEventType.NEW_REPLICATION_SUBSCRIBER,
                    subscriber));
            }
        }
    }

    @Override
    public void onError(Throwable throwable) {
        log.error("Error received on Registry Table Listener", throwable);
    }

}
