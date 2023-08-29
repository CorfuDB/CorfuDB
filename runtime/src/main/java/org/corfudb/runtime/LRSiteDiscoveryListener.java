package org.corfudb.runtime;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.collections.CorfuStore;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;

/**
 * This listener will notify when a new remote site for specific
 * log replication model is available for replication.
 */
@Slf4j
public abstract class LRSiteDiscoveryListener {

    protected final CorfuStore corfuStore;
    private final ScheduledExecutorService executorService;
    private final boolean executorServiceStartedByMe;

    private final String clientName;

    private final Map<String, Set<String>> clientToKnownSites = new HashMap<>();

    public LRSiteDiscoveryListener(CorfuStore corfuStore,
                                   LogReplication.ReplicationModel replicationModel, String clientName) {
        if (!replicationModel.equals(LogReplication.ReplicationModel.ROUTING_QUEUES)) {
            throw new UnsupportedOperationException("Only routing queue based clients are currently supported");
        }
        this.executorService = new ScheduledThreadPoolExecutor(1);
        this.executorServiceStartedByMe = true;
        this.corfuStore = corfuStore;
        this.clientName = clientName;
        clientToKnownSites.put(clientName, new HashSet<>());
        executorService.scheduleWithFixedDelay(this::pollForNewSites, 0, 10, TimeUnit.SECONDS);
    }

    public LRSiteDiscoveryListener(CorfuStore corfuStore,
                                   LogReplication.ReplicationModel replicationModel,
                                   ScheduledExecutorService executorService, String clientName) {
        if (!replicationModel.equals(LogReplication.ReplicationModel.ROUTING_QUEUES)) {
            throw new UnsupportedOperationException("Only routing queue based clients are currently supported");
        }
        this.corfuStore = corfuStore;
        this.executorService = executorService;
        this.executorServiceStartedByMe = false;
        this.clientName = clientName;
        clientToKnownSites.put(clientName, new HashSet<>());
        executorService.scheduleWithFixedDelay(this::pollForNewSites, 0, 10, TimeUnit.SECONDS);
    }

    public void pollForNewSites() {
        corfuStore.getRuntime().getTableRegistry().listTables().forEach(tableName -> {
            if (tableName.getTableName().startsWith(LogReplicationUtils.REPLICATED_RECV_Q_PREFIX)) {
                String siteId = StringUtils.substringBetween(tableName.getTableName(), "_");
                String client = StringUtils.substringAfterLast(tableName.getTableName(), "_");
                if (!Objects.equals(client, clientName)) {
                    return;
                }

                if (!clientToKnownSites.get(client).contains(siteId)) {
                    try {
                        log.info("Discovered new site {} for client {}", siteId, client);
                        this.onNewSiteUp(siteId);
                        clientToKnownSites.get(client).add(siteId);
                    } catch (Exception e) {
                        log.error("LRSiteDiscovery callback hit error on site {}, client {}, {}", siteId, client, e);
                    }
                }
            }
        });
    }

    public void shutdown() {
        if (executorServiceStartedByMe) {
            executorService.shutdown();
        }
    }

    // -------------- callback to be implemented by caller ----------
    public abstract void onNewSiteUp(String siteId);
}
