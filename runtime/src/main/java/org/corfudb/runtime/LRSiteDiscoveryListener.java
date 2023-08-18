package org.corfudb.runtime;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.corfudb.runtime.collections.CorfuStore;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * This listener will notify when a new remote site for specific
 * log replication model is available for replication.
 */
@Slf4j
public abstract class LRSiteDiscoveryListener {
    private final Set<String> knownSites;
    protected final CorfuStore corfuStore;
    private final ScheduledExecutorService executorService;
    private final boolean executorServiceStartedByMe;

    public LRSiteDiscoveryListener(CorfuStore corfuStore,
                                   LogReplication.ReplicationModel replicationModel) {
        if (!replicationModel.equals(LogReplication.ReplicationModel.ROUTING_QUEUES)) {
            throw new UnsupportedOperationException("Only routing queue based clients are currently supported");
        }
        this.executorService = new ScheduledThreadPoolExecutor(1);
        this.executorServiceStartedByMe = true;
        this.corfuStore = corfuStore;
        this.knownSites = new HashSet<>();
        executorService.scheduleWithFixedDelay(this::pollForNewSites, 0, 10, TimeUnit.SECONDS);
    }

    public LRSiteDiscoveryListener(CorfuStore corfuStore,
                                   LogReplication.ReplicationModel replicationModel,
                                   ScheduledExecutorService executorService) {
        if (!replicationModel.equals(LogReplication.ReplicationModel.ROUTING_QUEUES)) {
            throw new UnsupportedOperationException("Only routing queue based clients are currently supported");
        }
        this.corfuStore = corfuStore;
        this.knownSites = new HashSet<>();
        this.executorService = executorService;
        this.executorServiceStartedByMe = false;
        executorService.scheduleWithFixedDelay(this::pollForNewSites, 0, 10, TimeUnit.SECONDS);
    }

    public void pollForNewSites() {
        corfuStore.getRuntime().getTableRegistry().listTables().forEach(tableName -> {
            if (tableName.getTableName().startsWith(LogReplicationUtils.REPLICATED_RECV_Q_PREFIX)) {
                String siteId = StringUtils.substringAfter(tableName.getTableName(),
                        LogReplicationUtils.REPLICATED_RECV_Q_PREFIX);
                if (!knownSites.contains(siteId)) {
                    try {
                        log.info("Discovered new site {}", siteId);
                        this.onNewSiteUp(siteId);
                        knownSites.add(siteId);
                    } catch (Exception e) {
                        log.error("LRSiteDiscovery callback hit error on site {}, {}", siteId, e);
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
