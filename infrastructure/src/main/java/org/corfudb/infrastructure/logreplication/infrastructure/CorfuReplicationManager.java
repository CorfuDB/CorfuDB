package org.corfudb.infrastructure.logreplication.infrastructure;

import com.google.common.collect.Sets;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.corfudb.infrastructure.LogReplicationRuntimeParameters;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.SyncStatus;
import org.corfudb.infrastructure.logreplication.proto.Sample;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.infrastructure.logreplication.runtime.CorfuLogReplicationRuntime;
import org.corfudb.infrastructure.logreplication.runtime.fsm.LogReplicationRuntimeStateType;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.*;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.IntervalRetry;
import org.corfudb.util.retry.RetryNeededException;

import java.util.*;
import java.util.concurrent.*;

/**
 * This class manages Log Replication for multiple remote (standby) clusters.
 */
@Slf4j
public class CorfuReplicationManager {

    // Keep map of remote cluster ID and the associated log replication runtime (an abstract
    // client to that cluster)
    private final Map<String, CorfuLogReplicationRuntime> runtimeToRemoteCluster = new HashMap<>();

    @Setter
    @Getter
    private TopologyDescriptor topology;

    private final LogReplicationContext context;

    private final NodeDescriptor localNodeDescriptor;

    private final CorfuRuntime corfuRuntime;

    private final LogReplicationMetadataManager metadataManager;

    private final String pluginFilePath;

    private Map<String, Set<String>> domainToSitesMap;

    private ExecutorService executor;

    private LogReplicationConfig config;

    /**
     * Constructor
     */
    public CorfuReplicationManager(LogReplicationConfig config, LogReplicationContext context, NodeDescriptor localNodeDescriptor,
                                   LogReplicationMetadataManager metadataManager, String pluginFilePath,
                                   CorfuRuntime corfuRuntime) {
        this.context = context;
        this.metadataManager = metadataManager;
        this.pluginFilePath = pluginFilePath;
        this.corfuRuntime = corfuRuntime;
        this.localNodeDescriptor = localNodeDescriptor;
        this.config = config;
        domainToSitesMap = new HashMap<>();

    }

    private class StreamListenerImpl implements StreamListener {

        // update the domain,
        @Override
        public void onNext(CorfuStreamEntries results) {
            for(List<CorfuStreamEntry> entryList : results.getEntries().values()) {
                for(CorfuStreamEntry entry : entryList) {
                    log.info("OnNext entry: {}", entry);
                    Sample.SampleDomainTableKey key = (Sample.SampleDomainTableKey) entry.getKey();
                    Sample.SampleDomainTableValue incomingDestination = (Sample.SampleDomainTableValue) entry.getPayload();
                    log.info("Change was: {}",incomingDestination.getSitesList());
                    Set<String> incomingSiteChanges = new HashSet<>();
                    for(String site : incomingDestination.getSitesList()) {
                        incomingSiteChanges.add(site);
                    }

                    Set<String> currDestinationSites = domainToSitesMap.get(key.getLogicalGroupName());
                    // change is captured only if it differs from existing set. Ensures idempotency.
                    if(Sets.difference(currDestinationSites, incomingSiteChanges).size() > 0 ||
                            Sets.difference(incomingSiteChanges, currDestinationSites).size() > 0) {
                        captureDomainChange(key.getLogicalGroupName(), incomingSiteChanges);
                    }

                }
            }
        }

        @Override
        public void onError(Throwable throwable) {
            log.error("Error in domain listner {} ", throwable);
        }
    }

    private void captureDomainChange(String domainName, Set<String> incomingSiteChanges) {
        Set<String> currDestinationSites = domainToSitesMap.get(domainName);
        Set<String> sitesToAdd = Sets.difference(incomingSiteChanges, currDestinationSites);
        Set<String> sitesToRemove = Sets.difference(currDestinationSites, incomingSiteChanges);
        if (sitesToAdd.size() > 0) {
            // 1. change the map in config

            // 2. call the FSM. if FSM is already present, force-sync else start a new fsm.
            // for PoC assuming that there is a runtime against all LMs already.
            // In actual coding, need to handle addition of new LM -> This would also be a topology change I guess.
            log.info("currDestinationSites {}, domainToSitesMap {}", currDestinationSites, incomingSiteChanges);
            config.updateLMToStreamsMap(domainName, sitesToAdd, true);
            triggerForceSnapshotOnDomainChange(domainName, sitesToAdd);
            log.info("currDestinationSites {}, newSiteAdditions {}", currDestinationSites, sitesToAdd);
        }
        if (sitesToRemove.size() > 0) {
            //This is 1 out of the 3 cases in the confluence.

            config.updateLMToStreamsMap(domainName, sitesToRemove, false);
            triggerForceSnapshotOnDomainChange(domainName, sitesToRemove);
            log.info("currDestinationSites {}, newSiteRemoved {}", currDestinationSites, sitesToRemove);
        }
        domainToSitesMap.put(domainName, incomingSiteChanges);
    }

    private void triggerForceSnapshotOnDomainChange(String domainName, Set<String> sitesAddedOrRemoved) {
        for (String site : sitesAddedOrRemoved) {
            log.info("Starting force snapshot sync because site {} was added or removed from domain {}", site, domainName);
            CorfuLogReplicationRuntime remoteRuntime = runtimeToRemoteCluster.get(site);
            if (!remoteRuntime.getState().getType().equals(LogReplicationRuntimeStateType.REPLICATING)) {
                log.debug("The remote {} has not started replication. Adding the remote to domain {} " +
                        "did not inturrupt the state machine", site, domainName);
                continue;
            }
            //update domainToSitesMap
            remoteRuntime.getSourceManager().stopLogReplication();
            remoteRuntime.getSourceManager().startForcedSnapshotSync(UUID.randomUUID());
        }
    }


    private void initiateMaps() {
        domainToSitesMap.put("Domain1", new HashSet<>());
        domainToSitesMap.put("Domain2", new HashSet<>());
        domainToSitesMap.put("Domain3", new HashSet<>());

        final Map<String, String> LMtoIdMap = new HashMap<>();
        topology.getStandbyClusters().values().stream().forEach((x) -> {
            if (x.getClusterId().equals("111e4567-e89b-12d3-a456-556642440111")) {
                LMtoIdMap.put("LM1", x.getClusterId());
            } else if (x.getClusterId().equals("222e4567-e89b-12d3-a456-556642440222")) {
                LMtoIdMap.put("LM2", x.getClusterId());
            } else {
                LMtoIdMap.put("LM3", x.getClusterId());
            }
        });
        domainToSitesMap.get("Domain1").add("116e4567-e89b-12d3-a456-111664440011");
        domainToSitesMap.get("Domain1").add("226e4567-e89b-12d3-a456-111664440022");
        domainToSitesMap.get("Domain2").add("226e4567-e89b-12d3-a456-111664440022");
        domainToSitesMap.get("Domain3").add("336e4567-e89b-12d3-a456-111664440033");
        domainToSitesMap.get("Domain3").add("116e4567-e89b-12d3-a456-111664440011");

        log.info("setting up a subscriber ");
        CorfuStore corfuStore = new CorfuStore(corfuRuntime);

        try {
            corfuStore.openTable("LR-Test", "Table_Domains_Mapping",
                    Sample.SampleDomainTableKey.class,
                    Sample.SampleDomainTableValue.class, null,
                    TableOptions.fromProtoSchema(Sample.SampleDomainTableKey.class));
        } catch (Exception e) {
            log.error("caught trying to open Domain table in manager {}", e);
        }


        StreamListenerImpl listener1 = new StreamListenerImpl();
        corfuStore.subscribeListener(listener1, "LR-Test", "domain",
                Collections.singletonList("Table_Domains_Mapping"));
        log.info("have a subscriber ");
    }

    //Shama : StreamsToReplicate -> form a map: which LM replicates which table.


    /**
     * Start Log Replication Manager, this will initiate a runtime against
     * each standby cluster, to further start log replication.
     */
    public void start() {
        initiateMaps();
        BasicThreadFactory factory = new BasicThreadFactory.Builder()
                .namingPattern("LSM-thread-%d")
                .build();
        executor = Executors.newFixedThreadPool(topology.getStandbyClusters().values().size(), factory);

        for (ClusterDescriptor remoteCluster : topology.getStandbyClusters().values()) {
            try {
                log.info("Shama: {}", remoteCluster);
                executor.submit(() -> {

                    log.info("Shama, starting FSM: {}", remoteCluster.getClusterId());
                    startLogReplicationRuntime(remoteCluster);
                });
            } catch (Exception e) {
                log.error("Failed to start log replication runtime for remote cluster {}", remoteCluster.getClusterId());
            }
        }
        log.info("Shama, all runtimes against LMs: keys: {}, values:{}", runtimeToRemoteCluster.keySet(), runtimeToRemoteCluster.values());
    }

    /**
     * Stop log replication for all the standby sites
     */
    public void stop() {
        runtimeToRemoteCluster.values().forEach(runtime -> {
            try {
                log.info("Stop log replication runtime to remote cluster id={}", runtime.getRemoteClusterId());
                runtime.stop();
            } catch (Exception e) {
                log.warn("Failed to stop log replication runtime to remote cluster id={}", runtime.getRemoteClusterId());
            }
        });
        runtimeToRemoteCluster.clear();
    }

    /**
     * Restart connection to remote cluster
     */
    public void restart(ClusterDescriptor remoteCluster) {
        stopLogReplicationRuntime(remoteCluster.getClusterId());
        startLogReplicationRuntime(remoteCluster);
    }

    /**
     * Start Log Replication Runtime to a specific standby Cluster
     */
    private void startLogReplicationRuntime(ClusterDescriptor remoteClusterDescriptor) {
        String remoteClusterId = remoteClusterDescriptor.getClusterId();
        try {
            if (!runtimeToRemoteCluster.containsKey(remoteClusterId)) {
                log.info("Starting Log Replication Runtime to Standby Cluster id={}", remoteClusterId);
                connect(remoteClusterDescriptor);
            } else {
                log.warn("Log Replication Runtime to remote cluster {}, already exists. Skipping init.", remoteClusterId);
            }
        } catch (Exception e) {
            log.error("Caught exception, stop log replication runtime to {}", remoteClusterDescriptor, e);
            stopLogReplicationRuntime(remoteClusterId);
        }
    }

    /**
     * Connect to a remote Log Replicator, through a Log Replication Runtime.
     *
     * @throws InterruptedException
     */
    private void connect(ClusterDescriptor remoteCluster) throws InterruptedException {
        try {
            IRetry.build(IntervalRetry.class, () -> {
                try {
                    LogReplicationRuntimeParameters parameters = LogReplicationRuntimeParameters.builder()
                            .localCorfuEndpoint(context.getLocalCorfuEndpoint())
                            .remoteClusterDescriptor(remoteCluster)
                            .localClusterId(localNodeDescriptor.getClusterId())
                            .replicationConfig(context.getConfig())
                            .pluginFilePath(pluginFilePath)
                            .channelContext(context.getChannelContext())
                            .topologyConfigId(topology.getTopologyConfigId())
                            .keyStore(corfuRuntime.getParameters().getKeyStore())
                            .tlsEnabled(corfuRuntime.getParameters().isTlsEnabled())
                            .ksPasswordFile(corfuRuntime.getParameters().getKsPasswordFile())
                            .trustStore(corfuRuntime.getParameters().getTrustStore())
                            .tsPasswordFile(corfuRuntime.getParameters().getTsPasswordFile())
                            .build();
                    CorfuLogReplicationRuntime replicationRuntime = new CorfuLogReplicationRuntime(parameters, metadataManager,remoteCluster.getClusterId());
                    replicationRuntime.start();
                    runtimeToRemoteCluster.put(remoteCluster.getClusterId(), replicationRuntime);
                } catch (Exception e) {
                    log.error("Exception {}. / {}. Retry after 1 second.",
                            e, remoteCluster.getClusterId());
                    throw new RetryNeededException();
                }
                return null;
            }).run();
        } catch (InterruptedException e) {
            log.error("Unrecoverable exception when attempting to connect to remote cluster.", e);
            throw e;
        }
    }

    /**
     * Stop Log Replication to a specific standby Cluster
     */
    private void stopLogReplicationRuntime(String remoteClusterId) {
        CorfuLogReplicationRuntime logReplicationRuntime = runtimeToRemoteCluster.get(remoteClusterId);
        if (logReplicationRuntime != null) {
            log.info("Stop log replication runtime to remote cluster id={}", remoteClusterId);
            logReplicationRuntime.stop();
            runtimeToRemoteCluster.remove(remoteClusterId);
        } else {
            log.warn("Runtime not found to remote cluster {}", remoteClusterId);
        }
    }

    /**
     * Update Log Replication Runtime config id.
     */
    public void updateRuntimeConfigId(TopologyDescriptor newConfig) {
        runtimeToRemoteCluster.values().forEach(runtime -> runtime.updateFSMConfigId(newConfig));
    }

    /**
     * The notification of change of adding/removing standby's without epoch change.
     *
     * @param newConfig should have the same topologyConfigId as the current config
     */
    public void processStandbyChange(TopologyDescriptor newConfig) {
        // ConfigId mismatch could happen if customized cluster manager does not follow protocol
        if (newConfig.getTopologyConfigId() != topology.getTopologyConfigId()) {
            log.warn("Detected changes in the topology. The new topology descriptor {} doesn't have the same " +
                    "topologyConfigId as the current one {}", newConfig, topology);
        }

        Set<String> currentStandbys = new HashSet<>(topology.getStandbyClusters().keySet());
        Set<String> newStandbys = new HashSet<>(newConfig.getStandbyClusters().keySet());
        Set<String> intersection = Sets.intersection(currentStandbys, newStandbys);

        Set<String> standbysToRemove = new HashSet<>(currentStandbys);
        standbysToRemove.removeAll(intersection);

        // Remove standbys that are not in the new config
        for (String clusterId : standbysToRemove) {
            stopLogReplicationRuntime(clusterId);
            topology.removeStandbyCluster(clusterId);
        }

        // Start the standbys that are in the new config but not in the current config
        for (String clusterId : newStandbys) {
            if (!runtimeToRemoteCluster.containsKey(clusterId)) {
                ClusterDescriptor clusterInfo = newConfig.getStandbyClusters().get(clusterId);
                topology.addStandbyCluster(clusterInfo);
                startLogReplicationRuntime(clusterInfo);
            }
        }

        // The connection id or other transportation plugin's info could've changed for
        // existing standby cluster's, updating the routers will re-establish the connection
        // to the correct endpoints/nodes
        for (String clusterId : intersection) {
            ClusterDescriptor clusterInfo = newConfig.getStandbyClusters().get(clusterId);
            runtimeToRemoteCluster.get(clusterId).updateRouterClusterDescriptor(clusterInfo);
        }
    }

    /**
     * Stop the current log replication event and start a full snapshot sync for the given remote cluster.
     */
    public void enforceSnapshotSync(DiscoveryServiceEvent event) {
        CorfuLogReplicationRuntime standbyRuntime = runtimeToRemoteCluster.get(event.getRemoteClusterInfo().getClusterId());
        if (standbyRuntime == null) {
            log.warn("Failed to start enforceSnapshotSync for cluster {} as it is not on the standby list.",
                    event.getRemoteClusterInfo());
        } else {
            log.info("EnforceSnapshotSync for cluster {}", standbyRuntime.getRemoteClusterId());
            standbyRuntime.getSourceManager().stopLogReplication();
            standbyRuntime.getSourceManager().startForcedSnapshotSync(event.getEventId());
        }
    }

    /**
     * Update Replication Status as NOT_STARTED.
     * Should be called only once in an active lifecycle.
     */
    public void updateStatusAsNotStarted() {
        runtimeToRemoteCluster.values().forEach(corfuLogReplicationRuntime ->
                corfuLogReplicationRuntime
                        .getSourceManager()
                        .getAckReader()
                        .markSyncStatus(SyncStatus.NOT_STARTED));
    }
}
