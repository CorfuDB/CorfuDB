package org.corfudb.runtime.view;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.IClientRouter;
import org.corfudb.runtime.clients.LayoutClient;
import org.corfudb.runtime.exceptions.AlreadyBootstrappedException;
import org.corfudb.runtime.exceptions.WorkflowException;
import org.corfudb.runtime.exceptions.WorkflowResultUnknownException;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.runtime.view.ClusterStatusReport.ClusterStatus;
import org.corfudb.runtime.view.ClusterStatusReport.ClusterStatusReliability;
import org.corfudb.runtime.view.ClusterStatusReport.ConnectivityStatus;
import org.corfudb.runtime.view.ClusterStatusReport.NodeStatus;
import org.corfudb.runtime.view.Layout.LayoutSegment;
import org.corfudb.runtime.view.workflows.AddNode;
import org.corfudb.runtime.view.workflows.ForceRemoveNode;
import org.corfudb.runtime.view.workflows.HealNode;
import org.corfudb.runtime.view.workflows.RemoveNode;
import org.corfudb.runtime.view.workflows.RestoreRedundancyMergeSegments;
import org.corfudb.util.CFUtils;
import org.corfudb.util.NodeLocator;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A view of the Management Service to manage reconfigurations of the Corfu Cluster.
 * <p>
 * <p>Created by zlokhandwala on 11/20/17.</p>
 */
@Slf4j
public class ManagementView extends AbstractView {

    /**
     * Number of attempts to ping a node to query the cluster status.
     */
    private static final int CLUSTER_STATUS_QUERY_ATTEMPTS = 3;

    public ManagementView(@NonNull CorfuRuntime runtime) {
        super(runtime);
    }

    /**
     * Remove a node from the cluster.
     *
     * @param endpointToRemove Endpoint of the node to be removed from the cluster.
     * @param retry            the number of times to retry a workflow if it fails
     * @param timeout          total time to wait before the workflow times out
     * @param pollPeriod       the poll interval to check whether a workflow completed or not
     * @throws WorkflowResultUnknownException when the side affect of the operation
     *                                        can't be determined
     * @throws WorkflowException              when the remove operation fails
     */
    public void removeNode(@Nonnull String endpointToRemove, int retry,
                           @Nonnull Duration timeout, @Nonnull Duration pollPeriod) {

        new RemoveNode(endpointToRemove, runtime, retry, timeout, pollPeriod).invoke();
    }

    /**
     * Force remove a node from the cluster.
     *
     * @param endpointToRemove Endpoint of the node to be removed from the cluster.
     * @param retry            the number of times to retry a workflow if it fails
     * @param timeout          total time to wait before the workflow times out
     * @param pollPeriod       the poll interval to check whether a workflow completed or not
     * @throws WorkflowResultUnknownException when the side affect of the operation
     *                                        can't be determined
     * @throws WorkflowException              when the remove operation fails
     */
    public void forceRemoveNode(@Nonnull String endpointToRemove, int retry,
                                @Nonnull Duration timeout, @Nonnull Duration pollPeriod) {
        new ForceRemoveNode(endpointToRemove, runtime, retry, timeout, pollPeriod).invoke();
    }

    /**
     * Add a new node to the existing cluster.
     *
     * @param endpointToAdd Endpoint of the new node to be added to the cluster.
     * @param retry         the number of times to retry a workflow if it fails
     * @param timeout       total time to wait before the workflow times out
     * @param pollPeriod    the poll interval to check whether a workflow completed or not
     * @throws WorkflowResultUnknownException when the side affect of the operation
     *                                        can't be determined
     */
    public void addNode(@Nonnull String endpointToAdd, int retry,
                        @Nonnull Duration timeout, @Nonnull Duration pollPeriod) {
        new AddNode(endpointToAdd, runtime, retry, timeout, pollPeriod)
                .invoke();
    }

    /**
     * Heal an unresponsive node.
     *
     * @param endpointToHeal Endpoint of the new node to be healed in the cluster.
     * @param retry          the number of times to retry a workflow if it fails
     * @param timeout        total time to wait before the workflow times out
     * @param pollPeriod     the poll interval to check whether a workflow completed or not
     * @throws WorkflowResultUnknownException when the side affect of the operation
     *                                        can't be determined
     */
    public void healNode(@Nonnull String endpointToHeal, int retry, @Nonnull Duration timeout,
                         @Nonnull Duration pollPeriod) {
        new HealNode(endpointToHeal, runtime, retry, timeout, pollPeriod).invoke();
    }

    /**
     * Restore redundancy and merge all split segments.
     *
     * @param endpointToRestoreRedundancy Endpoint whose redundancy is to be restored.
     * @param retry                       the number of times to retry a workflow if it fails
     * @param timeout                     total time to wait before the workflow times out
     * @param pollPeriod                  the poll interval to check whether a workflow completed or not
     * @throws WorkflowResultUnknownException when the side affect of the operation
     *                                        can't be determined
     */
    public void mergeSegments(@Nonnull String endpointToRestoreRedundancy, int retry, @Nonnull Duration timeout,
                              @Nonnull Duration pollPeriod) {
        new RestoreRedundancyMergeSegments(endpointToRestoreRedundancy, runtime, retry, timeout, pollPeriod).invoke();
    }

    /**
     * If all the layout servers are responsive the cluster status is STABLE,
     * if a minority of them are unresponsive then the status is DEGRADED,
     * else the cluster is UNAVAILABLE.
     *
     * @param layout              Current layout on which responsiveness was checked.
     * @param peerResponsiveNodes responsive nodes in the current layout.
     * @return ClusterStatus
     */
    private ClusterStatus getLayoutServersClusterHealth(Layout layout,
                                                        Set<String> peerResponsiveNodes) {
        ClusterStatus clusterStatus = ClusterStatus.STABLE;
        // A quorum of layout servers need to be responsive for the cluster to be STABLE.
        List<String> responsiveLayoutServers = new ArrayList<>(layout.getLayoutServers());
        // Retain only the responsive servers.
        responsiveLayoutServers.retainAll(peerResponsiveNodes);
        if (responsiveLayoutServers.size() != layout.getLayoutServers().size()) {
            clusterStatus = ClusterStatus.DEGRADED;
            int quorumSize = (layout.getLayoutServers().size() / 2) + 1;
            if (responsiveLayoutServers.size() < quorumSize) {
                clusterStatus = ClusterStatus.UNAVAILABLE;
            }
        }
        return clusterStatus;
    }

    /**
     * If the primary sequencer is unresponsive then the cluster is UNAVAILABLE.
     *
     * @param layout              Current layout on which responsiveness was checked.
     * @param peerResponsiveNodes responsive nodes in the current layout.
     * @return ClusterStatus
     */
    private ClusterStatus getSequencerServersClusterHealth(Layout layout,
                                                           Set<String> peerResponsiveNodes) {
        // The primary sequencer should be reachable for the cluster to be STABLE.
        return !peerResponsiveNodes.contains(layout.getPrimarySequencer())
                ? ClusterStatus.UNAVAILABLE : ClusterStatus.STABLE;
    }

    /**
     * Gets the log unit cluster status based on the replication protocol.
     *
     * @param layout              Current layout on which responsiveness was checked.
     * @param peerResponsiveNodes responsive nodes in the current layout.
     * @return ClusterStatus
     */
    private ClusterStatus getLogUnitServersClusterHealth(Layout layout,
                                                         Set<String> peerResponsiveNodes) {
        // logUnitRedundancyStatus marks the cluster as DB_SYNCING if any of the nodes is performing
        // stateTransfer and is in process of achieving full redundancy.
        ClusterStatus logUnitRedundancyStatus = peerResponsiveNodes.stream()
                .anyMatch(s -> getLogUnitNodeStatusInLayout(layout, s) == NodeStatus.DB_SYNCING)
                ? ClusterStatus.DB_SYNCING : ClusterStatus.STABLE;
        // Check the availability of the log servers in all segments as reads to all addresses
        // should be accessible.
        ClusterStatus logunitClusterStatus = layout.getSegments().stream()
                .map(segment -> segment.getReplicationMode()
                        .getClusterHealthForSegment(segment, peerResponsiveNodes))
                .max(Comparator.comparingInt(ClusterStatus::getHealthValue))
                .orElse(ClusterStatus.UNAVAILABLE);
        // Gets max of cluster status and logUnitRedundancyStatus.
        return Stream.of(logunitClusterStatus, logUnitRedundancyStatus)
                .max(Comparator.comparingInt(ClusterStatus::getHealthValue))
                .orElse(ClusterStatus.UNAVAILABLE);
    }

    /**
     * Analyzes the health of the cluster based on the views of the cluster of all the
     * ManagementAgents.
     * STABLE: if all nodes in the layout are responsive.
     * DEGRADED: if a minority of Layout servers
     * or a minority of LogUnit servers - in QUORUM_REPLICATION mode only are unresponsive.
     * UNAVAILABLE: if a majority of Layout servers or the Primary Sequencer
     * or a node in the CHAIN_REPLICATION or a majority of nodes in QUORUM_REPLICATION is
     * unresponsive.
     *
     * @param layout              Layout based on which the health is analyzed.
     * @param peerResponsiveNodes Responsive nodes according to the management services.
     * @return ClusterStatus
     */
    private ClusterStatus getClusterHealth(Layout layout, Set<String> peerResponsiveNodes) {

        return Stream.of(getLayoutServersClusterHealth(layout, peerResponsiveNodes),
                getSequencerServersClusterHealth(layout, peerResponsiveNodes),
                getLogUnitServersClusterHealth(layout, peerResponsiveNodes))
                // Gets cluster status from the layout, sequencer and log unit clusters.
                // The status is then aggregated by the max of the 3 statuses acquired.
                .max(Comparator.comparingInt(ClusterStatus::getHealthValue))
                .orElse(ClusterStatus.UNAVAILABLE);
    }

    /**
     * Returns a LogUnit Server's status in the layout. It is marked as:
     * UP if it is present in all segments or none of the segments and not in the unresponsive list,
     * NOTE: A node is UP if its not in any of the segments as it might not be a LogUnit component
     * but has only the Layout or the Sequencer (or both) component(s) active.
     * DB_SYNCING if it is present in some but not all or none of the segments,
     * DOWN if it is present in the unresponsive servers list.
     *
     * @param layout Layout to check.
     * @param server LogUnit Server endpoint.
     * @return NodeState with respect to the layout specified.
     */
    private NodeStatus getLogUnitNodeStatusInLayout(Layout layout, String server) {
        if (layout.getUnresponsiveServers().contains(server)) {
            return NodeStatus.DOWN;
        }
        final int segmentsCount = layout.getSegments().size();
        int nodeInSegments = 0;
        for (LayoutSegment layoutSegment : layout.getSegments()) {
            if (layoutSegment.getAllLogServers().contains(server)) {
                nodeInSegments++;
            }
        }
        return nodeInSegments == segmentsCount || nodeInSegments == 0
                ? NodeStatus.UP : NodeStatus.DB_SYNCING;
    }

    /**
     * Get the Cluster Status.
     *
     * This is reported as follows:
     * - (1) The status of the cluster itself (regardless of clients connectivity) as reflected in the
     *   layout. This information is presented along each node's status (up, down, db_sync).
     *
     *   It is important to note that as the cluster state is obtained from the layout,
     *   when quorum is not available (majority of nodes) there are lower guarantees on the
     *   reliability of this state.
     *   For example, in the absence of quorum the system might be in an unstable state which
     *   cannot converge due to lack of consensus. This is reflected in the
     *   cluster status report as clusterStatusReliability.
     *
     * - (2) The connectivity status of the client to every node in the cluster,
     *   i.e., can the client connect to the cluster. This will be obtained by
     *   ping(ing) every node and show as RESPONSIVE, for successful connections or UNRESPONSIVE for
     *   clients unable to communicate.
     *
     *  In this sense a cluster can be STABLE with all nodes UP, while not being available for a
     *  client, as all connections from the client to the cluster nodes are down, showing in this
     *  case connectivity status to all nodes as UNRESPONSIVE.
     *
     *  The ClusterStatusReport consists of the following:
     *
     *  CLUSTER-SPECIFIC STATUS
     *  - clusterStatus: the cluster status a perceived by the system's layout.
     *  STABLE, DEGRADED, DB_SYNCING or UNAVAILABLE
     *  - nodeStatusMap: each node's status as perceived by the system's layout.
     *  (UP, DOWN or DB_SYNC)
     *  - Cluster Status Reliability: STRONG_QUORUM, WEAK_NO_QUORUM or UNAVAILABLE
     *
     *  CLIENT-CLUSTER SPECIFIC STATUS:
     *  - clientServerConnectivityStatusMap: the connectivity status of this client to the cluster.
     *    (RESPONSIVE, UNRESPONSIVE).
     *
     * @return ClusterStatusReport
     */
    public ClusterStatusReport getClusterStatus() {
        Layout layout;
        ClusterStatusReliability statusReliability = ClusterStatusReliability.STRONG_QUORUM;

        // Get layout servers from runtime set of layout servers (specified in connection string).
        // This list is refreshed whenever a new layout is fetched.
        List<String> layoutServers = runtime.getLayoutServers().stream()
                .map(endpoint -> NodeLocator.parseString(endpoint).toEndpointUrl())
                .collect(Collectors.toList());

        try {
            // Obtain Layout from layout servers:
            //      Attempt to get layout from quorum (majority of nodes),
            //            ---if we fail to get from quorum----
            //      we are unable to provide a reliable state of the cluster from the layout,
            //      therefore, the cluster status is UNAVAILABLE.

            // The initial list of layoutServers is obtained from the runtime, this list might
            // not be complete as it can be based on a a minimum of servers required for bootstrap,
            // by setting the discoverLayoutServers flag, we request layouts from all available
            // layout servers present in the actual layout file.
            Map<String, Layout> layoutMap = getLayouts(layoutServers, true);

            int quorum = runtime.getLayoutView().getQuorumNumber();

            if (layoutMap.isEmpty()) {
                // If no layout server responded with a Layout, report cluster status as UNAVAILABLE.
                log.debug("getClusterStatus: all layout servers {} failed to respond with layouts.", layoutServers);
                // Even if we weren't able to obtain any layout from LayoutServers, we attempt to ping all
                // layoutServers for this runtime, to provide info of connectivity.
                // Note: layout should be null, as this is not the layout that leads to cluster status.
                Map<String, ConnectivityStatus> connectivityStatusMap = getConnectivityStatusMap(runtime.getLayoutView().getLayout());
                return getUnavailableClusterStatusReport(layoutServers, ClusterStatusReliability.UNAVAILABLE,
                        connectivityStatusMap.entrySet().stream()
                                .filter(x -> x.getValue().equals(ConnectivityStatus.RESPONSIVE))
                                .map(x -> x.getKey()).collect(Collectors.toSet()), null);
            } else {
                int serversLayoutResponses = layoutMap.size();

                if (serversLayoutResponses < quorum) {
                    // If quorum unreachable, report cluster status unavailable, but still for debug purposes provide
                    // information of the highest epoch layout in the system.
                    log.info("getClusterStatus: Quorum unreachable, reachable={}, required={}. Cluster status in unavailable.",
                            serversLayoutResponses, quorum);
                    layout = getHighestEpochLayout(layoutMap);
                    // Note: we can't pass the list of layoutServers obtained from the CorfuRuntime,
                    // as this might only reflect the servers used for initialization of RT.
                    // (which does not necessarily mean all), so we should retrieve from the available layout(s)
                    return getUnavailableClusterStatusReport(getLayoutServers(layoutMap.values()),
                            ClusterStatusReliability.WEAK_NO_QUORUM, layoutMap.keySet(), layout);
                } else {
                    layout = getLayoutFromQuorum(layoutMap, quorum);

                    if (layout == null) {
                        // No quorum nodes with same layout
                        // Report cluster status unavailable, but still for debug purposes provide
                        // information of the highest epoch layout in the system.
                        log.info("getClusterStatus: majority of nodes sharing the same layout not found. Cluster status is unavailable.");
                        layout = getHighestEpochLayout(layoutMap);
                        return getUnavailableClusterStatusReport(getLayoutServers(layoutMap.values()),
                                ClusterStatusReliability.WEAK_NO_QUORUM, layoutMap.keySet(), layout);
                    }

                    log.debug("getClusterStatus: quorum layout {}", layout);
                }
            }

            // Get Cluster Status from Layout
            ClusterStatus clusterStatus = getClusterHealth(layout, layout.getAllActiveServers());

            // Get Node Status from Layout
            Map<String, NodeStatus> nodeStatusMap = getNodeStatusMap(layout);

            // To complete cluster status with connectivity information of this
            // client to every node in the cluster, ping all servers
            // TODO: we could eventually skip pinging the nodes, as the layout fetch is indirectly a measure of connectivity.
            Map <String, ClusterStatusReport.ConnectivityStatus> connectivityStatusMap = getConnectivityStatusMap(layout);

            log.debug("getClusterStatus: successful. Overall cluster status: {}", clusterStatus);

            return new ClusterStatusReport(layout, clusterStatus, statusReliability, nodeStatusMap, connectivityStatusMap);
        } catch (Exception e) {
            log.error("getClusterStatus[{}]: cluster status unavailable. Exception: {}",
                    this.runtime.getParameters().getClientId(), e);
            return getUnavailableClusterStatusReport(layoutServers);
        }
    }

    private ClusterStatusReport getUnavailableClusterStatusReport(List<String> layoutServers, ClusterStatusReliability reliability, @Nonnull Set<String> responsiveServers, Layout layout) {
        return new ClusterStatusReport(layout,
                ClusterStatus.UNAVAILABLE,
                reliability,
                layoutServers.stream().collect(Collectors.toMap(
                        endpoint -> endpoint,
                        node -> NodeStatus.NA)),
                layoutServers.stream().collect(Collectors.toMap(
                        endpoint -> endpoint,
                        endpoint -> responsiveServers.contains(endpoint) ? ConnectivityStatus.RESPONSIVE : ConnectivityStatus.UNRESPONSIVE)));
    }

    private ClusterStatusReport getUnavailableClusterStatusReport(List<String> layoutServers) {
        return getUnavailableClusterStatusReport(layoutServers, ClusterStatusReliability.UNAVAILABLE, Collections.emptySet(), null);
    }

    /**
     * Get Layouts from list of specified layout servers.
     *
     * @param layoutServers list of initial layout servers to retrieve layouts from.
     * @param discoverLayoutServers flag to indicate if we want to discover new layout servers from
     *                              the layouts retrieved from initial set of layout servers.
     *                              Note: this is required as runtime.getBootstrapLayoutServers might have been
     *                              initialized with a single server (from all in cluster).
     * @return Map of endpoint id and layout.
     */
    private Map<String, Layout> getLayouts(List<String> layoutServers, Boolean discoverLayoutServers) {
        Map<String, CompletableFuture<Layout>> layoutFuturesMap = getLayoutFutures(layoutServers);
        Map<String, Layout> layoutMap = new HashMap<>();

        // Wait on the Completable futures
        for (Entry<String, CompletableFuture<Layout>> entry : layoutFuturesMap.entrySet()) {
            try {
                Layout nodeLayout = entry.getValue().get();
                log.debug("Server:{} responded with layout: {}", entry.getKey(), nodeLayout);
                layoutMap.put(entry.getKey(), nodeLayout);
            } catch (Exception e) {
                log.error("getClusterStatus: Error while fetching layout from {}. Exception: ",
                        entry.getKey(), e);
            }
        }

        // If discoverLayoutServers is set, fetch layouts from all other layout servers
        // discovered in the first search which are not contained in the initial list.
        if (discoverLayoutServers) {
            List<String> discoveredLayoutServers = layoutMap.values().stream()
                    .map(layout -> layout.getLayoutServers())
                    .flatMap(servers -> servers.stream())
                    .distinct()
                    .filter(servers -> !layoutServers.contains(servers))
                    .collect(Collectors.toList());

            if (!discoveredLayoutServers.isEmpty()) {
                log.debug("Get layout from discovered layout servers: {} ", discoveredLayoutServers);
                Map <String, Layout> recursiveLayouts = getLayouts(new ArrayList<>(discoveredLayoutServers), false);
                layoutMap.putAll(recursiveLayouts);
            }
        }

        return layoutMap;
    }

    private Layout getLayoutFromQuorum(@Nonnull Map<String, Layout> layoutMap, int quorum) {
        Layout layout = null;
        // Map of layout to number of nodes containing this layout.
        Map<Layout, Integer> uniqueLayoutMap = new HashMap<>();

        // Find if layouts are the same in all nodes (at least quorum nodes should agree in the
        // same layout) - count occurrences of each layout in cluster nodes.
        for (Layout nodeLayout : layoutMap.values()) {
            if (uniqueLayoutMap.keySet().contains(nodeLayout)) {
                uniqueLayoutMap.merge(nodeLayout, 1, Integer::sum);
            } else {
                uniqueLayoutMap.put(nodeLayout, 1);
            }
        }

        // Filter layouts present in quorum number of nodes
        Map<Layout, Integer> uniqueLayoutsQuorumMap = uniqueLayoutMap.entrySet().stream()
                .filter(uniqueLayout -> uniqueLayout.getValue() >= quorum)
                .collect(Collectors.toMap(uniqueLayout -> uniqueLayout.getKey(),
                        uniqueLayout -> uniqueLayout.getValue()));

        // Select layout with greater number of occurrences.
        if (!uniqueLayoutsQuorumMap.isEmpty()) {
            Map.Entry<Layout, Integer> maxEntry = null;
            for (Map.Entry<Layout, Integer> entry : uniqueLayoutsQuorumMap.entrySet()) {
                if (maxEntry == null || entry.getValue().compareTo(maxEntry.getValue()) > 0) {
                    maxEntry = entry;
                }
            }
            layout = maxEntry.getKey();
        }

        return layout;
    }

    private Layout getHighestEpochLayout(Map<String, Layout> layoutMap) {
        Layout layout  = null;
        for (Entry<String, Layout> entry : layoutMap.entrySet()) {
            Layout nodeLayout = entry.getValue();
            if (layout == null || nodeLayout.getEpoch() > layout.getEpoch()) {
                layout = nodeLayout;
            }
        }

        return layout;
    }


    private Map<String, CompletableFuture<Layout>> getLayoutFutures(List<String> layoutServers) {

        Map<String, CompletableFuture<Layout>> layoutFuturesMap = new HashMap<>();

        // Get layout futures for layout requests from all layout servers.
        for (String server : layoutServers) {
            IClientRouter router = runtime.getRouter(server);
            layoutFuturesMap.put(server, new LayoutClient(router, Layout.INVALID_EPOCH, Layout.INVALID_CLUSTER_ID)
                    .getLayout());
        }

        return layoutFuturesMap;
    }

    private Map<String, ClusterStatusReport.ConnectivityStatus> getConnectivityStatusMap(Layout layout) {
        Map<String, ClusterStatusReport.ConnectivityStatus> connectivityStatusMap = new HashMap<>();

        // Initialize connectivity status map to all servers as unresponsive
        for (String serverEndpoint : layout.getAllServers()) {
            connectivityStatusMap.put(serverEndpoint, ClusterStatusReport.ConnectivityStatus.UNRESPONSIVE);
        }

        RuntimeLayout runtimeLayout = new RuntimeLayout(layout, runtime);
        Map<String, CompletableFuture<Boolean>> pingFutureMap = new HashMap<>();


        for (int i = 0; i < CLUSTER_STATUS_QUERY_ATTEMPTS; i++) {
            // If a server is unresponsive attempt to ping for cluster_status_query_attempts
            if(connectivityStatusMap.containsValue(ConnectivityStatus.UNRESPONSIVE)) {
                // Ping only unresponsive endpoints
                List<String> endpoints = connectivityStatusMap.entrySet()
                        .stream()
                        .filter(entry -> entry.getValue().equals(ClusterStatusReport.ConnectivityStatus.UNRESPONSIVE))
                        .map(Entry::getKey)
                        .collect(Collectors.toList());

                for (String serverEndpoint : endpoints) {
                    // Ping all nodes asynchronously
                    CompletableFuture<Boolean> cf = runtimeLayout.getBaseClient(serverEndpoint).ping();
                    pingFutureMap.put(serverEndpoint, cf);
                }

                // Accumulate all responses.
                pingFutureMap.forEach((endpoint, pingFuture) -> {
                    try {
                        ClusterStatusReport.ConnectivityStatus connectivityStatus = CFUtils.getUninterruptibly(pingFuture,
                                WrongEpochException.class) ? ConnectivityStatus.RESPONSIVE : ConnectivityStatus.UNRESPONSIVE;
                        connectivityStatusMap.put(endpoint, connectivityStatus);
                    } catch (WrongEpochException wee) {
                        connectivityStatusMap.put(endpoint, ConnectivityStatus.RESPONSIVE);
                    } catch (Exception e) {
                        connectivityStatusMap.put(endpoint, ConnectivityStatus.UNRESPONSIVE);
                    }
                });
            }
        }

        return connectivityStatusMap;
    }

    private Map<String, NodeStatus> getNodeStatusMap(Layout layout) {
        Map<String, NodeStatus> nodeStatusMap = new HashMap<>();
        for (String endpoint: layout.getAllServers()) {
            if (layout.getUnresponsiveServers().contains(endpoint)) {
                nodeStatusMap.put(endpoint, NodeStatus.DOWN);
            } else if (layout.getSegments().size() != layout.getSegmentsForEndpoint(endpoint).size()) {
                // Note: this is based on the assumption that all nodes in the layout are log unit servers
                // (TODO) We can go ahead with this assumption, but in the future we should change this accordingly.
                nodeStatusMap.put(endpoint, NodeStatus.DB_SYNCING);
            } else {
                nodeStatusMap.put(endpoint, NodeStatus.UP);
            }
        }

        return nodeStatusMap;
    }

    private List<String> getLayoutServers(Collection<Layout> layouts) {
        List<String> allLayoutServers = new ArrayList<>();
        for (Layout layout : layouts) {
            allLayoutServers.addAll(layout.getLayoutServers());
        }

        return allLayoutServers.stream().distinct().collect(Collectors.toList());
    }

    /**
     * Bootstraps the management server if not already bootstrapped.
     * If already bootstrapped, it completes silently.
     *
     * @param endpoint Endpoint to bootstrap.
     * @param layout   Layout to bootstrap with.
     * @return Completable Future which completes with True when the management server is bootstrapped.
     */
    public CompletableFuture<Boolean> bootstrapManagementServer(@Nonnull String endpoint, @Nonnull Layout layout) {
        return runtime.getLayoutView().getRuntimeLayout(layout)
                .getManagementClient(endpoint)
                .bootstrapManagement(layout)
                .exceptionally(throwable -> {
                    try {
                        CFUtils.unwrap(throwable, AlreadyBootstrappedException.class);
                    } catch (AlreadyBootstrappedException e) {
                        log.info("bootstrapManagementServer: Management Server {} already bootstrapped.", endpoint);
                    }
                    return true;
                })
                .thenApply(result -> {
                    log.info("bootstrapManagementServer: Management Server {} bootstrap successful.", endpoint);
                    return true;
                });
    }
}
