package org.corfudb.runtime.view;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.protocols.wireprotocol.NetworkMetrics;
import org.corfudb.protocols.wireprotocol.NodeView;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.IClientRouter;
import org.corfudb.runtime.clients.LayoutClient;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.exceptions.WorkflowException;
import org.corfudb.runtime.exceptions.WorkflowResultUnknownException;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.runtime.view.ClusterStatusReport.ClusterStatus;
import org.corfudb.runtime.view.ClusterStatusReport.NodeStatus;
import org.corfudb.runtime.view.Layout.LayoutSegment;
import org.corfudb.runtime.view.workflows.AddNode;
import org.corfudb.runtime.view.workflows.ForceRemoveNode;
import org.corfudb.runtime.view.workflows.HealNode;
import org.corfudb.runtime.view.workflows.RemoveNode;
import org.corfudb.util.CFUtils;
import org.corfudb.util.NodeLocator;

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
     * If all the layout servers are responsive the cluster status is STABLE,
     * if a minority of them are unresponsive then the status is DEGRADED,
     * else the cluster is UNAVAILABLE.
     *
     * @param layout              Current layout on which responsiveness was checked.
     * @param peerResponsiveNodes responsive nodes in the current layout.
     * @return ClusterStatus
     */
    private ClusterStatus getLayoutServersClusterHealth(Layout layout, Set<NodeLocator> peerResponsiveNodes) {
        ClusterStatus clusterStatus = ClusterStatus.STABLE;
        // A quorum of layout servers need to be responsive for the cluster to be STABLE.
        List<NodeLocator> responsiveLayoutServers = new ArrayList<>(layout.getLayoutServersNodes());
        // Retain only the responsive servers.
        responsiveLayoutServers.retainAll(peerResponsiveNodes);
        if (responsiveLayoutServers.size() != layout.getLayoutServersNodes().size()) {
            clusterStatus = ClusterStatus.DEGRADED;
            int quorumSize = (layout.getLayoutServersNodes().size() / 2) + 1;
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
    private ClusterStatus getSequencerServersClusterHealth(Layout layout, Set<NodeLocator> peerResponsiveNodes) {
        // The primary sequencer should be reachable for the cluster to be STABLE.
        return !peerResponsiveNodes.contains(layout.getPrimarySequencerNode())
                ? ClusterStatus.UNAVAILABLE : ClusterStatus.STABLE;
    }

    /**
     * Gets the log unit cluster status based on the replication protocol.
     *
     * @param layout              Current layout on which responsiveness was checked.
     * @param peerResponsiveNodes responsive nodes in the current layout.
     * @return ClusterStatus
     */
    private ClusterStatus getLogUnitServersClusterHealth(Layout layout, Set<NodeLocator> peerResponsiveNodes) {
        // logUnitRedundancyStatus marks the cluster as degraded if any of the nodes is performing
        // stateTransfer and is in process of achieving full redundancy.
        ClusterStatus logUnitRedundancyStatus = peerResponsiveNodes.stream()
                .anyMatch(s -> getLogUnitNodeStatusInLayout(layout, s) == NodeStatus.DB_SYNCING)
                ? ClusterStatus.DEGRADED : ClusterStatus.STABLE;
        // Check the availability of the log servers in all segments as reads to all addresses
        // should be accessible.
        ClusterStatus logunitClusterStatus = layout.getSegments().stream()
                .map(segment -> segment.getReplicationMode()
                        .getClusterHealthForSegment(segment, peerResponsiveNodes))
                .max(Comparator.comparingInt(ClusterStatus::getHealthValue))
                .get();
        // Gets max of cluster status and logUnitRedundancyStatus.
        return Stream.of(logunitClusterStatus, logUnitRedundancyStatus)
                .max(Comparator.comparingInt(ClusterStatus::getHealthValue)).get();
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
    private ClusterStatus getClusterHealth(Layout layout, Set<NodeLocator> peerResponsiveNodes) {

        return Stream.of(getLayoutServersClusterHealth(layout, peerResponsiveNodes),
                getSequencerServersClusterHealth(layout, peerResponsiveNodes),
                getLogUnitServersClusterHealth(layout, peerResponsiveNodes))
                // Gets cluster status from the layout, sequencer and log unit clusters.
                // The status is then aggregated by the max of the 3 statuses acquired.
                .max(Comparator.comparingInt(ClusterStatus::getHealthValue))
                .get();
    }

    /**
     * Prunes out the unresponsive nodes as seen by the Management Agents on the Corfu nodes.
     *
     * @param layout      Layout based on which the health is analyzed.
     * @param nodeViewMap Map of nodeViews from the ManagementAgents.
     * @return Set of responsive servers as seen by the Corfu cluster nodes.
     */
    private Set<NodeLocator> filterResponsiveNodes(Layout layout, Map<NodeLocator, NodeView> nodeViewMap) {

        // Using the peer views from the nodes to determine health of the cluster.
        Set<NodeLocator> peerResponsiveNodes = new HashSet<>(layout.getAllServersNodes());
        nodeViewMap.values().stream()
                .map(NodeView::getNetworkMetrics)
                // Get all peer connectivity maps from all node views.
                .map(NetworkMetrics::getPeerConnectivityView)
                .flatMap(stringBooleanMap -> stringBooleanMap.entrySet().stream())
                // Filter out all unresponsive nodes from all the fault detectors.
                .filter(entry -> !entry.getValue())
                .map(Entry::getKey)
                // Remove the unresponsive nodes from the set of all nodes.
                .forEach(endpoint -> peerResponsiveNodes.remove(NodeLocator.parseString(endpoint)));
        layout.getUnresponsiveServersNodes().forEach(peerResponsiveNodes::remove);
        return peerResponsiveNodes;
    }

    /**
     * Attempts to fetch the layout with the highest epoch.
     * This does not consume the corfu runtime layout as that can get stuck in an infinite retry
     * loop.
     *
     * @param layoutServers Layout Servers to fetch the layout from.
     * @return Latest layout from the servers or null if none of them responded with a layout.
     */
    private Layout getHighestEpochLayout(List<NodeLocator> layoutServers) {
        Layout layout = null;
        Map<NodeLocator, CompletableFuture<Layout>> layoutFuturesMap = new HashMap<>();
        // Get layout futures for layout requests to all layout servers.
        for (NodeLocator server : layoutServers) {
            try {
                // Router creation can throw a NetworkException.
                IClientRouter router = runtime.getRouter(server);
                layoutFuturesMap.put(server,
                        new LayoutClient(router, Layout.INVALID_EPOCH).getLayout());
            } catch (NetworkException e) {
                log.error("getClusterStatus: Exception encountered connecting to {}. ",
                        server, e);
                CompletableFuture<Layout> cf = new CompletableFuture<>();
                cf.completeExceptionally(e);
                layoutFuturesMap.put(server, cf);
            }
        }

        // Wait on the Completable futures and retain the layout with the highest epoch.
        for (Entry<NodeLocator, CompletableFuture<Layout>> entry : layoutFuturesMap.entrySet()) {
            try {
                Layout nodeLayout = entry.getValue().get();
                log.debug("Server:{} responded with: {}", entry.getKey(), nodeLayout);

                if (layout == null || nodeLayout.getEpoch() > layout.getEpoch()) {
                    layout = nodeLayout;
                }
            } catch (InterruptedException ie) {
                throw new UnrecoverableCorfuInterruptedError(ie);
            } catch (ExecutionException ee) {
                log.error("getClusterStatus: Error while fetching layout from {}.",
                        entry.getKey(), ee);
            }
        }
        log.info("getHighestEpochLayout: Layout for cluster status query: {}", layout);
        return layout;
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
     * @return NodeStatus with respect to the layout specified.
     */
    private NodeStatus getLogUnitNodeStatusInLayout(Layout layout, NodeLocator server) {
        if (layout.getUnresponsiveServersNodes().contains(server)) {
            return NodeStatus.DOWN;
        }
        final int segmentsCount = layout.getSegments().size();
        int nodeInSegments = 0;
        for (LayoutSegment layoutSegment : layout.getSegments()) {
            if (layoutSegment.getAllLogServersNodes().contains(server)) {
                nodeInSegments++;
            }
        }
        return nodeInSegments == segmentsCount || nodeInSegments == 0
                ? NodeStatus.UP : NodeStatus.DB_SYNCING;
    }

    /**
     * Create node status map.
     * Assigns a node status to all the nodes in the layout.
     * UP: If the node is responsive and present in all the segments OR present in none (for cases
     * where a node is a Layout or Sequencer Server only and does not possess a LogUnit component.)
     * DB_SYNCING: If the node is present in a few segments but not all. This node is syncing data.
     * DOWN: If the node is unresponsive.
     *
     * @param layout            Layout used to compute cluster status
     * @param responsiveServers Set of responsive servers excluding unresponsive Servers list.
     * @param allServers        All servers in the layout.
     * @return Map of nodes mapped to their status.
     */
    private Map<NodeLocator, NodeStatus> createNodeStatusMap(Layout layout,
                                                        Set<NodeLocator> responsiveServers,
                                                        Set<NodeLocator> allServers) {
        Map<NodeLocator, NodeStatus> nodeStatusMap = new HashMap<>();
        for (NodeLocator server : allServers) {
            nodeStatusMap.put(server, NodeStatus.DOWN);
            if (responsiveServers.contains(server)) {
                nodeStatusMap.put(server, getLogUnitNodeStatusInLayout(layout, server));
            }
        }
        return nodeStatusMap;
    }

    /**
     * Gets the cluster status by pinging each node.
     * These pings are then compared to the layout to decide whether the cluster is:
     * STABLE, DEGRADED OR UNAVAILABLE.
     * The report consists of the following:
     * Layout - at which the report was generated.
     * ClientServerConnectivityStatusMap - the connectivity status of this client to the cluster.
     * ClusterStatus - Health of the Corfu cluster.
     *
     * @return ClusterStatusReport
     */
    public ClusterStatusReport getClusterStatus() {

        List<NodeLocator> layoutServers = new ArrayList<>(runtime.getLayoutServersNodes());
        Layout layout = getHighestEpochLayout(layoutServers);

        // If layout is null, none of the existing layout servers have a layout.
        if (layout == null) {
            return new ClusterStatusReport(
                    null,
                    layoutServers.stream().collect(Collectors.toMap(NodeLocator::toEndpointUrl, node -> NodeStatus.DOWN)),
                    ClusterStatus.UNAVAILABLE
            );
        }

        RuntimeLayout runtimeLayout = new RuntimeLayout(layout, runtime);

        // All configured nodes.
        Set<NodeLocator> allNodes = new HashSet<>(layout.getAllServersNodes());

        // Counters to track heartbeat responses from nodes.
        // A counter is incremented even if we get a WrongEpochException as the node is alive.
        Map<NodeLocator, Integer> counters = new HashMap<>();
        allNodes.forEach(endpoint -> counters.put(endpoint, 0));

        // Map of nodeView received from heartbeatResponses.
        Map<NodeLocator, NodeView> nodeViewMap = new HashMap<>();

        // Ping all nodes CLUSTER_STATUS_QUERY_ATTEMPTS times.
        // Increment the counter map and save the NodeView response received in the
        // heartbeat responses.
        for (int i = 0; i < CLUSTER_STATUS_QUERY_ATTEMPTS; i++) {
            Map<NodeLocator, CompletableFuture<NodeView>> futureMap = new HashMap<>();

            // Ping all nodes asynchronously
            for (NodeLocator endpoint : allNodes) {
                CompletableFuture<NodeView> cf = new CompletableFuture<>();
                try {
                    cf = runtimeLayout.getManagementClient(endpoint).sendHeartbeatRequest();
                } catch (Exception e) {
                    // Requesting the heartbeat can cause NetworkException if connection cannot
                    // be established.
                    cf.completeExceptionally(e);
                }
                futureMap.put(endpoint, cf);
            }

            // Accumulate all responses.
            for (NodeLocator endpoint : futureMap.keySet()) {
                try {
                    nodeViewMap.put(endpoint, CFUtils.getUninterruptibly(futureMap.get(endpoint),
                            WrongEpochException.class));
                    counters.computeIfPresent(endpoint, (s, count) -> count + 1);
                } catch (WrongEpochException wee) {
                    counters.computeIfPresent(endpoint, (s, count) -> count + 1);
                } catch (Exception ignored) {
                    // Ignore all exceptions.
                }
            }
        }

        // Using the peer views from the nodes to determine health of the cluster.
        Set<NodeLocator> peerResponsiveNodes = filterResponsiveNodes(layout, nodeViewMap);

        // Analyzes the responsive nodes as seen by the fault detectors to determine the health
        // of the cluster.
        ClusterStatus clusterStatus = getClusterHealth(layout, peerResponsiveNodes);

        // Set of all nodes which are responsive to this client.
        // Client connectivity to the cluster.
        Set<NodeLocator> responsiveEndpoints = counters.entrySet().stream()
                // Only count nodes which have counter > 0
                .filter(entry -> entry.getValue() > 0)
                .map(Entry::getKey)
                .collect(Collectors.toSet());

        Map<NodeLocator, NodeStatus> nodeStatusMapNodes = createNodeStatusMap(layout, responsiveEndpoints, allNodes);

        Map<String, NodeStatus> nodeStatusMap = new HashMap<>();
        nodeStatusMapNodes.forEach((node, status) -> nodeStatusMap.put(node.toEndpointUrl(), status));

        return new ClusterStatusReport(layout, nodeStatusMap, clusterStatus);
    }
}
