package org.corfudb.infrastructure.management;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.RemoteMonitoringService;
import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.protocols.wireprotocol.NodeState;
import org.corfudb.protocols.wireprotocol.SequencerMetrics;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.IClientRouter;
import org.corfudb.runtime.clients.ManagementClient;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.CFUtils;
import org.corfudb.util.Sleep;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * FailureDetector polls all the "responsive members" in the layout.
 * Responsive members: All endpoints that are responding to heartbeats. This list can be derived by
 * excluding unresponsiveServers from all endpoints.
 * For every poll method call invoked, it starts a polling round and generates a poll report.
 * Each polling round comprises of "failureThreshold" number of iterations.
 * - We asynchronously poll every known responsive member in the layout.
 * - Poll result aggregation.
 * - If we complete an iteration without detecting failures, we end the round successfully.
 * The management Server ensures only one instance of this class and hence this is NOT thread safe.
 * Created by zlokhandwala on 11/29/17.
 */
@Slf4j
public class FailureDetector implements IDetector {


    /**
     * Number of iterations to execute to detect a failure in a round.
     */
    @Getter
    @Setter
    private int failureThreshold = 3;

    @NonNull
    private final String localEndpoint;

    @NonNull
    @Default
    @Setter
    private NetworkStretcher networkStretcher = NetworkStretcher.builder().build();

    public FailureDetector(String localEndpoint) {
        this.localEndpoint = localEndpoint;
    }

    /**
     * Executes the policy once.
     * Checks for changes in the layout.
     * Then polls all the servers generates pollReport.
     *
     * @param layout Current Layout
     */
    public PollReport poll(
            @Nonnull Layout layout, @Nonnull CorfuRuntime corfuRuntime, @NonNull SequencerMetrics sequencerMetrics) {

        log.trace("Poll report. Layout: {}", layout);

        Map<String, IClientRouter> routerMap;

        // Collect and set all responsive servers in the members array.
        Set<String> allServers = layout.getAllServers();

        // Set up arrays for routers to the endpoints.
        routerMap = new HashMap<>();
        allServers.forEach(s -> {
            IClientRouter router = corfuRuntime.getRouter(s);
            router.setTimeoutResponse(networkStretcher.getCurrentPeriod().toMillis());
            routerMap.put(s, router);
        });

        // Perform polling of all responsive servers.
        return pollRound(
                layout.getEpoch(), layout.getClusterId(), allServers, routerMap, sequencerMetrics,
                ImmutableList.copyOf(layout.getUnresponsiveServers())
        );
    }

    /**
     * PollRound consists of iterations. In each iteration, the FailureDetector pings all the
     * responsive nodes in the layout and also collects their {@link NodeState}-s to provide {@link ClusterState}.
     * Failure detector collect a number of {@link PollReport}-s equals to failureThreshold number then
     * aggregates final {@link PollReport}.
     * Aggregation step:
     * - go through all reports
     * - aggregate all wrong epochs from all intermediate poll reports then remove all responsive nodes from the list.
     * If wrongEpochsAggregated variable still contains wrong epochs
     * then it will be corrected by {@link RemoteMonitoringService}
     * - Aggregate connected and failed nodes from all reports
     * - Timeouts tuning
     * - provide a poll report based on latest pollIteration report and aggregated values
     *
     * @return Poll Report with detected failed nodes and out of phase epoch nodes.
     */
    @VisibleForTesting
    PollReport pollRound(
            long epoch, UUID clusterID, Set<String> allServers, Map<String, IClientRouter> router,
            SequencerMetrics sequencerMetrics, ImmutableList<String> layoutUnresponsiveNodes) {

        if (failureThreshold < 1) {
            throw new IllegalStateException("Invalid failure threshold");
        }

        List<PollReport> reports = new ArrayList<>();
        for (int iteration = 0; iteration < failureThreshold; iteration++) {
            PollReport currReport = pollIteration(
                    allServers, router, epoch, clusterID, sequencerMetrics, layoutUnresponsiveNodes
            );
            reports.add(currReport);

            Duration restInterval = networkStretcher.getRestInterval(currReport.getElapsedTime());
            if (!currReport.getFailedNodes().isEmpty()) {
                networkStretcher.modifyIterationTimeouts();

                Set<String> allReachableNodes = currReport.getAllReachableNodes();
                tuneRoutersResponseTimeout(
                        router, allReachableNodes, networkStretcher.getCurrentPeriod()
                );
            }

            // Sleep for the provided poll interval before starting the next iteration
            Sleep.sleepUninterruptibly(restInterval);
        }

        //Aggregation step
        Map<String, Long> wrongEpochsAggregated = new HashMap<>();
        Set<String> connectedNodesAggregated = new HashSet<>();
        Set<String> failedNodesAggregated = new HashSet<>();

        reports.forEach(report -> {
            //Calculate wrong epochs
            wrongEpochsAggregated.putAll(report.getWrongEpochs());
            report.getReachableNodes().forEach(wrongEpochsAggregated::remove);

            //Aggregate failed/connected nodes
            connectedNodesAggregated.addAll(report.getReachableNodes());
            failedNodesAggregated.addAll(report.getFailedNodes());
        });

        failedNodesAggregated.removeAll(connectedNodesAggregated);

        Set<String> allConnectedNodes = Sets.union(connectedNodesAggregated, wrongEpochsAggregated.keySet());

        tunePollReportTimeouts(router, failedNodesAggregated, allConnectedNodes);

        List<ClusterState> clusterStates = reports.stream()
                .map(PollReport::getClusterState)
                .collect(Collectors.toList());

        ClusterStateAggregator aggregator = ClusterStateAggregator.builder()
                .localEndpoint(localEndpoint)
                .clusterStates(clusterStates)
                .unresponsiveNodes(layoutUnresponsiveNodes)
                .build();

        Duration totalElapsedTime = reports.stream()
                .map(PollReport::getElapsedTime)
                .reduce(Duration.ZERO, Duration::plus);

        final ClusterState aggregatedClusterState = aggregator.getAggregatedState();
        return PollReport.builder()
                .pollEpoch(epoch)
                .elapsedTime(totalElapsedTime)
                .pingResponsiveServers(aggregatedClusterState.getPingResponsiveNodes())
                .wrongEpochs(ImmutableMap.copyOf(wrongEpochsAggregated))
                .clusterState(aggregatedClusterState)
                .build();
    }

    /**
     * We can try to scale back the network latency time after every poll round.
     * If there are no failures, after a few rounds our polling period converges back to initPeriodDuration.
     *
     * @param clientRouters     clientRouters
     * @param failedNodes       list of failed nodes
     * @param allConnectedNodes all connected nodes
     */
    private void tunePollReportTimeouts(
            Map<String, IClientRouter> clientRouters, Set<String> failedNodes,
            Set<String> allConnectedNodes) {

        networkStretcher.modifyDecreasedPeriod();
        tuneRoutersResponseTimeout(
                clientRouters, allConnectedNodes, networkStretcher.getCurrentPeriod()
        );

        // Reset the timeout of all the failed nodes to the max value to set a longer
        // timeout period to detect their response.
        tuneRoutersResponseTimeout(clientRouters, failedNodes, networkStretcher.getMaxPeriod());
    }

    /**
     * Poll iteration step, provides a {@link PollReport} composed from pings and {@link NodeState}-s collected by
     * this node from the cluster.
     * Algorithm:
     * - ping all nodes
     * - collect all node states
     * - collect wrong epochs
     * - collect connected/failed nodes
     * - calculate if current layout slot is unfilled
     * - build poll report
     *
     * @param allServers              all servers in the cluster
     * @param clientRouters           client clientRouters
     * @param epoch                   current epoch
     * @param clusterID               current cluster id
     * @param sequencerMetrics        metrics
     * @param layoutUnresponsiveNodes all unresponsive servers in a cluster
     * @return a poll report
     */
    private PollReport pollIteration(
            Set<String> allServers, Map<String, IClientRouter> clientRouters, long epoch, UUID clusterID,
            SequencerMetrics sequencerMetrics, ImmutableList<String> layoutUnresponsiveNodes) {

        log.trace("Poll iteration. Epoch: {}", epoch);

        long start = System.currentTimeMillis();

        ClusterStateCollector clusterCollector = ClusterStateCollector.builder()
                .localEndpoint(localEndpoint)
                .clusterState(pollAsync(allServers, clientRouters, epoch, clusterID))
                .build();

        //Cluster state internal map.
        ClusterState clusterState = clusterCollector.collectClusterState(
                epoch, layoutUnresponsiveNodes, sequencerMetrics
        );

        Duration elapsedTime = Duration.ofMillis(System.currentTimeMillis() - start);

        return PollReport.builder()
                .pollEpoch(epoch)
                .pingResponsiveServers(clusterState.getPingResponsiveNodes())
                .wrongEpochs(clusterCollector.collectWrongEpochs())
                .clusterState(clusterState)
                .elapsedTime(elapsedTime)
                .build();
    }

    /**
     * Poll all members servers once asynchronously and store their futures in
     * pollCompletableFutures.
     *
     * @param allServers    All active members in the layout.
     * @param clientRouters Map of routers for all active members.
     * @param epoch         Current epoch for the polling round to stamp the ping messages.
     * @param clusterId     Current clusterId
     * @return Map of Completable futures for the pings.
     */
    private Map<String, CompletableFuture<NodeState>> pollAsync(
            Set<String> allServers, Map<String, IClientRouter> clientRouters, long epoch, UUID clusterId) {
        // Poll servers for health.  All ping activity will happen in the background.
        Map<String, CompletableFuture<NodeState>> clusterState = new HashMap<>();
        allServers.forEach(s -> {
            try {
                clusterState.put(s, new ManagementClient(clientRouters.get(s), epoch, clusterId)
                        .sendNodeStateRequest());
            } catch (Exception e) {
                CompletableFuture<NodeState> cf = new CompletableFuture<>();
                cf.completeExceptionally(e);
                clusterState.put(s, cf);
            }
        });

        //Ping all nodes in parallel.
        //Possible exceptions are held by their CompletableFutures. They will be handled in pollIteration method
        try {
            CFUtils.allOf(clusterState.values()).join();
        } catch (Exception ex) {
            //ignore
        }

        return clusterState;
    }

    /**
     * Set the timeoutResponse for all the routers connected to the given endpoints with the
     * given value.
     *
     * @param endpoints Router endpoints.
     * @param timeout   New timeout value.
     */
    private void tuneRoutersResponseTimeout(
            Map<String, IClientRouter> clientRouters, Set<String> endpoints, Duration timeout) {

        log.trace("Tuning router timeout responses for endpoints:{} to {}ms", endpoints, timeout);
        endpoints.forEach(server -> {
            if (clientRouters.get(server) != null) {
                clientRouters.get(server).setTimeoutResponse(timeout.toMillis());
            }
        });
    }

}
