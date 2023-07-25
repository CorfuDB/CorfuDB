package org.corfudb.infrastructure.management;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.micrometer.core.instrument.Timer;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MicroMeterUtils;
import org.corfudb.common.util.Tuple;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.protocols.wireprotocol.NodeState;
import org.corfudb.protocols.wireprotocol.SequencerMetrics;
import org.corfudb.protocols.wireprotocol.failuredetector.FileSystemStats;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.IClientRouter;
import org.corfudb.runtime.clients.ManagementClient;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.CFUtils;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * FailureDetector polls all the "responsive members" in the layout.
 * Responsive members: All endpoints that are responding to heartbeats. This list can be derived by
 * excluding unresponsiveServers from all endpoints.
 * For every poll method call invoked, it starts a polling round and generates a poll report.
 * Each polling round comprises "failureThreshold" number of iterations.
 * - We asynchronously poll every known responsive member in the layout.
 * - Poll result aggregation.
 * - If we complete an iteration without detecting failures, we end the round successfully.
 * The management Server ensures only one instance of this class and hence this is NOT thread safe.
 */
@Slf4j
public class FailureDetector implements IDetector {

    /**
     * Max number of iterations to execute to detect a failure in a round.
     */
    @Getter
    private static final int MAX_POLL_ROUNDS = 3;

    @Getter
    private static final Duration MAX_SLEEP_BETWEEN_RETRIES = Duration.ofSeconds(2);

    @NonNull
    private ServerContext serverContext;

    @Setter
    private PollConfig pollConfig = new PollConfig(MAX_POLL_ROUNDS, MAX_SLEEP_BETWEEN_RETRIES);

    public FailureDetector(ServerContext serverContext) {
        this.serverContext = serverContext;
    }

    private Map<String, IClientRouter> adjustRouters(CorfuRuntime corfuRuntime, Set<String> allServers) {
        // Set up arrays for routers to the endpoints.
        Map<String, IClientRouter> routers = new HashMap<>();
        allServers.forEach(server -> {
            IClientRouter router = corfuRuntime.getRouter(server);
            routers.put(server, router);
        });
        return routers;
    }

    /**
     * Executes the policy once.
     * Checks for changes in the layout.
     * Then polls all the servers generates pollReport.
     *
     * @param layout Current Layout
     */
    public PollReport poll(
            @Nonnull Layout layout, @Nonnull CorfuRuntime corfuRuntime, @NonNull SequencerMetrics sequencerMetrics,
            FileSystemStats fileSystemStats) {

        log.trace("Poll report. Layout: {}", layout);

        // Collect and set all responsive servers in the members array.
        Set<String> allServers = layout.getAllServers();

        Map<String, IClientRouter> routers = adjustRouters(corfuRuntime, allServers);

        // Perform polling of all responsive servers.
        return pollRound(
                layout.getEpoch(), layout.getClusterId(), allServers, routers, sequencerMetrics,
                ImmutableList.copyOf(layout.getUnresponsiveServers()),
                fileSystemStats);
    }

    private PollReport aggregatePollReports(long epoch, List<PollReport> reports,
                                            ImmutableList<String> layoutUnresponsiveNodes) {
        Map<String, Long> wrongEpochsAggregated = new HashMap<>();

        reports.forEach(report -> {
            //Calculate wrong epochs
            wrongEpochsAggregated.putAll(report.getWrongEpochs());
            report.getReachableNodes().forEach(wrongEpochsAggregated::remove);
        });

        List<ClusterState> clusterStates = reports.stream()
                .map(PollReport::getClusterState)
                .collect(Collectors.toList());

        ClusterStateAggregator aggregator = ClusterStateAggregator.builder()
                .localEndpoint(serverContext.getLocalEndpoint())
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

    static Map<String, Long> getAdjustedResponseTimeouts(Map<String, IClientRouter> routers, long maxSleepBetweenPolls) {
        return routers.entrySet().stream().map(entry -> {
            final IClientRouter router = entry.getValue();
            long currentConnectionTimeout = router.getTimeoutConnect();
            Preconditions.checkArgument(maxSleepBetweenPolls > currentConnectionTimeout,
                    "Max sleep between polls: " + maxSleepBetweenPolls +
                            " should be greater than connection timeout: " + currentConnectionTimeout);
            long currentResponseTimeout = router.getTimeoutResponse();
            long overallRouterTimeout = currentConnectionTimeout + currentResponseTimeout;
            long proposedResponseTimeout = currentResponseTimeout;
            if (maxSleepBetweenPolls < overallRouterTimeout) {
                proposedResponseTimeout = maxSleepBetweenPolls - currentConnectionTimeout;
            }
            final String endpoint = entry.getKey();
            log.trace("For {}: connection timeout - {}, response timeout - {}, sleep between polls - {}", endpoint,
                    currentConnectionTimeout, proposedResponseTimeout, maxSleepBetweenPolls);
            return Tuple.of(endpoint, proposedResponseTimeout);
        }).collect(Collectors.toMap(tuple -> tuple.first, tuple -> tuple.second));
    }


    PollReport pollRound(long epoch, UUID clusterID, Set<String> allServers, Map<String, IClientRouter> router,
                         SequencerMetrics sequencerMetrics, ImmutableList<String> layoutUnresponsiveNodes,
                         FileSystemStats fileSystemStats) {
        Map<String, Long> timeouts = getAdjustedResponseTimeouts(router, pollConfig.sleepBetweenPolls.toMillis());
        for (String server : allServers) {
            final Long adjustedTimeout = timeouts.get(server);
            router.get(server).setTimeoutResponse(adjustedTimeout);
        }
        List<PollReport> reports = new ArrayList<>();
        for (int i = 0; i < pollConfig.pollRounds; i++) {
            try {
                PollReport currReport = pollIteration(
                        allServers, router, epoch, clusterID, sequencerMetrics, layoutUnresponsiveNodes, fileSystemStats
                );
                reports.add(currReport);

                if (!currReport.getClusterState().isReady()) {
                    log.trace("Cluster state is not ready. Skipping iterations.");
                    throw new IllegalStateException();
                }
                long pollIterationTook = currReport.getElapsedTime().toMillis();
                log.debug("Poll iteration {} took {} millis.", i, pollIterationTook);

                final long waitBetweenRetries =
                        Math.max(pollConfig.sleepBetweenPolls.toMillis() - pollIterationTook, 0);

                TimeUnit.MILLISECONDS.sleep(waitBetweenRetries);

            } catch (InterruptedException ie) {
                log.error("Interrupted exception occurred.");
                throw new UnrecoverableCorfuInterruptedError(ie);
            } catch (IllegalStateException is) {
                log.warn("Iterations skipped due to incomplete cluster state. " +
                        "Returning last not ready report.");
                return Iterables.getLast(reports);
            }

        }
        return aggregatePollReports(epoch, reports, layoutUnresponsiveNodes);
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
            SequencerMetrics sequencerMetrics, ImmutableList<String> layoutUnresponsiveNodes,
            FileSystemStats fileSystemStats) {

        log.trace("Poll iteration. Epoch: {}", epoch);

        long start = System.nanoTime();

        ClusterStateCollector clusterCollector = ClusterStateCollector.builder()
                .localEndpoint(serverContext.getLocalEndpoint())
                .clusterState(pollAsync(allServers, clientRouters, epoch, clusterID))
                .localNodeFileSystem(fileSystemStats)
                .build();

        //Cluster state internal map.
        ClusterState clusterState = clusterCollector
                .collectClusterState(layoutUnresponsiveNodes, sequencerMetrics);

        Duration elapsedTime = Duration.ofNanos(System.nanoTime() - start);

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
                Optional<Timer.Sample> sample = MicroMeterUtils.startTimer();
                CompletableFuture<NodeState> request = new ManagementClient(clientRouters.get(s), epoch, clusterId)
                        .sendNodeStateRequest();
                CompletableFuture<NodeState> nodeStateFuture = MicroMeterUtils.timeWhenCompletes(
                        request, sample, "failure-detector.ping-latency", "node", s
                );
                clusterState.put(s, nodeStateFuture);
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

    @AllArgsConstructor
    public static class PollConfig {
        private final int pollRounds;
        private final Duration sleepBetweenPolls;
    }

}
