package org.corfudb.infrastructure.management;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.micrometer.core.instrument.Timer;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MicroMeterUtils;
import org.corfudb.infrastructure.RemoteMonitoringService;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.protocols.wireprotocol.NodeState;
import org.corfudb.protocols.wireprotocol.SequencerMetrics;
import org.corfudb.protocols.wireprotocol.failuredetector.FileSystemStats;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.IClientRouter;
import org.corfudb.runtime.clients.ManagementClient;
import org.corfudb.runtime.exceptions.RetryExhaustedException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.CFUtils;
import org.corfudb.util.Sleep;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.MixedBoundRetry;
import org.corfudb.util.retry.RetryNeededException;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
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
    /**
     * Max duration of the entire poll round
     */
    @Getter
    private static final Duration MAX_DETECTION_DURATION = Duration.ofSeconds(12);
    /**
     * Max sleep duration between poll round iterations.
     */
    @Getter
    private static final Duration MAX_SLEEP_BETWEEN_RETRIES = Duration.ofSeconds(5);
    /**
     * Initial sleep between poll round iterations.
     */
    @Getter
    private static final Duration INIT_SLEEP_BETWEEN_RETRIES = Duration.ofSeconds(1);

    @Getter
    private static final float JITTER_FACTOR = 0.2f;

    @Setter
    @Getter
    private Optional<PollConfig> pollConfig = Optional.empty();

    @NonNull
    private ServerContext serverContext;

    public FailureDetector(ServerContext serverContext) {
        this.serverContext = serverContext;
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
        return pollRoundExpBackoff(
                layout.getEpoch(), layout.getClusterId(), allServers, routers, sequencerMetrics,
                ImmutableList.copyOf(layout.getUnresponsiveServers()),
                fileSystemStats);
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

    PollReport pollRoundExpBackoff(long epoch, UUID clusterID, Set<String> allServers, Map<String, IClientRouter> router,
                                   SequencerMetrics sequencerMetrics, ImmutableList<String> layoutUnresponsiveNodes,
                                   FileSystemStats fileSystemStats) {

        final PollConfig pollConfig = this.pollConfig.orElseGet(() -> PollConfig.builder().build());
        Consumer<MixedBoundRetry> retrySettings = settings -> {
            settings.setOverallMaxRetryDuration(pollConfig.getMaxDetectionDuration());
            settings.setMaxRetryDuration(pollConfig.getMaxSleepBetweenRetries());
            settings.setBaseDuration(pollConfig.getInitSleepBetweenRetries());
            settings.setRandomPart(pollConfig.getJitterFactor());
        };
        List<PollReport> reports = new ArrayList<>();
        final long start = System.currentTimeMillis();
        try {
            IRetry.build(MixedBoundRetry.class, () -> {
                PollReport currReport = pollIteration(
                        allServers, router, epoch, clusterID, sequencerMetrics, layoutUnresponsiveNodes, fileSystemStats
                );
                reports.add(currReport);
                // Finish the poll round if we've reached the desired number of reports or when
                // the current cluster state is not ready.
                // Cluster state is not ready unless the overall cluster view is updated.
                // The resulting report will not be used in the failure detection anyway.
                // Cut this round short and continue on the next FD iteration.
                if (!currReport.getClusterState().isReady()) {
                    log.trace("Cluster state is not ready. Skipping iterations.");
                    throw new RetryExhaustedException();
                }
                if (reports.size() == pollConfig.getMaxPollRounds()) {
                    log.trace("Collected all {} reports.", reports.size());
                    throw new RetryExhaustedException();
                }
                // There are failed nodes. Increase the sleep interval.
                if (!currReport.getFailedNodes().isEmpty()) {
                    throw new MixedBoundRetry.BackoffRetryNeededException();
                } else {
                    throw new RetryNeededException();
                }
            }).setOptions(retrySettings).run();
        } catch (RetryExhaustedException re) {
            log.debug("Poll round finished. Took: {}ms", System.currentTimeMillis() - start);
        }
        catch (InterruptedException ie) {
            log.error("Interrupted exception occurred.");
            throw new UnrecoverableCorfuInterruptedError(ie);
        }

        //Aggregation step
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

        long start = System.currentTimeMillis();

        ClusterStateCollector clusterCollector = ClusterStateCollector.builder()
                .localEndpoint(serverContext.getLocalEndpoint())
                .clusterState(pollAsync(allServers, clientRouters, epoch, clusterID))
                .localNodeFileSystem(fileSystemStats)
                .build();

        //Cluster state internal map.
        ClusterState clusterState = clusterCollector
                .collectClusterState(layoutUnresponsiveNodes, sequencerMetrics);

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
    @Builder(toBuilder = true)
    @Getter
    public static class PollConfig {
        @Default
        private final int maxPollRounds = MAX_POLL_ROUNDS;
        @Default
        private final Duration maxDetectionDuration = MAX_DETECTION_DURATION;
        @Default
        private final Duration maxSleepBetweenRetries = MAX_SLEEP_BETWEEN_RETRIES;
        @Default
        private final Duration initSleepBetweenRetries = INIT_SLEEP_BETWEEN_RETRIES;
        @Default
        private final float jitterFactor = JITTER_FACTOR;
    }

}
