package org.corfudb.infrastructure.management.failuredetector;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.management.failuredetector.LayoutRateLimit.LayoutRateLimitParams;
import org.corfudb.infrastructure.management.failuredetector.LayoutRateLimit.ProbeCalc;
import org.corfudb.infrastructure.management.failuredetector.LayoutRateLimit.ProbeStatus;
import org.corfudb.infrastructure.management.failuredetector.RemoteMonitoringService.DetectorTask;
import org.corfudb.infrastructure.management.ClusterAdvisor;
import org.corfudb.infrastructure.management.FileSystemAdvisor;
import org.corfudb.infrastructure.management.PollReport;
import org.corfudb.infrastructure.management.ReconfigurationEventHandler;
import org.corfudb.infrastructure.redundancy.RedundancyCalculator;
import org.corfudb.protocols.wireprotocol.failuredetector.FailureDetectorMetrics;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeRank;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeRank.NodeRankByPartitionAttributes;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.concurrent.SingletonResource;

import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

import static org.corfudb.infrastructure.management.failuredetector.RemoteMonitoringService.DetectorTask.SKIPPED;

/**
 * Healing algorithm
 */
@Slf4j
@Builder
public class HealingAgent {
    /**
     * Duration in which the restore redundancy and merge segments workflow status is queried.
     */
    @Builder.Default
    private final Duration mergeSegmentsRetryQueryTimeout = Duration.ofSeconds(1);

    @NonNull
    private final ClusterAdvisor advisor;

    @NonNull
    private final FileSystemAdvisor fsAdvisor;
    /**
     * Future which is reset every time a new task to mergeSegments is launched.
     * This is to avoid multiple mergeSegments requests.
     */
    @Builder.Default
    private volatile CompletableFuture<Boolean> mergeSegmentsTask = CompletableFuture.completedFuture(true);

    private final CompletableFuture<DetectorTask> skippedTask = CompletableFuture.completedFuture(SKIPPED);

    @NonNull
    private final SingletonResource<CorfuRuntime> runtimeSingleton;

    @NonNull
    private final ExecutorService failureDetectorWorker;

    @NonNull
    private final FailureDetectorDataStore dataStore;

    /**
     * Handle healed node.
     * Cluster advisor provides healed node based on current cluster state if healed node found then
     * save healed node in the history and send a message with the detected healed node
     * to the relevant management server.
     *
     * @param pollReport poll report
     * @param layout     current layout
     */
    public CompletableFuture<DetectorTask> detectAndHandleHealing(PollReport pollReport, Layout layout, String localEndpoint, LayoutRateLimitParams layoutRateLimitParams) {
        log.trace("Handle healing, layout: {}", layout);

        Optional<NodeRankByPartitionAttributes> fsHealth = fsAdvisor.healedServer(pollReport.getClusterState());
        if (!fsHealth.isPresent()) {
            log.trace("Unhealthy node. Read only partition");
            return skippedTask;
        }

        return advisor
                .healedServer(pollReport.getClusterState(), localEndpoint)
                .map(healedNode -> {
                    Set<String> healedNodes = ImmutableSet.of(healedNode.getEndpoint());

                    ProbeCalc probeCalc = ProbeCalc.builder()
                            .localEndpoint(localEndpoint)
                            .layoutRateLimitParams(layoutRateLimitParams)
                            .build();
                    probeCalc.updateFromLayout(layout);

                    ProbeStatus status = probeCalc.calcStatsForNewUpdate();
                    if (!status.isAllowed()) {
                        log.warn("Healing disabled due to timeout: probCalc {}, status {}", probeCalc, status);
                        return skippedTask;
                    }

                    return handleHealing(pollReport, layout, healedNodes, localEndpoint)
                            .thenApply(healingResult -> {
                                saveFailureDetectorStats(pollReport, layout, healedNode, localEndpoint);
                                return DetectorTask.COMPLETED;
                            })
                            .exceptionally(ex -> {
                                log.error("Healing local node failed: {}", ex.getMessage());
                                return DetectorTask.NOT_COMPLETED;
                            });
                })
                .orElseGet(() -> {
                    log.trace("Nothing to heal");
                    return skippedTask;
                });
    }

    @VisibleForTesting
    CompletableFuture<Boolean> handleHealing(PollReport pollReport, Layout layout, Set<String> healedNodes, String localEndpoint) {
        return runtimeSingleton.get()
                .getLayoutView()
                .getRuntimeLayout(layout)
                .getManagementClient(localEndpoint)
                //handle healing asynchronously
                .handleHealing(pollReport.getPollEpoch(), healedNodes);
    }

    private void saveFailureDetectorStats(PollReport pollReport, Layout layout, NodeRank healed, String localEndpoint) {
        //save history
        FailureDetectorMetrics history = FailureDetectorMetrics.builder()
                .localNode(localEndpoint)
                .graph(advisor.getGraph(pollReport.getClusterState()).connectivityGraph())
                .healed(healed)
                .action(FailureDetectorMetrics.FailureDetectorAction.HEAL)
                .unresponsiveNodes(layout.getUnresponsiveServers())
                .layout(layout.getLayoutServers())
                .epoch(layout.getEpoch())
                .build();

        log.info("Healing local node successful: {}", history.toJson());

        dataStore.saveFailureDetectorMetrics(history);
    }

    /**
     * Spawns a new asynchronous task to restore redundancy and merge segments.
     * A new task is not spawned if a task is already in progress.
     * This method does not wait on the completion of the restore redundancy and merge segments task.
     */
    public void restoreRedundancyAndMergeSegments(Layout layout, String localEndpoint) {
        // Check that the task is not currently running.
        if (!mergeSegmentsTask.isDone()) {
            log.trace("Merge segments task already in progress. Skipping spawning another task.");
            return;
        }

        // Check that the current node can invoke a restoration action,
        // also verify that the current node is healed (not present in the unresponsive list).
        if (RedundancyCalculator.canRestoreRedundancyOrMergeSegments(layout, localEndpoint) &&
                !layout.getUnresponsiveServers().contains(localEndpoint)) {
            log.info("Layout requires restoration: {}. Spawning task to merge segments on {}.",
                    layout, localEndpoint);
            Supplier<Boolean> redundancyAction = () -> handleMergeSegments(localEndpoint, layout);
            mergeSegmentsTask = CompletableFuture.supplyAsync(redundancyAction, failureDetectorWorker);

            return;
        }

        log.trace("No segments to merge. Skipping step.");
    }

    @VisibleForTesting
    boolean handleMergeSegments(String localEndpoint, Layout layout) {
        return ReconfigurationEventHandler
                .handleMergeSegments(localEndpoint, runtimeSingleton.get(), layout, mergeSegmentsRetryQueryTimeout);
    }
}
