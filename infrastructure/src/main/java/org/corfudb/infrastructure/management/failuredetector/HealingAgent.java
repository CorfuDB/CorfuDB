package org.corfudb.infrastructure.management.failuredetector;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.RemoteMonitoringService.DetectorTask;
import org.corfudb.infrastructure.health.Component;
import org.corfudb.infrastructure.health.HealthMonitor;
import org.corfudb.infrastructure.health.Issue;
import org.corfudb.infrastructure.health.Issue.IssueId;
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

import static org.corfudb.infrastructure.RemoteMonitoringService.DetectorTask.SKIPPED;

/**
 * Healing algorithm
 */
@Slf4j
@Builder
public class HealingAgent {
    /**
     * Identifies a failure of a recovery workflow that was reported by the orchestrator, so that it
     * can be resolved. The description is not a part of the issue's identity, it only documents why
     * the issue is resolved.
     */
    private static final Issue RECOVERY_WORKFLOW_FAILURE_RESOLVED = Issue.createIssue(
            Component.ORCHESTRATOR,
            IssueId.ORCHESTRATOR_RECOVERY_TASK_FAILED,
            "The cluster has recovered");

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
    public CompletableFuture<DetectorTask> detectAndHandleHealing(PollReport pollReport, Layout layout, String localEndpoint) {
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
     *
     * <p>If the cluster has recovered, a failure of a recovery workflow that was reported earlier
     * by the orchestrator is stale and is resolved. Otherwise it would remain in the health report
     * until some later workflow completes, which might never happen if the cluster recovered on
     * its own.
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

        if (isRecovered(layout)) {
            HealthMonitor.resolveIssue(RECOVERY_WORKFLOW_FAILURE_RESOLVED);
        }

        log.trace("No segments to merge. Skipping step.");
    }

    /**
     * The cluster has recovered if all the nodes are responsive and the segments of the layout are
     * merged into one, which is when a restore redundancy and merge segments request is complete.
     * The check is not limited to the current node, because the orchestrator that runs a recovery
     * workflow is not always the one that belongs to the node which is being recovered. For
     * example, a restore redundancy and merge segments workflow runs on another node.
     *
     * @param layout the latest layout
     * @return true if there is nothing left to heal, restore or merge in the cluster
     */
    private static boolean isRecovered(Layout layout) {
        return layout.getUnresponsiveServers().isEmpty() && layout.getSegments().size() == 1;
    }

    @VisibleForTesting
    boolean handleMergeSegments(String localEndpoint, Layout layout) {
        return ReconfigurationEventHandler
                .handleMergeSegments(localEndpoint, runtimeSingleton.get(), layout, mergeSegmentsRetryQueryTimeout);
    }
}
