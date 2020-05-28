package org.corfudb.infrastructure.management;

import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.management.failuredetector.ClusterGraph;
import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.protocols.wireprotocol.failuredetector.FailureDetectorMetrics;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeRank;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.concurrent.SingletonResource;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

@Builder
@Slf4j
public class FailureHandler {
    @NonNull
    private final ServerContext serverContext;
    @NonNull
    private final SingletonResource<CorfuRuntime> runtimeSingletonResource;
    @NonNull
    private final ClusterAdvisor advisor;

    /**
     * Analyzes the poll report and triggers the failure handler if node failure detected.
     * ClusterAdvisor provides a failed node in the cluster.
     * If a failed node have found:
     * - save detected failure in the history
     * - handle failure
     *
     * @param pollReport Poll report obtained from failure detection policy.
     * @return boolean result if failure was handled. False if there is no failure
     */
    public RemoteMonitoringService.DetectorTask detectFailure(PollReport pollReport, Layout layout) {
        log.trace("Handle failures for the report: {}", pollReport);

        try {
            ClusterState clusterState = pollReport.getClusterState();

            if (clusterState.size() != layout.getAllServers().size()) {
                String err = String.format(
                        "Cluster representation is different than layout. Cluster: %s, layout: %s",
                        clusterState, layout
                );
                throw new IllegalStateException(err);
            }

            Optional<NodeRank> maybeFailedNode = advisor.failedServer(clusterState);

            if (maybeFailedNode.isPresent()) {
                NodeRank failedNode = maybeFailedNode.get();

                //Collect failures history
                FailureDetectorMetrics history = FailureDetectorMetrics.builder()
                        .localNode(serverContext.getLocalEndpoint())
                        .graph(advisor.getGraph(pollReport.getClusterState()).connectivityGraph())
                        .healed(failedNode)
                        .action(FailureDetectorMetrics.FailureDetectorAction.FAIL)
                        .unresponsiveNodes(layout.getUnresponsiveServers())
                        .layout(layout.getLayoutServers())
                        .epoch(layout.getEpoch())
                        .build();

                serverContext.saveFailureDetectorMetrics(history);

                Set<String> failedNodes = new HashSet<>();
                failedNodes.add(failedNode.getEndpoint());
                return detectFailure(layout, failedNodes, pollReport).get();
            }
        } catch (Exception e) {
            log.error("Exception invoking failure handler", e);
        }

        return RemoteMonitoringService.DetectorTask.NOT_COMPLETED;
    }

    /**
     * Handle failures, sending message with detected failure to relevant management server.
     *
     * @param failedNodes list of failed nodes
     * @param pollReport  poll report
     */
    public CompletableFuture<RemoteMonitoringService.DetectorTask> detectFailure(
            Layout layout, Set<String> failedNodes, PollReport pollReport) {

        ClusterGraph graph = advisor.getGraph(pollReport.getClusterState());

        log.info("Detected failed nodes in node responsiveness: Failed:{}, is slot unfilled: {}, clusterState:{}",
                failedNodes, pollReport.getLayoutSlotUnFilled(layout), graph.toJson()
        );

        return getCorfuRuntime()
                .getLayoutView()
                .getRuntimeLayout(layout)
                .getManagementClient(serverContext.getLocalEndpoint())
                .handleFailure(layout.getEpoch(), failedNodes)
                .thenApply(RemoteMonitoringService.DetectorTask::fromBool);
    }

    private CorfuRuntime getCorfuRuntime() {
        return runtimeSingletonResource.get();
    }
}
