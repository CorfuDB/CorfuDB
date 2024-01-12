package org.corfudb.infrastructure.management.failuredetector;

import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.RemoteMonitoringService.DetectorTask;
import org.corfudb.infrastructure.management.ClusterAdvisor;
import org.corfudb.infrastructure.management.FileSystemAdvisor;
import org.corfudb.infrastructure.management.PollReport;
import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.protocols.wireprotocol.failuredetector.FailureDetectorMetrics;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeRank;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeRank.NodeRankByPartitionAttributes;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.concurrent.SingletonResource;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

@Slf4j
@Builder
public class FailuresAgent {

    @NonNull
    private final ClusterAdvisor advisor;

    @NonNull
    private final FileSystemAdvisor fsAdvisor;

    @NonNull
    private final FailureDetectorDataStore fdDataStore;

    @NonNull
    private final SingletonResource<CorfuRuntime> runtimeSingleton;

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
    public DetectorTask detectAndHandleFailure(PollReport pollReport, Layout layout, String localEndpoint) {
        log.trace("Handle failures for the report: {}", pollReport);

        DetectorTask resultDetectorTask = DetectorTask.NOT_COMPLETED;

        try {
            ClusterState clusterState = pollReport.getClusterState();

            if (clusterState.size() != layout.getAllServers().size()) {
                throw FailureDetectorException.layoutMismatch(clusterState, layout);
            }

            DecisionMakerAgent decisionMakerAgent = new DecisionMakerAgent(clusterState, advisor);
            Optional<String> maybeDecisionMaker = decisionMakerAgent.findDecisionMaker();

            if (maybeDecisionMaker.isPresent()) {
                String decisionMaker = maybeDecisionMaker.get();

                Optional<NodeRankByPartitionAttributes> maybeFailedNodeByPartitionAttr = fsAdvisor
                        .findFailedNodeByPartitionAttributes(clusterState);

                if (maybeFailedNodeByPartitionAttr.isPresent()) {
                    NodeRankByPartitionAttributes failedNode = maybeFailedNodeByPartitionAttr.get();

                    if (decisionMaker.equals(failedNode.getEndpoint())) {
                        log.error("Decision maker and failed node are the same node: {}", decisionMakerAgent);
                    } else {
                        Set<String> failedNodes = new HashSet<>();
                        failedNodes.add(failedNode.getEndpoint());
                        resultDetectorTask = handleFailure(layout, failedNodes, pollReport, localEndpoint).join();
                    }
                } else {
                    Optional<NodeRank> maybeFailedNode = advisor.failedServer(clusterState);

                    if (maybeFailedNode.isPresent()) {
                        NodeRank failedNode = maybeFailedNode.get();

                        if (decisionMaker.equals(failedNode.getEndpoint())) {
                            log.error("Decision maker and failed node are the same node: {}", decisionMakerAgent);
                        } else {
                            //Collect failures history
                            FailureDetectorMetrics history = FailureDetectorMetrics.builder()
                                    .localNode(localEndpoint)
                                    .graph(advisor.getGraph(pollReport.getClusterState()).connectivityGraph())
                                    .healed(failedNode)
                                    .action(FailureDetectorMetrics.FailureDetectorAction.FAIL)
                                    .unresponsiveNodes(layout.getUnresponsiveServers())
                                    .layout(layout.getLayoutServers())
                                    .epoch(layout.getEpoch())
                                    .build();

                            fdDataStore.saveFailureDetectorMetrics(history);

                            Set<String> failedNodes = new HashSet<>();
                            failedNodes.add(failedNode.getEndpoint());
                            resultDetectorTask = handleFailure(layout, failedNodes, pollReport, localEndpoint).join();
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("Exception invoking failure handler", e);
        }

        return resultDetectorTask;
    }

    /**
     * Handle failures, sending message with detected failure to relevant management server.
     *
     * @param failedNodes list of failed nodes
     * @param pollReport  poll report
     */
    public CompletableFuture<DetectorTask> handleFailure(
            Layout layout, Set<String> failedNodes, PollReport pollReport, String localEndpoint) {

        ClusterGraph graph = advisor.getGraph(pollReport.getClusterState());

        log.info("Detected failed nodes in node responsiveness: Failed:{}, is slot unfilled: {}, clusterState:{}",
                failedNodes, pollReport.getLayoutSlotUnFilled(layout), graph.toJson()
        );

        return runtimeSingleton.get()
                .getLayoutView()
                .getRuntimeLayout(layout)
                .getManagementClient(localEndpoint)
                .handleFailure(layout.getEpoch(), failedNodes)
                .thenApply(DetectorTask::fromBool);
    }
}
