package org.corfudb.infrastructure.management.failuredetector;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.corfudb.infrastructure.NodeNames;
import org.corfudb.infrastructure.RemoteMonitoringService.DetectorTask;
import org.corfudb.infrastructure.datastore.DataStore;
import org.corfudb.infrastructure.health.HealthMonitor;
import org.corfudb.infrastructure.health.Issue;
import org.corfudb.infrastructure.management.PollReport;
import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.protocols.wireprotocol.failuredetector.FileSystemStats;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.Layout.LayoutSegment;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.corfudb.infrastructure.health.Component.ORCHESTRATOR;
import static org.corfudb.infrastructure.health.Issue.IssueId.ORCHESTRATOR_RECOVERY_TASK_FAILED;
import static org.corfudb.infrastructure.health.Issue.IssueId.ORCHESTRATOR_TASK_FAILED;
import static org.corfudb.infrastructure.management.NodeStateTestUtil.nodeState;
import static org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity.ConnectionStatus.OK;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class HealingAgentTest {

    @Test
    void unsuccessfulHealingOfAFailedNode() {
        final String localEndpoint = NodeNames.C;

        FailureDetectorTestContext ctx = new FailureDetectorTestContext(localEndpoint);

        //an example how "setup" function can be used to customize FD test context
        ctx.setupDataStore(fsDs -> {
            DataStore dsMock = mock(DataStore.class);
            fsDs.dataStore(dsMock);
        });

        ctx.setupClusterState(clusterStateCtx -> {
            clusterStateCtx.unresponsiveNodes(ctx.localEndpoint);

            final long epoch = 0;
            FileSystemStats fsStats = clusterStateCtx.getFsStats();
            clusterStateCtx.setupNodes(
                    nodeState(NodeNames.A, epoch, Optional.of(fsStats), OK, OK, OK),
                    nodeState(NodeNames.B, epoch, Optional.of(fsStats), OK, OK, OK),
                    nodeState(localEndpoint, epoch, Optional.of(fsStats), OK, OK, OK)
            );
        });

        HealingAgent agentSpy = ctx.getHealingAgentSpy();
        ClusterState clusterState = ctx.getClusterState();
        PollReport pollReportMock = ctx.getPollReportMock(clusterState);
        Layout layoutMock = ctx.getLayoutMock();

        CompletableFuture<Boolean> handle = new CompletableFuture<>();
        handle.completeExceptionally(new FailureDetectorException("err"));

        doReturn(handle)
                .when(agentSpy)
                .handleHealing(same(pollReportMock), same(layoutMock), anySet(), same(localEndpoint));

        CompletableFuture<DetectorTask> healingFailed = agentSpy.detectAndHandleHealing(pollReportMock, layoutMock, localEndpoint);

        verify(agentSpy, times(1))
                .handleHealing(same(pollReportMock), same(layoutMock), anySet(), same(localEndpoint));

        assertEquals(DetectorTask.NOT_COMPLETED, healingFailed.join());
    }

    @Test
    void noNodesToHeal() {
        final String localEndpoint = NodeNames.C;

        FailureDetectorTestContext ctx = new FailureDetectorTestContext(localEndpoint);

        HealingAgent agentSpy = ctx.getHealingAgentSpy();
        ClusterState clusterState = ctx.getClusterState();
        PollReport pollReportMock = ctx.getPollReportMock(clusterState);
        Layout layoutMock = ctx.getLayoutMock();

        CompletableFuture<Boolean> handle = CompletableFuture.completedFuture(true);
        doReturn(handle)
                .when(agentSpy)
                .handleHealing(same(pollReportMock), same(layoutMock), anySet(), same(localEndpoint));

        CompletableFuture<DetectorTask> healingFailed = agentSpy.detectAndHandleHealing(pollReportMock, layoutMock, localEndpoint);

        assertEquals(DetectorTask.SKIPPED, healingFailed.join());
    }

    @Test
    void detectAndHandleHealingAFullyConnectedNode() {
        final String localEndpoint = NodeNames.C;

        FailureDetectorTestContext ctx = new FailureDetectorTestContext(localEndpoint);
        ctx.setupClusterState(clusterStateCtx -> {
            clusterStateCtx.unresponsiveNodes(ctx.localEndpoint);
        });

        HealingAgent agentSpy = ctx.getHealingAgentSpy();
        ClusterState clusterState = ctx.getClusterState();
        PollReport pollReportMock = ctx.getPollReportMock(clusterState);
        Layout layoutMock = ctx.getLayoutMock();

        CompletableFuture<Boolean> handle = CompletableFuture.completedFuture(true);
        doReturn(handle)
                .when(agentSpy)
                .handleHealing(same(pollReportMock), same(layoutMock), anySet(), same(localEndpoint));

        CompletableFuture<DetectorTask> healingFailed = agentSpy.detectAndHandleHealing(pollReportMock, layoutMock, localEndpoint);

        verify(agentSpy, times(1))
                .handleHealing(same(pollReportMock), same(layoutMock), anySet(), same(localEndpoint));

        assertEquals(DetectorTask.COMPLETED, healingFailed.join());
    }

    /**
     * Initializes the health monitor with a failure of the orchestrator. Runtime issues
     * can be reported only for an initialized component.
     */
    private static void initHealthMonitorWithOrchestratorFailure(Issue failure) {
        HealthMonitor.init();
        HealthMonitor.reportIssue(Issue.createInitIssue(ORCHESTRATOR));
        HealthMonitor.resolveIssue(Issue.createInitIssue(ORCHESTRATOR));
        HealthMonitor.reportIssue(failure);
    }

    private static boolean orchestratorIsRuntimeHealthy() {
        return HealthMonitor.getHealthStatusSnapshot().get(ORCHESTRATOR).isRuntimeHealthy();
    }

    /**
     * Creates a layout with the given unresponsive servers and one segment per set of log servers.
     */
    private static Layout layoutMock(List<String> unresponsiveServers, List<Set<String>> segmentLogServers) {
        List<LayoutSegment> segments = segmentLogServers.stream()
                .map(logServers -> {
                    LayoutSegment segment = mock(LayoutSegment.class);
                    when(segment.getAllLogServers()).thenReturn(logServers);
                    return segment;
                })
                .collect(Collectors.toList());

        Layout layout = mock(Layout.class);
        when(layout.getSegments()).thenReturn(segments);
        when(layout.getUnresponsiveServers()).thenReturn(unresponsiveServers);
        return layout;
    }

    /**
     * A single segment contains all the servers: nothing is left to restore or merge.
     */
    private static Layout convergedLayoutMock() {
        return layoutMock(ImmutableList.of(), ImmutableList.of(ImmutableSet.of(NodeNames.A, NodeNames.B, NodeNames.C)));
    }

    /**
     * A failure of a recovery workflow is stale once the cluster has recovered (all the nodes are
     * responsive and the layout has a single segment): the failure detector has to resolve it,
     * since no further workflow is going to be run to resolve it.
     */
    @Test
    void staleRecoveryWorkflowFailureIsResolvedWhenNothingIsLeftToRestore() {
        final String localEndpoint = NodeNames.C;
        initHealthMonitorWithOrchestratorFailure(Issue.createIssue(ORCHESTRATOR, ORCHESTRATOR_RECOVERY_TASK_FAILED,
                "Failed to execute action RestoreRedundancyAndMergeSegments for workflow HEAL_NODE"));

        try {
            assertFalse(orchestratorIsRuntimeHealthy());

            HealingAgent agent = new FailureDetectorTestContext(localEndpoint).getHealingAgentSpy();
            agent.restoreRedundancyAndMergeSegments(convergedLayoutMock(), localEndpoint);

            assertTrue(orchestratorIsRuntimeHealthy());
        } finally {
            HealthMonitor.shutdown();
        }
    }

    /**
     * The failure of a recovery workflow is not stale while the local node is still in the
     * unresponsive list, even though the layout has nothing to restore or merge.
     */
    @Test
    void recoveryWorkflowFailureIsKeptWhileLocalNodeIsUnresponsive() {
        final String localEndpoint = NodeNames.C;
        initHealthMonitorWithOrchestratorFailure(Issue.createIssue(ORCHESTRATOR, ORCHESTRATOR_RECOVERY_TASK_FAILED,
                "Failed to execute action HealNodeToLayout for workflow HEAL_NODE"));

        try {
            HealingAgent agent = new FailureDetectorTestContext(localEndpoint).getHealingAgentSpy();
            Layout layout = layoutMock(ImmutableList.of(localEndpoint), ImmutableList.of(ImmutableSet.of(NodeNames.A, NodeNames.B)));
            agent.restoreRedundancyAndMergeSegments(layout, localEndpoint);

            assertFalse(orchestratorIsRuntimeHealthy());
        } finally {
            HealthMonitor.shutdown();
        }
    }

    /**
     * The failure of a recovery workflow is not stale while another node is unresponsive, even though
     * the local node is healed. A restore redundancy and merge segments workflow of a node runs on the
     * orchestrator of another node, so the local node can not tell from its own state that the
     * recovery of the other nodes is complete.
     */
    @Test
    void recoveryWorkflowFailureIsKeptWhileAPeerIsUnresponsive() {
        final String localEndpoint = NodeNames.C;
        initHealthMonitorWithOrchestratorFailure(Issue.createIssue(ORCHESTRATOR, ORCHESTRATOR_RECOVERY_TASK_FAILED,
                "Failed to execute action RestoreRedundancyAndMergeSegments for workflow RESTORE_REDUNDANCY_MERGE_SEGMENTS"));

        try {
            HealingAgent agent = new FailureDetectorTestContext(localEndpoint).getHealingAgentSpy();
            Layout layout = layoutMock(ImmutableList.of(NodeNames.B), ImmutableList.of(ImmutableSet.of(NodeNames.A, NodeNames.C)));
            agent.restoreRedundancyAndMergeSegments(layout, localEndpoint);

            assertFalse(orchestratorIsRuntimeHealthy());
        } finally {
            HealthMonitor.shutdown();
        }
    }

    /**
     * The failure of a recovery workflow is not stale while another node still has to restore its
     * redundancy, even though the local node has nothing to restore or merge.
     */
    @Test
    void recoveryWorkflowFailureIsKeptWhileAPeerNeedsToRestoreItsRedundancy() {
        final String localEndpoint = NodeNames.C;
        initHealthMonitorWithOrchestratorFailure(Issue.createIssue(ORCHESTRATOR, ORCHESTRATOR_RECOVERY_TASK_FAILED,
                "Failed to execute action RestoreRedundancyAndMergeSegments for workflow RESTORE_REDUNDANCY_MERGE_SEGMENTS"));

        try {
            HealingAgent agent = new FailureDetectorTestContext(localEndpoint).getHealingAgentSpy();

            // The peer (B) is missing from the first segment, the local node is not. The local node
            // can neither restore the redundancy nor merge the segments, so no task is spawned.
            Layout layout = layoutMock(ImmutableList.of(), ImmutableList.of(
                    ImmutableSet.of(NodeNames.A, NodeNames.C),
                    ImmutableSet.of(NodeNames.A, NodeNames.B, NodeNames.C)));
            agent.restoreRedundancyAndMergeSegments(layout, localEndpoint);

            assertFalse(orchestratorIsRuntimeHealthy());
        } finally {
            HealthMonitor.shutdown();
        }
    }

    /**
     * The failure of a recovery workflow is not stale while the local node still has to restore its
     * redundancy. Neither is it resolved while the task that restores the redundancy is in progress.
     */
    @Test
    void recoveryWorkflowFailureIsKeptWhileRedundancyIsBeingRestored() {
        final String localEndpoint = NodeNames.C;
        initHealthMonitorWithOrchestratorFailure(Issue.createIssue(ORCHESTRATOR, ORCHESTRATOR_RECOVERY_TASK_FAILED,
                "Failed to execute action RestoreRedundancyAndMergeSegments for workflow HEAL_NODE"));

        try {
            HealingAgent agent = new FailureDetectorTestContext(localEndpoint).getHealingAgentSpy();

            // The local node is missing from the first segment, so it can restore the redundancy.
            // The executor of the test context never runs the spawned task, so it stays in progress.
            Layout restorationNeeded = layoutMock(ImmutableList.of(), ImmutableList.of(
                    ImmutableSet.of(NodeNames.A, NodeNames.B),
                    ImmutableSet.of(NodeNames.A, NodeNames.B, NodeNames.C)));
            agent.restoreRedundancyAndMergeSegments(restorationNeeded, localEndpoint);
            assertFalse(orchestratorIsRuntimeHealthy());

            agent.restoreRedundancyAndMergeSegments(convergedLayoutMock(), localEndpoint);
            assertFalse(orchestratorIsRuntimeHealthy());
        } finally {
            HealthMonitor.shutdown();
        }
    }

    /**
     * A failure of a workflow that was explicitly requested by an operator (add, remove or force
     * remove a node) stays in the health report until a later workflow completes successfully.
     * The failure detector must not resolve it.
     */
    @Test
    void requestedWorkflowFailureIsNotResolvedByTheFailureDetector() {
        final String localEndpoint = NodeNames.C;
        initHealthMonitorWithOrchestratorFailure(Issue.createIssue(ORCHESTRATOR, ORCHESTRATOR_TASK_FAILED,
                "Failed to execute action AddNodeToLayout for workflow ADD_NODE"));

        try {
            HealingAgent agent = new FailureDetectorTestContext(localEndpoint).getHealingAgentSpy();
            agent.restoreRedundancyAndMergeSegments(convergedLayoutMock(), localEndpoint);

            assertFalse(orchestratorIsRuntimeHealthy());
        } finally {
            HealthMonitor.shutdown();
        }
    }
}
