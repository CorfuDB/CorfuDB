package org.corfudb.infrastructure.management.failuredetector;

import org.corfudb.infrastructure.NodeNames;
import org.corfudb.infrastructure.management.failuredetector.RemoteMonitoringService.DetectorTask;
import org.corfudb.infrastructure.datastore.DataStore;
import org.corfudb.infrastructure.management.PollReport;
import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.protocols.wireprotocol.failuredetector.FileSystemStats;
import org.corfudb.runtime.view.Layout;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.corfudb.infrastructure.management.NodeStateTestUtil.nodeState;
import static org.corfudb.infrastructure.management.failuredetector.LayoutRateLimit.LayoutRateLimitParams.getDefaultParams;
import static org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity.ConnectionStatus.OK;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

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

        CompletableFuture<DetectorTask> healingFailed = agentSpy.detectAndHandleHealing(pollReportMock, layoutMock, localEndpoint, getDefaultParams());

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

        CompletableFuture<DetectorTask> healingFailed = agentSpy.detectAndHandleHealing(pollReportMock, layoutMock, localEndpoint, getDefaultParams());

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

        CompletableFuture<DetectorTask> healingFailed = agentSpy.detectAndHandleHealing(pollReportMock, layoutMock, localEndpoint, getDefaultParams());

        verify(agentSpy, times(1))
                .handleHealing(same(pollReportMock), same(layoutMock), anySet(), same(localEndpoint));

        assertEquals(DetectorTask.COMPLETED, healingFailed.join());
    }
}
