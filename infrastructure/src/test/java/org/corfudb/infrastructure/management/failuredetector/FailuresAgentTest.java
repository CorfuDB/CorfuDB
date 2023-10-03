package org.corfudb.infrastructure.management.failuredetector;

import org.corfudb.infrastructure.NodeNames;
import org.corfudb.infrastructure.management.failuredetector.RemoteMonitoringService.DetectorTask;
import org.corfudb.infrastructure.management.PollReport;
import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.protocols.wireprotocol.failuredetector.FileSystemStats;
import org.corfudb.runtime.view.Layout;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.corfudb.infrastructure.NodeNames.A;
import static org.corfudb.infrastructure.NodeNames.C;
import static org.corfudb.infrastructure.management.NodeStateTestUtil.B;
import static org.corfudb.infrastructure.management.NodeStateTestUtil.nodeState;
import static org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity.ConnectionStatus.FAILED;
import static org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity.ConnectionStatus.OK;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class FailuresAgentTest {

    @Test
    void detectAndHandleConnectionFailures() {
        final String localEndpoint = NodeNames.B;
        FailureDetectorTestContext ctx = new FailureDetectorTestContext(localEndpoint);

        ctx.setupClusterState(clusterStateCtx -> {
            final long epoch = 0;
            FileSystemStats fsStats = clusterStateCtx.getFsStats();
            //failure between node A and C
            clusterStateCtx.setupNodes(
                    nodeState(A, epoch, Optional.of(fsStats), OK, OK, FAILED),
                    nodeState(localEndpoint, epoch, Optional.of(fsStats), OK, OK, OK),
                    nodeState(C, epoch, Optional.of(fsStats), FAILED, OK, OK)
            );
        });

        ClusterState clusterState = ctx.getClusterState();
        PollReport pollReportMock = ctx.getPollReportMock(clusterState);

        Layout layoutMock = ctx.getLayoutMock();
        when(layoutMock.getAllServers()).thenReturn(new HashSet<>(Arrays.asList(A, B, C)));

        FailuresAgent failuresAgent = ctx.getFailuresAgentSpy();

        CompletableFuture<DetectorTask> handle = CompletableFuture.completedFuture(DetectorTask.COMPLETED);
        HashSet<String> expectedFailedNodes = new HashSet<>();
        expectedFailedNodes.add(C);

        doReturn(handle)
                .when(failuresAgent)
                .handleFailure(same(layoutMock), eq(expectedFailedNodes), same(pollReportMock), same(localEndpoint));

        DetectorTask result = failuresAgent.detectAndHandleFailure(pollReportMock, layoutMock, localEndpoint);

        verify(failuresAgent, times(1))
                .handleFailure(same(layoutMock), eq(expectedFailedNodes), same(pollReportMock), same(localEndpoint));

        assertEquals(DetectorTask.COMPLETED, result);
    }

    @Test
    void detectAndHandleDiskReadOnlyFailures() {
        final String localEndpoint = NodeNames.B;
        FailureDetectorTestContext ctx = new FailureDetectorTestContext(localEndpoint);

        ctx.setupClusterState(clusterStateCtx -> {
            final long epoch = 0;
            FileSystemStats fsStatsRedOnly = clusterStateCtx.getFileSystemStatsWithReadOnlyDisk();
            FileSystemStats fsStatsOk = clusterStateCtx.getFsStats();
            //failure between node A and C
            clusterStateCtx.setupNodes(
                    nodeState(A, epoch, Optional.of(fsStatsRedOnly), OK, OK, OK),
                    nodeState(localEndpoint, epoch, Optional.of(fsStatsOk), OK, OK, FAILED),
                    nodeState(C, epoch, Optional.of(fsStatsOk), OK, FAILED, OK)
            );
        });

        ClusterState clusterState = ctx.getClusterState();
        PollReport pollReportMock = ctx.getPollReportMock(clusterState);

        Layout layoutMock = ctx.getLayoutMock();
        when(layoutMock.getAllServers()).thenReturn(new HashSet<>(Arrays.asList(A, B, C)));

        FailuresAgent failuresAgent = ctx.getFailuresAgentSpy();

        CompletableFuture<DetectorTask> handle = CompletableFuture.completedFuture(DetectorTask.COMPLETED);
        HashSet<String> expectedFailedNodes = new HashSet<>();
        expectedFailedNodes.add(A);

        doReturn(handle)
                .when(failuresAgent)
                .handleFailure(same(layoutMock), eq(expectedFailedNodes), same(pollReportMock), same(localEndpoint));

        DetectorTask result = failuresAgent.detectAndHandleFailure(pollReportMock, layoutMock, localEndpoint);

        verify(failuresAgent, times(1))
                .handleFailure(same(layoutMock), eq(expectedFailedNodes), same(pollReportMock), same(localEndpoint));

        assertEquals(DetectorTask.COMPLETED, result);
    }

    @Test
    void detectAndHandleBatchProcessorFailures() {
        final String localEndpoint = NodeNames.B;
        FailureDetectorTestContext ctx = new FailureDetectorTestContext(localEndpoint);

        ctx.setupClusterState(clusterStateCtx -> {
            final long epoch = 0;
            FileSystemStats fsStatsBpError = clusterStateCtx.getFileSystemStatsWithBpError();
            FileSystemStats fsStatsOk = clusterStateCtx.getFsStats();
            //failure between node A and C
            clusterStateCtx.setupNodes(
                    nodeState(A, epoch, Optional.of(fsStatsBpError), OK, OK, FAILED),
                    nodeState(localEndpoint, epoch, Optional.of(fsStatsOk), OK, OK, OK),
                    nodeState(C, epoch, Optional.of(fsStatsOk), FAILED, OK, OK)
            );
        });

        ClusterState clusterState = ctx.getClusterState();
        PollReport pollReportMock = ctx.getPollReportMock(clusterState);

        Layout layoutMock = ctx.getLayoutMock();
        when(layoutMock.getAllServers()).thenReturn(new HashSet<>(Arrays.asList(A, B, C)));

        FailuresAgent failuresAgent = ctx.getFailuresAgentSpy();

        CompletableFuture<DetectorTask> handle = CompletableFuture.completedFuture(DetectorTask.COMPLETED);
        HashSet<String> expectedFailedNodes = new HashSet<>();
        expectedFailedNodes.add(A);

        doReturn(handle)
                .when(failuresAgent)
                .handleFailure(same(layoutMock), eq(expectedFailedNodes), same(pollReportMock), same(localEndpoint));

        DetectorTask result = failuresAgent.detectAndHandleFailure(pollReportMock, layoutMock, localEndpoint);

        verify(failuresAgent, times(1))
                .handleFailure(same(layoutMock), eq(expectedFailedNodes), same(pollReportMock), same(localEndpoint));

        assertEquals(DetectorTask.COMPLETED, result);
    }
}
