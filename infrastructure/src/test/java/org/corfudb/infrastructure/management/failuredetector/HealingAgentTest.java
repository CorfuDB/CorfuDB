package org.corfudb.infrastructure.management.failuredetector;

import com.google.common.collect.ImmutableList;
import org.corfudb.infrastructure.NodeNames;
import org.corfudb.infrastructure.RemoteMonitoringService.DetectorTask;
import org.corfudb.infrastructure.management.CompleteGraphAdvisor;
import org.corfudb.infrastructure.management.FileSystemAdvisor;
import org.corfudb.infrastructure.management.PollReport;
import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.protocols.wireprotocol.failuredetector.FileSystemStats;
import org.corfudb.protocols.wireprotocol.failuredetector.FileSystemStats.PartitionAttributeStats;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.concurrent.SingletonResource;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import static org.corfudb.infrastructure.management.NodeStateTestUtil.nodeState;
import static org.corfudb.protocols.wireprotocol.ClusterState.buildClusterState;
import static org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity.ConnectionStatus.OK;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class HealingAgentTest {

    @Test
    void unsuccessfulHealingOfAFailedNode() {
        final String localEndpoint = NodeNames.C;
        final long epoch = 0;

        HealingAgent agentSpy = getHealingAgentSpy(localEndpoint);
        ClusterState clusterState = getClusterState(localEndpoint, epoch);
        PollReport pollReportMock = getPollReportMock(clusterState);
        Layout layoutMock = mock(Layout.class);

        CompletableFuture<Boolean> handle = new CompletableFuture<>();
        handle.completeExceptionally(new FailureDetectorException("err"));
        doReturn(handle)
                .when(agentSpy)
                .handleHealing(same(pollReportMock), same(layoutMock), anySet());

        CompletableFuture<DetectorTask> healingFailed = agentSpy.detectAndHandleHealing(pollReportMock, layoutMock);

        verify(agentSpy, times(1))
                .handleHealing(same(pollReportMock), same(layoutMock), anySet());

        assertEquals(DetectorTask.NOT_COMPLETED, healingFailed.join());
    }

    @Test
    void detectAndHandleHealingAFullyConnectedNode() {
        final String localEndpoint = NodeNames.C;
        final long epoch = 0;

        HealingAgent agentSpy = getHealingAgentSpy(localEndpoint);
        ClusterState clusterState = getClusterState(localEndpoint, epoch);
        PollReport pollReportMock = getPollReportMock(clusterState);
        Layout layoutMock = mock(Layout.class);

        CompletableFuture<Boolean> handle = CompletableFuture.completedFuture(true);
        doReturn(handle)
                .when(agentSpy)
                .handleHealing(same(pollReportMock), same(layoutMock), anySet());

        CompletableFuture<DetectorTask> healingFailed = agentSpy.detectAndHandleHealing(pollReportMock, layoutMock);

        verify(agentSpy, times(1))
                .handleHealing(same(pollReportMock), same(layoutMock), anySet());

        assertEquals(DetectorTask.COMPLETED, healingFailed.join());
    }

    private PollReport getPollReportMock(ClusterState clusterState) {
        PollReport pollReportMock = mock(PollReport.class);
        when(pollReportMock.getClusterState()).thenReturn(clusterState);
        return pollReportMock;
    }

    private ClusterState getClusterState(String localEndpoint, long epoch) {
        return buildClusterState(
                localEndpoint,
                ImmutableList.of(localEndpoint),
                nodeState(NodeNames.A, epoch, OK, OK, OK),
                nodeState(NodeNames.B, epoch, OK, OK, OK),
                nodeState(localEndpoint, epoch, Optional.of(getFileSystemStats()), OK, OK, OK)
        );
    }

    private FileSystemStats getFileSystemStats() {
        PartitionAttributeStats attributes = new PartitionAttributeStats(false, 100, 200);
        return new FileSystemStats(attributes);
    }

    private HealingAgent getHealingAgentSpy(String localEndpoint) {
        CompleteGraphAdvisor advisor = new CompleteGraphAdvisor(localEndpoint);
        FileSystemAdvisor fsAdvisor = new FileSystemAdvisor();
        FailureDetectorDataStore dataStoreMock = mock(FailureDetectorDataStore.class);
        ExecutorService fdWorkerMock = mock(ExecutorService.class);
        SingletonResource<CorfuRuntime> runtimeMock = mock(SingletonResource.class);

        HealingAgent agent = HealingAgent.builder()
                .dataStore(dataStoreMock)
                .advisor(advisor)
                .fsAdvisor(fsAdvisor)
                .failureDetectorWorker(fdWorkerMock)
                .localEndpoint(localEndpoint)
                .runtimeSingleton(runtimeMock)
                .build();

        return spy(agent);
    }
}
