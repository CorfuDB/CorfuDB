package org.corfudb.infrastructure.management.failuredetector;

import com.google.common.collect.ImmutableList;
import org.corfudb.infrastructure.NodeNames;
import org.corfudb.infrastructure.RemoteMonitoringService.DetectorTask;
import org.corfudb.infrastructure.management.ClusterAdvisor;
import org.corfudb.infrastructure.management.CompleteGraphAdvisor;
import org.corfudb.infrastructure.management.FileSystemAdvisor;
import org.corfudb.infrastructure.management.PollReport;
import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.protocols.wireprotocol.failuredetector.FileSystemStats;
import org.corfudb.protocols.wireprotocol.failuredetector.FileSystemStats.ResourceQuotaStats;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.concurrent.SingletonResource;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import static org.corfudb.infrastructure.management.NodeStateTestUtil.nodeState;
import static org.corfudb.protocols.wireprotocol.ClusterState.buildClusterState;
import static org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity.ConnectionStatus.OK;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class HealingAgentTest {

    @Test
    void detectHealingNoFailedNodes() {
        ClusterAdvisor adviserMock = mock(ClusterAdvisor.class);

        HealingAgent agent = HealingAgent.builder()
                .dataStore(mock(FailureDetectorDataStore.class))
                .advisor(adviserMock)
                .fsAdvisor(mock(FileSystemAdvisor.class))
                .failureDetectorWorker(mock(ExecutorService.class))
                .localEndpoint(NodeNames.A)
                .runtimeSingleton(mock(SingletonResource.class))
                .build();

        when(adviserMock.healedServer(isA(ClusterState.class))).thenReturn(Optional.empty());
        CompletableFuture<DetectorTask> skipped = agent.detectAndHandleHealing(mock(PollReport.class), mock(Layout.class));

        assertEquals(DetectorTask.SKIPPED, skipped.join());
    }

    @Test
    void detectHealingAFailedNode() {
        final String localEndpoint = NodeNames.C;
        final long epoch = 0;

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

        HealingAgent agentSpy = spy(agent);

        final int limit = 100;
        final int used = 80;
        ResourceQuotaStats quota = new ResourceQuotaStats(limit, used);
        FileSystemStats fsStats = new FileSystemStats(quota, Mockito.mock(FileSystemStats.PartitionAttributeStats.class));

        PollReport pollReportMock = mock(PollReport.class);
        ClusterState clusterState = buildClusterState(
                localEndpoint,
                ImmutableList.of(localEndpoint),
                nodeState(NodeNames.A, epoch, OK, OK, OK),
                nodeState(NodeNames.B, epoch, OK, OK, OK),
                nodeState(localEndpoint, epoch, Optional.of(fsStats), OK, OK, OK)
        );

        when(pollReportMock.getClusterState()).thenReturn(clusterState);

        CompletableFuture<Boolean> handle = new CompletableFuture<>();
        handle.completeExceptionally(new FailureDetectorException("err"));
        doReturn(handle)
                .when(agentSpy)
                .handleHealing(any(PollReport.class), any(Layout.class), anySet());

        CompletableFuture<DetectorTask> healingFailed = agentSpy.detectAndHandleHealing(pollReportMock, mock(Layout.class));

        verify(agentSpy, times(1))
                .handleHealing(any(PollReport.class), any(Layout.class), anySet());

        assertEquals(DetectorTask.NOT_COMPLETED, healingFailed.join());
    }
}
