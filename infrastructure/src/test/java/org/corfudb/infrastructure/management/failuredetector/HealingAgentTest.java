package org.corfudb.infrastructure.management.failuredetector;

import org.corfudb.infrastructure.NodeNames;
import org.corfudb.infrastructure.RemoteMonitoringService.DetectorTask;
import org.corfudb.infrastructure.management.ClusterAdvisor;
import org.corfudb.infrastructure.management.PollReport;
import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeRank;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.concurrent.SingletonResource;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

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
                .failureDetectorWorker(mock(ExecutorService.class))
                .localEndpoint(NodeNames.A)
                .runtimeSingleton(mock(SingletonResource.class))
                .build();

        when(adviserMock.healedServer(isA(ClusterState.class))).thenReturn(Optional.empty());
        CompletableFuture<DetectorTask> skipped = agent.detectHealing(mock(PollReport.class), mock(Layout.class));

        assertEquals(DetectorTask.SKIPPED, skipped.join());
    }

    @Test
    void detectHealingAFailedNode() {
        ClusterAdvisor adviserMock = mock(ClusterAdvisor.class);
        NodeRank failedNode = new NodeRank(NodeNames.A, 3);
        when(adviserMock.healedServer(any(ClusterState.class))).thenReturn(Optional.of(failedNode));

        HealingAgent agent = HealingAgent.builder()
                .dataStore(mock(FailureDetectorDataStore.class))
                .advisor(adviserMock)
                .failureDetectorWorker(mock(ExecutorService.class))
                .localEndpoint(NodeNames.A)
                .runtimeSingleton(mock(SingletonResource.class))
                .build();

        HealingAgent agentSpy = spy(agent);

        PollReport pollReportMock = mock(PollReport.class);
        when(pollReportMock.getClusterState()).thenReturn(mock(ClusterState.class));

        CompletableFuture<Boolean> handle = new CompletableFuture<>();
        handle.completeExceptionally(new FailureDetectorException("err"));
        doReturn(handle)
                .when(agentSpy)
                .handleHealing(any(PollReport.class), any(Layout.class), anySet());

        CompletableFuture<DetectorTask> healingFailed = agentSpy.detectHealing(pollReportMock, mock(Layout.class));

        verify(agentSpy, times(1))
                .handleHealing(any(PollReport.class), any(Layout.class), anySet());

        assertEquals(DetectorTask.NOT_COMPLETED, healingFailed.join());
    }
}
