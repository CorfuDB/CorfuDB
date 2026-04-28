package org.corfudb.infrastructure.management.failuredetector;

import com.google.common.collect.ImmutableMap;
import org.corfudb.infrastructure.RemoteMonitoringService.DetectorTask;
import org.corfudb.infrastructure.management.PollReport;
import org.corfudb.infrastructure.management.failuredetector.FailureDetectorService.SequencerBootstrapper;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.runtime.view.Layout;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class FailureDetectorServiceTest {

    @Test
    public void testFailureDetectorAndWrongEpochs() {
        ExecutorService fdWorker = Executors.newFixedThreadPool(3);

        CompletableFuture<Layout> wrongEpochResult = new CompletableFuture<>();
        wrongEpochResult.completeExceptionally(new WrongEpochException(123));

        EpochHandler epochHandler = mock(EpochHandler.class);
        when(epochHandler.correctWrongEpochs(any(), any())).thenReturn(wrongEpochResult);

        FailureDetectorService fdService = FailureDetectorService.builder()
                .epochHandler(epochHandler)
                .failureDetectorWorker(fdWorker)
                .failuresAgent(mock(FailuresAgent.class))
                .healingAgent(mock(HealingAgent.class))
                .sequencerBootstrapper(mock(SequencerBootstrapper.class))
                .build();

        PollReport pollReportMock = mock(PollReport.class);

        Layout layoutMock = mock(Layout.class);

        String endpoint = "localhost";

        fdService.runFailureDetectorTask(pollReportMock, layoutMock, endpoint).whenComplete((result, ex) -> {
            assertEquals("Wrong epoch. [expected=123]", ex.getMessage());
        });
    }

    /**
     * Sets up the common PollReport mock for unfilled-slot tests: sealed servers,
     * unfilled slot present, wrong epochs empty.
     */
    private PollReport setupUnfilledSlotPollReport(Set<String> failedNodes) {
        PollReport pollReportMock = mock(PollReport.class);
        when(pollReportMock.areAllResponsiveServersSealed()).thenReturn(true);
        when(pollReportMock.getLayoutSlotUnFilled(any())).thenReturn(Optional.of(535L));
        when(pollReportMock.getFailedNodes()).thenReturn(failedNodes);
        when(pollReportMock.getWrongEpochs()).thenReturn(ImmutableMap.of());
        return pollReportMock;
    }

    /**
     * When the unfilled-slot path is taken and PollReport.getFailedNodes() returns a dead node,
     * failuresAgent.handleFailure() must be called with that dead node set, NOT empty set.
     */
    @Test
    public void testUnfilledSlotPassesFailedNodesToHandler()
            throws ExecutionException, InterruptedException, TimeoutException {
        ExecutorService fdWorker = Executors.newFixedThreadPool(3);
        String endpoint = "localhost:9000";
        String deadNode = "192.161.160.32:9000";

        Layout layoutMock = mock(Layout.class);

        EpochHandler epochHandler = mock(EpochHandler.class);
        when(epochHandler.correctWrongEpochs(any(), any()))
                .thenReturn(CompletableFuture.completedFuture(layoutMock));

        Set<String> failedNodes = new HashSet<>();
        failedNodes.add(deadNode);
        PollReport pollReportMock = setupUnfilledSlotPollReport(failedNodes);

        FailuresAgent failuresAgent = mock(FailuresAgent.class);
        when(failuresAgent.handleFailure(any(), any(), any(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(DetectorTask.COMPLETED));

        FailureDetectorService fdService = FailureDetectorService.builder()
                .epochHandler(epochHandler)
                .failureDetectorWorker(fdWorker)
                .failuresAgent(failuresAgent)
                .healingAgent(mock(HealingAgent.class))
                .sequencerBootstrapper(mock(SequencerBootstrapper.class))
                .build();

        DetectorTask result = fdService
                .runFailureDetectorTask(pollReportMock, layoutMock, endpoint)
                .get(10, TimeUnit.SECONDS);

        assertEquals(DetectorTask.COMPLETED, result);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Set<String>> failedNodesCaptor = ArgumentCaptor.forClass(Set.class);
        verify(failuresAgent).handleFailure(
                same(layoutMock), failedNodesCaptor.capture(), same(pollReportMock), eq(endpoint));

        Set<String> capturedFailedNodes = failedNodesCaptor.getValue();
        assertEquals(1, capturedFailedNodes.size());
        assertTrue(capturedFailedNodes.contains(deadNode),
                "handleFailure must receive the dead node, not an empty set");
    }

    /**
     * When the unfilled-slot path is taken but PollReport.getFailedNodes() returns empty
     * (no failures detected), handleFailure() should still be called with an empty set.
     * This verifies no regression in the no-failure case.
     */
    @Test
    public void testUnfilledSlotPassesEmptySetWhenNoFailures()
            throws ExecutionException, InterruptedException, TimeoutException {
        ExecutorService fdWorker = Executors.newFixedThreadPool(3);
        String endpoint = "localhost:9000";

        Layout layoutMock = mock(Layout.class);

        EpochHandler epochHandler = mock(EpochHandler.class);
        when(epochHandler.correctWrongEpochs(any(), any()))
                .thenReturn(CompletableFuture.completedFuture(layoutMock));

        PollReport pollReportMock = setupUnfilledSlotPollReport(Collections.emptySet());

        FailuresAgent failuresAgent = mock(FailuresAgent.class);
        when(failuresAgent.handleFailure(any(), any(), any(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(DetectorTask.COMPLETED));

        FailureDetectorService fdService = FailureDetectorService.builder()
                .epochHandler(epochHandler)
                .failureDetectorWorker(fdWorker)
                .failuresAgent(failuresAgent)
                .healingAgent(mock(HealingAgent.class))
                .sequencerBootstrapper(mock(SequencerBootstrapper.class))
                .build();

        DetectorTask result = fdService
                .runFailureDetectorTask(pollReportMock, layoutMock, endpoint)
                .get(10, TimeUnit.SECONDS);

        assertEquals(DetectorTask.COMPLETED, result);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Set<String>> failedNodesCaptor = ArgumentCaptor.forClass(Set.class);
        verify(failuresAgent).handleFailure(
                same(layoutMock), failedNodesCaptor.capture(), same(pollReportMock), eq(endpoint));

        assertTrue(failedNodesCaptor.getValue().isEmpty(),
                "handleFailure must receive empty set when no failures detected");
    }

    /**
     * When the thread is interrupted during the backoff sleep in the unfilled-slot path,
     * failuresAgent.handleFailure() must NOT be called (the interrupt skips it).
     */
    @Test
    public void testUnfilledSlotInterruptDuringBackoff()
            throws ExecutionException, InterruptedException, TimeoutException {
        ExecutorService fdWorker = Executors.newSingleThreadExecutor();
        String endpoint = "localhost:9000";
        String deadNode = "192.161.160.32:9000";

        Layout layoutMock = mock(Layout.class);

        EpochHandler epochHandler = mock(EpochHandler.class);
        when(epochHandler.correctWrongEpochs(any(), any()))
                .thenReturn(CompletableFuture.completedFuture(layoutMock));

        Set<String> failedNodes = new HashSet<>();
        failedNodes.add(deadNode);
        PollReport pollReportMock = setupUnfilledSlotPollReport(failedNodes);

        FailuresAgent failuresAgent = mock(FailuresAgent.class);

        HealingAgent healingAgent = mock(HealingAgent.class);
        when(healingAgent.detectAndHandleHealing(any(), any(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(DetectorTask.COMPLETED));

        FailureDetectorService fdService = FailureDetectorService.builder()
                .epochHandler(epochHandler)
                .failureDetectorWorker(fdWorker)
                .failuresAgent(failuresAgent)
                .healingAgent(healingAgent)
                .sequencerBootstrapper(mock(SequencerBootstrapper.class))
                .build();

        CompletableFuture<DetectorTask> future =
                fdService.runFailureDetectorTask(pollReportMock, layoutMock, endpoint);

        Thread.sleep(50);
        fdWorker.shutdownNow();

        future.get(10, TimeUnit.SECONDS);

        verify(failuresAgent, never())
                .handleFailure(any(), any(), any(), anyString());
    }
}
