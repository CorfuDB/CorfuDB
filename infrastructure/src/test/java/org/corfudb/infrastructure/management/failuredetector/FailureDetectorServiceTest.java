package org.corfudb.infrastructure.management.failuredetector;

import org.corfudb.infrastructure.management.PollReport;
import org.corfudb.infrastructure.management.failuredetector.FailureDetectorService.SequencerBootstrapper;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.runtime.view.Layout;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.corfudb.infrastructure.management.failuredetector.LayoutRateLimit.LayoutRateLimitParams.getDefaultParams;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
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

        fdService.runFailureDetectorTask(pollReportMock, layoutMock, endpoint, getDefaultParams()).whenComplete((result, ex) -> {
            assertEquals("Wrong epoch. [expected=123]", ex.getMessage());
        });
    }
}
