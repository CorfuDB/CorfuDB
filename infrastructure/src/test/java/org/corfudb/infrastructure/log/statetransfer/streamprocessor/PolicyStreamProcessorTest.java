package org.corfudb.infrastructure.log.statetransfer.streamprocessor;

import com.google.common.collect.ImmutableList;
import org.corfudb.common.result.Result;
import org.corfudb.common.tailcall.TailCall;
import org.corfudb.infrastructure.log.statetransfer.batch.BatchResult;
import org.corfudb.infrastructure.log.statetransfer.batch.BatchResultData;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.BatchProcessorFailure;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.StateTransferBatchProcessor;
import org.corfudb.infrastructure.log.statetransfer.streamprocessor.PolicyStreamProcessor.SlidingWindow;
import org.corfudb.util.CFUtils;
import org.corfudb.util.Sleep;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

class PolicyStreamProcessorTest {

    @Test
    void updateWindow() {
        PolicyStreamProcessorData data = mock(PolicyStreamProcessorData.class);
        StateTransferBatchProcessor st = mock(StateTransferBatchProcessor.class);
        PolicyStreamProcessor processor = new PolicyStreamProcessor(5, 5, data, st);
        SlidingWindow window = SlidingWindow.builder().build();
        CompletableFuture<BatchResult> first =
                CompletableFuture.completedFuture(new BatchResult(Result.ok(new BatchResultData(5))));
        SlidingWindow newWindow = processor.updateWindow(first, window);
        assertThat(newWindow.getWindow().get(0).join()).isEqualTo(first.join());
        CompletableFuture<BatchResult> second =
                CompletableFuture.completedFuture(new BatchResult(Result.ok(new BatchResultData(10))));
        newWindow = processor.updateWindow(second, newWindow);
        assertThat(newWindow.getWindow().get(1).join()).isEqualTo(second.join());
    }

    @Test
    void slideWindow() {

        PolicyStreamProcessorData data = mock(PolicyStreamProcessorData.class);
        StateTransferBatchProcessor st = mock(StateTransferBatchProcessor.class);
        PolicyStreamProcessor processor = new PolicyStreamProcessor(5, 5, data, st);

        // If window is empty, can't slide
        SlidingWindow window = SlidingWindow.builder().build();
        assertThat(window.canSlideWindow()).isFalse();
        // Window has 3 elements in it: 0, 1, 2, we about to add a forth - 3. Element 0 has failed.
        // A new window should now contain 1, 2, 3, and element 0 should be added to the failed list.
        Result<BatchResultData, BatchProcessorFailure> failed = Result.error(BatchProcessorFailure.builder().build());
        ImmutableList<CompletableFuture<BatchResult>> initialData = ImmutableList.copyOf(ImmutableList.of(
                new BatchResult(failed),
                new BatchResult(Result.ok(new BatchResultData(10L))),
                new BatchResult(Result.ok(new BatchResultData(10L)))
        ).stream().map(CompletableFuture::completedFuture).collect(Collectors.toList()));

        CompletableFuture<BatchResult> result =
                CompletableFuture.completedFuture(new BatchResult(Result.ok(new BatchResultData(8L))));

        window = SlidingWindow.builder().window(initialData).build();
        SlidingWindow newWindow = processor.slideWindow(result, window).invoke().get();
        assertThat(newWindow.getFailed().get(0).join().getResult().getError().getAddresses())
                .isEqualTo(new BatchResult(failed).getResult().getError().getAddresses());

        List<CompletableFuture<BatchResult>> collect = Stream.of
                (new BatchResult(Result.ok(new BatchResultData(10L))),
                new BatchResult(Result.ok(new BatchResultData(10L))))
                .map(CompletableFuture::completedFuture).collect(Collectors.toList());
        collect.add(result);

        List<BatchResultData> collect1 = newWindow.getWindow().stream().map(x -> x.join().getResult().get()).collect(Collectors.toList());
        List<BatchResultData> insideWindow = collect.stream().map(x -> x.join().getResult().get()).collect(Collectors.toList());
        assertThat(collect1).isEqualTo(insideWindow);

        // Window has 3 elements in it: 0, 1, 2, we about to add a forth - 3. Element 0 succeeded.
        // A new window should now contain 1, 2, 3, and element 0 should be added to the successful list.
        // Total number of processed elements should increase by 9.

        Result<BatchResultData, BatchProcessorFailure> successful = Result.ok(new BatchResultData(9L));
        initialData = ImmutableList.copyOf(ImmutableList.of(
                new BatchResult(successful),
                new BatchResult(Result.ok(new BatchResultData(10L))),
                new BatchResult(Result.ok(new BatchResultData(10L)))
        ).stream().map(CompletableFuture::completedFuture).collect(Collectors.toList()));

        window = SlidingWindow.builder().window(initialData).build();
        newWindow = processor.slideWindow(result, window).invoke().get();
        assertThat(newWindow.getFailed()).isEmpty();
        assertThat(newWindow.getSucceeded().get(0).join().getResult().get()).isEqualTo(successful.get());
        assertThat(newWindow.getTotalAddressesTransferred().join()).isEqualTo(9L);

        // Window has 3 elements in it: 0, 1, 2, we about to add a forth - 3. Element 0 is being processed, but eventually succeeds.
        // Do the same as for the above test case
        CompletableFuture<BatchResult> eventuallySuccessful =
                CompletableFuture.supplyAsync(() -> {
                    Sleep.sleepUninterruptibly(Duration.ofMillis(500));
                    return new BatchResult(Result.ok(new BatchResultData(9L)));
                });

        List<CompletableFuture<BatchResult>> futures = Arrays.asList(new BatchResult(Result.ok(new BatchResultData(10L))),
                new BatchResult(Result.ok(new BatchResultData(10L)))).stream()
                .map(CompletableFuture::completedFuture).collect(Collectors.toList());

        futures.add(0, eventuallySuccessful);

        initialData = ImmutableList.copyOf(futures);
        window = SlidingWindow.builder().window(initialData).build();
        assertThat(window.canSlideWindow()).isFalse();
        newWindow = processor.slideWindow(result, window).invoke().get();
        assertThat(newWindow.getFailed()).isEmpty();
        assertThat(newWindow.getSucceeded().get(0).join().getResult().get()).isEqualTo(successful.get());
        assertThat(newWindow.getTotalAddressesTransferred().join()).isEqualTo(9L);

    }

    @Test
    void finalizeSuccessfulTransfers() {
        PolicyStreamProcessorData data = mock(PolicyStreamProcessorData.class);
        StateTransferBatchProcessor st = mock(StateTransferBatchProcessor.class);
        PolicyStreamProcessor processor = new PolicyStreamProcessor(5, 5, data, st);

        Result<BatchResultData, BatchProcessorFailure> successful = Result.ok(new BatchResultData(9L));
        ImmutableList<CompletableFuture<BatchResult>> initialData = ImmutableList.copyOf(ImmutableList.of(
                new BatchResult(successful),
                new BatchResult(Result.ok(new BatchResultData(10L))),
                new BatchResult(Result.ok(new BatchResultData(10L)))
        ).stream().map(CompletableFuture::completedFuture).collect(Collectors.toList()));

        // If window is empty, can't slide
        SlidingWindow window = SlidingWindow
                .builder()
                .succeeded(initialData)
                .totalAddressesTransferred(CompletableFuture.completedFuture(3L)).build();

        CompletableFuture<List<BatchResult>> sequence = CFUtils.sequence(initialData.stream().collect(Collectors.toList()));

        CompletableFuture<Result<Long, StreamProcessFailure>> res =
                processor.finalizeSuccessfulTransfers(window.getTotalAddressesTransferred(), sequence);

        assertThat(res.join().get()).isEqualTo(6L);

    }

    @Test
    void finalizeFailedTransfers() {

        PolicyStreamProcessorData data = mock(PolicyStreamProcessorData.class);
        StateTransferBatchProcessor st = mock(StateTransferBatchProcessor.class);
        PolicyStreamProcessor processor = new PolicyStreamProcessor(5, 5, data, st);

        Result<BatchResultData, BatchProcessorFailure> failed = Result.error(BatchProcessorFailure.builder().build());
        ImmutableList<CompletableFuture<BatchResult>> initialData = ImmutableList.copyOf(ImmutableList.of(
                new BatchResult(failed)).stream().map(CompletableFuture::completedFuture).collect(Collectors.toList()));

        CompletableFuture<List<BatchResult>> sequence = CFUtils.sequence(initialData.stream().collect(Collectors.toList()));
        CompletableFuture<Result<Long, StreamProcessFailure>> res =
                processor.finalizeFailedTransfers(sequence);

        assertThat(res.join().getError()).isInstanceOf(StreamProcessFailure.class);

        initialData = ImmutableList.of();

        CompletableFuture<List<BatchResult>> emptySequence = CFUtils.sequence(initialData.stream().collect(Collectors.toList()));;

        res = processor.finalizeFailedTransfers(emptySequence);
        assertThat(res.join().isValue()).isTrue();


    }

    @Test
    void finalizePendingTransfers() {
        PolicyStreamProcessorData data = mock(PolicyStreamProcessorData.class);
        StateTransferBatchProcessor st = mock(StateTransferBatchProcessor.class);
        PolicyStreamProcessor processor = new PolicyStreamProcessor(5, 5, data, st);

        Result<BatchResultData, BatchProcessorFailure> successful = Result.ok(new BatchResultData(9L));
        ImmutableList<CompletableFuture<BatchResult>> initialData = ImmutableList.copyOf(ImmutableList.of(
                new BatchResult(successful),
                new BatchResult(Result.ok(new BatchResultData(10L))),
                new BatchResult(Result.ok(new BatchResultData(10L)))
        ).stream().map(CompletableFuture::completedFuture).collect(Collectors.toList()));


        CompletableFuture<List<BatchResult>> sequence = CFUtils.sequence(initialData.stream().collect(Collectors.toList()));

        CompletableFuture<Result<Long, StreamProcessFailure>> res =
                processor.finalizePendingTransfers(sequence);

        assertThat(res.join().get()).isEqualTo(29L);

        // One has failed
        initialData = ImmutableList.copyOf(ImmutableList.of(
                new BatchResult(successful),
                new BatchResult(Result.error(BatchProcessorFailure.builder().build())),
                new BatchResult(Result.ok(new BatchResultData(10L)))
        ).stream().map(CompletableFuture::completedFuture).collect(Collectors.toList()));


        sequence = CFUtils.sequence(initialData.stream().collect(Collectors.toList()));
        res = processor.finalizePendingTransfers(sequence);

        assertThat(res.join().getError()).isInstanceOf(StreamProcessFailure.class);
    }


    @Test
    void doProcessStream() {
    }

    @Test
    void processStream() {
    }
}