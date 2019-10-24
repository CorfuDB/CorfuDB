package org.corfudb.infrastructure.log.statetransfer.streamprocessor;

import com.google.common.collect.ImmutableList;
import org.corfudb.common.result.Result;
import org.corfudb.infrastructure.log.statetransfer.FaultyBatchProcessor;
import org.corfudb.infrastructure.log.statetransfer.GoodBatchProcessor;
import org.corfudb.infrastructure.log.statetransfer.batch.BatchResult;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.BatchProcessorFailure;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.StateTransferBatchProcessor;
import org.corfudb.infrastructure.log.statetransfer.streamprocessor.PolicyStreamProcessor.SlidingWindow;
import org.corfudb.infrastructure.log.statetransfer.streamprocessor.policy.staticpolicy.StaticPolicyData;
import org.corfudb.util.CFUtils;
import org.corfudb.util.Sleep;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class PolicyStreamProcessorTest {

    @Test
    void updateWindow() {
        PolicyStreamProcessorData data = mock(PolicyStreamProcessorData.class);
        StateTransferBatchProcessor st = mock(StateTransferBatchProcessor.class);
        PolicyStreamProcessor processor =
                new PolicyStreamProcessor(5, 5, data, st);
        SlidingWindow window = SlidingWindow.builder().build();
        CompletableFuture<BatchResult> first =
                CompletableFuture.completedFuture(BatchResult
                        .builder()
                        .addresses(Arrays.asList(0L, 1L, 2L, 3L, 4L))
                        .build());
        SlidingWindow newWindow = processor.updateWindow(first, window);
        assertThat(newWindow.getWindow().get(0).join()).isEqualTo(first.join());
        CompletableFuture<BatchResult> second =
                CompletableFuture.completedFuture(BatchResult
                        .builder()
                        .addresses(Arrays.asList(5L, 6L, 7L, 8L, 9L))
                        .build());
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
        // A total processed elements at the should be 0.

        ImmutableList<Long> failedAddresses = LongStream.range(0L, 10L).boxed()
                .collect(ImmutableList.toImmutableList());

        ImmutableList<CompletableFuture<BatchResult>> initialData = ImmutableList.copyOf(ImmutableList.of(
                BatchResult.builder().addresses(failedAddresses)
                        .status(BatchResult.FailureStatus.FAILED).build(),
                BatchResult.builder().addresses(LongStream.range(10L, 20L).boxed()
                        .collect(Collectors.toList())).build(),
                BatchResult.builder().addresses(LongStream.range(20L, 30L).boxed()
                        .collect(Collectors.toList())).build()
        ).stream().map(CompletableFuture::completedFuture).collect(Collectors.toList()));

        CompletableFuture<BatchResult> result =
                CompletableFuture.completedFuture(
                        BatchResult.builder().addresses(LongStream.range(30L, 38L)
                                .boxed().collect(Collectors.toList())).build()
                );

        window = SlidingWindow.builder().window(initialData).build();

        SlidingWindow newWindow = processor.slideWindow(result, window).invoke().get();

        assertThat(newWindow.getFailed().get(0).join())
                .isEqualTo(initialData.get(0).join());

        ImmutableList<BatchResult> expectedWindow =
                Stream.concat(initialData.stream().skip(1), Stream.of(result))
                        .map(future -> future.join())
                        .collect(ImmutableList.toImmutableList());

        assertThat(newWindow
                .getWindow()
                .stream()
                .map(CompletableFuture::join)
                .collect(ImmutableList.toImmutableList())).isEqualTo(expectedWindow);

        assertThat(newWindow.getTotalAddressesTransferred().join()).isEqualTo(0L);

        // Window has 3 elements in it: 0, 1, 2, we about to add a forth - 3. Element 0 succeeded.
        // A new window should now contain 1, 2, 3, and element 0 should be added to the successful list.
        // Total number of processed elements should increase by 9.

        ImmutableList<Long> succeededAddresses = LongStream.range(0L, 9L).boxed()
                .collect(ImmutableList.toImmutableList());

        initialData = ImmutableList.copyOf(ImmutableList.of(
                BatchResult.builder().addresses(succeededAddresses)
                        .status(BatchResult.FailureStatus.SUCCEEDED).build(),
                BatchResult.builder().addresses(LongStream.range(10L, 20L).boxed()
                        .collect(Collectors.toList())).build(),
                BatchResult.builder().addresses(LongStream.range(20L, 30L).boxed()
                        .collect(Collectors.toList())).build()
        ).stream().map(CompletableFuture::completedFuture).collect(Collectors.toList()));

        result = CompletableFuture.completedFuture(
                BatchResult.builder().addresses(LongStream.range(30L, 38L)
                        .boxed().collect(Collectors.toList())).build()
        );

        window = SlidingWindow.builder().window(initialData).build();

        newWindow = processor.slideWindow(result, window).invoke().get();

        assertThat(newWindow.getSucceeded().get(0).join())
                .isEqualTo(initialData.get(0).join());

        expectedWindow = Stream.concat(initialData.stream().skip(1), Stream.of(result))
                        .map(CompletableFuture::join)
                        .collect(ImmutableList.toImmutableList());

        assertThat(newWindow
                .getWindow()
                .stream()
                .map(CompletableFuture::join)
                .collect(ImmutableList.toImmutableList())).isEqualTo(expectedWindow);

        assertThat(newWindow.getTotalAddressesTransferred().join()).isEqualTo(9L);


        // Window has 3 elements in it: 0, 1, 2, we about to add a forth - 3.
        // Element 0 is being processed, but eventually succeeds.
        // The result should be the same as for the test case above.
        CompletableFuture<BatchResult> eventuallySuccessful =
                CompletableFuture.supplyAsync(() -> {
                    Sleep.sleepUninterruptibly(Duration.ofMillis(500));
                    return BatchResult.builder().addresses(succeededAddresses)
                            .status(BatchResult.FailureStatus.SUCCEEDED).build();
                });

        List<CompletableFuture<BatchResult>> modData = ImmutableList.of(
                BatchResult.builder().addresses(LongStream.range(10L, 20L).boxed()
                        .collect(Collectors.toList())).build(),
                BatchResult.builder().addresses(LongStream.range(20L, 30L).boxed()
                        .collect(Collectors.toList())).build()
        ).stream().map(CompletableFuture::completedFuture).collect(Collectors.toList());

        modData.add(0, eventuallySuccessful);

        ImmutableList<CompletableFuture<BatchResult>> d = ImmutableList.copyOf(modData);
        window = SlidingWindow.builder().window(d).build();
        assertThat(window.canSlideWindow()).isFalse();
        newWindow = processor.slideWindow(result, window).invoke().get();
        assertThat(newWindow.getFailed()).isEmpty();
        assertThat(newWindow.getSucceeded().get(0).join()).isEqualTo(eventuallySuccessful.join());
        assertThat(newWindow.getTotalAddressesTransferred().join()).isEqualTo(9L);

    }

    @Test
    void finalizeSuccessfulTransfers() {
        PolicyStreamProcessorData data = mock(PolicyStreamProcessorData.class);
        StateTransferBatchProcessor st = mock(StateTransferBatchProcessor.class);
        PolicyStreamProcessor processor = new PolicyStreamProcessor(5, 5, data, st);

        ImmutableList<Long> succeededAddresses = LongStream.range(0L, 9L).boxed()
                .collect(ImmutableList.toImmutableList());

        ImmutableList<CompletableFuture<BatchResult>> initialData = ImmutableList.copyOf(ImmutableList.of(
                BatchResult.builder().addresses(succeededAddresses)
                        .status(BatchResult.FailureStatus.SUCCEEDED).build(),
                BatchResult.builder().addresses(LongStream.range(10L, 20L).boxed()
                        .collect(Collectors.toList())).build(),
                BatchResult.builder().addresses(LongStream.range(20L, 30L).boxed()
                        .collect(Collectors.toList())).build()
        ).stream().map(CompletableFuture::completedFuture).collect(Collectors.toList()));

        SlidingWindow window = SlidingWindow
                .builder()
                .succeeded(initialData)
                .totalAddressesTransferred(CompletableFuture.completedFuture(3L)).build();

        CompletableFuture<List<BatchResult>> sequence = CFUtils.sequence(initialData.stream().collect(Collectors.toList()));

        CompletableFuture<Result<Long, StreamProcessFailure>> res =
                processor.finalizeSuccessfulTransfers(window.getTotalAddressesTransferred(), sequence);

        assertThat(res.join().get()).isEqualTo(32L);

    }

    @Test
    void finalizeFailedTransfers() {

        PolicyStreamProcessorData data = mock(PolicyStreamProcessorData.class);
        StateTransferBatchProcessor st = mock(StateTransferBatchProcessor.class);
        PolicyStreamProcessor processor = new PolicyStreamProcessor(5, 5, data, st);


        ImmutableList<CompletableFuture<BatchResult>> initialData =
                ImmutableList.of(CompletableFuture.completedFuture(BatchResult.builder().status(BatchResult.FailureStatus.FAILED).build()));

        CompletableFuture<List<BatchResult>> sequence = CFUtils.sequence(initialData.stream().collect(Collectors.toList()));
        CompletableFuture<Result<Long, StreamProcessFailure>> res =
                processor.finalizeFailedTransfers(sequence);

        assertThat(res.join().getError()).isInstanceOf(StreamProcessFailure.class);

        initialData = ImmutableList.of();

        CompletableFuture<List<BatchResult>> emptySequence = CFUtils.sequence(initialData.stream().collect(Collectors.toList()));
        ;

        res = processor.finalizeFailedTransfers(emptySequence);
        assertThat(res.join().isValue()).isTrue();


    }

    @Test
    void finalizePendingTransfers() {
        PolicyStreamProcessorData data = mock(PolicyStreamProcessorData.class);
        StateTransferBatchProcessor st = mock(StateTransferBatchProcessor.class);
        PolicyStreamProcessor processor = new PolicyStreamProcessor(5, 5, data, st);

        ImmutableList<Long> succeededAddresses = LongStream.range(0L, 9L).boxed()
                .collect(ImmutableList.toImmutableList());

        ImmutableList<CompletableFuture<BatchResult>> initialData = ImmutableList.copyOf(ImmutableList.of(
                BatchResult.builder().addresses(succeededAddresses)
                        .status(BatchResult.FailureStatus.SUCCEEDED).build(),
                BatchResult.builder().addresses(LongStream.range(10L, 20L).boxed()
                        .collect(Collectors.toList())).build(),
                BatchResult.builder().addresses(LongStream.range(20L, 30L).boxed()
                        .collect(Collectors.toList())).build()
        ).stream().map(CompletableFuture::completedFuture).collect(Collectors.toList()));


        CompletableFuture<List<BatchResult>> sequence = CFUtils.sequence(initialData.stream().collect(Collectors.toList()));

        CompletableFuture<Result<Long, StreamProcessFailure>> res =
                processor.finalizePendingTransfers(sequence);

        assertThat(res.join().get()).isEqualTo(29L);

        // One has failed
        initialData = ImmutableList.copyOf(ImmutableList.of(
                BatchResult.builder().status(BatchResult.FailureStatus.FAILED).build(),
                BatchResult.builder().addresses(LongStream.range(10L, 20L).boxed()
                        .collect(Collectors.toList())).build(),
                BatchResult.builder().addresses(LongStream.range(20L, 30L).boxed()
                        .collect(Collectors.toList())).build()
        ).stream().map(CompletableFuture::completedFuture).collect(Collectors.toList()));


        sequence = CFUtils.sequence(initialData.stream().collect(Collectors.toList()));
        res = processor.finalizePendingTransfers(sequence);

        assertThat(res.join().getError()).isInstanceOf(StreamProcessFailure.class);
    }


    @Test
    void testProcessStreamOkWindowNotSlid() {
        StateTransferBatchProcessor batchProcessor = new GoodBatchProcessor(Optional.of(100L));
        StaticPolicyData data = new StaticPolicyData(LongStream.range(0L, 5L).boxed().collect(Collectors.toList()), Optional.empty(), 10);
        PolicyStreamProcessor streamProcessor = PolicyStreamProcessor
                .builder()
                .windowSize(10)
                .dynamicProtocolWindowSize(10)
                .policyData(PolicyStreamProcessorData.builder().build())
                .batchProcessor(batchProcessor)
                .build();
        CompletableFuture<Result<Long, StreamProcessFailure>> res = streamProcessor.processStream(data);
        assertThat(res.join().get()).isEqualTo(5L);
    }

    @Test
    void testProcessStreamFailWindowNotSlid() {
        StateTransferBatchProcessor batchProcessor = new FaultyBatchProcessor(3,
                Optional.of(100L));
        StaticPolicyData data = new StaticPolicyData(LongStream.range(0L, 5L)
                .boxed().collect(Collectors.toList()), Optional.empty(), 10);
        PolicyStreamProcessor streamProcessor = PolicyStreamProcessor
                .builder()
                .windowSize(10)
                .dynamicProtocolWindowSize(10)
                .policyData(PolicyStreamProcessorData.builder().build())
                .batchProcessor(batchProcessor)
                .build();
        CompletableFuture<Result<Long, StreamProcessFailure>> res = streamProcessor.processStream(data);
        assertThat(res.join().getError()).isInstanceOf(StreamProcessFailure.class);
    }

    @Test
    void testProcessStreamEmptyStream() {
        StateTransferBatchProcessor batchProcessor = new GoodBatchProcessor(
                Optional.of(100L));

        StaticPolicyData data = new StaticPolicyData(LongStream.range(0L, 0L)
                .boxed().collect(Collectors.toList()), Optional.empty(), 10);
        PolicyStreamProcessor streamProcessor = PolicyStreamProcessor
                .builder()
                .windowSize(10)
                .dynamicProtocolWindowSize(10)
                .policyData(PolicyStreamProcessorData.builder().build())
                .batchProcessor(batchProcessor)
                .build();
        CompletableFuture<Result<Long, StreamProcessFailure>> res = streamProcessor.processStream(data);
        assertThat(res.join().get()).isEqualTo(0L);

    }

    @Test
    void testProcessStreamOkWindowSlid() {
        StateTransferBatchProcessor batchProcessor = new GoodBatchProcessor(
                Optional.of(100L));

        StaticPolicyData data = new StaticPolicyData(LongStream.range(0L, 98L)
                .boxed().collect(Collectors.toList()), Optional.empty(), 10);


        PolicyStreamProcessor streamProcessor = PolicyStreamProcessor
                .builder()
                .windowSize(10)
                .dynamicProtocolWindowSize(10)
                .policyData(PolicyStreamProcessorData.builder().build())
                .batchProcessor(batchProcessor)
                .build();

        CompletableFuture<Result<Long, StreamProcessFailure>> res =
                streamProcessor.processStream(data);

        assertThat(res.join().get()).isEqualTo(98L);

    }

    @Test
    void testProcessStreamFailWindowSlid() {
        StateTransferBatchProcessor batchProcessor = new FaultyBatchProcessor(50,
                Optional.of(100L));
        StaticPolicyData data = new StaticPolicyData(LongStream.range(0L, 100L)
                .boxed().collect(Collectors.toList()), Optional.empty(), 10);

        PolicyStreamProcessor streamProcessor = PolicyStreamProcessor
                .builder()
                .windowSize(10)
                .dynamicProtocolWindowSize(10)
                .policyData(PolicyStreamProcessorData.builder().build())
                .batchProcessor(batchProcessor)
                .build();

        CompletableFuture<Result<Long, StreamProcessFailure>> res =
                streamProcessor.processStream(data);

        assertThat(res.join().getError()).isInstanceOf(StreamProcessFailure.class);

    }


}