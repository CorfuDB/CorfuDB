package org.corfudb.infrastructure.log.statetransfer.transferprocessor;

import org.corfudb.infrastructure.log.statetransfer.FaultyBatchProcessor;
import org.corfudb.infrastructure.log.statetransfer.SuccessfulBatchProcessor;
import org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchRequest;
import org.corfudb.infrastructure.log.statetransfer.exceptions.StateTransferBatchProcessorException;
import org.corfudb.infrastructure.log.statetransfer.exceptions.TransferSegmentException;
import org.corfudb.infrastructure.log.statetransfer.transferprocessor.TransferProcessor.TransferProcessorResult;
import org.junit.jupiter.api.Test;
import org.mockito.Matchers;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.infrastructure.log.statetransfer.transferprocessor.TransferProcessor.TransferProcessorResult.TransferProcessorStatus.TRANSFER_FAILED;
import static org.corfudb.infrastructure.log.statetransfer.transferprocessor.TransferProcessor.TransferProcessorResult.TransferProcessorStatus.TRANSFER_SUCCEEDED;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

class ParallelTransferProcessorTest {

    @Test
    public void testRunStateTransferEmptyStream() {
        Stream<TransferBatchRequest> emptyBatchStream = Stream.of();
        ParallelTransferProcessor transferProcessor = new ParallelTransferProcessor(1, new SuccessfulBatchProcessor());
        TransferProcessorResult res =
                transferProcessor.runStateTransfer(emptyBatchStream).join();
        assertThat(res.getTransferState()).isEqualTo(TRANSFER_SUCCEEDED);
    }

    @Test
    public void testRunStateTransferProducerTimesOutOfferingData() {
        // Producer waits for 1 second before timing out, but it takes consumer 3 seconds to process
        // the task. Producer offers request1 -> enqueue and deque immediately,
        // producer offers request2 -> enqueue, producer offers request3 -> time out.
        SuccessfulBatchProcessor slowProcessor = new SuccessfulBatchProcessor(Optional.of(3000L));
        ParallelTransferProcessor transferProcessor = new ParallelTransferProcessor(1,
                slowProcessor, 1L, 2L);


        TransferBatchRequest request1 = TransferBatchRequest.builder().build();
        TransferBatchRequest request2 = TransferBatchRequest.builder().build();
        TransferBatchRequest request3 = TransferBatchRequest.builder().build();
        Stream<TransferBatchRequest> requestStream = Stream.of(request1, request2, request3);

        CompletableFuture<TransferProcessorResult> future =
                transferProcessor.runStateTransfer(requestStream);

        TransferProcessorResult result = future.join();
        assertThat(result.getTransferState()).isEqualTo(TRANSFER_FAILED);
        assertThat(result.getCauseOfFailure()).isPresent();
        assertThat(result.getCauseOfFailure().get())
                .isInstanceOf(TransferSegmentException.class)
                .hasRootCauseInstanceOf(TimeoutException.class);

        assertThat(transferProcessor.getQueue()).isEmpty();
    }

    @Test
    public void testRunStateTransferProducerTimesOutOfferingPoisonPill() {
        // Producer waits for 1 second before timing out, but it takes consumer 3 seconds to process
        // the task. Producer offers request1 -> enqueue and deque immediately,
        // producer offers request2 -> enqueue, producer offers poison pill -> time out.
        SuccessfulBatchProcessor slowProcessor = new SuccessfulBatchProcessor(Optional.of(3000L));
        ParallelTransferProcessor transferProcessor = new ParallelTransferProcessor(1,
                slowProcessor, 1L, 2L);


        TransferBatchRequest request1 = TransferBatchRequest.builder().build();
        TransferBatchRequest request2 = TransferBatchRequest.builder().build();
        Stream<TransferBatchRequest> requestStream = Stream.of(request1, request2);

        CompletableFuture<TransferProcessorResult> future =
                transferProcessor.runStateTransfer(requestStream);

        TransferProcessorResult result = future.join();
        assertThat(result.getTransferState()).isEqualTo(TRANSFER_FAILED);
        assertThat(result.getCauseOfFailure()).isPresent();
        assertThat(result.getCauseOfFailure().get())
                .isInstanceOf(TransferSegmentException.class)
                .hasRootCauseInstanceOf(TimeoutException.class);
        assertThat(transferProcessor.getQueue()).isEmpty();
    }


    @Test
    public void testRunStateTransferConsumerTimesOut() {
        // Consumer waits for a second to poll, but producer waits for 3 seconds.
        ParallelTransferProcessor transferProcessor = new ParallelTransferProcessor(1,
                new SuccessfulBatchProcessor());

        ParallelTransferProcessor spy = spy(transferProcessor);

        doAnswer(invoke -> {
            Thread.sleep(3000);
            return CompletableFuture.completedFuture(null);
        }).when(spy).runProducer(Matchers.anyObject(), Matchers.anyInt());

        Stream<TransferBatchRequest> batch = Stream.of(TransferBatchRequest.builder().build());

        TransferProcessorResult result = spy.runStateTransfer(batch).join();
        assertThat(result.getTransferState()).isEqualTo(TRANSFER_FAILED);
        assertThat(result.getCauseOfFailure().get())
                .isInstanceOf(TransferSegmentException.class)
                .hasRootCauseInstanceOf(TimeoutException.class);
        assertThat(transferProcessor.getQueue()).isEmpty();
    }

    @Test
    public void testRunStateTransferMultipleConsumersSuccessful() {
        Stream<TransferBatchRequest> transferBatchRequestStream =
                IntStream.range(0, 100).boxed().map(x -> TransferBatchRequest.builder().build());

        ParallelTransferProcessor transferProcessor = new ParallelTransferProcessor(3,
                new SuccessfulBatchProcessor());

        TransferProcessorResult result =
                transferProcessor.runStateTransfer(transferBatchRequestStream).join();

        assertThat(result.getTransferState()).isEqualTo(TRANSFER_SUCCEEDED);
        assertThat(transferProcessor.getQueue()).isEmpty();
    }

    @Test
    public void testRunStateTransferMultipleConsumersOneFailsATransfer() {
        // There are 9 messages in the stream, and this batch processor fails
        // when processing a 5th one.

        Stream<TransferBatchRequest> transferBatchRequestStream =
                IntStream.range(0, 9).boxed().map(x -> TransferBatchRequest.builder().build());

        FaultyBatchProcessor faultyBatchProcessor =
                new FaultyBatchProcessor(5);


        ParallelTransferProcessor transferProcessor = new ParallelTransferProcessor(3,
                faultyBatchProcessor);

        TransferProcessorResult result =
                transferProcessor.runStateTransfer(transferBatchRequestStream).join();

        assertThat(result.getTransferState()).isEqualTo(TRANSFER_FAILED);
        assertThat(result.getCauseOfFailure()).isPresent();
        assertThat(result.getCauseOfFailure().get()).isInstanceOf(TransferSegmentException.class)
                .hasRootCauseInstanceOf(StateTransferBatchProcessorException.class);
        assertThat(transferProcessor.getQueue()).isEmpty();

    }

    @Test
    public void testRunStateTransferMultipleConsumersFailAndDontBlockOnProducerTimeout() {
        // There are 9 messages in the stream, and this batch processor fails
        // when processing a 5th one. Producer's timeout to offer a request is 3600 seconds.
        // Make sure that the state transfer task fails before that.

        Stream<TransferBatchRequest> transferBatchRequestStream =
                IntStream.range(0, 9).boxed().map(x -> TransferBatchRequest.builder().build());

        FaultyBatchProcessor faultyBatchProcessor =
                new FaultyBatchProcessor(5);

        ParallelTransferProcessor transferProcessor = new ParallelTransferProcessor(3,
                faultyBatchProcessor, 3600L, 2L);

        TransferProcessorResult result =
                transferProcessor.runStateTransfer(transferBatchRequestStream).join();

        assertThat(result.getTransferState()).isEqualTo(TRANSFER_FAILED);
        assertThat(result.getCauseOfFailure()).isPresent();
        assertThat(result.getCauseOfFailure().get()).isInstanceOf(TransferSegmentException.class)
                .hasRootCauseInstanceOf(StateTransferBatchProcessorException.class);
        assertThat(transferProcessor.getQueue()).isEmpty();
    }

    @Test
    public void testRunStateTransferProducerTimesOutOfferingDataStateTransferDoesNotBlock() {
        // Producer waits for 1 second before timing out, but it takes consumer 3600 seconds to process
        // the task. Producer offers request1 -> enqueue and deque immediately,
        // producer offers request2 -> enqueue, producer offers request3 -> time out.
        // State transfer does not block for 3600 seconds.
        SuccessfulBatchProcessor slowProcessor = new SuccessfulBatchProcessor(Optional.of(3600000L));
        ParallelTransferProcessor transferProcessor = new ParallelTransferProcessor(1,
                slowProcessor, 1L, 2L);


        TransferBatchRequest request1 = TransferBatchRequest.builder().build();
        TransferBatchRequest request2 = TransferBatchRequest.builder().build();
        TransferBatchRequest request3 = TransferBatchRequest.builder().build();
        Stream<TransferBatchRequest> requestStream = Stream.of(request1, request2, request3);

        CompletableFuture<TransferProcessorResult> future =
                transferProcessor.runStateTransfer(requestStream);

        TransferProcessorResult result = future.join();
        assertThat(result.getTransferState()).isEqualTo(TRANSFER_FAILED);
        assertThat(result.getCauseOfFailure()).isPresent();
        assertThat(result.getCauseOfFailure().get())
                .isInstanceOf(TransferSegmentException.class)
                .hasRootCauseInstanceOf(TimeoutException.class);
        assertThat(transferProcessor.getQueue()).isEmpty();
    }




}