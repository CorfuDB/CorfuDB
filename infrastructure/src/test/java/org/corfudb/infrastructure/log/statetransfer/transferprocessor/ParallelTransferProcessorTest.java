package org.corfudb.infrastructure.log.statetransfer.transferprocessor;

import com.google.common.collect.ImmutableList;
import org.corfudb.infrastructure.log.statetransfer.FaultyBatchProcessor;
import org.corfudb.infrastructure.log.statetransfer.SuccessfulBatchProcessor;
import org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchRequest;
import org.corfudb.infrastructure.log.statetransfer.exceptions.StateTransferBatchProcessorException;
import org.corfudb.infrastructure.log.statetransfer.exceptions.TransferSegmentException;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.infrastructure.log.statetransfer.transferprocessor.TransferProcessorResult.TransferProcessorStatus.TRANSFER_FAILED;
import static org.corfudb.infrastructure.log.statetransfer.transferprocessor.TransferProcessorResult.TransferProcessorStatus.TRANSFER_SUCCEEDED;


class ParallelTransferProcessorTest {


    private final Stream<TransferBatchRequest> createStream(int numBatches,
                                                            Optional<ImmutableList<String>> nodes) {
        return IntStream.range(0, numBatches)
                .boxed()
                .map(x -> TransferBatchRequest
                        .builder().destinationNodes(nodes)
                        .build());
    }

    @Test
    void testRunEmptyStream() {
        Stream<TransferBatchRequest> emptyBatchStream = createStream(0, Optional.empty());
        ParallelTransferProcessor transferProcessor =
                new ParallelTransferProcessor(new SuccessfulBatchProcessor());
        TransferProcessorResult res =
                transferProcessor.runStateTransfer(emptyBatchStream, 1).join();
        assertThat(res.getTransferState()).isEqualTo(TRANSFER_SUCCEEDED);
    }

    @Test
    void testRunSingleTransferError() {
        Stream<TransferBatchRequest> stream = createStream(1, Optional.empty());
        FaultyBatchProcessor faultyBatchProcessor =
                new FaultyBatchProcessor(1);
        ParallelTransferProcessor transferProcessor =
                new ParallelTransferProcessor(faultyBatchProcessor);
        CompletableFuture<TransferProcessorResult> result =
                transferProcessor.runStateTransfer(stream, 1);
        TransferProcessorResult res = result.join();
        assertThat(res.getTransferState()).isEqualTo(TRANSFER_FAILED);
        assertThat(res.getCauseOfFailure().isPresent()).isTrue();
        assertThat(res.getCauseOfFailure().get())
                .isInstanceOf(TransferSegmentException.class)
                .hasRootCauseInstanceOf(StateTransferBatchProcessorException.class);
    }

    @Test
    void testRunSingleTransferIllegalArgument() {
        Stream<TransferBatchRequest> stream = createStream(1, Optional.of(ImmutableList.of()));
        FaultyBatchProcessor faultyBatchProcessor =
                new FaultyBatchProcessor(1);
        ParallelTransferProcessor transferProcessor =
                new ParallelTransferProcessor(faultyBatchProcessor);
        CompletableFuture<TransferProcessorResult> result =
                transferProcessor.runStateTransfer(stream, 0);
        TransferProcessorResult res = result.join();
        assertThat(res.getTransferState()).isEqualTo(TRANSFER_FAILED);
        assertThat(res.getCauseOfFailure().isPresent()).isTrue();
        assertThat(res.getCauseOfFailure().get())
                .isInstanceOf(TransferSegmentException.class)
                .hasRootCauseInstanceOf(IllegalArgumentException.class);
    }


    @Test
    void testRunSingleTransferSuccess() {
        SuccessfulBatchProcessor batchProcessor =
                new SuccessfulBatchProcessor();
        Stream<TransferBatchRequest> stream = createStream(1, Optional.empty());
        ParallelTransferProcessor transferProcessor = new ParallelTransferProcessor(batchProcessor);
        CompletableFuture<TransferProcessorResult> result =
                transferProcessor.runStateTransfer(stream, 1);
        TransferProcessorResult res = result.join();
        assertThat(res.getTransferState()).isEqualTo(TRANSFER_SUCCEEDED);
    }

    @Test
    void testRunParallelTransferSuccess() {
        // Batch processor waits for 300 milliseconds before processing a request.
        SuccessfulBatchProcessor batchProcessor =
                new SuccessfulBatchProcessor(Optional.of(300L));

        // There are 100 requests in the stream and 2 * 5 = 10 in-flight requests are allowed.
        Stream<TransferBatchRequest> stream =
                createStream(100, Optional.of(ImmutableList.of("node1", "node2")));
        ParallelTransferProcessor transferProcessor = new ParallelTransferProcessor(batchProcessor);
        CompletableFuture<TransferProcessorResult> result =
                transferProcessor.runStateTransfer(stream, 2);
        TransferProcessorResult res = result.join();
        assertThat(res.getTransferState()).isEqualTo(TRANSFER_SUCCEEDED);
    }

    @Test
    void testRunParallelTransferFailure() {
        // Batch processor waits for 300 milliseconds before processing a request.
        // Batch processor fails on the 9th request, which should fail the transfer.
        FaultyBatchProcessor batchProcessor =
                new FaultyBatchProcessor(9, Optional.of(300L));
        // There are 1000 requests in the stream and 3 * 5 = 15 in-flight requests are allowed.
        Stream<TransferBatchRequest> stream =
                IntStream.range(0, 1000).boxed().map(x -> TransferBatchRequest
                        .builder().destinationNodes(Optional.of(ImmutableList.of("node1", "node2", "node3")))
                        .build());
        ParallelTransferProcessor transferProcessor = new ParallelTransferProcessor(batchProcessor);
        CompletableFuture<TransferProcessorResult> result =
                transferProcessor.runStateTransfer(stream, 3);
        TransferProcessorResult res = result.join();
        assertThat(res.getTransferState()).isEqualTo(TRANSFER_FAILED);
        assertThat(res.getCauseOfFailure().isPresent()).isTrue();
        assertThat(res.getCauseOfFailure().get())
                .isInstanceOf(TransferSegmentException.class)
                .hasRootCauseInstanceOf(StateTransferBatchProcessorException.class);
    }

    @Test
    void testRunParallelTransferShortCircuitsOnFailure() {
        // There are 30 requests in the stream and 5 x 2 = 10 in-flight requests are allowed.
        // The processing time of every batch is 2 seconds.
        // The transfer fails on the 13th batch.
        // Make sure that the transfer short-circuits immediately. The number of processed batches
        // is less than 30.
        final int numBatches = 30;
        FaultyBatchProcessor batchProcessor =
                new FaultyBatchProcessor(13, Optional.of(2000L));
        Stream<TransferBatchRequest> stream =
                createStream(numBatches, Optional.of(ImmutableList.of("node1", "node2")));
        ParallelTransferProcessor transferProcessor = new ParallelTransferProcessor(batchProcessor);
        CompletableFuture<TransferProcessorResult> result =
                transferProcessor.runStateTransfer(stream, 2);
        TransferProcessorResult res = result.join();
        assertThat(res.getTransferState()).isEqualTo(TRANSFER_FAILED);
        assertThat(batchProcessor.getTotalProcessed().get() < numBatches).isTrue();
    }
}