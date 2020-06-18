package org.corfudb.infrastructure.log.statetransfer.transferprocessor;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchRequest;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.StateTransferBatchProcessor;
import org.corfudb.infrastructure.log.statetransfer.exceptions.StateTransferBatchProcessorException;
import org.corfudb.infrastructure.log.statetransfer.exceptions.TransferSegmentException;
import org.corfudb.util.CFUtils;

import java.util.AbstractCollection;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.stream.Stream;

import static org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchRequest.TransferBatchType.DATA;
import static org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchRequest.TransferBatchType.SEGMENT_INIT;
import static org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchResponse.TransferStatus.FAILED;
import static org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchResponse.TransferStatus.SUCCEEDED;
import static org.corfudb.infrastructure.log.statetransfer.transferprocessor.TransferProcessor.TransferProcessorResult.TransferProcessorStatus.TRANSFER_FAILED;
import static org.corfudb.infrastructure.log.statetransfer.transferprocessor.TransferProcessor.TransferProcessorResult.TransferProcessorStatus.TRANSFER_SUCCEEDED;

/**
 * A transfer processor that performs state transfer by dynamically distributing and parallelizing
 * the workload among the source log unit servers of each segment.
 */
@Slf4j
public class ParallelTransferProcessor implements TransferProcessor {
    /**
     * A state transfer batch processor that performs the batch transfer.
     */
    private final StateTransferBatchProcessor stateTransferBatchProcessor;

    /**
     * A number of in-flight requests per one node.
     */
    private static final int NUM_REQUESTS_PER_NODE = 5;

    public ParallelTransferProcessor(StateTransferBatchProcessor stateTransferBatchProcessor) {
        this.stateTransferBatchProcessor = stateTransferBatchProcessor;
    }

    private CompletableFuture<Void> handleNonTransferException(CompletableFuture<Void> futures,
                                                               Throwable ex) {
        CompletableFuture<Void> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(ex);
        return CFUtils.allOfOrTerminateExceptionally(futures, failedFuture);
    }

    private CompletableFuture<Void> handleBatchRequest(TransferBatchRequest request,
                                                       CompletableFuture<Void> allFutures,
                                                       Semaphore semaphore) throws InterruptedException {
        // If the request is of type SEGMENT_INIT - it signals to the batch processor that the
        // consecutive DATA requests all belong to the same segment. Since all the DATA requests
        // that belong to the same segment have the same number of source nodes, we can dynamically
        // configure the semaphore to support the average parallelism level for the entire segment
        // which is equal to the NUM_REQUEST_PER_NODE * number of source nodes in the segment.
        if (request.getBatchType() == SEGMENT_INIT) {
            int numPermits = request.getDestinationNodes()
                    .map(AbstractCollection::size).orElse(1) *
                    NUM_REQUESTS_PER_NODE;
            if (numPermits == 0) {
                throw new IllegalStateException("Number of permits should be equal to" +
                        " at least one.");
            }
            log.trace("Starting a new segment transfer. Parallelism level: {}", numPermits);
            // Acquire all the permits if any.
            semaphore.drainPermits();
            // Release numPermits for the batch transfer futures to acquire.
            semaphore.release(numPermits);
        }
        else if (request.getBatchType() == DATA) {
            semaphore.acquire();
            CompletableFuture<Void> batchTransferResult =
                    stateTransferBatchProcessor
                            .transfer(request)
                            .thenApply(response -> {
                                semaphore.release();
                                if (response.getStatus() == SUCCEEDED) {
                                    return null;
                                } else if (response.getStatus() == FAILED &&
                                        response.getCauseOfFailure().isPresent()) {
                                    throw response.getCauseOfFailure().get();
                                } else {
                                    throw new StateTransferBatchProcessorException();
                                }
                            });

            allFutures = CFUtils.allOfOrTerminateExceptionally(allFutures, batchTransferResult);
        }
        else {
            throw new IllegalStateException("Unrecognized batch type: " + request.getBatchType());
        }

        return allFutures;
    }

    @Override
    public CompletableFuture<TransferProcessorResult> runStateTransfer
            (Stream<TransferBatchRequest> batchStream) {
        // Get a transfer batch request stream iterator
        Iterator<TransferBatchRequest> iterator = batchStream.iterator();

        // No need to spawn tasks if there is no work to do.
        if (!iterator.hasNext()) {
            return CompletableFuture.completedFuture(TransferProcessorResult.builder()
                    .transferState(TRANSFER_SUCCEEDED).build());
        }
        // The number of available permits will be configured dynamically as the transfer
        // progresses based on the number of source log unit servers in each segment.
        Semaphore semaphore = new Semaphore(NUM_REQUESTS_PER_NODE, true);
        CompletableFuture<Void> allFutures = CompletableFuture.completedFuture(null);

        while (iterator.hasNext() && !allFutures.isCompletedExceptionally()) {
            try {
                TransferBatchRequest request = iterator.next();
                allFutures = handleBatchRequest(request, allFutures, semaphore);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                allFutures = handleNonTransferException(allFutures, ie);
            } catch (IllegalStateException ise) {
                allFutures = handleNonTransferException(allFutures, ise);
            }
        }

        return allFutures.handle((res, ex) -> {
            if (ex == null) {
                return TransferProcessorResult.builder().transferState(TRANSFER_SUCCEEDED).build();
            }

            return TransferProcessorResult
                    .builder()
                    .transferState(TRANSFER_FAILED)
                    .causeOfFailure(Optional.of(new TransferSegmentException(ex)))
                    .build();
        });
    }
}
