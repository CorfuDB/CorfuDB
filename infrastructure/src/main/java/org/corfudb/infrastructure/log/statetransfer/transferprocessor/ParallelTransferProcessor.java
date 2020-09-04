package org.corfudb.infrastructure.log.statetransfer.transferprocessor;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchRequest;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.StateTransferBatchProcessor;
import org.corfudb.infrastructure.log.statetransfer.exceptions.StateTransferBatchProcessorException;
import org.corfudb.infrastructure.log.statetransfer.exceptions.TransferSegmentException;
import org.corfudb.util.CFUtils;

import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.stream.Stream;

import static org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchResponse.TransferStatus.FAILED;
import static org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchResponse.TransferStatus.SUCCEEDED;
import static org.corfudb.infrastructure.log.statetransfer.transferprocessor.TransferProcessorResult.TransferProcessorStatus.TRANSFER_FAILED;
import static org.corfudb.infrastructure.log.statetransfer.transferprocessor.TransferProcessorResult.TransferProcessorStatus.TRANSFER_SUCCEEDED;

/**
 * A transfer processor that performs state transfer by distributing and parallelizing
 * the workload among the source log unit servers of each segment.
 */
@Slf4j
public class ParallelTransferProcessor {
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
                                                       Semaphore semaphore)
            throws InterruptedException {
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

        return allFutures;
    }

    /**
     * Transfer a segment in parallel.
     *
     * @param batchStream A stream of batch requests.
     * @param parFactor   A number of available nodes for this segment to distribute a workload over.
     * @return A future of transfer processor result.
     */
    public CompletableFuture<TransferProcessorResult> runStateTransfer
    (Stream<TransferBatchRequest> batchStream, int parFactor) {
        // Get a transfer batch request stream iterator
        Iterator<TransferBatchRequest> iterator = batchStream.iterator();

        // No need to spawn tasks if there is no work to do.
        if (!iterator.hasNext()) {
            return CompletableFuture.completedFuture(TransferProcessorResult.builder()
                    .transferState(TRANSFER_SUCCEEDED).build());
        }

        CompletableFuture<Void> allFutures = CompletableFuture.completedFuture(null);
        if (parFactor <= 0) {
            allFutures = handleNonTransferException(allFutures,
                    new IllegalArgumentException("Number of nodes should be > 0"));
        } else {
            // The number of available permits is NUM_REQUESTS_PER_NODE * parFactor.
            Semaphore semaphore = new Semaphore(parFactor * NUM_REQUESTS_PER_NODE, true);
            while (iterator.hasNext() && !allFutures.isCompletedExceptionally()) {
                try {
                    TransferBatchRequest request = iterator.next();
                    allFutures = handleBatchRequest(request, allFutures, semaphore);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    allFutures = handleNonTransferException(allFutures, ie);
                }
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
