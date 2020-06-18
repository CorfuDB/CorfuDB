package org.corfudb.infrastructure.log.statetransfer.transferprocessor;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchRequest;
import org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchResponse;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.StateTransferBatchProcessor;
import org.corfudb.infrastructure.log.statetransfer.exceptions.TransferSegmentException;

import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import static org.corfudb.infrastructure.log.statetransfer.transferprocessor.TransferProcessor.TransferProcessorResult.TransferProcessorStatus.TRANSFER_FAILED;
import static org.corfudb.infrastructure.log.statetransfer.transferprocessor.TransferProcessor.TransferProcessorResult.TransferProcessorStatus.TRANSFER_SUCCEEDED;


/**
 * A transfer processor that runs a state transfer one batch a time.
 */
@AllArgsConstructor
@Slf4j
public class BasicTransferProcessor implements TransferProcessor {

    private final StateTransferBatchProcessor batchProcessor;

    @Override
    public CompletableFuture<TransferProcessorResult> runStateTransfer(
            Stream<TransferBatchRequest> batchStream) {
        Iterator<TransferBatchRequest> iterator = batchStream.iterator();
        return CompletableFuture.supplyAsync(() -> {
            while (iterator.hasNext()) {
                TransferBatchRequest request = iterator.next();

                if (request.getBatchType() == TransferBatchRequest.TransferBatchType.SEGMENT_INIT) {
                    // For a basic transfer processor we don't care whether the consecutive batches
                    // belong to a certain segment since we transfer all of them
                    // via replication protocol.
                    log.trace("Starting a new segment transfer.");
                } else if (request.getBatchType() == TransferBatchRequest.TransferBatchType.DATA) {
                    TransferBatchResponse result = batchProcessor.transfer(request).join();
                    if (result.getStatus() == TransferBatchResponse.TransferStatus.FAILED) {
                        String errorMessage = "Failed to transfer: " +
                                result.getTransferBatchRequest();
                        TransferSegmentException transferSegmentException = result
                                .getCauseOfFailure().map(ex -> new TransferSegmentException(errorMessage, ex))
                                .orElse(new TransferSegmentException(errorMessage));
                        return TransferProcessorResult.builder()
                                .causeOfFailure(Optional.of(transferSegmentException))
                                .transferState(TRANSFER_FAILED)
                                .build();
                    }
                } else {
                    TransferSegmentException ex =
                            new TransferSegmentException(
                                    new IllegalStateException("Unrecognized batch type: " +
                                            request.getBatchType()));
                    return TransferProcessorResult.builder()
                            .causeOfFailure(Optional.of(ex))
                            .transferState(TRANSFER_FAILED)
                            .build();
                }

            }
            return TransferProcessorResult.builder().transferState(TRANSFER_SUCCEEDED).build();
        });
    }
}
