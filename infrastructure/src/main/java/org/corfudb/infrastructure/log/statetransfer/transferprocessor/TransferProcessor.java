package org.corfudb.infrastructure.log.statetransfer.transferprocessor;

import lombok.Builder;
import lombok.Builder.Default;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchRequest;
import org.corfudb.infrastructure.log.statetransfer.exceptions.TransferSegmentException;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import static lombok.EqualsAndHashCode.Exclude;
import static org.corfudb.infrastructure.log.statetransfer.transferprocessor.TransferProcessor.TransferProcessorResult.TransferProcessorStatus.TRANSFER_SUCCEEDED;

/**
 * An interface that runs the actual state transfer on the stream of transfer batch requests.
 */
public interface TransferProcessor {

    @Getter
    @ToString
    @EqualsAndHashCode
    @Builder(toBuilder = true)
    class TransferProcessorResult {
        public enum TransferProcessorStatus {
            TRANSFER_FAILED,
            TRANSFER_SUCCEEDED
        }

        @Default
        private final TransferProcessorStatus transferState = TRANSFER_SUCCEEDED;

        @Default
        @Exclude
        private final Optional<TransferSegmentException> causeOfFailure = Optional.empty();
    }

    /**
     * Run state transfer on the stream of TransferBatchRequests and produce a future
     * of a TransferProcessorResult.
     *
     * @param batchStream A stream of TransferBatchRequest.
     * @return A future of TransferProcessorResult with a transferState of TRANSFER_SUCCEEDED
     * if the entire transfer succeeded and with a transferState of TRANSFER_FAILED and the
     * causeOfFailure otherwise.
     */
    CompletableFuture<TransferProcessorResult> runStateTransfer(Stream<TransferBatchRequest>
                                                                        batchStream);
}
