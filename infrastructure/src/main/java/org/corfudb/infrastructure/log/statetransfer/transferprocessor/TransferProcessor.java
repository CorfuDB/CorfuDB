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

    CompletableFuture<TransferProcessorResult> runStateTransfer(Stream<TransferBatchRequest> batchStream);
}
