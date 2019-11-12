package org.corfudb.infrastructure.log.statetransfer.batch;

import lombok.Builder;
import lombok.Builder.Default;
import lombok.EqualsAndHashCode;
import lombok.EqualsAndHashCode.Exclude;
import lombok.Getter;
import org.corfudb.infrastructure.log.statetransfer.exceptions.StateTransferBatchProcessorException;

import java.util.Optional;

import static org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchResponse.TransferStatus.SUCCEEDED;

/**
 * A result of a transfer of a batch of addresses.
 * If completed successfully, contains a status SUCCEEDED,
 * a {@link #transferBatchRequest} that contains a list of addresses transferred,
 * and an optional destination server.
 * If completed exceptionally, contains a status FAILED, and a cause of failure,
 * a {@link #transferBatchRequest} that contains an empty list,
 * and an optional destination server.
 */
@Builder(toBuilder = true)
@Getter
@EqualsAndHashCode
public class TransferBatchResponse {

    public enum TransferStatus {
        SUCCEEDED,
        FAILED
    }

    /**
     * An original request.
     */
    @Default
    @Getter
    private final TransferBatchRequest transferBatchRequest = TransferBatchRequest.builder().build();
    /**
     * A success status.
     */
    @Default
    private final TransferStatus status = SUCCEEDED;

    /**
     * An optional exception if the transfer batch has failed.
     */
    @Default
    @Exclude
    private final Optional<StateTransferBatchProcessorException> causeOfFailure = Optional.empty();

}
