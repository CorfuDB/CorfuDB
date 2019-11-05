package org.corfudb.infrastructure.log.statetransfer.batch;

import lombok.Builder;
import lombok.Builder.Default;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import static org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchResponse.TransferStatus.SUCCEEDED;

/**
 * A result of a transfer. If completed successfully, contains a {@link #transferBatchRequest}
 * that contains a list of addresses transferred, status SUCCEEDED and an optional destination
 * server. If completed exceptionally, a {@link #transferBatchRequest} contains an empty list,
 * status FAILED and an optional destination server.
 */
@Builder
@Getter
@EqualsAndHashCode
public class TransferBatchResponse {

    public enum TransferStatus {
        SUCCEEDED,
        FAILED
    }

    @Default
    @Getter
    private final TransferBatchRequest transferBatchRequest = TransferBatchRequest.builder().build();
    @Default
    private final TransferStatus status = SUCCEEDED;

}
