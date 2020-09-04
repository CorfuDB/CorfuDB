package org.corfudb.infrastructure.log.statetransfer.transferprocessor;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.corfudb.infrastructure.log.statetransfer.exceptions.TransferSegmentException;

import java.util.Optional;

@Getter
@ToString
@EqualsAndHashCode
@Builder(toBuilder = true)
public class TransferProcessorResult {
    public enum TransferProcessorStatus {
        TRANSFER_FAILED,
        TRANSFER_SUCCEEDED
    }

    @Builder.Default
    private final TransferProcessorStatus transferState = TransferProcessorStatus.TRANSFER_SUCCEEDED;

    @Builder.Default
    @EqualsAndHashCode.Exclude
    private final Optional<TransferSegmentException> causeOfFailure = Optional.empty();
}