package org.corfudb.infrastructure.log.statetransfer.batchprocessor;

import lombok.Getter;
import lombok.NonNull;
import org.corfudb.infrastructure.log.statetransfer.StateTransferException;

import java.util.ArrayList;
import java.util.List;

/**
 * An error that should be handled by the batch processor instance.
 */
@Getter
public class BatchProcessorError extends StateTransferException {
    @NonNull
    private final List<Long> addresses;

    public BatchProcessorError(List<Long> addresses) {
        this.addresses = addresses;
    }

    public BatchProcessorError(Throwable cause) {
        super(cause);
        addresses = new ArrayList<>();
    }
}
