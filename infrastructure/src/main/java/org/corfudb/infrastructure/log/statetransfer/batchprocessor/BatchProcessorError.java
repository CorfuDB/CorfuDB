package org.corfudb.infrastructure.log.statetransfer.batchprocessor;

import lombok.Getter;
import lombok.NonNull;
import org.corfudb.infrastructure.log.statetransfer.StateTransferException;

import java.util.ArrayList;
import java.util.List;

/**
 * An exception that should be handled by the batch processor.
 */
@Getter
public class BatchProcessorError extends StateTransferException {
    @NonNull
    private List<Long> addresses;

    public BatchProcessorError(List<Long> addresses, Throwable cause) {
        super(cause);
        this.addresses = addresses;
    }

    public BatchProcessorError(List<Long> addresses) {
        this.addresses = addresses;
    }

    public BatchProcessorError(Throwable cause) {
        super(cause);
        addresses = new ArrayList<>();
    }
}
