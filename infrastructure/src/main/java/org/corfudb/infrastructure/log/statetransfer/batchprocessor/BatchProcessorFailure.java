package org.corfudb.infrastructure.log.statetransfer.batchprocessor;


import lombok.Builder;
import lombok.Getter;
import org.corfudb.infrastructure.log.statetransfer.StateTransferException;
import java.util.List;
import java.util.Optional;

/**
 * A state transfer exception that is propagated to the caller of a batch processor,
 * after a batch processor failed to handle a batch transfer.
 */
@Getter
@Builder
public class BatchProcessorFailure extends StateTransferException {

    /**
     * An endpoint from which the data was supposed to be read (if known in advance).
     */
    private final Optional<String> endpoint;

    /**
     * A batch of addresses that were supposed to get transferred.
     */
    private final List<Long> addresses;

    /**
     * An exception that a batch processor could not handle.
     */
    private final Throwable throwable;

}
