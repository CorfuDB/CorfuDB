package org.corfudb.infrastructure.log.statetransfer.batchprocessor;


import com.google.common.collect.ImmutableList;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.corfudb.infrastructure.log.statetransfer.StateTransferException;

import java.util.List;
import java.util.Optional;

/**
 * A state transfer exception that is propagated to the caller of a batch processor,
 * after a batch processor failed to handle a batch transfer.
 */
@Getter
@Builder
@ToString
public class BatchProcessorFailure extends StateTransferException {

    /**
     * An endpoint from which the data was supposed to be read (if known in advance).
     */
    @Builder.Default
    private final Optional<String> endpoint = Optional.empty();

    /**
     * A batch of addresses that were supposed to get transferred.
     */
    @Builder.Default
    private final List<Long> addresses = ImmutableList.of();

    /**
     * An exception that a batch processor could not handle.
     */
    @Builder.Default
    private final Throwable throwable = new IllegalStateException();

}
