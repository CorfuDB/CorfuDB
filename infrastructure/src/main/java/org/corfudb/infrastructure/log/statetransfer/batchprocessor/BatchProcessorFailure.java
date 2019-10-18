package org.corfudb.infrastructure.log.statetransfer.batchprocessor;


import lombok.Builder;
import lombok.Getter;
import org.corfudb.infrastructure.log.statetransfer.StateTransferException;

import javax.annotation.Nullable;
import java.util.List;

/**
 * An exception that is propagated to the caller of the batch processor,
 * after the batch processor failed to handle it.
 */
@Getter
@Builder
public class BatchProcessorFailure extends StateTransferException {

    private String endpoint;

    private List<Long> addresses;

    private Throwable throwable;

}
