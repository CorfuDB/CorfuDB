package org.corfudb.infrastructure.log.statetransfer.batch;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.corfudb.common.result.Result;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.BatchProcessorFailure;

@AllArgsConstructor
@Getter
public class BatchResult {
    private final Result<BatchResultData, BatchProcessorFailure> result;
}
