package org.corfudb.infrastructure.log.statetransfer.batchprocessor;

import com.google.common.collect.Ordering;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.result.Result;
import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.infrastructure.log.statetransfer.exceptions.IncompleteDataReadException;
import org.corfudb.infrastructure.log.statetransfer.exceptions.IncompleteReadException;
import org.corfudb.infrastructure.log.statetransfer.exceptions.RejectedAppendException;
import org.corfudb.infrastructure.log.statetransfer.exceptions.StateTransferException;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.view.RuntimeLayout;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

@Slf4j
public class RegularBatchProcessor extends TransferBatchProcessor {


    public RegularBatchProcessor(StreamLog streamLog) {
        super(streamLog);
    }

    @Override
    public Supplier<CompletableFuture<Result<Long, StateTransferException>>> getErrorHandlingPipeline(StateTransferException exception, RuntimeLayout runtimeLayout) {
        if (exception instanceof IncompleteReadException) {
            IncompleteReadException incompleteReadException = (IncompleteDataReadException) exception;
            List<Long> missingAddresses =
                    Ordering.natural().sortedCopy(incompleteReadException.getMissingAddresses());
            return () -> transfer(missingAddresses, null, runtimeLayout);

        } else {
            RejectedAppendException rejectedAppendException = (RejectedAppendException) exception;
            List<LogData> missingData = rejectedAppendException.getDataEntries();
            return () -> CompletableFuture.supplyAsync(() -> writeData(missingData));
        }
    }

    /**
     * Read data -> write data.
     *
     * @param addresses     A batch of consecutive addresses.
     * @param server        A server to read garbage from.
     * @param runtimeLayout A runtime layout to use for connections.
     * @return Result of an empty value if a pipeline executes correctly;
     * a result containing the first encountered error otherwise.
     */
    public CompletableFuture<Result<Long, StateTransferException>> transfer(List<Long> addresses, String server, RuntimeLayout runtimeLayout) {
        return CompletableFuture.supplyAsync(() -> readRecords(addresses, runtimeLayout))
                .thenApply(records -> records.flatMap(this::writeData));
    }

}
