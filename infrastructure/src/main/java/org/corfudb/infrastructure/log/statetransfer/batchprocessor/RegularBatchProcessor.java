package org.corfudb.infrastructure.log.statetransfer.batchprocessor;

import org.corfudb.common.result.Result;
import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.infrastructure.log.statetransfer.batch.Batch;
import org.corfudb.protocols.wireprotocol.IMetadata;
import org.corfudb.protocols.wireprotocol.LogData;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.corfudb.runtime.view.Address.*;

public interface RegularBatchProcessor {

    CompletableFuture<Result<Long, StateTransferException>> transfer(Batch batch);

    /**
     * Appends records to the stream log.
     *
     * @param dataEntries The list of entries (data or garbage).
     * @return A result of a record append, containing the max written address.
     */
    default Result<Long, StateTransferException>
    writeRecords(List<LogData> dataEntries, StreamLog streamlog) {
        return Result.of(() -> {
            streamlog.append(dataEntries);
            Optional<Long> maxWrittenAddress =
                    dataEntries.stream()
                            .map(IMetadata::getGlobalAddress)
                            .max(Comparator.comparing(Long::valueOf));
            return maxWrittenAddress.orElse(NON_ADDRESS);
        }).mapError(StateTransferException::new);
    }
}
