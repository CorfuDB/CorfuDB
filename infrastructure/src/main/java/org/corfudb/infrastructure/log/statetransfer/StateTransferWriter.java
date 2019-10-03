package org.corfudb.infrastructure.log.statetransfer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.result.Result;
import org.corfudb.infrastructure.log.statetransfer.transferbatchprocessor.RegularBatchProcessor;
import org.corfudb.infrastructure.log.statetransfer.exceptions.StateTransferException;
import org.corfudb.infrastructure.log.statetransfer.exceptions.StateTransferFailure;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.AddressSpaceView;
import org.corfudb.runtime.view.RuntimeLayout;
import org.corfudb.util.CFUtils;
import java.util.AbstractMap.SimpleEntry;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * This class is responsible for reading from the remote log units and writing to the local log.
 */
@Slf4j
@AllArgsConstructor
public class StateTransferWriter {

    @Getter
    @NonNull
    private final RegularBatchProcessor batchProcessor;

    public CompletableFuture<Result<Long, StateTransferException>> stateTransfer(List<Long> addresses,
                                                                                 int readSize) {
        List<CompletableFuture<Result<Long, StateTransferException>>> listOfFutureResults =
                Lists.partition(addresses, readSize).stream().map(batch -> batchProcessor
                        .transfer(batch)
                        .thenCompose(transferResult ->
                                batchProcessor.handlePossibleTransferFailures(transferResult,
                                        new AtomicInteger()))).collect(Collectors.toList());
        return coalesceResults(listOfFutureResults);
    }

    CompletableFuture<Result<Long, StateTransferException>> coalesceResults
            (List<CompletableFuture<Result<Long, StateTransferException>>> allResults) {
        CompletableFuture<List<Result<Long, StateTransferException>>> futureOfListResults =
                CFUtils.sequence(allResults);

        CompletableFuture<Optional<Result<Long, StateTransferException>>> possibleSingleResult = futureOfListResults
                .thenApply(multipleResults ->
                        multipleResults
                                .stream()
                                .reduce(this::mergeBatchResults));

        return possibleSingleResult.thenApply(result -> result.orElseGet(() ->
                new Result<>(-1L, new StateTransferFailure("Coalesced transfer batch result is empty."))));

    }

    Result<Long, StateTransferException> mergeBatchResults(Result<Long, StateTransferException> firstResult,
                                                                          Result<Long, StateTransferException> secondResult){
        return firstResult.flatMap(firstMaxAddressTransferred ->
                secondResult.map(secondMaxAddressTransferred ->
                        Math.max(firstMaxAddressTransferred, secondMaxAddressTransferred)));
    }
}
