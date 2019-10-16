package org.corfudb.infrastructure.log.statetransfer;

import com.google.common.collect.Lists;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.result.Result;
import org.corfudb.infrastructure.log.statetransfer.StateTransferManager.CommittedTransferData;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.BatchTransferPlan;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.RegularBatchProcessor;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.StateTransferException;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.StateTransferFailure;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.protocolbatchprocessor.ProtocolTransferPlan;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.weightedbatchprocessor.WeightedRoundRobinTransferPlan;
import org.corfudb.util.CFUtils;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.corfudb.runtime.view.Address.*;

/**
 * This class is responsible for reading from the remote log units and writing to the local log.
 */
@Slf4j
@AllArgsConstructor
public class StateTransferPlanner {

    @Getter
    @NonNull
    private final RegularBatchProcessor batchProcessor;

    public CompletableFuture<Result<Long, StateTransferException>> stateTransfer(List<Long> addresses,
                                                                                 int readSize,
                                                                                 CommittedTransferData transferData) {
        List<CompletableFuture<Result<Long, StateTransferException>>> listOfFutureResults =
                Lists.partition(addresses, readSize).stream().map(batch ->
                {
                    Optional<CommittedTransferData> data = Optional.ofNullable(transferData);
                    BatchTransferPlan plan;
                    if (data.isPresent()) {
                        plan = new WeightedRoundRobinTransferPlan(batch, data.get().getSourceServers());
                    } else {
                        plan = new ProtocolTransferPlan(batch);
                    }

                    return batchProcessor.transfer(plan);
                }).collect(Collectors.toList());
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
                new Result<>(NON_ADDRESS,
                        new StateTransferFailure("Coalesced transfer batch result is empty."))));

    }

    Result<Long, StateTransferException> mergeBatchResults(Result<Long, StateTransferException> firstResult,
                                                           Result<Long, StateTransferException> secondResult) {
        return firstResult.flatMap(firstMaxAddressTransferred ->
                secondResult.map(secondMaxAddressTransferred ->
                        Math.max(firstMaxAddressTransferred, secondMaxAddressTransferred)));
    }
}
