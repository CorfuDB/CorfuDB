package org.corfudb.infrastructure.log.statetransfer;

import com.google.common.collect.Lists;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.result.Result;
import org.corfudb.infrastructure.log.statetransfer.StateTransferManager.CommittedTransferData;
import org.corfudb.infrastructure.log.statetransfer.transferbatchprocessor.RegularBatchProcessor;
import org.corfudb.infrastructure.log.statetransfer.exceptions.StateTransferException;
import org.corfudb.infrastructure.log.statetransfer.exceptions.StateTransferFailure;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.CFUtils;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.corfudb.infrastructure.log.statetransfer.StateTransferWriter.TransferMethod.*;
import static org.corfudb.runtime.view.Address.*;

/**
 * This class is responsible for reading from the remote log units and writing to the local log.
 */
@Slf4j
@AllArgsConstructor
public class StateTransferWriter {

    @Getter
    @NonNull
    private final RegularBatchProcessor batchProcessor;

    @AllArgsConstructor
    @Getter
    public class BatchTransferPlan {
        private final List<Long> transferAddresses;
    }

    @Getter
    public class ProtocolTransferPlan extends BatchTransferPlan {
        public ProtocolTransferPlan(List<Long> transferAddresses) {
            super(transferAddresses);
        }
    }

    @Getter
    public class ManyToOneReadTransferPlan extends BatchTransferPlan {

        private final List<String> exclusiveEndpoints;

        public ManyToOneReadTransferPlan(List<Long> transferAddresses, List<String> exclusiveEndpoints) {
            super(transferAddresses);
            this.exclusiveEndpoints = exclusiveEndpoints;
        }
    }

    public CompletableFuture<Result<Long, StateTransferException>> stateTransfer(List<Long> addresses,
                                                                                 int readSize,
                                                                                 CommittedTransferData transferData) {
        List<CompletableFuture<Result<Long, StateTransferException>>> listOfFutureResults =
                Lists.partition(addresses, readSize).stream().map(batch ->
                {
                    Optional<CommittedTransferData> data = Optional.ofNullable(transferData);
                    BatchTransferPlan plan;
                    if (data.isPresent()) {
                        plan = new ManyToOneReadTransferPlan(batch, data.get().getSourceServers());
                    } else {
                        plan = new ProtocolTransferPlan(batch);
                    }

                    return batchProcessor.transfer(plan)
                            .thenCompose(transferResult ->
                                    batchProcessor.handlePossibleTransferFailures(transferResult,
                                            new AtomicInteger()));
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
