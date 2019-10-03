package org.corfudb.infrastructure.log.statetransfer;

import org.corfudb.common.result.Result;
import org.corfudb.infrastructure.log.statetransfer.StateTransferManager;
import org.corfudb.infrastructure.log.statetransfer.exceptions.StateTransferException;
import org.corfudb.infrastructure.log.statetransfer.exceptions.StateTransferFailure;
import org.corfudb.infrastructure.log.statetransfer.transferbatchprocessor.RegularBatchProcessor;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

import static org.mockito.Mockito.mock;

public class StateTransferWriterTest {

    @Test
    public void testMergeBatchResults(){
        RegularBatchProcessor batchProcessor = mock(RegularBatchProcessor.class);
        StateTransferWriter stateTransferWriter = new StateTransferWriter(batchProcessor);
        // first is ok, second is ok
        Result<Long, StateTransferException> first =
                Result.ok(0L).mapError(x -> new StateTransferException());
        Result<Long, StateTransferException> second =
                Result.ok(3L).mapError(x -> new StateTransferException());
        assertThat(stateTransferWriter.mergeBatchResults(first, second).get()).isEqualTo(3L);
        // first is failure, second is ok
        first = Result.error(new StateTransferFailure());
        assertThat(stateTransferWriter.mergeBatchResults(first, second).getError())
                .isInstanceOf(StateTransferFailure.class);
        // first is ok, second is failure
        first = Result.ok(0L).mapError(x -> new StateTransferException());
        second = Result.error(new StateTransferFailure());
        assertThat(stateTransferWriter.mergeBatchResults(first, second).getError())
                .isInstanceOf(StateTransferFailure.class);
        // first is failure, second is failure
        first = Result.error(new StateTransferFailure());
        assertThat(stateTransferWriter.mergeBatchResults(first, second).getError())
                .isInstanceOf(StateTransferFailure.class);

    }

    @Test
    public void testCoalesceResults(){
        RegularBatchProcessor batchProcessor = mock(RegularBatchProcessor.class);
        StateTransferWriter stateTransferWriter = new StateTransferWriter(batchProcessor);
        List<CompletableFuture<Result<Long, StateTransferException>>> resultSet = Stream.of(0L, 1L, 2L)
                .map(x -> CompletableFuture
                        .completedFuture(Result.ok(x).mapError(y -> new StateTransferException())))
                .collect(Collectors.toList());
        // all ok
        CompletableFuture<Result<Long, StateTransferException>> finalResult =
                stateTransferWriter.coalesceResults(resultSet);
        assertThat(finalResult.join().get()).isEqualTo(2L);

        // one is a failure
        resultSet.add(CompletableFuture.completedFuture(Result.error(new StateTransferFailure())));
        finalResult = stateTransferWriter.coalesceResults(resultSet);
        assertThat(finalResult.join().getError()).isInstanceOf(StateTransferFailure.class);

        // one is completed exceptionally
        resultSet.remove(resultSet.size() - 1);
        CompletableFuture<Result<Long, StateTransferException>> exceptional = CompletableFuture.supplyAsync(() -> {
            throw new RuntimeException();
        });

        resultSet.add(exceptional);

        resultSet.add(CompletableFuture
                .completedFuture(Result.ok(3L).mapError(y -> new StateTransferException())));

        finalResult = stateTransferWriter.coalesceResults(resultSet);
        assertThat(finalResult).isCompletedExceptionally();

        // the input is empty
        CompletableFuture<Result<Long, StateTransferException>> future =
                stateTransferWriter.coalesceResults(new ArrayList<>());
        assertThat(future.join().getError()).isInstanceOf(StateTransferFailure.class);
    }

    @Test
    public void testTransfer(){
        // no issues

        // read retries come through

        // read retries fail

        // all writes already written

        // some writes already written

        // some writes fail with unknown exception

        // some transfer failed exceptionally



    }
}
