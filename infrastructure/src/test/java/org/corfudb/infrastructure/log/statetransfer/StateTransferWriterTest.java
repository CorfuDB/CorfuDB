package org.corfudb.infrastructure.log.statetransfer;

import com.google.common.collect.Ordering;
import org.corfudb.common.result.Result;
import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.protocolbatchprocessor.IncompleteDataReadException;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.BatchProcessorFailure;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.RegularBatchProcessor;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.view.AddressSpaceView;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

import static org.corfudb.runtime.view.Address.NON_ADDRESS;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

public class StateTransferPlannerTest extends  DataTest{

    @Test
    public void testMergeBatchResults(){
        RegularBatchProcessor batchProcessor = mock(RegularBatchProcessor.class);
        StateTransferPlanner StateTransferPlanner = new StateTransferPlanner(batchProcessor);
        // first is ok, second is ok
        Result<Long, StateTransferException> first =
                Result.ok(0L).mapError(x -> new StateTransferException());
        Result<Long, StateTransferException> second =
                Result.ok(3L).mapError(x -> new StateTransferException());
        assertThat(StateTransferPlanner.mergeBatchResults(first, second).get()).isEqualTo(3L);
        // first is a failure, second is ok
        first = Result.error(new BatchProcessorFailure());
        assertThat(StateTransferPlanner.mergeBatchResults(first, second).getError())
                .isInstanceOf(BatchProcessorFailure.class);
        // first is ok, second is a failure
        first = Result.ok(0L).mapError(x -> new StateTransferException());
        second = Result.error(new BatchProcessorFailure());
        assertThat(StateTransferPlanner.mergeBatchResults(first, second).getError())
                .isInstanceOf(BatchProcessorFailure.class);
        // first is a failure, second is a failure
        first = Result.error(new BatchProcessorFailure());
        assertThat(StateTransferPlanner.mergeBatchResults(first, second).getError())
                .isInstanceOf(BatchProcessorFailure.class);

    }

    @Test
    public void testCoalesceResults(){
        RegularBatchProcessor batchProcessor = mock(RegularBatchProcessor.class);
        StateTransferPlanner StateTransferPlanner = new StateTransferPlanner(batchProcessor);
        List<CompletableFuture<Result<Long, StateTransferException>>> resultSet = Stream.of(0L, 1L, 2L)
                .map(x -> CompletableFuture
                        .completedFuture(Result.ok(x).mapError(y -> new StateTransferException())))
                .collect(Collectors.toList());
        // all ok
        CompletableFuture<Result<Long, StateTransferException>> finalResult =
                StateTransferPlanner.coalesceResults(resultSet);
        assertThat(finalResult.join().get()).isEqualTo(2L);

        // one is a failure
        resultSet.add(CompletableFuture.completedFuture(Result.error(new BatchProcessorFailure())));
        finalResult = StateTransferPlanner.coalesceResults(resultSet);
        assertThat(finalResult.join().getError()).isInstanceOf(BatchProcessorFailure.class);

        // one is completed exceptionally
        resultSet.remove(resultSet.size() - 1);
        CompletableFuture<Result<Long, StateTransferException>> exceptional = CompletableFuture.supplyAsync(() -> {
            throw new RuntimeException();
        });

        resultSet.add(exceptional);

        resultSet.add(CompletableFuture
                .completedFuture(Result.ok(3L).mapError(y -> new StateTransferException())));

        finalResult = StateTransferPlanner.coalesceResults(resultSet);
        assertThat(finalResult).isCompletedExceptionally();

        // the input is empty
        CompletableFuture<Result<Long, StateTransferException>> future =
                StateTransferPlanner.coalesceResults(new ArrayList<>());
        assertThat(future.join().getError()).isInstanceOf(BatchProcessorFailure.class);
    }

    @Test
    public void testTransfer(){
        // transfer is ok, one chunk
        StreamLog streamLog = mock(StreamLog.class);
        AddressSpaceView addressSpaceView = mock(AddressSpaceView.class);

        RegularBatchProcessor batchProcessor = new RegularBatchProcessor(streamLog, addressSpaceView);
        RegularBatchProcessor spy = spy(batchProcessor);

        Result<Long, StateTransferException> value =
                Result.ok(2L).mapError(y -> new StateTransferException());

        List<Long> addresses = Arrays.asList(0L, 1L, 2L);
        doReturn(CompletableFuture.completedFuture(value)).when(spy).transfer(addresses);

        StateTransferPlanner StateTransferPlanner = new StateTransferPlanner(spy);
        Result<Long, StateTransferException> res =
                StateTransferPlanner.stateTransfer(addresses, 10, NON_ADDRESS).join();
        assertThat(res.get()).isEqualTo(2L);

        // transfer is ok, multiple chunks
        addresses = LongStream.range(1L, 11L).boxed().collect(Collectors.toList());
        List<Long> firstRange = LongStream.range(1L, 6L).boxed().collect(Collectors.toList());
        List<Long> secondRange = LongStream.range(6L, 11L).boxed().collect(Collectors.toList());

        Result<Long, StateTransferException> firstValue =
                Result.ok(5L).mapError(y -> new StateTransferException());

        Result<Long, StateTransferException> secondValue =
                Result.ok(10L).mapError(y -> new StateTransferException());

        doReturn(CompletableFuture.completedFuture(firstValue)).when(spy).transfer(firstRange);
        doReturn(CompletableFuture.completedFuture(secondValue)).when(spy).transfer(secondRange);

        StateTransferPlanner = new StateTransferPlanner(spy);
        res = StateTransferPlanner.stateTransfer(addresses, 5, NON_ADDRESS).join();
        assertThat(res.get()).isEqualTo(10L);

        // first half is already written, second half is ok
        List<LogData> stubListFirstRange = createStubList(firstRange);
        CompletableFuture<Result<Long, StateTransferException>> writtenException =
                CompletableFuture.completedFuture(new Result<>(null, new RejectedDataException(stubListFirstRange)));

        doReturn(writtenException)
                .when(spy)
                .transfer(firstRange);

        doReturn(CompletableFuture.completedFuture(secondValue)).when(spy).transfer(secondRange);
        StateTransferPlanner = new StateTransferPlanner(spy);
        res = StateTransferPlanner.stateTransfer(addresses, 5, NON_ADDRESS).join();
        assertThat(res.get()).isEqualTo(10L);

        // first half got some missing addresses, second half is ok
        List<Long> missingAddresses = Arrays.asList(1L, 3L, 5L);
        CompletableFuture<Result<Long, StateTransferException>> missingAddressesException =
                CompletableFuture.completedFuture(
                        new Result<>(null, new IncompleteDataReadException(new HashSet<>(missingAddresses))));

        Map<Long, ILogData> stubMap = createStubMap(missingAddresses);
        Map<Long, ILogData> returnedData = new HashMap<>(stubMap);

        doReturn(returnedData).when(addressSpaceView)
                .read(missingAddresses, batchProcessor.getReadOptions());

        List<LogData> logData =
                Ordering.natural().sortedCopy(returnedData.values().stream()
                        .map(x -> (LogData) x).collect(Collectors.toList()));

        doNothing().when(streamLog).append(logData);
        batchProcessor = new RegularBatchProcessor(streamLog, addressSpaceView);
        spy = spy(batchProcessor);

        doReturn(missingAddressesException)
                .when(spy).transfer(firstRange);

        doReturn(CompletableFuture.completedFuture(secondValue)).when(spy).transfer(secondRange);

        StateTransferPlanner = new StateTransferPlanner(spy);
        res = StateTransferPlanner.stateTransfer(addresses, 5, NON_ADDRESS).join();
        assertThat(res.get()).isEqualTo(10L);

        // first half got some unrecoverable error, second half is ok
        CompletableFuture<Result<Long, StateTransferException>> failure =
                CompletableFuture.completedFuture(
                        new Result<>(null, new BatchProcessorFailure()));
        streamLog = mock(StreamLog.class);
        addressSpaceView = mock(AddressSpaceView.class);

        batchProcessor = new RegularBatchProcessor(streamLog, addressSpaceView);
        spy = spy(batchProcessor);
        doReturn(failure)
                .when(spy).transfer(firstRange);
        doReturn(CompletableFuture.completedFuture(secondValue)).when(spy).transfer(secondRange);
        StateTransferPlanner = new StateTransferPlanner(spy);
        res = StateTransferPlanner.stateTransfer(addresses, 5, NON_ADDRESS).join();
        assertThat(res.getError()).isInstanceOf(BatchProcessorFailure.class);
    }
}
