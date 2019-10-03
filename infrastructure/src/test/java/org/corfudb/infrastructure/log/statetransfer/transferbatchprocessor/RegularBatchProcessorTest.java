package org.corfudb.infrastructure.log.statetransfer.transferbatchprocessor;

import com.google.common.collect.Ordering;
import org.corfudb.common.result.Result;
import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.infrastructure.log.StreamLogFiles;
import org.corfudb.infrastructure.log.statetransfer.exceptions.IncompleteDataReadException;
import org.corfudb.infrastructure.log.statetransfer.exceptions.RejectedDataException;
import org.corfudb.infrastructure.log.statetransfer.exceptions.StateTransferException;
import org.corfudb.infrastructure.log.statetransfer.exceptions.StateTransferFailure;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.exceptions.OverwriteCause;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.view.AddressSpaceView;
import org.junit.Test;
import static org.assertj.core.api.Assertions.assertThat;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

public class RegularBatchProcessorTest {

    private LogData createStubRecord(long address){
        LogData data = mock(LogData.class);
        doReturn(address).when(data).getGlobalAddress();
        return data;
    }

    private List<LogData> createStubList(List<Long> addresses){
        return addresses.stream().map(this::createStubRecord).collect(Collectors.toList());
    }

    private Map<Long, ILogData> createStubMap(List<Long> addresses){
        return addresses.stream().map(addr -> new SimpleEntry<>(addr, createStubRecord(addr)))
                .collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue));
    }

    @Test
    public void testReadRecordsSuccessful(){

        StreamLog streamLog = mock(StreamLog.class);
        AddressSpaceView addressSpaceView = mock(AddressSpaceView.class);

        List<Long> testAddresses = Arrays.asList(0L, 1L, 2L, 3L, 4L, 5L);
        Map<Long, ILogData> testData = createStubMap(testAddresses);
        RegularBatchProcessor regularBatchProcessor = new RegularBatchProcessor(streamLog, addressSpaceView);
        doReturn(testData).when(addressSpaceView).read(testAddresses, regularBatchProcessor.getReadOptions());

        Result<List<LogData>, StateTransferException> result =
                regularBatchProcessor.readRecords(testAddresses);
        // result is a value with data values
        assertThat(result.isValue()).isTrue();
        assertThat(result.get()).isEqualTo(new ArrayList<>(testData.values()));
    }

    @Test
    public void testReadIncompleteRead(){
        StreamLog streamLog = mock(StreamLog.class);
        AddressSpaceView addressSpaceView = mock(AddressSpaceView.class);
        List<Long> testAddresses = Arrays.asList(0L, 1L, 2L, 3L, 4L, 5L);
        Map<Long, ILogData> testData = createStubMap(testAddresses);
        Map<Long, ILogData> returnedData = new HashMap<>(testData);
        returnedData.remove(0L);
        returnedData.remove(1L);
        returnedData.remove(2L);
        RegularBatchProcessor regularBatchProcessor = new RegularBatchProcessor(streamLog, addressSpaceView);
        doReturn(returnedData).when(addressSpaceView)
                .read(testAddresses, regularBatchProcessor.getReadOptions());
        Result<List<LogData>, StateTransferException> result =
                regularBatchProcessor.readRecords(testAddresses);

        // result is an error of type IncompleteDataReadException and got the missing addresses.
        assertThat(result.isError()).isTrue();
        assertThat(result.getError()).isInstanceOf(IncompleteDataReadException.class);
        IncompleteDataReadException error = (IncompleteDataReadException) result.getError();
        assertThat(error.getMissingAddresses())
                .isEqualTo(new HashSet<>(Arrays.asList(0L, 1L, 2L)));

    }

    @Test
    public void testWriteRecordsSuccessful(){
        StreamLogFiles streamLog = mock(StreamLogFiles.class);
        AddressSpaceView addressSpaceView = mock(AddressSpaceView.class);
        List<Long> testAddresses = Arrays.asList(0L, 1L, 2L, 3L, 4L, 5L);
        List<LogData> stubRecords = createStubList(testAddresses);
        doNothing().when(streamLog).append(stubRecords);
        RegularBatchProcessor regularBatchProcessor =
                new RegularBatchProcessor(streamLog, addressSpaceView);
        Result<Long, StateTransferException> result = regularBatchProcessor.writeRecords(stubRecords);
        assertThat(result.isValue()).isTrue();
        assertThat(result.get()).isEqualTo(5L);
    }

    @Test
    public void testWriteRecordsError(){
        StreamLogFiles streamLogFiles = mock(StreamLogFiles.class);
        AddressSpaceView addressSpaceView = mock(AddressSpaceView.class);
        List<Long> testAddresses = Arrays.asList(0L, 1L, 2L, 3L, 4L, 5L);
        List<LogData> stubRecords = createStubList(testAddresses);
        doThrow(new OverwriteException(OverwriteCause.SAME_DATA)).when(streamLogFiles)
                .append(stubRecords);
        RegularBatchProcessor regularBatchProcessor =
                new RegularBatchProcessor(streamLogFiles, addressSpaceView);
        Result<Long, StateTransferException> result =
                regularBatchProcessor.writeRecords(stubRecords);
        assertThat(result.isError()).isTrue();
        assertThat(result.getError()).isInstanceOf(RejectedDataException.class);

        // Unexpected exception will call failure
        doThrow(new OverwriteException(OverwriteCause.DIFF_DATA)).when(streamLogFiles)
                .append(stubRecords);

        result =
                regularBatchProcessor.writeRecords(stubRecords);

        assertThat(result.getError()).isInstanceOf(StateTransferFailure.class);

    }

    @Test
    public void testGetErrorHandlingPipeline(){
        StreamLogFiles streamLogFiles = mock(StreamLogFiles.class);
        AddressSpaceView addressSpaceView = mock(AddressSpaceView.class);

        List<Long> addresses = Arrays.asList(0L, 1L, 2L);

        RegularBatchProcessor regularBatchProcessor =
                new RegularBatchProcessor(streamLogFiles, addressSpaceView);

        IncompleteDataReadException incompleteDataReadException =
                new IncompleteDataReadException(new HashSet<>(Arrays.asList(0L, 1L, 2L)));

        Map<Long, ILogData> stubMap = createStubMap(addresses);
        Map<Long, ILogData> returnedData = new HashMap<>(stubMap);

        doReturn(returnedData).when(addressSpaceView)
                .read(addresses, regularBatchProcessor.getReadOptions());

        List<LogData> logData =
                Ordering.natural().sortedCopy(returnedData.values().stream()
                        .map(x -> (LogData) x).collect(Collectors.toList()));

        doNothing().when(streamLogFiles).append(logData);

        Supplier<CompletableFuture<Result<Long, StateTransferException>>> errorHandlingPipeline
                = regularBatchProcessor.getErrorHandlingPipeline(incompleteDataReadException);

        CompletableFuture<Result<Long, StateTransferException>>
                function = errorHandlingPipeline.get();

        Result<Long, StateTransferException> result = function.join();
        assertThat(result.isValue()).isTrue();
        assertThat(result.get()).isEqualTo(2L);

        StateTransferFailure stateTransferFailure = new StateTransferFailure();
        Supplier<CompletableFuture<Result<Long, StateTransferException>>> failedPipeline =
                regularBatchProcessor.getErrorHandlingPipeline(stateTransferFailure);

        CompletableFuture<Result<Long, StateTransferException>> value =
                failedPipeline.get();
        Result<Long, StateTransferException> failedResult = value.join();
        assertThat(failedResult.isError()).isTrue();
    }

    @Test
    public void testTryHandleIncompleteRead(){

        StreamLogFiles streamLogFiles = mock(StreamLogFiles.class);
        AddressSpaceView addressSpaceView = mock(AddressSpaceView.class);

        RegularBatchProcessor regularBatchProcessor =
                new RegularBatchProcessor(streamLogFiles, addressSpaceView);

        List<Long> addresses = Arrays.asList(0L, 1L, 2L);
        Map<Long, ILogData> stubMap = createStubMap(addresses);
        Map<Long, ILogData> returnedData = new HashMap<>(stubMap);

        IncompleteDataReadException incompleteDataReadException =
                new IncompleteDataReadException(new HashSet<>(Arrays.asList(0L, 1L, 2L)));

        // Case 1: exception is an incomplete read and retry works
        doReturn(returnedData).when(addressSpaceView)
                .read(addresses, regularBatchProcessor.getReadOptions());

        List<LogData> logData =
        Ordering.natural().sortedCopy(returnedData.values().stream()
                .map(x -> (LogData) x).collect(Collectors.toList()));

        doNothing().when(streamLogFiles).append(logData);

        Result<Long, StateTransferException> result =
                regularBatchProcessor.tryHandleIncompleteRead(incompleteDataReadException,
                        new AtomicInteger(3)).join();

        assertThat(result.isValue()).isTrue();
        // Case 2: retries exhausted, as a read keeps returning an incomplete data
        returnedData.remove(2L);

        doReturn(returnedData).when(addressSpaceView)
                .read(addresses, regularBatchProcessor.getReadOptions());

        result = regularBatchProcessor.tryHandleIncompleteRead(incompleteDataReadException,
                        new AtomicInteger(3)).join();

        assertThat(result.isError()).isTrue();
        assertThat(result.getError()).isInstanceOf(StateTransferFailure.class);

        // Case 3: exception during a retry is an unrecoverable write exception
        returnedData = new HashMap<>(stubMap);

        doReturn(returnedData).when(addressSpaceView)
                .read(addresses, regularBatchProcessor.getReadOptions());

        logData = Ordering.natural().sortedCopy(returnedData.values().stream()
                        .map(x -> (LogData) x).collect(Collectors.toList()));

        doThrow(new IllegalStateException("Illegal state")).when(streamLogFiles)
                .append(logData);

         result = regularBatchProcessor.tryHandleIncompleteRead(incompleteDataReadException,
                        new AtomicInteger(3)).join();

        assertThat(result.isError()).isTrue();
        assertThat(result.getError()).isInstanceOf(StateTransferFailure.class);

        // Case 4: exception during a retry is a recoverable overwrite exception

        doThrow(new OverwriteException(OverwriteCause.SAME_DATA)).when(streamLogFiles)
                .append(logData);

        result = regularBatchProcessor.tryHandleIncompleteRead(incompleteDataReadException,
                        new AtomicInteger(3)).join();

        assertThat(result.isError()).isTrue();
        assertThat(result.getError()).isInstanceOf(RejectedDataException.class);

        // Case 5: pipeline completes exceptionally
        RegularBatchProcessor spy = spy(regularBatchProcessor);

        CompletableFuture<Result<Long, StateTransferException>> failedPipeLine =
                CompletableFuture.supplyAsync(() -> {
            throw new IllegalArgumentException("Error occurred.");
        });

        Supplier<CompletableFuture<Result<Long, StateTransferException>>> fxn = () -> failedPipeLine;
        doReturn(fxn).when(spy).getErrorHandlingPipeline(incompleteDataReadException);

        result = spy.tryHandleIncompleteRead(incompleteDataReadException,
                new AtomicInteger(3)).join();
        assertThat(result.getError()).isInstanceOf(StateTransferFailure.class);

    }

    @Test
    public void testHandlePossibleTransferFailures(){

        // Case 0: it's not an error, just return result.
        StreamLogFiles streamLogFiles = mock(StreamLogFiles.class);
        AddressSpaceView addressSpaceView = mock(AddressSpaceView.class);

        RegularBatchProcessor regularBatchProcessor =
                new RegularBatchProcessor(streamLogFiles, addressSpaceView);

        Result<Long, StateTransferException> result =
                Result.ok(2L).mapError(x -> new StateTransferException());
        CompletableFuture<Result<Long, StateTransferException>> completableFuture =
                regularBatchProcessor.handlePossibleTransferFailures(result,
                new AtomicInteger(3));

        assertThat(completableFuture.join().isValue()).isTrue();

        // Case 1: it's unknown error, lift to the StateTransferFailure and return.
        completableFuture =
                regularBatchProcessor.handlePossibleTransferFailures(
                        Result.error(new StateTransferException("Unknown exception")),
                        new AtomicInteger(3));

        assertThat(completableFuture.join().isError()).isTrue();
        assertThat(completableFuture.join().getError()).isInstanceOf(StateTransferFailure.class);

        // Case 2: It's a rejected data exception, lift result to the value.
        completableFuture = regularBatchProcessor.handlePossibleTransferFailures(
        Result.error(new RejectedDataException(createStubList(Arrays.asList(0L, 1L, 2L, 3L)))),
                new AtomicInteger(3));

        assertThat(completableFuture.join().isValue()).isTrue();
        assertThat(completableFuture.join().get()).isEqualTo(3L);

        // Case 3a: It's an incomplete read exception, retries go through ok and a value is returned.
        List<Long> addresses = Arrays.asList(0L, 1L, 2L);
        Map<Long, ILogData> stubMap = createStubMap(addresses);
        Map<Long, ILogData> returnedData = new HashMap<>(stubMap);

        doReturn(returnedData).when(addressSpaceView)
                .read(addresses, regularBatchProcessor.getReadOptions());

        List<LogData> logData =
                Ordering.natural().sortedCopy(returnedData.values().stream()
                        .map(x -> (LogData) x).collect(Collectors.toList()));

        doNothing().when(streamLogFiles).append(logData);

        completableFuture = regularBatchProcessor.handlePossibleTransferFailures(
                Result.error(new IncompleteDataReadException(new HashSet(addresses))),
                new AtomicInteger(3));

        assertThat(completableFuture.join().isValue()).isTrue();
        assertThat(completableFuture.join().get()).isEqualTo(2L);

        // Case 3b: retries are exhausted and failure is returned.
        returnedData.remove(2L);

        doReturn(returnedData).when(addressSpaceView)
                .read(addresses, regularBatchProcessor.getReadOptions());

        completableFuture = regularBatchProcessor.handlePossibleTransferFailures(
                Result.error(new IncompleteDataReadException(new HashSet(addresses))),
                new AtomicInteger(3));

        assertThat(completableFuture.join().isError()).isTrue();
        assertThat(completableFuture.join().getError()).isInstanceOf(StateTransferFailure.class);

        // Case 3c: exception during a retry is an unrecoverable write exception -> failure.
        returnedData = new HashMap<>(stubMap);

        doReturn(returnedData).when(addressSpaceView)
                .read(addresses, regularBatchProcessor.getReadOptions());

        logData = Ordering.natural().sortedCopy(returnedData.values().stream()
                .map(x -> (LogData) x).collect(Collectors.toList()));

        doThrow(new IllegalStateException("Illegal state")).when(streamLogFiles)
                .append(logData);

        completableFuture = regularBatchProcessor.handlePossibleTransferFailures(
                Result.error(new IncompleteDataReadException(new HashSet(addresses))),
                new AtomicInteger(3));

        assertThat(completableFuture.join().isError()).isTrue();
        assertThat(completableFuture.join().getError()).isInstanceOf(StateTransferFailure.class);

        // Case 3d: exception during a retry is a recoverable overwrite exception -> return value.
        doThrow(new OverwriteException(OverwriteCause.SAME_DATA)).when(streamLogFiles)
                .append(logData);

        completableFuture = regularBatchProcessor.handlePossibleTransferFailures(
                Result.error(new IncompleteDataReadException(new HashSet(addresses))),
                new AtomicInteger(3));

        assertThat(completableFuture.join().isValue()).isTrue();
        assertThat(completableFuture.join().get()).isEqualTo(2L);

    }


}
