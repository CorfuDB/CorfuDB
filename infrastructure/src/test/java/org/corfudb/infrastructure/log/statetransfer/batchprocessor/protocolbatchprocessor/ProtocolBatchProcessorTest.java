package org.corfudb.infrastructure.log.statetransfer.batchprocessor.protocolbatchprocessor;

import org.corfudb.common.result.Result;
import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.infrastructure.log.statetransfer.DataTest;
import org.corfudb.infrastructure.log.statetransfer.batch.Batch;
import org.corfudb.infrastructure.log.statetransfer.batch.BatchResult;
import org.corfudb.infrastructure.log.statetransfer.batch.ReadBatch;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.BatchProcessorError;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.BatchProcessorFailure;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.exceptions.RetryExhaustedException;
import org.corfudb.runtime.view.AddressSpaceView;
import org.corfudb.runtime.view.ReadOptions;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

class ProtocolBatchProcessorTest extends DataTest {

    @Test
    void transferFull() {
        List<Long> addresses = LongStream.range(0L, 10L).boxed().collect(Collectors.toList());
        List<LogData> stubList = createStubList(addresses);
        Map<Long, ILogData> stubMap = createStubMap(stubList);
        ReadOptions readOptions = ProtocolBatchProcessor.getReadOptions();
        StreamLog streamLog = mock(StreamLog.class);
        doNothing().when(streamLog).append(stubList);
        AddressSpaceView addressSpaceView = mock(AddressSpaceView.class);
        doReturn(stubMap).when(addressSpaceView).simpleProtocolRead(addresses, readOptions);
        ProtocolBatchProcessor batchProcessor = new ProtocolBatchProcessor(streamLog, addressSpaceView);
        CompletableFuture<BatchResult> f =
                batchProcessor.transfer(new Batch(addresses, Optional.empty()));
        BatchResult join = f.join();
        assertThat(join.getStatus() == BatchResult.FailureStatus.SUCCEEDED).isTrue();
        assertThat(join.getAddresses()).isEqualTo(addresses);
    }

    @Test
    void transferFail() {
        List<Long> addresses = LongStream.range(0L, 10L).boxed().collect(Collectors.toList());
        Map<Long, ILogData> stubMap = createStubMapFromLongs(addresses);
        ReadOptions readOptions = ProtocolBatchProcessor.getReadOptions();
        List<LogData> recordsFromStubMap = getRecordsFromStubMap(stubMap);
        StreamLog streamLog = mock(StreamLog.class);
        doThrow(new IllegalStateException()).when(streamLog).append(recordsFromStubMap);
        AddressSpaceView addressSpaceView = mock(AddressSpaceView.class);
        doReturn(stubMap).when(addressSpaceView).simpleProtocolRead(addresses, readOptions);
        ProtocolBatchProcessor batchProcessor = new ProtocolBatchProcessor(streamLog, addressSpaceView);
        CompletableFuture<BatchResult> f =
                batchProcessor.transfer(new Batch(addresses, Optional.empty()));
        BatchResult join = f.join();

        assertThat(join.getStatus() == BatchResult.FailureStatus.FAILED).isTrue();
    }

    @Test
    void testReadRecordsSuccess() {
        List<Long> addresses = LongStream.range(0L, 10L).boxed().collect(Collectors.toList());
        List<LogData> stubList = createStubList(addresses);
        Map<Long, ILogData> stubMap = createStubMap(stubList);
        ReadOptions readOptions = ProtocolBatchProcessor.getReadOptions();
        StreamLog streamLog = mock(StreamLog.class);
        doNothing().when(streamLog).append(stubList);
        AddressSpaceView addressSpaceView = mock(AddressSpaceView.class);
        doReturn(stubMap).when(addressSpaceView).simpleProtocolRead(addresses, readOptions);
        ProtocolBatchProcessor batchProcessor = new ProtocolBatchProcessor(streamLog, addressSpaceView);
        CompletableFuture<ReadBatch> f =
                batchProcessor.readRecords(new Batch(addresses, Optional.empty()), 0);
        ReadBatch join = f.join();
        List<LogData> expected = getRecordsFromStubMap(stubMap);
        assertThat(join.getStatus() == ReadBatch.FailureStatus.SUCCEEDED).isTrue();
        assertThat(join.getData()).isEqualTo(expected);
    }


    /**
     * On the first try returns the incomplete set of records, on the second retry returns a complete set of records.
     */
    @Test
    void retryReadRecordsIncomplete() {
        List<Long> addresses = LongStream.range(0L, 10L).boxed().collect(Collectors.toList());
        List<Long> readAddresses = LongStream.range(0L, 10L).boxed().filter(x -> x % 2 == 0).collect(Collectors.toList());
        List<Long> unreadAddresses = LongStream.range(0L, 10L).boxed().filter(x -> x % 2 != 0).collect(Collectors.toList());

        List<LogData> firstReadList = createStubList(readAddresses);
        Map<Long, ILogData> firstReadMap = createStubMap(firstReadList);
        List<LogData> secondReadList = createStubList(unreadAddresses);
        Map<Long, ILogData> secondReadMap = createStubMap(secondReadList);

        ReadOptions readOptions = ProtocolBatchProcessor.getReadOptions();
        StreamLog streamLog = mock(StreamLog.class);
        doNothing().when(streamLog).append(firstReadList);
        doNothing().when(streamLog).append(secondReadList);

        AddressSpaceView addressSpaceView = mock(AddressSpaceView.class);
        doReturn(firstReadMap).when(addressSpaceView).simpleProtocolRead(addresses, readOptions);
        doReturn(secondReadMap).when(addressSpaceView).simpleProtocolRead(unreadAddresses, readOptions);

        List<LogData> secondReturnedRecords = getRecordsFromStubMap(secondReadMap);

        ProtocolBatchProcessor batchProcessor = new ProtocolBatchProcessor(streamLog, addressSpaceView);
        ProtocolBatchProcessor spy = spy(batchProcessor);

        CompletableFuture<ReadBatch> res =
                spy.retryReadRecords(Batch.builder().addresses(addresses).build(), 0);

        ReadBatch join = res.join();
        assertThat(join.getStatus() == ReadBatch.FailureStatus.SUCCEEDED).isTrue();
        assertThat(join.getData()).isEqualTo(secondReturnedRecords);

    }

    public CompletableFuture<Result<List<LogData>, BatchProcessorFailure>> failFuture(Throwable throwable) {
        CompletableFuture<Result<List<LogData>, BatchProcessorFailure>> future = new CompletableFuture<>();
        future.completeExceptionally(throwable);
        return future;
    }

    /**
     * Handle time outs..
     */
    @Test
    void retryReadRecordsTimeout() {
        List<Long> addresses = LongStream.range(0L, 10L).boxed().collect(Collectors.toList());

        List<LogData> secondReadList = createStubList(addresses);
        Map<Long, ILogData> secondReadMap = createStubMap(secondReadList);

        ReadOptions readOptions = ProtocolBatchProcessor.getReadOptions();
        StreamLog streamLog = mock(StreamLog.class);

        doNothing().when(streamLog).append(secondReadList);

        AddressSpaceView addressSpaceView = mock(AddressSpaceView.class);

        doAnswer(answer -> {
            throw new TimeoutException();
        }).when(addressSpaceView).simpleProtocolRead(addresses, readOptions);

        ProtocolBatchProcessor batchProcessor = new ProtocolBatchProcessor(streamLog, addressSpaceView);
        ProtocolBatchProcessor spy = spy(batchProcessor);

        CompletableFuture<ReadBatch> res =
                spy.retryReadRecords(Batch.builder().addresses(addresses).build(), 0);

        ReadBatch join = res.join();
        assertThat(join.getStatus() == ReadBatch.FailureStatus.FAILED).isTrue();

    }


    @Test
    void checkReadRecordsComplete() {
        List<Long> addresses = LongStream.range(0L, 10L).boxed().collect(Collectors.toList());
        List<LogData> stubList = createStubList(addresses);
        Map<Long, ILogData> stubMap = createStubMap(stubList);
        ReadOptions readOptions = ProtocolBatchProcessor.getReadOptions();
        StreamLog streamLog = mock(StreamLog.class);
        doNothing().when(streamLog).append(stubList);
        AddressSpaceView addressSpaceView = mock(AddressSpaceView.class);
        doReturn(stubMap).when(addressSpaceView).simpleProtocolRead(addresses, readOptions);
        ProtocolBatchProcessor batchProcessor = new ProtocolBatchProcessor(streamLog, addressSpaceView);
        ReadBatch res = batchProcessor.checkReadRecords(addresses, stubMap, Optional.empty());
        List<LogData> expected = getRecordsFromStubMap(stubMap);
        assertThat(res.getStatus() == ReadBatch.FailureStatus.SUCCEEDED).isTrue();
        assertThat(res.getData()).isEqualTo(expected);
    }

    @Test
    void checkReadRecordsInComplete() {
        List<Long> addresses = LongStream.range(0L, 10L).boxed().collect(Collectors.toList());
        List<Long> readAddresses = LongStream.range(0L, 10L).boxed().filter(x -> x % 2 == 0).collect(Collectors.toList());
        List<Long> unreadAddresses = LongStream.range(0L, 10L).boxed().filter(x -> x % 2 != 0).collect(Collectors.toList());
        List<LogData> stubList = createStubList(readAddresses);
        Map<Long, ILogData> stubMap = createStubMap(stubList);
        ReadOptions readOptions = ProtocolBatchProcessor.getReadOptions();
        StreamLog streamLog = mock(StreamLog.class);
        doNothing().when(streamLog).append(stubList);
        AddressSpaceView addressSpaceView = mock(AddressSpaceView.class);
        doReturn(stubMap).when(addressSpaceView).simpleProtocolRead(readAddresses, readOptions);
        ProtocolBatchProcessor batchProcessor = new ProtocolBatchProcessor(streamLog, addressSpaceView);
        ReadBatch res = batchProcessor.checkReadRecords(addresses, stubMap, Optional.empty());
        assertThat(res.getStatus() == ReadBatch.FailureStatus.FAILED).isTrue();
        assertThat(res.getFailedAddress()).isEqualTo(unreadAddresses);
    }
}