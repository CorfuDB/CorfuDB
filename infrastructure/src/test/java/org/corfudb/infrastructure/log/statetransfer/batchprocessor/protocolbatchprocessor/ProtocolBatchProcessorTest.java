package org.corfudb.infrastructure.log.statetransfer.batchprocessor.protocolbatchprocessor;

import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.infrastructure.log.statetransfer.DataTest;
import org.corfudb.infrastructure.log.statetransfer.batch.ReadBatch;
import org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchRequest;
import org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchResponse;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.view.AddressSpaceView;
import org.corfudb.runtime.view.ReadOptions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
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

        ProtocolBatchProcessor batchProcessor = ProtocolBatchProcessor
                .builder()
                .streamLog(streamLog)
                .addressSpaceView(addressSpaceView)
                .build();
        CompletableFuture<TransferBatchResponse> f =
                batchProcessor.transfer(new TransferBatchRequest(addresses, Optional.empty()));
        TransferBatchResponse join = f.join();
        assertThat(join.getStatus() == TransferBatchResponse.TransferStatus.SUCCEEDED).isTrue();
        assertThat(join.getTransferBatchRequest().getAddresses()).isEqualTo(addresses);
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
        ProtocolBatchProcessor batchProcessor = ProtocolBatchProcessor
                .builder()
                .streamLog(streamLog)
                .addressSpaceView(addressSpaceView)
                .build();
        CompletableFuture<TransferBatchResponse> f =
                batchProcessor.transfer(new TransferBatchRequest(addresses, Optional.empty()));
        TransferBatchResponse join = f.join();

        assertThat(join.getStatus() == TransferBatchResponse.TransferStatus.FAILED).isTrue();
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
        ProtocolBatchProcessor batchProcessor = ProtocolBatchProcessor
                .builder()
                .streamLog(streamLog)
                .addressSpaceView(addressSpaceView)
                .build();
        CompletableFuture<ReadBatch> f =
                batchProcessor.readRecords(new TransferBatchRequest(addresses, Optional.empty()), 0);
        ReadBatch join = f.join();
        List<LogData> expected = getRecordsFromStubMap(stubMap);
        assertThat(join.getStatus() == ReadBatch.ReadStatus.SUCCEEDED).isTrue();
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

        ProtocolBatchProcessor batchProcessor = ProtocolBatchProcessor
                .builder()
                .streamLog(streamLog)
                .addressSpaceView(addressSpaceView)
                .build();
        ProtocolBatchProcessor spy = spy(batchProcessor);

        CompletableFuture<ReadBatch> res =
                spy.retryReadRecords(TransferBatchRequest.builder().addresses(addresses).build(), 0);

        ReadBatch join = res.join();
        assertThat(join.getStatus() == ReadBatch.ReadStatus.SUCCEEDED).isTrue();
        assertThat(join.getData()).isEqualTo(secondReturnedRecords);

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

        ProtocolBatchProcessor batchProcessor = ProtocolBatchProcessor
                .builder()
                .streamLog(streamLog)
                .addressSpaceView(addressSpaceView)
                .build();
        ProtocolBatchProcessor spy = spy(batchProcessor);

        CompletableFuture<ReadBatch> res =
                spy.retryReadRecords(TransferBatchRequest.builder().addresses(addresses).build(), 0);

        assertThat(res).isCompletedExceptionally();

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
        ProtocolBatchProcessor batchProcessor = ProtocolBatchProcessor
                .builder()
                .streamLog(streamLog)
                .addressSpaceView(addressSpaceView)
                .build();
        ReadBatch res = batchProcessor.checkReadRecords(addresses, stubMap, Optional.empty());
        List<LogData> expected = getRecordsFromStubMap(stubMap);
        assertThat(res.getStatus() == ReadBatch.ReadStatus.SUCCEEDED).isTrue();
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
        ProtocolBatchProcessor batchProcessor = ProtocolBatchProcessor
                .builder()
                .streamLog(streamLog)
                .addressSpaceView(addressSpaceView)
                .build();
        ReadBatch res = batchProcessor.checkReadRecords(addresses, stubMap, Optional.empty());
        assertThat(res.getStatus() == ReadBatch.ReadStatus.FAILED).isTrue();
        assertThat(res.getFailedAddresses()).isEqualTo(unreadAddresses);
    }
}