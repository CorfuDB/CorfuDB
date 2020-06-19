package org.corfudb.infrastructure.log.statetransfer.batchprocessor.protocolbatchprocessor;

import org.corfudb.infrastructure.log.statetransfer.DataTest;
import org.corfudb.infrastructure.log.statetransfer.batch.ReadBatch;
import org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchRequest;
import org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchResponse;
import org.corfudb.infrastructure.log.statetransfer.exceptions.StateTransferBatchProcessorException;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.runtime.view.AddressSpaceView;
import org.corfudb.runtime.view.ReadOptions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.corfudb.infrastructure.log.statetransfer.batch.ReadBatch.ReadStatus.FAILED;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

class ProtocolBatchProcessorTest extends DataTest {

    @Test
    public void transferFull() {
        List<Long> addresses = LongStream.range(0L, 10L).boxed().collect(Collectors.toList());
        List<LogData> stubList = createStubList(addresses);
        Map<Long, ILogData> stubMap = createStubMap(stubList);
        ReadOptions readOptions = ReadOptions.builder()
                .waitForHole(true)
                .clientCacheable(false)
                .serverCacheable(false)
                .build();
        LogUnitClient logUnitClient = mock(LogUnitClient.class);
        doReturn(CompletableFuture.completedFuture(true)).when(logUnitClient).writeRange(stubList);
        AddressSpaceView addressSpaceView = mock(AddressSpaceView.class);
        doReturn(stubMap).when(addressSpaceView).simpleProtocolRead(addresses, readOptions);

        ProtocolBatchProcessor batchProcessor = ProtocolBatchProcessor
                .builder()
                .logUnitClient(logUnitClient)
                .addressSpaceView(addressSpaceView)
                .build();
        CompletableFuture<TransferBatchResponse> f =
                batchProcessor.transfer(new TransferBatchRequest(addresses, Optional.empty()));
        TransferBatchResponse join = f.join();
        assertThat(join.getStatus() == TransferBatchResponse.TransferStatus.SUCCEEDED).isTrue();
        assertThat(join.getTransferBatchRequest().getAddresses()).isEqualTo(addresses);
    }

    @Test
    public void transferFail() {
        List<Long> addresses = LongStream.range(0L, 10L).boxed().collect(Collectors.toList());
        Map<Long, ILogData> stubMap = createStubMapFromLongs(addresses);
        ReadOptions readOptions = ReadOptions.builder()
                .waitForHole(true)
                .clientCacheable(false)
                .serverCacheable(false)
                .build();
        List<LogData> recordsFromStubMap = getRecordsFromStubMap(stubMap);
        LogUnitClient logUnitClient = mock(LogUnitClient.class);
        doThrow(new IllegalStateException()).when(logUnitClient).writeRange(recordsFromStubMap);
        AddressSpaceView addressSpaceView = mock(AddressSpaceView.class);
        doReturn(stubMap).when(addressSpaceView).simpleProtocolRead(addresses, readOptions);
        ProtocolBatchProcessor batchProcessor = ProtocolBatchProcessor
                .builder()
                .logUnitClient(logUnitClient)
                .addressSpaceView(addressSpaceView)
                .build();
        CompletableFuture<TransferBatchResponse> f =
                batchProcessor.transfer(new TransferBatchRequest(addresses, Optional.empty()));
        TransferBatchResponse join = f.join();

        assertThat(join.getStatus() == TransferBatchResponse.TransferStatus.FAILED).isTrue();
    }

    @Test
    public void testReadRecordsSuccess() {
        List<Long> addresses = LongStream.range(0L, 10L).boxed().collect(Collectors.toList());
        List<LogData> stubList = createStubList(addresses);
        Map<Long, ILogData> stubMap = createStubMap(stubList);
        ReadOptions readOptions = ReadOptions.builder()
                .waitForHole(true)
                .clientCacheable(false)
                .serverCacheable(false)
                .build();
        LogUnitClient logUnitClient = mock(LogUnitClient.class);
        doReturn(CompletableFuture.completedFuture(true)).when(logUnitClient).writeRange(stubList);
        AddressSpaceView addressSpaceView = mock(AddressSpaceView.class);
        doReturn(stubMap).when(addressSpaceView).simpleProtocolRead(addresses, readOptions);
        ProtocolBatchProcessor batchProcessor = ProtocolBatchProcessor
                .builder()
                .logUnitClient(logUnitClient)
                .addressSpaceView(addressSpaceView)
                .build();
        CompletableFuture<ReadBatch> f =
                batchProcessor.readRecords(new TransferBatchRequest(addresses, Optional.empty()));
        ReadBatch join = f.join();
        List<LogData> expected = getRecordsFromStubMap(stubMap);
        assertThat(join.getStatus() == ReadBatch.ReadStatus.SUCCEEDED).isTrue();
        assertThat(join.getData()).isEqualTo(expected);
    }

    /**
     * Handle wrong epoch exception
     */
    @Test
    public void handleWrongEpochException() {
        List<Long> addresses = LongStream.range(0L, 10L).boxed().collect(Collectors.toList());

        List<LogData> secondReadList = createStubList(addresses);

        ReadOptions readOptions = ReadOptions.builder()
                .waitForHole(true)
                .clientCacheable(false)
                .serverCacheable(false)
                .build();
        LogUnitClient logUnitClient = mock(LogUnitClient.class);

        doReturn(CompletableFuture.completedFuture(true)).when(logUnitClient).writeRange(secondReadList);

        AddressSpaceView addressSpaceView = mock(AddressSpaceView.class);

        doAnswer(answer -> {
            throw new WrongEpochException(0L);
        }).when(addressSpaceView).simpleProtocolRead(addresses, readOptions);

        ProtocolBatchProcessor batchProcessor = ProtocolBatchProcessor
                .builder()
                .logUnitClient(logUnitClient)
                .addressSpaceView(addressSpaceView)
                .build();
        ProtocolBatchProcessor spy = spy(batchProcessor);

        CompletableFuture<ReadBatch> res =
                spy.readRecords(TransferBatchRequest.builder().addresses(addresses).build());

        assertThatThrownBy(res::join)
                .isInstanceOf(CompletionException.class)
                .hasRootCauseInstanceOf(WrongEpochException.class);
    }

    @Test
    public void checkReadRecordsComplete() {
        List<Long> addresses = LongStream.range(0L, 10L).boxed().collect(Collectors.toList());
        List<LogData> stubList = createStubList(addresses);
        Map<Long, ILogData> stubMap = createStubMap(stubList);
        ReadOptions readOptions = ReadOptions.builder()
                .waitForHole(true)
                .clientCacheable(false)
                .serverCacheable(false)
                .build();
        LogUnitClient logUnitClient = mock(LogUnitClient.class);
        doReturn(CompletableFuture.completedFuture(true)).when(logUnitClient).writeRange(stubList);
        AddressSpaceView addressSpaceView = mock(AddressSpaceView.class);
        doReturn(stubMap).when(addressSpaceView).simpleProtocolRead(addresses, readOptions);
        ProtocolBatchProcessor batchProcessor = ProtocolBatchProcessor
                .builder()
                .logUnitClient(logUnitClient)
                .addressSpaceView(addressSpaceView)
                .build();
        ReadBatch res = batchProcessor.checkReadRecords(addresses, stubMap, Optional.empty());
        List<LogData> expected = getRecordsFromStubMap(stubMap);
        assertThat(res.getStatus() == ReadBatch.ReadStatus.SUCCEEDED).isTrue();
        assertThat(res.getData()).isEqualTo(expected);
    }

    @Test
    public void checkReadRecordsInComplete() {
        List<Long> addresses = LongStream.range(0L, 10L).boxed().collect(Collectors.toList());
        List<Long> readAddresses = LongStream.range(0L, 10L).boxed().filter(x -> x % 2 == 0).collect(Collectors.toList());
        List<Long> unreadAddresses = LongStream.range(0L, 10L).boxed().filter(x -> x % 2 != 0).collect(Collectors.toList());
        List<LogData> stubList = createStubList(readAddresses);
        Map<Long, ILogData> stubMap = createStubMap(stubList);
        ReadOptions readOptions = ReadOptions.builder()
                .waitForHole(true)
                .clientCacheable(false)
                .serverCacheable(false)
                .build();
        LogUnitClient logUnitClient = mock(LogUnitClient.class);
        doReturn(CompletableFuture.completedFuture(true)).when(logUnitClient).writeRange(stubList);
        AddressSpaceView addressSpaceView = mock(AddressSpaceView.class);
        doReturn(stubMap).when(addressSpaceView).simpleProtocolRead(readAddresses, readOptions);
        ProtocolBatchProcessor batchProcessor = ProtocolBatchProcessor
                .builder()
                .logUnitClient(logUnitClient)
                .addressSpaceView(addressSpaceView)
                .build();
        ReadBatch res = batchProcessor.checkReadRecords(addresses, stubMap, Optional.empty());
        assertThat(res.getStatus() == FAILED).isTrue();
        assertThat(res.getFailedAddresses()).isEqualTo(unreadAddresses);
    }

    @Test
    public void checkThatReadExceptionIsPropagated() {
        List<Long> addresses = LongStream.range(0L, 10L).boxed().collect(Collectors.toList());
        List<LogData> stubList = createStubList(addresses);
        Map<Long, ILogData> stubMap = createStubMap(stubList);
        ReadOptions readOptions = ReadOptions.builder()
                .waitForHole(true)
                .clientCacheable(false)
                .serverCacheable(false)
                .build();

        LogUnitClient logUnitClient = mock(LogUnitClient.class);
        doReturn(CompletableFuture.completedFuture(true)).when(logUnitClient).writeRange(stubList);
        AddressSpaceView addressSpaceView = mock(AddressSpaceView.class);
        doThrow(new WrongEpochException(0L)).when(addressSpaceView).simpleProtocolRead(addresses, readOptions);
        ProtocolBatchProcessor batchProcessor = ProtocolBatchProcessor
                .builder()
                .logUnitClient(logUnitClient)
                .addressSpaceView(addressSpaceView)
                .build();

        TransferBatchResponse response =
                batchProcessor.transfer(new TransferBatchRequest(addresses, Optional.empty()))
                        .join();

        assertThat(response.getStatus()).isEqualTo(TransferBatchResponse.TransferStatus.FAILED);
        assertThatThrownBy(() -> {
            throw response.getCauseOfFailure().get();
        }).isInstanceOf(StateTransferBatchProcessorException.class)
                .hasRootCauseInstanceOf(WrongEpochException.class);

    }
}