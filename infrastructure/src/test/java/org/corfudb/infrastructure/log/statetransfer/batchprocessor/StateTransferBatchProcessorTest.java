package org.corfudb.infrastructure.log.statetransfer.batchprocessor;


import org.corfudb.infrastructure.log.statetransfer.DataTest;
import org.corfudb.infrastructure.log.statetransfer.batch.ReadBatch;
import org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchResponse;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.protocolbatchprocessor.ProtocolBatchProcessor;
import org.corfudb.protocols.wireprotocol.KnownAddressResponse;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.exceptions.RetryExhaustedException;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.runtime.view.AddressSpaceView;
import org.corfudb.util.NodeLocator;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchResponse.TransferStatus.SUCCEEDED;
import static org.corfudb.runtime.exceptions.OverwriteCause.SAME_DATA;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

class StateTransferBatchProcessorTest extends DataTest {

    @Test
    public void writeRecordsSuccess() {
        // Write successfully immediately, a response should be sent to the caller.
        List<Long> addresses = LongStream.range(0L, 10L).boxed().collect(Collectors.toList());
        List<LogData> stubList = createStubList(addresses);
        LogUnitClient logUnitClient = mock(LogUnitClient.class);
        doReturn(CompletableFuture.completedFuture(true)).when(logUnitClient).writeRange(stubList);
        AddressSpaceView addressSpaceView = mock(AddressSpaceView.class);

        ProtocolBatchProcessor batchProcessor = ProtocolBatchProcessor
                .builder()
                .logUnitClient(logUnitClient)
                .addressSpaceView(addressSpaceView)
                .build();
        TransferBatchResponse res = batchProcessor.writeRecords(ReadBatch.builder().data(stubList).build(),
                logUnitClient, 1, Duration.ofMillis(50));
        assertThat(res.getStatus() == SUCCEEDED).isTrue();
        assertThat(res.getTransferBatchRequest().getAddresses()).isEqualTo(addresses);
    }

    @Test
    public void writeRecordsIllegalArgumentException() {
        // Write and fail immediately with illegal argument exception.
        List<Long> addresses = LongStream.range(0L, 10L).boxed().collect(Collectors.toList());
        List<LogData> stubList = createStubList(addresses);
        LogUnitClient logUnitClient = mock(LogUnitClient.class);
        AddressSpaceView addressSpaceView = mock(AddressSpaceView.class);
        doThrow(new IllegalArgumentException("Illegal argument")).when(logUnitClient).writeRange(stubList);
        doReturn(CompletableFuture.completedFuture(new KnownAddressResponse(new HashSet<>())))
                .when(logUnitClient)
                .requestKnownAddresses(0L, 9L);
        ProtocolBatchProcessor batchProcessor = ProtocolBatchProcessor
                .builder()
                .logUnitClient(logUnitClient)
                .addressSpaceView(addressSpaceView)
                .build();
        assertThatThrownBy(() -> batchProcessor.writeRecords(ReadBatch.builder().data(stubList).build(),
                logUnitClient, 1, Duration.ofMillis(50)))
                .isInstanceOf(RetryExhaustedException.class)
                .hasRootCauseInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void writeRecordsWrongEpochException() {
        // Write and fail immediately with wrong epoch exception.
        List<Long> addresses = LongStream.range(0L, 10L).boxed().collect(Collectors.toList());
        List<LogData> stubList = createStubList(addresses);
        LogUnitClient logUnitClient = mock(LogUnitClient.class);
        AddressSpaceView addressSpaceView = mock(AddressSpaceView.class);
        doThrow(new WrongEpochException(0L)).when(logUnitClient).writeRange(stubList);
        doReturn(CompletableFuture.completedFuture(new KnownAddressResponse(new HashSet<>())))
                .when(logUnitClient)
                .requestKnownAddresses(0L, 9L);
        ProtocolBatchProcessor batchProcessor = ProtocolBatchProcessor
                .builder()
                .logUnitClient(logUnitClient)
                .addressSpaceView(addressSpaceView)
                .build();
        assertThatThrownBy(() -> batchProcessor.writeRecords(ReadBatch.builder().data(stubList).build(),
                logUnitClient, 1, Duration.ofMillis(50)))
                .isInstanceOf(RetryExhaustedException.class)
                .hasRootCauseInstanceOf(WrongEpochException.class);
    }

    @Test
    public void writeRecordsOverwriteRetry() {
        // Try writing 10 records, write only 5 of them, catch the overwrite exception.
        // Then retry writing the rest and succeed.
        List<Long> addresses = LongStream.range(0L, 10L).boxed().collect(Collectors.toList());
        List<LogData> stubList = createStubList(addresses);
        List<LogData> knownStubList = stubList.stream()
                .filter(d -> d.getGlobalAddress() % 2 != 0).collect(Collectors.toList());
        List<Long> known = addresses.stream().filter(a -> a % 2 == 0).collect(Collectors.toList());
        LogUnitClient logUnitClient = mock(LogUnitClient.class);
        AddressSpaceView addressSpaceView = mock(AddressSpaceView.class);
        CompletableFuture<Boolean> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new OverwriteException(SAME_DATA));

        doReturn(failedFuture).when(logUnitClient).writeRange(stubList);

        doReturn(CompletableFuture.completedFuture(true)).when(logUnitClient)
                .writeRange(knownStubList);
        // Even addresses are written only
        doReturn(CompletableFuture.completedFuture(new KnownAddressResponse(new HashSet<>(known))))
                .when(logUnitClient)
                .requestKnownAddresses(0L, 9L);

        ProtocolBatchProcessor batchProcessor = ProtocolBatchProcessor
                .builder()
                .logUnitClient(logUnitClient)
                .addressSpaceView(addressSpaceView)
                .build();

        TransferBatchResponse resp =
                batchProcessor.writeRecords(ReadBatch.builder().data(stubList).build(),
                        logUnitClient, 2, Duration.ofMillis(50));
        // Should succeed and carry the initial addresses after the retry
        assertThat(resp.getStatus()).isEqualTo(SUCCEEDED);
        assertThat(resp.getTransferBatchRequest().getAddresses()).isEqualTo(addresses);
    }

    @Test
    public void writeRecordsTimeoutRetry() {
        // Try writing 10 records, but fail with a timeout exception.
        // Retry and succeed.
        List<Long> addresses = LongStream.range(0L, 10L).boxed().collect(Collectors.toList());
        List<LogData> allAddressesStub = createStubList(addresses);

        List<Long> written = addresses.stream().filter(x -> x < 5).collect(Collectors.toList());
        List<LogData> secondBatchStub = allAddressesStub.stream().filter(x -> x.getGlobalAddress() >= 5).collect(Collectors.toList());
        LogUnitClient logUnitClient = mock(LogUnitClient.class);
        CompletableFuture<Boolean> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new TimeoutException());

        doReturn(failedFuture).when(logUnitClient).writeRange(allAddressesStub);
        doReturn(CompletableFuture.completedFuture(true)).when(logUnitClient).writeRange(secondBatchStub);

        doReturn(CompletableFuture.completedFuture(new KnownAddressResponse(new HashSet<>(written))))
                .when(logUnitClient)
                .requestKnownAddresses(0L, 9L);

        AddressSpaceView addressSpaceView = mock(AddressSpaceView.class);
        ProtocolBatchProcessor batchProcessor = ProtocolBatchProcessor
                .builder()
                .logUnitClient(logUnitClient)
                .addressSpaceView(addressSpaceView)
                .build();
        TransferBatchResponse resp =
                batchProcessor.writeRecords(ReadBatch.builder().data(allAddressesStub).build(),
                        logUnitClient, 2, Duration.ofMillis(50));
        assertThat(resp.getStatus()).isEqualTo(SUCCEEDED);
        assertThat(resp.getTransferBatchRequest().getAddresses()).isEqualTo(addresses);

    }

    @Test
    public void writeRecordsNetworkExceptionRetry() {
        // Try writing 10 records, but fail with a network exception.
        // Retry and succeed.
        List<Long> addresses = LongStream.range(0L, 10L).boxed().collect(Collectors.toList());
        List<LogData> allAddressesStub = createStubList(addresses);

        List<Long> written = addresses.stream().filter(x -> x < 5).collect(Collectors.toList());
        List<LogData> secondBatchStub = allAddressesStub.stream().filter(x -> x.getGlobalAddress() >= 5).collect(Collectors.toList());
        LogUnitClient logUnitClient = mock(LogUnitClient.class);
        CompletableFuture<Boolean> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new NetworkException("Error", NodeLocator.parseString("127.0.0.1:9000")));

        doReturn(failedFuture).when(logUnitClient).writeRange(allAddressesStub);
        doReturn(CompletableFuture.completedFuture(true)).when(logUnitClient).writeRange(secondBatchStub);

        doReturn(CompletableFuture.completedFuture(new KnownAddressResponse(new HashSet<>(written))))
                .when(logUnitClient)
                .requestKnownAddresses(0L, 9L);

        AddressSpaceView addressSpaceView = mock(AddressSpaceView.class);
        ProtocolBatchProcessor batchProcessor = ProtocolBatchProcessor
                .builder()
                .logUnitClient(logUnitClient)
                .addressSpaceView(addressSpaceView)
                .build();
        TransferBatchResponse resp =
                batchProcessor.writeRecords(ReadBatch.builder().data(allAddressesStub).build(),
                        logUnitClient, 2, Duration.ofMillis(50));
        assertThat(resp.getStatus()).isEqualTo(SUCCEEDED);
        assertThat(resp.getTransferBatchRequest().getAddresses()).isEqualTo(addresses);

    }

    @Test
    public void writeRecordsRetriesExhausted() {
        // Try writing 10 records, but fail every retry with a network exception.
        // At the end an exception is thrown, wrapped in the retry exhausted exception.

        List<Long> addresses = LongStream.range(0L, 10L).boxed().collect(Collectors.toList());
        List<LogData> allAddressesStub = createStubList(addresses);

        LogUnitClient logUnitClient = mock(LogUnitClient.class);
        CompletableFuture<Boolean> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new TimeoutException("Timeout"));

        doReturn(failedFuture).when(logUnitClient).writeRange(allAddressesStub);

        doReturn(CompletableFuture.completedFuture(new KnownAddressResponse(new HashSet<>())))
                .when(logUnitClient)
                .requestKnownAddresses(0L, 9L);

        AddressSpaceView addressSpaceView = mock(AddressSpaceView.class);
        ProtocolBatchProcessor batchProcessor = ProtocolBatchProcessor
                .builder()
                .logUnitClient(logUnitClient)
                .addressSpaceView(addressSpaceView)
                .build();
        assertThatThrownBy(() -> batchProcessor.writeRecords(ReadBatch.builder().data(allAddressesStub).build(),
                logUnitClient, 3, Duration.ofMillis(50)))
                .isInstanceOf(RetryExhaustedException.class)
                .hasRootCauseInstanceOf(TimeoutException.class);

    }
}