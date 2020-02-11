package org.corfudb.infrastructure.log.statetransfer.batchprocessor;


import com.google.common.collect.ImmutableList;
import org.corfudb.infrastructure.log.statetransfer.DataTest;
import org.corfudb.infrastructure.log.statetransfer.batch.ReadBatch;
import org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchResponse;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.protocolbatchprocessor.ProtocolBatchProcessor;
import org.corfudb.protocols.wireprotocol.KnownAddressResponse;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.view.AddressSpaceView;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
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
        // Write successfully, a response should be sent to the caller.
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
                logUnitClient, 1, Duration.ofMillis(500));
        assertThat(res.getStatus() == SUCCEEDED).isTrue();
        assertThat(res.getTransferBatchRequest().getAddresses()).isEqualTo(addresses);
    }

    @Test
    public void writeRecordsFailure() {
        // Write and fail immediately, exception should be propagated to the caller.
        List<Long> addresses = LongStream.range(0L, 10L).boxed().collect(Collectors.toList());
        List<LogData> stubList = createStubList(addresses);
        LogUnitClient logUnitClient = mock(LogUnitClient.class);
        AddressSpaceView addressSpaceView = mock(AddressSpaceView.class);
        doThrow(new OverwriteException(SAME_DATA)).when(logUnitClient).writeRange(stubList);
        doReturn(CompletableFuture.completedFuture(new KnownAddressResponse(new HashSet<>())))
                .when(logUnitClient)
                .requestKnownAddresses(0L, 9L);
        ProtocolBatchProcessor batchProcessor = ProtocolBatchProcessor
                .builder()
                .logUnitClient(logUnitClient)
                .addressSpaceView(addressSpaceView)
                .build();
        assertThatThrownBy(() -> batchProcessor.writeRecords(ReadBatch.builder().data(stubList).build(),
                logUnitClient, 1, Duration.ofMillis(100)))
                .isInstanceOf(IllegalStateException.class)
                .hasRootCauseInstanceOf(OverwriteException.class);

    }

    @Test
    public void writeRecordsRetry() {
        // Try writing 10 records, write only 5 of them, catch the exception.
        // Then retry writing the rest and succeed.
        // At the end return a transfer batch response with status SUCCEEDED and the correct
        // initial addresses.
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
                        logUnitClient, 2, Duration.ofMillis(100));
        // Should succeed and carry the initial addresses after the retry
        assertThat(resp.getStatus()).isEqualTo(SUCCEEDED);
        assertThat(resp.getTransferBatchRequest().getAddresses()).isEqualTo(addresses);
    }

    @Test
    public void writeRecordsRetryEmpty() {
        // Try writing 10 records, write all of them, but catch the exception.
        // Retry and succeed.
        List<Long> addresses = LongStream.range(0L, 10L).boxed().collect(Collectors.toList());
        List<LogData> stubList = createStubList(addresses);
        LogUnitClient logUnitClient = mock(LogUnitClient.class);
        CompletableFuture<Boolean> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new IllegalStateException("Illegal state"));

        doReturn(failedFuture).when(logUnitClient).writeRange(stubList);
        doReturn(CompletableFuture.completedFuture(true)).when(logUnitClient).writeRange(ImmutableList.of());
        doReturn(CompletableFuture.completedFuture(new KnownAddressResponse(new HashSet<>(addresses))))
                .when(logUnitClient)
                .requestKnownAddresses(0L, 9L);

        AddressSpaceView addressSpaceView = mock(AddressSpaceView.class);
        ProtocolBatchProcessor batchProcessor = ProtocolBatchProcessor
                .builder()
                .logUnitClient(logUnitClient)
                .addressSpaceView(addressSpaceView)
                .build();
        TransferBatchResponse resp =
                batchProcessor.writeRecords(ReadBatch.builder().data(stubList).build(),
                        logUnitClient, 2, Duration.ofMillis(100));
        assertThat(resp.getStatus()).isEqualTo(SUCCEEDED);
        assertThat(resp.getTransferBatchRequest().getAddresses()).isEqualTo(addresses);

    }
}