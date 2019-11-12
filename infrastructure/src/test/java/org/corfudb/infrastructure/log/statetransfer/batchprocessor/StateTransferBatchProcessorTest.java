package org.corfudb.infrastructure.log.statetransfer.batchprocessor;

import org.corfudb.infrastructure.log.InMemoryStreamLog;
import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.infrastructure.log.statetransfer.DataTest;
import org.corfudb.infrastructure.log.statetransfer.batch.ReadBatch;
import org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchResponse;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.protocolbatchprocessor.ProtocolBatchProcessor;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.view.AddressSpaceView;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchResponse.TransferStatus.SUCCEEDED;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

class StateTransferBatchProcessorTest extends DataTest {

    @Test
    void writeRecordsSuccess() {
        // Write successfully, a response should be sent to the caller.
        List<Long> addresses = LongStream.range(0L, 10L).boxed().collect(Collectors.toList());
        List<LogData> stubList = createStubList(addresses);
        StreamLog streamLog = mock(StreamLog.class);
        doNothing().when(streamLog).append(stubList);
        AddressSpaceView addressSpaceView = mock(AddressSpaceView.class);

        ProtocolBatchProcessor batchProcessor = ProtocolBatchProcessor
                .builder()
                .streamLog(streamLog)
                .addressSpaceView(addressSpaceView)
                .build();
        TransferBatchResponse res = batchProcessor.writeRecords(ReadBatch.builder().data(stubList).build(),
                streamLog, new AtomicInteger(0), Duration.ofMillis(500));
        assertThat(res.getStatus() == SUCCEEDED).isTrue();
        assertThat(res.getTransferBatchRequest().getAddresses()).isEqualTo(addresses);
    }

    @Test
    void writeRecordsFailure(){
        // Write and fail immediately, exception should be propagated to the caller.
        List<Long> addresses = LongStream.range(0L, 10L).boxed().collect(Collectors.toList());
        List<LogData> stubList = createStubList(addresses);
        StreamLog streamLog = mock(StreamLog.class);
        AddressSpaceView addressSpaceView = mock(AddressSpaceView.class);
        doThrow(new IllegalStateException("Illegal state")).when(streamLog).append(stubList);
        ProtocolBatchProcessor batchProcessor = ProtocolBatchProcessor
                .builder()
                .streamLog(streamLog)
                .addressSpaceView(addressSpaceView)
                .build();
        assertThatThrownBy(() -> batchProcessor.writeRecords(ReadBatch.builder().data(stubList).build(),
                streamLog, new AtomicInteger(1), Duration.ofMillis(100)))
                .isInstanceOf(IllegalStateException.class);

    }

    @Test
    void writeRecordsRetry(){
        // Try writing 10 records, write only 5 of them, catch the exception.
        // Then retry writing the rest and succeed.
        // At the end return a transfer batch response with status SUCCEEDED and the correct
        // initial addresses.
        List<Long> addresses = LongStream.range(0L, 10L).boxed().collect(Collectors.toList());
        List<LogData> stubList = createStubList(addresses);
        List<Long> known = addresses.stream().filter(a -> a % 2 == 0).collect(Collectors.toList());
        StreamLog streamLog = mock(StreamLog.class);
        AddressSpaceView addressSpaceView = mock(AddressSpaceView.class);
        doThrow(new IllegalStateException("Illegal state")).when(streamLog).append(stubList);
        // Even addresses are written only
        doReturn(new HashSet<>(known)).when(streamLog).getKnownAddressesInRange(0L, 9L);

        ProtocolBatchProcessor batchProcessor = ProtocolBatchProcessor
                .builder()
                .streamLog(streamLog)
                .addressSpaceView(addressSpaceView)
                .build();
        TransferBatchResponse resp =
                batchProcessor.writeRecords(ReadBatch.builder().data(stubList).build(),
                streamLog, new AtomicInteger(2), Duration.ofMillis(100));
        // Should succeed and carry the initial addresses after the retry
        assertThat(resp.getStatus()).isEqualTo(SUCCEEDED);
        assertThat(resp.getTransferBatchRequest().getAddresses()).isEqualTo(addresses);
    }

    @Test
    void writeRecordsRetryEmpty(){
        // Try writing 10 records, write all of them, but catch the exception.
        // Retry and succeed.
        List<Long> addresses = LongStream.range(0L, 10L).boxed().collect(Collectors.toList());
        List<LogData> stubList = createStubList(addresses);
        StreamLog streamLog = new InMemoryStreamLog();
        StreamLog spy = spy(streamLog);

        doAnswer(answer -> {
            answer.callRealMethod();
            throw new IllegalStateException("Illegal state");
        }).when(spy).append(stubList);

        AddressSpaceView addressSpaceView = mock(AddressSpaceView.class);
        ProtocolBatchProcessor batchProcessor = ProtocolBatchProcessor
                .builder()
                .streamLog(streamLog)
                .addressSpaceView(addressSpaceView)
                .build();
        TransferBatchResponse resp =
                batchProcessor.writeRecords(ReadBatch.builder().data(stubList).build(),
                        streamLog, new AtomicInteger(2), Duration.ofMillis(100));
        assertThat(resp.getStatus()).isEqualTo(SUCCEEDED);
        assertThat(resp.getTransferBatchRequest().getAddresses()).isEqualTo(addresses);

    }
}