package org.corfudb.infrastructure.log.statetransfer.batchprocessor;

import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.infrastructure.log.statetransfer.DataTest;
import org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchResponse;
import org.corfudb.infrastructure.log.statetransfer.batch.ReadBatch;
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
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

class StateTransferBatchProcessorTest extends DataTest {

    @Test
    void writeRecords() {
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
}