package org.corfudb.infrastructure.log.statetransfer.batchprocessor;

import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.infrastructure.log.statetransfer.DataTest;
import org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchResponse;
import org.corfudb.infrastructure.log.statetransfer.batch.ReadBatch;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.protocolbatchprocessor.ProtocolBatchProcessor;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.view.AddressSpaceView;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

class StateTransferTransferBatchRequestProcessorTest extends DataTest {

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
        TransferBatchResponse res = batchProcessor.writeRecords(ReadBatch.builder().data(stubList).build(), streamLog);
        assertThat(res.getStatus() == TransferBatchResponse.TransferStatus.SUCCEEDED).isTrue();
        assertThat(res.getTransferBatchRequest().getAddresses()).isEqualTo(addresses);
        doThrow(new IllegalStateException()).when(streamLog).append(stubList);
        batchProcessor = ProtocolBatchProcessor
                .builder()
                .streamLog(streamLog)
                .addressSpaceView(addressSpaceView)
                .build();
        res = batchProcessor.writeRecords(ReadBatch.builder().data(stubList).build(), streamLog);
        assertThat(res.getStatus() == TransferBatchResponse.TransferStatus.FAILED).isTrue();
        assertThat(res.getTransferBatchRequest().getAddresses()).isEqualTo(addresses);
    }
}