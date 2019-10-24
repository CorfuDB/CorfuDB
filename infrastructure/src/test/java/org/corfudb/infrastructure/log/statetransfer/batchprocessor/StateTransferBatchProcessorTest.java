package org.corfudb.infrastructure.log.statetransfer.batchprocessor;

import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.infrastructure.log.statetransfer.DataTest;
import org.corfudb.infrastructure.log.statetransfer.batch.BatchResult;
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

class StateTransferBatchProcessorTest extends DataTest {

    @Test
    void writeRecords() {
        List<Long> addresses = LongStream.range(0L, 10L).boxed().collect(Collectors.toList());
        List<LogData> stubList = createStubList(addresses);
        StreamLog streamLog = mock(StreamLog.class);
        doNothing().when(streamLog).append(stubList);
        AddressSpaceView addressSpaceView = mock(AddressSpaceView.class);

        ProtocolBatchProcessor batchProcessor = new ProtocolBatchProcessor(streamLog, addressSpaceView);
        BatchResult res = batchProcessor.writeRecords(ReadBatch.builder().data(stubList).build(), streamLog);
        assertThat(res.getStatus() == BatchResult.FailureStatus.SUCCEEDED).isTrue();
        assertThat(res.getAddresses()).isEqualTo(addresses);
        doThrow(new IllegalStateException()).when(streamLog).append(stubList);
        batchProcessor = new ProtocolBatchProcessor(streamLog, addressSpaceView);
        res = batchProcessor.writeRecords(ReadBatch.builder().data(stubList).build(), streamLog);
        assertThat(res.getStatus() == BatchResult.FailureStatus.FAILED).isTrue();
        assertThat(res.getAddresses()).isEqualTo(addresses);
    }
}