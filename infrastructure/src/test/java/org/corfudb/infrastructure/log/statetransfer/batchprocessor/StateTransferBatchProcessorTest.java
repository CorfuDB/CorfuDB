package org.corfudb.infrastructure.log.statetransfer.batchprocessor;

import org.corfudb.common.result.Result;
import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.infrastructure.log.statetransfer.DataTest;
import org.corfudb.infrastructure.log.statetransfer.batch.Batch;
import org.corfudb.infrastructure.log.statetransfer.batch.BatchResult;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.protocolbatchprocessor.ProtocolBatchProcessor;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.view.AddressSpaceView;
import org.corfudb.runtime.view.ReadOptions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;
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

        ProtocolBatchProcessor batchProcessor = new ProtocolBatchProcessor(streamLog, addressSpaceView);
        Result<Long, BatchProcessorFailure> res = batchProcessor.writeRecords(stubList, streamLog);
        assertThat(res.isValue()).isTrue();
        assertThat(res.get()).isEqualTo((long)addresses.size());
        doThrow(new IllegalStateException()).when(streamLog).append(stubList);
        batchProcessor = new ProtocolBatchProcessor(streamLog, addressSpaceView);
        res = batchProcessor.writeRecords(stubList, streamLog);
        assertThat(res.isError()).isTrue();
        assertThat(res.getError().getAddresses()).isEqualTo(addresses);
        assertThat(res.getError().getThrowable()).isInstanceOf(IllegalStateException.class);

    }
}