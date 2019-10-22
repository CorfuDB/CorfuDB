package org.corfudb.infrastructure.log.statetransfer.batchprocessor;

import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.committedbatchprocessor.CommittedBatchProcessor;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.protocolbatchprocessor.ProtocolBatchProcessor;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.view.AddressSpaceView;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.corfudb.infrastructure.log.statetransfer.batchprocessor.BatchProcessorFactory.BatchProcessorType.COMMITTED;
import static org.corfudb.infrastructure.log.statetransfer.batchprocessor.BatchProcessorFactory.BatchProcessorType.PROTOCOL;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

import static org.assertj.core.api.Assertions.assertThat;

class BatchProcessorFactoryTest {

    @Test
    void createBatchProcessor() {
        StreamLog streamLog = mock(StreamLog.class);
        AddressSpaceView addressSpaceView = mock(AddressSpaceView.class);
        Map<String, LogUnitClient> map = mock(Map.class);
        StateTransferBatchProcessorData stateTransferBatchProcessorData = new StateTransferBatchProcessorData(streamLog, addressSpaceView, map);
        StateTransferBatchProcessor batchProcessor = BatchProcessorFactory.createBatchProcessor(stateTransferBatchProcessorData, COMMITTED);
        assertThat(batchProcessor).isInstanceOf(CommittedBatchProcessor.class);
        batchProcessor = BatchProcessorFactory.createBatchProcessor(stateTransferBatchProcessorData, PROTOCOL);
        assertThat(batchProcessor).isInstanceOf(ProtocolBatchProcessor.class);

    }
}