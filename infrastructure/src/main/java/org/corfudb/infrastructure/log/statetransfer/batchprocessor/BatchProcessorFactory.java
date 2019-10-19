package org.corfudb.infrastructure.log.statetransfer.batchprocessor;

import org.corfudb.infrastructure.log.statetransfer.batchprocessor.committedbatchprocessor.CommittedBatchProcessor;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.protocolbatchprocessor.ProtocolBatchProcessor;

import static org.corfudb.infrastructure.log.statetransfer.batchprocessor.BatchProcessorFactory.BatchProcessorType.COMMITTED;
import static org.corfudb.infrastructure.log.statetransfer.batchprocessor.BatchProcessorFactory.BatchProcessorType.PROTOCOL;

public class BatchProcessorFactory {

    public enum BatchProcessorType {
        COMMITTED,
        PROTOCOL
    }

    public static StateTransferBatchProcessor createBatchProcessor(StateTransferBatchProcessorData data,
                                                                   BatchProcessorType batchProcessorType) {
        if (batchProcessorType.equals(COMMITTED)) {
            return new CommittedBatchProcessor(data.getStreamLog(), data.getClientMap());
        } else if (batchProcessorType.equals(PROTOCOL)) {
            return new ProtocolBatchProcessor(data.getStreamLog(), data.getAddressSpaceView());
        } else {
            throw new IllegalStateException("Not implemented.");
        }
    }
}
