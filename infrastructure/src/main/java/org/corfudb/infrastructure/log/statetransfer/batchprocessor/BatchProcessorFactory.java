package org.corfudb.infrastructure.log.statetransfer.batchprocessor;

import org.corfudb.infrastructure.log.statetransfer.batchprocessor.committedbatchprocessor.CommittedBatchProcessor;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.protocolbatchprocessor.ProtocolBatchProcessor;

import static org.corfudb.infrastructure.log.statetransfer.batchprocessor.BatchProcessorFactory.BatchProcessorType.COMMITTED;
import static org.corfudb.infrastructure.log.statetransfer.batchprocessor.BatchProcessorFactory.BatchProcessorType.PROTOCOL;

/**
 * A factory that produces batch processors.
 */
public class BatchProcessorFactory {

    public enum BatchProcessorType {
        COMMITTED,
        PROTOCOL
    }

    /**
     * Produces an instance of a batch processor.
     * @param data A batch processor data.
     * @param batchProcessorType A batch processor type.
     * @return A concrete instance of a batch processor.
     */
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
