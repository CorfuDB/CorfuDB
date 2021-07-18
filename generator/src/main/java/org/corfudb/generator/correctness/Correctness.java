package org.corfudb.generator.correctness;

import org.corfudb.generator.distributions.Keys;
import org.corfudb.generator.operations.Operation;
import org.corfudb.generator.state.TxState.TxStatus;
import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Correctness recorder
 */
public class Correctness {

    //operation_type, tx_state, version
    private static final String TX_PATTERN_VERSION = "%s, %s, %s";

    private static final Logger CORRECTNESS_LOGGER = LoggerFactory.getLogger("correctness");

    public void recordOperation(String operation) {
        if (TransactionalContext.isInTransaction()) {
            recordOperation(operation, OperationTxType.TX);
        } else {
            recordOperation(operation, OperationTxType.NON_TX);
        }
    }

    /**
     * The format of the operation:
     * 2021-02-02_23:44:57.853, [pool-6-thread-7], TxRead, table_36:key_69=3a1f57b1-35a4-4a7f-aee0-99e00d7e1cf2, 136
     *
     * @param operation log message
     * @param txType    tx prefix
     */
    public void recordOperation(String operation, OperationTxType txType) {
        if (txType == OperationTxType.TX) {
            long version = TransactionalContext.getCurrentContext().getSnapshotTimestamp().getSequence();
            CORRECTNESS_LOGGER.info("Tx{}, {}", operation, version);
        } else {
            CORRECTNESS_LOGGER.info(operation);
        }
    }

    /**
     * Record a transaction marker operation
     *
     * @param version  a version to report
     * @param txStatus the status of a transaction
     * @param opType   operation type
     */
    public void recordTransactionMarkers(Operation.Type opType, TxStatus txStatus, Keys.Version version) {
        String msg = String.format(TX_PATTERN_VERSION, opType.getOpType(), txStatus.getStatus(), version.getVer());
        recordOperation(msg, OperationTxType.NON_TX);
    }

    public void recordTransactionMarkers(Operation.Type opType, TxStatus txStatus) {
        recordTransactionMarkers(opType, txStatus, Keys.Version.noVersion());
    }

    public enum OperationTxType {
        TX, NON_TX
    }

}
