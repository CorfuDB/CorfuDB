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

    private static final String TX_PATTERN = "%s, %s";
    private static final String TX_PATTERN_VERSION = "%s, %s, %s";

    private static final Logger CORRECTNESS_LOGGER = LoggerFactory.getLogger("correctness");

    public void recordOperation(LogMessage operation) {
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
    public void recordOperation(LogMessage operation, OperationTxType txType) {
        if (txType == OperationTxType.TX) {
            long version = TransactionalContext.getCurrentContext().getSnapshotTimestamp().getSequence();
            CORRECTNESS_LOGGER.info("Tx{}, {}", operation.getMessage(), version);
        } else {
            CORRECTNESS_LOGGER.info(operation.getMessage());
        }
    }

    /**
     * Record a transaction marker operation
     *
     * @param version  a version to report
     * @param txStatus the status of a transaction
     * @param opType   operation type
     */
    public void recordTransactionMarkers(Operation.Type opType, TxStatus txStatus, Keys.Version version, boolean useVersion) {
        String msg;
        if (version.equals(Keys.Version.noVersion())) {
            msg = String.format(TX_PATTERN, opType.getOpType(), txStatus.getStatus());
        } else {
            msg = String.format(TX_PATTERN_VERSION, opType.getOpType(), txStatus.getStatus(), version.getVer());
        }
        recordOperation(() -> msg, OperationTxType.NON_TX);
    }

    public void recordTransactionMarkers(Operation.Type opType, TxStatus txStatus, boolean useVersion) {
        recordTransactionMarkers(opType, txStatus, Keys.Version.noVersion(), useVersion);
    }

    public enum OperationTxType {
        TX, NON_TX
    }

    public interface LogMessage {
        String getMessage();
    }
}
