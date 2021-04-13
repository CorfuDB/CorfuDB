package org.corfudb.generator;

import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Correctness recorder
 */
public class Correctness {

    private static final String TX_PATTERN = "%s, %s";
    private static final String TX_PATTERN_VERSION = "%s, %s, %s";

    public static final String TX_START = "start";
    public static final String TX_END = "end";
    public static final String TX_ABORTED = "aborted";

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
     * @param txType tx prefix
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
     * @param version if we have a version to report
     * @param fields  fields to report
     */
    public void recordTransactionMarkers(boolean version, String... fields) {
        if (version) {
            recordOperation(String.format(TX_PATTERN_VERSION, fields[0], fields[1], fields[2]), OperationTxType.NON_TX);
        } else {
            recordOperation(String.format(TX_PATTERN, fields[0], fields[1]), OperationTxType.NON_TX);
        }
    }

    public enum OperationTxType {
        TX, NON_TX
    }

}
