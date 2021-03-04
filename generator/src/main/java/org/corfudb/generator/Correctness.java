package org.corfudb.generator;

import ch.qos.logback.classic.Logger;
import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.slf4j.LoggerFactory;

/**
 * Correctness recorder
 */
public class Correctness {

    private Correctness() {
        throw new IllegalStateException("Utility class");
    }

    private static final String TX_PATTERN = "%s, %s";
    private static final String TX_PATTERN_VERSION = "%s, %s, %s";

    public static final String TX_START = "start";
    public static final String TX_END = "end";
    public static final String TX_ABORTED = "aborted";

    private static final Logger correctnessLogger = (Logger) LoggerFactory.getLogger("correctness");

    public static void recordOperation(String operation, boolean transactionPrefix) {
        if (transactionPrefix) {
            long sequence = TransactionalContext.getCurrentContext().getSnapshotTimestamp().getSequence();
            correctnessLogger.info("Tx{}, {}", operation, sequence);
        } else {
            correctnessLogger.info(operation);
        }
    }

    /**
     * Record a transaction marker operation
     *
     * @param version if we have a version to report
     * @param fields  fields to report
     */
    public static void recordTransactionMarkers(boolean version, String... fields) {
        if (version) {
            recordOperation(String.format(TX_PATTERN_VERSION, fields[0], fields[1], fields[2]), false);
        } else {
            recordOperation(String.format(TX_PATTERN, fields[0], fields[1]), false);
        }
    }

}
