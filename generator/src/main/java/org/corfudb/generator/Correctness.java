package org.corfudb.generator;

import org.corfudb.generator.distributions.Keys;
import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.corfudb.runtime.view.Address;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

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

    private static final Logger correctnessLogger = LoggerFactory.getLogger("correctness");

    /**
     * The format of the operation:
     * 2021-02-02_23:44:57.853, [pool-6-thread-7], TxRead, table_36:key_69=3a1f57b1-35a4-4a7f-aee0-99e00d7e1cf2, 136
     *
     * @param operation log message
     * @param transactionPrefix tx prefix
     */
    public static void recordOperation(String operation, boolean transactionPrefix) {
        if (transactionPrefix) {
            long version = TransactionalContext.getCurrentContext().getSnapshotTimestamp().getSequence();
            correctnessLogger.info("Tx{}, {}", operation, version);
        } else {
            correctnessLogger.info(operation);
        }
    }

    public static Keys.Version getVersion() {
        long ver = Optional.ofNullable(TransactionalContext.getCurrentContext())
                .map(ctx-> ctx.getSnapshotTimestamp().getSequence())
                .orElse(Address.NON_ADDRESS);

        return Keys.Version.build(ver);
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
