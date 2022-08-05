package org.corfudb.runtime.object.transactions;

import org.junit.Test;

/**
 * Created by mwei on 11/22/16.
 */
public class SnapshotTransactionContextTest extends AbstractTransactionContextTest {
    @Override
    public void TXBegin() { SnapshotTXBegin(); }


    @Test
    public void defaultSnapshotTest() {
        t1(() -> put("k" , "v1"));    // TS = 0
        t1(() -> put("k" , "v2"));    // TS = 1

        // Start a snapshot transaction with the default timestamp
        t2(() ->  getRuntime().getObjectsView().TXBuild()
                .type(TransactionType.SNAPSHOT)
                .build()
                .begin());
        // Verify that the snapshot transaction reads the table
        // with the latest snapshot
        t2(() -> get("k")).assertResult().isEqualTo("v2");
        t2(() -> TXEnd());
    }

    /** Check if we can read a snapshot from the past, without
     * concurrent modifications.
     */
    @Test
    public void snapshotReadable() {

        t(1, () -> put("k" , "v1"));    // TS = 0
        t(1, () -> put("k" , "v2"));    // TS = 1
        t(1, () -> put("k" , "v3"));    // TS = 2
        t(1, () -> put("k" , "v4"));    // TS = 3

        t(1, this::SnapshotTXBegin);
        t(1, () -> get("k"))
            .assertResult().isEqualTo("v3");
        t(1, this::TXEnd);
    }

    /** Ensure that a snapshot remains stable, even with
     * concurrent modifications.
     */
    @Test
    public void snapshotReadableWithConcurrentWrites() {

        t1(() -> put("k" , "v1"));    // TS = 0
        t2(() -> put("k" , "v2"));    // TS = 1
        t3(() -> put("k" , "v3"));    // TS = 2
        t4(() -> put("k" , "v4"));    // TS = 3

        t2(this::SnapshotTXBegin);
        t2(() -> get("k"))
                .assertResult().isEqualTo("v3");
        t4(() -> put("k" , "v4"));    // TS = 4
        t2(() -> get("k"))
                .assertResult().isEqualTo("v3");
        t2(this::TXEnd);
    }
}
