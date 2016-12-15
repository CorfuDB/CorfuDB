package org.corfudb.runtime.object.transactions;

import org.junit.Test;

/**
 * Created by mwei on 11/22/16.
 */
public class SnapshotTransactionContextTest extends AbstractTransactionContextTest {

    /** Check if we can read a snapshot from the past, without
     * concurrent modifications.
     */
    @Test
    public void snapshotReadable() {

        t(1, () -> put("k" , "v1"));    // TS = 0
        t(1, () -> put("k" , "v2"));    // TS = 1
        t(1, () -> put("k" , "v3"));    // TS = 2
        t(1, () -> put("k" , "v4"));    // TS = 3

        t(1, this::TXBegin);
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

        t2(this::TXBegin);
        t2(() -> get("k"))
                .assertResult().isEqualTo("v3");
        t4(() -> put("k" , "v4"));    // TS = 4
        t2(() -> get("k"))
                .assertResult().isEqualTo("v3");
        t2(this::TXEnd);
    }


    @Override
    void TXBegin() {
        // By default, begin a snapshot at address 2L
        getRuntime().getObjectsView().TXBuild()
                .setType(TransactionType.SNAPSHOT)
                .setSnapshot(2L)
                .begin();
    }
}
