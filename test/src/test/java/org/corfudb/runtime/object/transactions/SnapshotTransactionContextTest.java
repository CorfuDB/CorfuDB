package org.corfudb.runtime.object.transactions;

import com.google.common.reflect.TypeToken;
import org.corfudb.runtime.collections.SMRMap;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 11/22/16.
 */
public class SnapshotTransactionContextTest extends AbstractTransactionContextTest {
    @Override
    public void TXBegin() { SnapshotTXBegin(); }



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

    /* Test if we can have implicit nested transaction for SnapshotTransactions. */
    @Test
    public void testSnapshotTxNestedImplicitTx() {
        SMRMap<String, Integer> map = (SMRMap<String, Integer>)
                instantiateCorfuObject(
                        new TypeToken<SMRMap<String, Integer>>() {
                        },
                        "A"
                );
        t(0, () -> map.put("a", 1));
        t(0, () -> map.put("b", 1));
        t(0, this::SnapshotTXBegin);
        t(0, () -> map.forEach((k,v) ->{
            return;
        }));
        t(0, () -> TXEnd());
    }

    /* Test if we can have explicit nested transaction for SnapshotTransactions. */
    @Test
    public void testSnapshotTxNestedExplicitTx() {
        SMRMap<String, Integer> map = (SMRMap<String, Integer>)
                instantiateCorfuObject(
                        new TypeToken<SMRMap<String, Integer>>() {
                        },
                        "A"
                );
        t(0, () -> map.put("a", 1));
        t(0, () -> map.put("b", 1));
        t(0, this::SnapshotTXBegin);

        t(0, this::TXBegin);
        t(0, () -> map.forEach((k,v) ->{
            return;
        }));
        t(0, this::TXEnd);
        t(0, this::TXEnd);

    }

    /* Test if the default address for a snapshot transaction is the current tail */
    @Test
    public void snapshotTxnDefaultIsTailOfStream() {
        final int NUM_ITEMS = 5;
        SMRMap<String, Integer> map = (SMRMap<String, Integer>)
                instantiateCorfuObject(
                        new TypeToken<SMRMap<String, Integer>>() {
                        },
                        "A"
                );

        for (int i = 0; i < NUM_ITEMS; i++) {
            map.put("key" + i, i);
        }

        // Intentionally omit .setSnapshot() on the builder.
        getDefaultRuntime().getObjectsView().TXBuild()
                .setType(TransactionType.SNAPSHOT)
                .begin();
        try {
            assertThat(map.size()).isEqualTo(NUM_ITEMS);
        } finally {
            TXEnd();
        }
    }
}
