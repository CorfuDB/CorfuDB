package org.corfudb.runtime.object.transactions;

import com.google.common.reflect.TypeToken;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.util.Utils;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.infrastructure.log.StreamLogParams.RECORDS_PER_SEGMENT;

/**
 * Created by mwei on 11/22/16.
 */
public class SnapshotTransactionContextTest extends AbstractTransactionContextTest {
    @Override
    public void TXBegin() { SnapshotTXBeginWithTimestamp(2L); }

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

        t(1, this::TXBegin);
        t(1, () -> get("k"))
            .assertResult().isEqualTo("v3");
        t(1, this::TXEnd);
    }

    /* Test if we can have implicit nested transaction for SnapshotTransactions. */
    @Test
    public void testSnapshotTxNestedImplicitTx() {
        CorfuTable<String, Integer> map = (CorfuTable<String, Integer>)
                instantiateCorfuObject(
                        new TypeToken<CorfuTable<String, Integer>>() {
                        },
                        "A"
                );
        t(0, () -> map.put("a", 1));
        t(0, () -> map.put("b", 1));
        t(0, this::TXBegin);
        t(0, () -> map.forEach((k,v) ->{
            return;
        }));
        t(0, () -> TXEnd());
    }

    /* Test if we can have explicit nested transaction for SnapshotTransactions. */
    @Test
    public void testSnapshotTxNestedExplicitTx() {
        CorfuTable<String, Integer> map = (CorfuTable<String, Integer>)
                instantiateCorfuObject(
                        new TypeToken<CorfuTable<String, Integer>>() {
                        },
                        "A"
                );
        t(0, () -> map.put("a", 1));
        t(0, () -> map.put("b", 1));
        t(0, this::TXBegin);

        t(0, this::TXBegin);
        t(0, () -> map.forEach((k,v) ->{
            return;
        }));
        t(0, this::TXEnd);
        t(0, this::TXEnd);

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

    @Test
    public void snapshotReadBeforeCompactionMark() {
        final int entryNum = RECORDS_PER_SEGMENT + 1;

        t(1, () -> put("k" , "v0"));    // TS = 0
        t(1, () -> put("k" , "v1"));    // TS = 1
        t(1, () -> put("k" , "v2"));    // TS = 2

        t(2, () -> get("k"));           // thread2 syncs to TS = 2
        t(2, this::TXBegin);            // snapshotTimestamp = 2

        final int timestamp = 3;
        for (int i = timestamp; i < entryNum; ++i) {
            AtomicInteger version = new AtomicInteger(i);
            t(1, () -> put("k" , "v" + version.get()));
        }

        // Update committed tail so that compactor can run.
        Utils.updateCommittedTail(getRuntime().getLayoutView().getLayout(), getRuntime(), entryNum - 1);

        // run compaction
        t(1, () -> startCompaction(getRuntime(), getLogUnit(SERVERS.PORT_0)));
        t(2, () -> get("k"))
                .assertThrows().hasCauseInstanceOf(TrimmedException.class);
        t(2, () -> assertThat(getRuntime().getAddressSpaceView().getCompactionMark().get())
                .isEqualTo(RECORDS_PER_SEGMENT));
        t(2, this::TXEnd);
    }

    @Test
    public void snapshotReadAfterCompactionMark() {
        t(2, this::SnapshotTXBegin);
        t(2, () -> get("k2"));
        final int entryNum = RECORDS_PER_SEGMENT;
        for (int i = 0; i <= entryNum; ++i) {
            AtomicInteger version = new AtomicInteger(i);
            t(1, () -> put("k" , "v" + version.get()));
        }

        // Update committed tail so that compactor can run.
        Utils.updateCommittedTail(getRuntime().getLayoutView().getLayout(), getRuntime(), entryNum);
        startCompaction(getRuntime(), getLogUnit(SERVERS.PORT_0));
        t(2, () -> get("k2"));
        t(1, this::SnapshotTXBegin); // SnapshotTimeStamp = RECORDS_PER_SEGMENT
        t(1, () -> get("k"))
                .assertResult().isEqualTo("v" + entryNum);
        t(1, () -> assertThat(getRuntime().getAddressSpaceView().getCompactionMark().get())
                .isEqualTo(RECORDS_PER_SEGMENT));
        t(1, this::TXEnd);
    }
}
