package org.corfudb.runtime.collections;

import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.test.SampleSchema;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Documents how concurrent clear() and put() transactions on the same table are resolved.
 * <p>
 * clear() logs its update with a null conflict field, so its conflict set for the stream is empty
 * and the sequencer resolves it against the stream tail (any later write to the stream conflicts).
 * put() carries a per-key conflict hash and is resolved against the sequencer's key cache only.
 * clear() adds no keys to that cache, so a put that started before a committed clear is not aborted.
 * The outcome is therefore asymmetric and depends on which transaction commits first.
 */
public class CorfuStoreClearConflictTest extends AbstractViewTest {

    private static final String NAMESPACE = "test_namespace";
    private static final String TABLE_NAME = "SampleTableA";

    private interface TxnBody {
        void run(TxnContext txn);
    }

    private CorfuStore corfuStore;
    private Table<SampleSchema.Uuid, SampleSchema.SampleTableAMsg, SampleSchema.Uuid> table;

    private void openTableWithExistingRecord() throws Exception {
        corfuStore = new CorfuStore(getDefaultRuntime());
        table = corfuStore.openTable(NAMESPACE, TABLE_NAME,
                SampleSchema.Uuid.class, SampleSchema.SampleTableAMsg.class,
                SampleSchema.Uuid.class,
                TableOptions.fromProtoSchema(SampleSchema.SampleTableAMsg.class));
        try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
            put(txn, 0);
            txn.commit();
        }
    }

    private static SampleSchema.Uuid key(long id) {
        return SampleSchema.Uuid.newBuilder().setLsb(id).setMsb(id).build();
    }

    private void put(TxnContext txn, long id) {
        txn.putRecord(table, key(id),
                SampleSchema.SampleTableAMsg.newBuilder().setPayload("payload" + id).build(),
                key(id));
    }

    private int tableSize() {
        try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
            int size = txn.count(table);
            txn.commit();
            return size;
        }
    }

    /**
     * Begin, run and commit a whole transaction on another thread. Transactional contexts are
     * thread-local and TxnContext forbids nesting, so a second concurrent transaction
     * needs its own thread.
     */
    private Throwable commitOnOtherThread(TxnBody body) throws InterruptedException {
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread t = new Thread(() -> {
            try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
                body.run(txn);
                txn.commit();
            } catch (Throwable th) {
                failure.set(th);
            }
        });
        t.start();
        t.join();
        return failure.get();
    }

    /**
     * Both transactions take the same snapshot. The put commits first, so the clear, which
     * holds a whole-stream conflict, must abort.
     */
    @Test
    public void testClearAbortsWhenPutCommitsFirst() throws Exception {
        openTableWithExistingRecord();

        TxnContext clearTxn = corfuStore.txn(NAMESPACE);
        try {
            clearTxn.clear(table);

            assertThat(commitOnOtherThread(txn -> put(txn, 1))).isNull();

            assertThatThrownBy(clearTxn::commit).isInstanceOf(TransactionAbortedException.class);
        } finally {
            clearTxn.close();
        }

        // The clear was rolled back: the original record and the new one both survive.
        assertThat(tableSize()).isEqualTo(2);
    }

    /**
     * Both transactions take the same snapshot. The clear commits first, but it registers no
     * per-key conflicts, so the put (which is checked only against per-key conflicts) still commits.
     */
    @Test
    public void testPutCommitsWhenClearCommitsFirst() throws Exception {
        openTableWithExistingRecord();

        TxnContext putTxn = corfuStore.txn(NAMESPACE);
        try {
            put(putTxn, 1);

            assertThat(commitOnOtherThread(txn -> txn.clear(table))).isNull();

            // Does not throw: the put is not aborted by the concurrent clear.
            putTxn.commit();
        } finally {
            putTxn.close();
        }

        // Serial order is clear, then put: only the put's record remains.
        assertThat(tableSize()).isEqualTo(1);
        try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
            assertThat(txn.isExists(table, key(0))).isFalse();
            assertThat(txn.isExists(table, key(1))).isTrue();
            txn.commit();
        }
    }

    /**
     * A transaction that both clears and puts has a non-empty conflict set for the stream, so it
     * loses its whole-stream conflict and is only checked against the key it wrote. A concurrent put
     * of a different key therefore does not abort it, even though the clear wipes that key.
     */
    @Test
    public void testClearPlusPutOnlyConflictsOnPutKey() throws Exception {
        openTableWithExistingRecord();

        TxnContext clearAndPutTxn = corfuStore.txn(NAMESPACE);
        try {
            clearAndPutTxn.clear(table);
            put(clearAndPutTxn, 5);

            assertThat(commitOnOtherThread(txn -> put(txn, 1))).isNull();

            // Different key: no conflict, so this commits.
            clearAndPutTxn.commit();
        } finally {
            clearAndPutTxn.close();
        }
    }

    /**
     * Same as above, but the concurrent writer commits the very key the clear+put wrote,
     * so the per-key conflict fires.
     */
    @Test
    public void testClearPlusPutConflictsOnSamePutKey() throws Exception {
        openTableWithExistingRecord();

        TxnContext clearAndPutTxn = corfuStore.txn(NAMESPACE);
        try {
            clearAndPutTxn.clear(table);
            put(clearAndPutTxn, 5);

            assertThat(commitOnOtherThread(txn -> put(txn, 5))).isNull();

            assertThatThrownBy(clearAndPutTxn::commit)
                    .isInstanceOf(TransactionAbortedException.class);
        } finally {
            clearAndPutTxn.close();
        }
    }
}
