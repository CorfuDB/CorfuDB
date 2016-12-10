package org.corfudb.runtime.object.transactions;

import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.CorfuSharedCounter;
import org.corfudb.runtime.view.AbstractViewTest;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by dalia on 12/8/16.
 */
public class OptimisticTXConcurrencyTest extends AbstractViewTest {

    @Before
    public void before() { getDefaultRuntime(); }

    static final int INITIAL = 32;
    static final int OVERWRITE_ONCE = 33;
    static final int OVERWRITE_TWICE = 34;

    /**
     * test concurrent transactions for opacity. This works as follows:
     *
     * there are 'n' shared counters numbered 0..n-1, and 'n' threads.
     * Thread j initializes counter j to INITIAL. Then all threads start transactions simultaneously.
     *
     * Within each transaction, each thread repeat twice modify-read own-read other.
     * Specifically,
     *  - thread j modifies counter j to OVERWRITE_ONCE,
     *  - reads counter j (own), expecting to read OVERWRITE_ONCE,
     *  - reads counter (j+1) mod n, expecting to read either OVERWRITE_ONCE or INITIAL,
     *
     *  - then thread j modifies counter j to OVERWRITE_TWICE,
     *  - reads counter j (own), expecting to read OVERWRITE_TWICE,
     *  - reads counter (j+1) mod n, expecting to read either OVERWRITE_ONCE or INITIAL,
     *
     *  Then all transactions try to commit.
     *  - The first n-1 should succeed
     *  - The last must fail, since it is wrapping-around and reading counter 0, which has been modified.
     *
     * @throws Exception
     */
    @Test
    public void testOpacity() throws Exception {

        final int n = 5;
        assertThat(n).isGreaterThan(1); // don't change concurrency to less than 2, test will break

        CorfuSharedCounter[] sharedCounters = new CorfuSharedCounter[n];

        for (int i = 0; i < n; i++)
          sharedCounters[i] = getRuntime().getObjectsView()
                  .build()
                  .setStreamName("test"+i)
                  .setType(CorfuSharedCounter.class)
                  .open();

        // initialize all shared counters
        for (int i = 0; i < n; i++)
            sharedCounters[i].setValue(INITIAL);

        // start concurrent transacstions on all threads
        for (int i = 0; i < n; i++)
            t(i, this::TXBegin);

        // modify shared counter per thread
        for (int i = 0; i < n; i++) {
            final int threadind = i;
            t(threadind, () -> sharedCounters[threadind].setValue(OVERWRITE_ONCE));
        }

        // each thread reads a shared counter modified by another thread
        for (int i = 0; i < n; i++) {
            final int threadind = i;
            t(threadind, () ->
                    assertThat(sharedCounters[(threadind+1)%n].getValue())
            .isBetween(INITIAL, OVERWRITE_ONCE)
            );
            t(threadind, () ->
                    assertThat(sharedCounters[threadind].getValue())
                            .isEqualTo(OVERWRITE_ONCE)
            );
        }

        // each thread modifies its own shared counter
        for (int i = 0; i < n; i++) {
            final int threadind = i;
            t(threadind, () -> sharedCounters[threadind].setValue(OVERWRITE_TWICE) );
        }


        // verify opacity: Each thread reads its own writes,
        // and reads a version of other objects that it started a transaction with
        for (int i = 0; i < n; i++) {
            final int threadind = i;
            t(threadind, () ->
                    assertThat(sharedCounters[(threadind+1)%n].getValue())
                            .isBetween(INITIAL, OVERWRITE_ONCE)
            );
            t(threadind, () ->
                    assertThat(sharedCounters[threadind].getValue())
                            .isEqualTo(OVERWRITE_TWICE)
            );
        }

        // try to commit all transactions; all but the last should succeed
        for (int i = 0; i < n-1; i++)
            t(0, this::TXEnd);

        t(n-1, this::TXEnd)
                    .assertThrows()
                    .isInstanceOf(TransactionAbortedException.class);
    }

    /**
     * test multiple threads optimistically manipulating objects concurrently. This works as follows:
     *
     * there are 'n' shared counters numbered 0..n-1, and 'n' threads.
     * Thread j initializes counter j to INITIAL. Then all threads start transactions simultaneously.
     *
     * Within each transaction, each thread repeat twice modify-read own-read other.
     * Specifically,
     *  - thread j modifies counter j to OVERWRITE_ONCE,
     *  - reads counter j (own), expecting to read OVERWRITE_ONCE,
     *  - reads counter (j+1) mod n, expecting to read either OVERWRITE_ONCE or INITIAL,
     *
     *  - then thread j modifies counter (j+1) mod n to OVERWRITE_TWICE,
     *  - reads counter j+1 mod n (own), expecting to read OVERWRITE_TWICE,
     *  - reads counter j (the one it changed before), expecting to read OVERWRITE_ONCE ,
     *
     *  Then all transactions try to commit.
     *  They should succeed in alteration -- one succeeds, next one fails (because it reads a counter modified by previous), next one succeeds, etc.
     *  The last one will always fail, because it reads the counter of the first.
     */
    @Test
    public void testOptimism() throws Exception {

        final int n = 5;
        assertThat(n).isGreaterThan(1); // don't change concurrency to less than 2, test will break

        CorfuSharedCounter[] sharedCounters = new CorfuSharedCounter[n];

        for (int i = 0; i < n; i++)
            sharedCounters[i] = getRuntime().getObjectsView()
                    .build()
                    .setStreamName("test"+i)
                    .setType(CorfuSharedCounter.class)
                    .open();

        // initialize all shared counters
        for (int i = 0; i < n; i++)
            sharedCounters[i].setValue(INITIAL);

        // start concurrent transacstions on all threads
        for (int i = 0; i < n; i++)
            t(i, this::TXBegin);

        // modify shared counter per thread
        for (int i = 0; i < n; i++) {
            final int threadind = i;
            t(threadind, () -> sharedCounters[threadind].setValue(OVERWRITE_ONCE));
        }

        // each thread reads a shared counter modified by another thread
        for (int i = 0; i < n; i++) {
            final int threadind = i;
            t(threadind, () ->
                    assertThat(sharedCounters[(threadind+1)%n].getValue())
                            .isBetween(INITIAL, OVERWRITE_ONCE)
            );
        }

        // each thread modifies a counter written by another thread
        for (int i = 0; i < n; i++) {
            final int threadind = i;
            t(threadind, () -> sharedCounters[(threadind+1)%n].setValue(OVERWRITE_TWICE) );
        }


        // verify opacity: Each thread reads its own writes,
        // and reads a version of other objects that it started a transaction with
        for (int i = 0; i < n; i++) {
            final int threadind = i;
            t(threadind, () ->
                    assertThat(sharedCounters[(threadind+1)%n].getValue())
                            .isEqualTo(OVERWRITE_TWICE)
            );
            t(threadind, () ->
                    assertThat(sharedCounters[threadind].getValue())
                            .isEqualTo(OVERWRITE_ONCE)
            );
        }

        // try to commit all transactions but the last.
        // they will succeed in alteration -- one succeeds, next one fails (because it reads a counter modified by previous), next one succeeds, etc.
        int i = 0;
        while (i < n-1) {
            final int threadind = i;
            if (i % 2 == 0) // should succeed
                t(threadind, this::TXEnd);
            else {
                t(threadind, this::TXEnd)
                        .assertThrows()
                        .isInstanceOf(TransactionAbortedException.class);
            }
            i++;
        }

        // the last one will always fail, because it reads the counter of the first.
        t(n-1, this::TXEnd)
                .assertThrows()
                .isInstanceOf(TransactionAbortedException.class);
    }


    void TXEnd() {
        getRuntime().getObjectsView().TXEnd();
    }

    void TXBegin() {
        getRuntime().getObjectsView().TXBuild()
                .setType(TransactionType.OPTIMISTIC)
                .begin();
    }
}
