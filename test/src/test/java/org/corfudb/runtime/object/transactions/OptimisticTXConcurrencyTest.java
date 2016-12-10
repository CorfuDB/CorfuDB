package org.corfudb.runtime.object.transactions;

import lombok.Getter;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.CorfuSharedCounter;
import org.corfudb.runtime.view.AbstractViewTest;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by dalia on 12/8/16.
 */
public class OptimisticTXConcurrencyTest extends AbstractViewTest {
    @Getter
    final String defaultConfigurationString = getDefaultEndpoint();

    @Before
    public void before() { getDefaultRuntime(); }

    @Test
    public void testOpacity() throws Exception {

        int n = 5;
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
            sharedCounters[i].setValue(32);

        // start concurrent transacstions on all threads
        for (int i = 0; i < n; i++)
            t(i, this::TXBegin);

        // modify shared counter per thread
        for (int i = 0; i < n; i++) {
            final int threadind = i;
            t(threadind, () -> sharedCounters[threadind].setValue(33));
        }

        // each thread reads a shared counter modified by another thread
        for (int i = 0; i < n; i++) {
            final int threadind = i;
            t(threadind, () ->
                    assertThat(sharedCounters[(threadind+1)%n].getValue())
            .isBetween(32, 33)
            );
        }

        // each thread modifies its own shared counter
        for (int i = 0; i < n; i++) {
            final int threadind = i;
            t(threadind, () -> sharedCounters[threadind].setValue(34) );
        }


        // verify opacity: Each thread reads its own writes,
        // and reads a version of other objects that it started a transaction with
        for (int i = 0; i < n; i++) {
            final int threadind = i;
            t(threadind, () ->
                    assertThat(sharedCounters[(threadind+1)%n].getValue())
                            .isBetween(32, 33)
            );
            t(threadind, () ->
                    assertThat(sharedCounters[threadind].getValue())
                            .isEqualTo(34)
            );
        }

        // try to commit all transactions; only first one should should succeed
        t(0, this::TXEnd);
        for (int i = 1; i < n; i++) {
            final int threadind = i;
            t(threadind, this::TXEnd)
                    .assertThrows()
                    .isInstanceOf(TransactionAbortedException.class);
        }
    }

    // test multiple threads optimistically manipulating the same objects concurrently
    @Test
    public void testOptimism() throws Exception {

        int n = 5;
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
            sharedCounters[i].setValue(32);

        // start concurrent transacstions on all threads
        for (int i = 0; i < n; i++)
            t(i, this::TXBegin);

        // modify shared counter per thread
        for (int i = 0; i < n; i++) {
            final int threadind = i;
            t(threadind, () -> sharedCounters[threadind].setValue(33));
        }

        // each thread reads a shared counter modified by another thread
        for (int i = 0; i < n; i++) {
            final int threadind = i;
            t(threadind, () ->
                    assertThat(sharedCounters[(threadind+1)%n].getValue())
                            .isBetween(32, 33)
            );
        }

        // each thread modifies a counter written by another thread
        for (int i = 0; i < n; i++) {
            final int threadind = i;
            t(threadind, () -> sharedCounters[(threadind+1)%n].setValue(34) );
        }


        // verify opacity: Each thread reads its own writes,
        // and reads a version of other objects that it started a transaction with
        for (int i = 0; i < n; i++) {
            final int threadind = i;
            t(threadind, () ->
                    assertThat(sharedCounters[(threadind+1)%n].getValue())
                            .isEqualTo(34)
            );
            t(threadind, () ->
                    assertThat(sharedCounters[threadind].getValue())
                            .isBetween(32, 33)
            );
        }

        // try to commit all transactions but the last.
        // they will succeed in alteration -- one succeed, next one fails (because it reads a counter modified by previous), next one succeeds, etc.
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
