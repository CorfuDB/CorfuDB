package org.corfudb.infrastructure.log;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.AbstractCorfuTest;
import org.junit.Test;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Created by kspirov on 3/8/17.
 */
@Slf4j
public class MultiReadWriteLockTest extends AbstractCorfuTest {

    @Test
    public void testWriteAndReadLocksAreReentrant() throws Exception {
        CallableConsumer c = (r) -> {
            MultiReadWriteLock locks = new MultiReadWriteLock();
            try (MultiReadWriteLock.AutoCloseableLock ignored1 = locks.acquireWriteLock(1l)) {
                try (MultiReadWriteLock.AutoCloseableLock ignored2 = locks.acquireWriteLock(1l)) {
                    try (MultiReadWriteLock.AutoCloseableLock ignored3 = locks.acquireReadLock(1l)) {
                        try (MultiReadWriteLock.AutoCloseableLock ignored4 = locks.acquireReadLock(1l)) {
                        }
                    }
                }
            }
        };
        scheduleConcurrently(PARAMETERS.CONCURRENCY_ONE, c);
        executeScheduled(PARAMETERS.CONCURRENCY_ONE, PARAMETERS.TIMEOUT_NORMAL);
        // victory - we were not canceled
    }

    @Test
    public void testIndependentWriteLocksDoNotSynchronize() throws Exception {
        MultiReadWriteLock locks = new MultiReadWriteLock();
        CyclicBarrier entry = new CyclicBarrier(PARAMETERS.CONCURRENCY_SOME);
        // Here  test that the lock does not synchronize when the value is different.
        // Otherwise one of the locks would halt on await, and the others - when acquiring the write log.
        CallableConsumer c = (r) -> {
            try(MultiReadWriteLock.AutoCloseableLock ignored = locks.acquireWriteLock((long)r)){
                entry.await();
            }
        };
        scheduleConcurrently(PARAMETERS.CONCURRENCY_SOME, c);
        executeScheduled(PARAMETERS.CONCURRENCY_SOME, PARAMETERS.TIMEOUT_NORMAL);
        // victory - we were not canceled
    }



    @Test
    public void testWriteLockSynchronizes() throws Exception {
        MultiReadWriteLock locks = new MultiReadWriteLock();
        CyclicBarrier entry = new CyclicBarrier(PARAMETERS.CONCURRENCY_SOME);
        // All threads should block, nobody should exit
        AtomicBoolean noProblems = new AtomicBoolean(true);
        CallableConsumer c = (r) -> {
            try(MultiReadWriteLock.AutoCloseableLock ignored = locks.acquireWriteLock(1l)) {
                entry.await();
                noProblems.set(false);
            }
        };
        scheduleConcurrently(PARAMETERS.CONCURRENCY_SOME, c);
        try {
            executeScheduled(PARAMETERS.CONCURRENCY_SOME, PARAMETERS.TIMEOUT_VERY_SHORT);
            fail();
        } catch (CancellationException e) {
            assertTrue(noProblems.get());
        }
    }



    @Test
    public void testReadLock() throws Exception {
        MultiReadWriteLock locks = new MultiReadWriteLock();
        CyclicBarrier entry = new CyclicBarrier(PARAMETERS.CONCURRENCY_SOME);
        CallableConsumer c = (r) -> {
            try(MultiReadWriteLock.AutoCloseableLock ignored = locks.acquireReadLock(1l)){
                entry.await();
            }
        };
        scheduleConcurrently(PARAMETERS.CONCURRENCY_SOME, c);
        executeScheduled(PARAMETERS.CONCURRENCY_SOME, PARAMETERS.TIMEOUT_NORMAL);
    }



    @Test
    public void testWriteLockNotPermittedInReadLock()  {
        MultiReadWriteLock locks = new MultiReadWriteLock();
        try (MultiReadWriteLock.AutoCloseableLock ignored1 = locks.acquireReadLock(1l)) {
            try (MultiReadWriteLock.AutoCloseableLock ignored2 = locks.acquireWriteLock(1l)) {
            }
            fail();
        } catch (IllegalStateException e) {
            // expected
        }
    }

    @Test
    public void testIncorrectLockOrder()  {
        MultiReadWriteLock locks = new MultiReadWriteLock();
        try (MultiReadWriteLock.AutoCloseableLock ignored1 = locks.acquireReadLock(2l)) {
            try (MultiReadWriteLock.AutoCloseableLock ignored2 = locks.acquireWriteLock(1l)) {
            }
            fail();
        } catch (IllegalStateException e) {
            // expected
        }
    }

    @Test
    public void testCorrectLockOrder()  {
        MultiReadWriteLock locks = new MultiReadWriteLock();
        try (MultiReadWriteLock.AutoCloseableLock ignored1 = locks.acquireReadLock(1l)) {
            try (MultiReadWriteLock.AutoCloseableLock ignored2 = locks.acquireWriteLock(2l)) {
            }
        }
        // no RuntimeException as expected
    }

    @Test
    public void testLockCorrectlyDeregistered()  {
        MultiReadWriteLock locks = new MultiReadWriteLock();
        try (MultiReadWriteLock.AutoCloseableLock ignored1 = locks.acquireReadLock(2l)) {
        }
        try (MultiReadWriteLock.AutoCloseableLock ignored2 = locks.acquireWriteLock(1l)) {
        }
        // let's nest
        try (MultiReadWriteLock.AutoCloseableLock ignored0 = locks.acquireReadLock(0l)) {
            try (MultiReadWriteLock.AutoCloseableLock ignored1 = locks.acquireReadLock(2l)) {
            }
            try (MultiReadWriteLock.AutoCloseableLock ignored2 = locks.acquireWriteLock(1l)) {
            }
        }
        // no RuntimeException as expected
    }

    @Test
    public void testLockCloseIsIdempotent()  {
        MultiReadWriteLock locks = new MultiReadWriteLock();
        MultiReadWriteLock.AutoCloseableLock ignored1 = locks.acquireWriteLock(2l);
        ignored1.close();
        ignored1.close();
        MultiReadWriteLock.AutoCloseableLock ignored2 = locks.acquireReadLock(1l);
        ignored2.close();
        ignored2.close();
    }

    @Test
    public void testWrongUnlocksOrderCatched()  {
        MultiReadWriteLock locks = new MultiReadWriteLock();
        MultiReadWriteLock.AutoCloseableLock ignored1 = locks.acquireWriteLock(1l);
        MultiReadWriteLock.AutoCloseableLock ignored2 = locks.acquireReadLock(2l);
        try {
            ignored1.close();
            fail();
        } catch (IllegalStateException e) {
            // expected
        }
    }

    @Test
    public void testWrongUnlocksTypeCatched()  {
        MultiReadWriteLock locks = new MultiReadWriteLock();
        MultiReadWriteLock.AutoCloseableLock ignored1 = locks.acquireWriteLock(1l);
        MultiReadWriteLock.AutoCloseableLock ignored2 = locks.acquireReadLock(1l);
        try {
            ignored1.close();
            fail();
        } catch (IllegalStateException e) {
            // expected
        }
    }



}
