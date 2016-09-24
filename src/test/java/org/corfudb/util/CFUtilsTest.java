package org.corfudb.util;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import org.corfudb.protocols.wireprotocol.LogUnitReadResponseMsg.ReadResult;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Created by maithem on 9/23/16.
 */

public class CFUtilsTest {

    @Test
    public void testGetInterruptible() throws Exception {
        NonEndingTask<ReadResult> future = new NonEndingTask();
        CountDownLatch interrupted = new CountDownLatch(1);

        Thread thread = new Thread() {
            public void run() {
                // Wait on a future that is never completed
                assertNull(CFUtils.getInterruptible(future));
                interrupted.countDown();
            }
        };

        thread.start();

        // Wait for the thread till it calls future.get
        for(int x = 0; x < 5; x++){
            if(future.isCalled()){
                break;
            }

            Thread.sleep(2000);
        }

        // Interrupt and verify that the thread called future.get
        // and returned on interruption
        assertTrue(future.isCalled());
        thread.interrupt();
        interrupted.await(4, TimeUnit.SECONDS);
        assertEquals(0, interrupted.getCount());
    }

    class NonEndingTask<T> extends CompletableFuture {
        private boolean called = false;
        @Override
        public T get() throws InterruptedException, ExecutionException {
            called = true;
            Thread.sleep(Long.MAX_VALUE);
            return null;
        }

        boolean isCalled(){
            return called;
        }
    }

}
