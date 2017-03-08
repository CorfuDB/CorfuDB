package org.corfudb.runtime.concurrent;

import lombok.extern.slf4j.Slf4j;

import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.object.transactions.AbstractObjectTest;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by dmalkhi on 3/17/17.
 */
@Slf4j
public class StreamSeekAtomicity extends AbstractObjectTest  {
    @Test
    public void ckCommitAtomicity() throws Exception {
        String mapName1 = "testMapA";
        Map<Long, Long> testMap1 = instantiateCorfuObject(SMRMap.class, mapName1);
        CountDownLatch l1 = new CountDownLatch(1 /* 2 */);
        AtomicBoolean commitDone = new AtomicBoolean(false);
        final int NTHREADS = 3;

        scheduleConcurrently(t -> {
            TXBegin();

            // wait for all to start
            l1.await();

            // generate optimistic mutation
            testMap1.put(1L, 1L);

            System.out.println("commit..");
            // wait for it to be undon
            TXEnd();
            System.out.println("committed");

            // signal done
            commitDone.set(true);

            Assert.assertEquals((long)testMap1.get(1L), 1L);
        });

        // thread that keeps syncing with the tail of log
        /* scheduleConcurrently(t -> {
            TXBegin();
            testMap1.get(1L);

            // signal that transaction has started and obtained a snapshot
            l1.countDown();

            // keep accessing the snapshot, causing optimistic rollback

            int i = 0;
            final int delayCnt = 1000;
            while (i++ < delayCnt ){
                testMap1.get(1L);
            }
        });*/

        // thread that keeps syncing with the tail of log
        scheduleConcurrently(t -> {
            // signal that thread has started
            l1.countDown();

            // keep updating the in-memory proxy from the log
            int i = 0;
            final int delayCnt = 1000;
            while (i++ < delayCnt /*!commitDone.get() */){
                testMap1.get(1L);
            }
        });

        executeScheduled(NTHREADS, PARAMETERS.TIMEOUT_NORMAL);
    }

}
