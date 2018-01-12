package org.corfudb.runtime.object.transactions;

import com.google.common.reflect.TypeToken;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.ISMRMap;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.junit.Test;

import java.util.concurrent.Semaphore;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by dalia on 1/6/17.
 */
public class TXsFromTwoRuntimesTest extends AbstractTransactionsTest {
    @Override
    public void TXBegin() { OptimisticTXBegin(); }



    @Test
    public void staggeredTXsConflict() throws Exception {
        final int nTXs = 5;
        final Semaphore sem0 = new Semaphore(0),
                sem1 = new Semaphore(0);

        // create two parallel worlds, with separate runtimes.
        // both instantiate the same shared map
        //

        final Thread thread1 = new Thread( () -> {
            CorfuRuntime myruntime = getNewRuntime(getDefaultNode());
            myruntime.connect();

            ISMRMap<Integer, Integer> mymap =
                    myruntime.getObjectsView()
                            .build()
                            .setStreamName("nonidepmpotentmaptest")    // stream name
                            .setTypeToken(new TypeToken<SMRMap<Integer, Integer>>() {}) // object TokenType class 
                            .open() ;

            assertThat(mymap.get("world1"))
                    .isEqualTo(null);

            for (int t = 0; t < nTXs; t++) {
                myruntime.getObjectsView().TXBegin();
                mymap.put(nTXs+t, t);
                myruntime.getObjectsView().TXEnd();
            }
            // expect to see nTXS entries in this map
            assertThat(mymap.size())
                    .isEqualTo(nTXs);

            // now allow for thread0 to commit its own transaction
            sem0.release();

            // next, wait for the commit to have completed
            try {
                sem1.acquire();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            // expect to (still) see nTXS entries in this map
            assertThat(mymap.size())
                    .isEqualTo(nTXs);
        } );

        final Thread thread0 = new Thread( () -> {
            CorfuRuntime myruntime = getNewRuntime(getDefaultNode());
            myruntime.connect();

            SMRMap<Integer, Integer> mymap =
                    myruntime.getObjectsView()
                        .build()
                            .setStreamName("nonidepmpotentmaptest")    // stream name
                            .setTypeToken(new TypeToken<SMRMap<Integer, Integer>>() {}) // object TokenType class 
                        .open();

            // start a transaction and then hand over to thread 1
            myruntime.getObjectsView().TXBegin();
            assertThat(mymap.get(nTXs))
                    .isEqualTo(null);
            mymap.put(0, mymap.size());

            // enable thread1: it will do nTXS increments on the stream
            thread1.start();

            boolean isAbort = false;
            try {
                // wait for thread 1 to do its work;
                // completion is indicated thru sem0
                sem0.acquire();
                myruntime.getObjectsView().TXEnd();

            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (TransactionAbortedException te) {
                isAbort = true;
            }

            // release thread1 to complete
            sem1.release();

            // expect to abort
            assertThat(isAbort)
                    .isTrue();

            // expect to see nTXs entries on the stream
            assertThat(mymap.size())
                    .isEqualTo(nTXs);

        });

        thread0.start();

        final long WAIT_TIME = 10000;
        try {
            thread0.join(WAIT_TIME);
            thread1.join(WAIT_TIME);
        } catch (InterruptedException ie) {
            throw new RuntimeException();
        }
    }

    @Test
    public void staggeredTXsNoConflict() throws Exception {
        final int nTXs = 5;
        final Semaphore sem0 = new Semaphore(0),
                sem1 = new Semaphore(0);

        // create two parallel worlds, with separate runtimes.
        // both instantiate the same shared map
        //

        final Thread thread1 = new Thread( () -> {
            CorfuRuntime myruntime = new CorfuRuntime(getDefaultEndpoint());
            myruntime.connect();

            ISMRMap<Integer, Integer> mymap =
                    myruntime.getObjectsView()
                            .build()
                            .setStreamName("nonidepmpotentmaptest")    // stream name
                            .setTypeToken(new TypeToken<SMRMap<Integer, Integer>>() {}) // object TokenType class
                            .open() ;

            assertThat(mymap.get("world1"))
                    .isEqualTo(null);

            for (int t = 0; t < nTXs; t++) {
                myruntime.getObjectsView().TXBegin();
                mymap.put(nTXs+t, t);
                myruntime.getObjectsView().TXEnd();
            }
            // expect to see nTXS entries in this map
            assertThat(mymap.size())
                    .isEqualTo(nTXs);

            // now allow for thread0 to commit its own transaction
            sem0.release();

            // next, wait for the commit to have completed
            try {
                sem1.acquire();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            // expect to (still) see nTXS+1 entries in this map
            assertThat(mymap.size())
                    .isEqualTo(nTXs+1);
            assertThat(mymap.get(0))
                    .isEqualTo(0);
        } );

        final Thread thread0 = new Thread( () -> {
            CorfuRuntime myruntime = getNewRuntime(getDefaultNode());
            myruntime.connect();

            ISMRMap<Integer, Integer> mymap =
                    myruntime.getObjectsView()
                            .build()
                            .setStreamName("nonidepmpotentmaptest")    // stream name
                            .setTypeToken(new TypeToken<SMRMap<Integer, Integer>>() {}) // object TokenType class
                            .open();

                    // start a transaction and then hand over to thread 1
            myruntime.getObjectsView().TXBegin();
            assertThat(mymap.get(0))
                    .isEqualTo(null);
            mymap.computeIfAbsent(0, (K) -> {
                // should be computed deterministically, does it?
                return mymap.size();
            });

            // enable thread1: it will do nTXS increments on the stream
            thread1.start();

            boolean isAbort = false;
            try {
                // wait for thread 1 to do its work;
                // completion is indicated thru sem0
                sem0.acquire();
                myruntime.getObjectsView().TXEnd();

            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (TransactionAbortedException te) {
                isAbort = true;
            }

            // release thread1 to complete
            sem1.release();

            // expect to abort
            assertThat(isAbort)
                    .isFalse();

            // this currently fails, due to incorrect sync on commit in VersionLockedObject
            // expect to see nTXs+1 entries on the stream
            assertThat(mymap.size())
                    .isEqualTo(nTXs+1);
            assertThat(mymap.get(0))
                    .isEqualTo(0);


        });

        thread0.start();

        final long WAIT_TIME = 10000;
        try {
            thread0.join(WAIT_TIME);
            thread1.join(WAIT_TIME);
        } catch (InterruptedException ie) {
            throw new RuntimeException();
        }
    }

}
