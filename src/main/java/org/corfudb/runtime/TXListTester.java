/**
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.corfudb.runtime;

import java.util.*;
import java.util.concurrent.CyclicBarrier;

import org.corfudb.runtime.collections.CDBDoublyLinkedList;
import org.corfudb.runtime.collections.CDBLinkedList;
import org.corfudb.runtime.collections.CDBLogicalList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.corfudb.runtime.collections.CDBAbstractList;


class TXListTester<E, L extends CDBAbstractList<E>> implements Runnable {

    private static Logger dbglog = LoggerFactory.getLogger(TXListTester.class);

    // early versions of the system did (do?) not handle write only
    // operations very well so the implementations below optionally
    // insert unnecessary reads on objects that would be blindly written
    // to work-around this problem.
    // TODO: eventually this should become obsolete and should be removed!
    private static boolean writeOnlyTxSupport = true;

    // lock and dump the state of lists after every
    // completed transaction (aborted or otherwise)
    public static boolean extremeDebug = false;

    AbstractRuntime m_rt;           // corfu runtime
    List<L> m_v;                    // list of lists we are reading/updating
    CyclicBarrier m_startbarrier;   // start barrier to ensure all threads finish init before tx loop
    CyclicBarrier m_stopbarrier;    // stop barrier to ensure no thread returns until all have finished
    int m_nOps;                     // number of operations over the field of lists
    int m_nKeys;                    // number of items to add to each list
    int m_nId;                      // worker id
    long m_startwork;               // system time in milliseconds when tx loop starts
    long m_endwork;                 // system time in milliseconds when tx loop completes
    double m_readWriteRatio;        // ratio of reads to writes
    ElemGenerator<E> m_generator;   // element generator to populate random lists.
    boolean m_verbose;              // emit copious dbg text to console?
    int m_nattempts;                // number of attempts (a tx may need to be retried many times)
    int m_numcommits;               // number of committed transactions
    int m_naborts;                  // number of aborts
    int m_ntotalretries;            // retries due to inconsistent views (opacity violations)

    /**
     * getEndToEndLatency
     * Execution time for the benchmark is the time delta between when the
     * first tester thread enters its transaction phase and when the last
     * thread exits the same. The run method tracks these per object.
     * @param testers
     * @return
     */
    static long
    getEndToEndLatency(TXListTester[] testers) {
        long startmin = Long.MAX_VALUE;
        long endmax = Long.MIN_VALUE;
        for(TXListTester tester : testers) {
            startmin = Math.min(startmin, tester.m_startwork);
            endmax = Math.max(endmax, tester.m_endwork);
        }
        return endmax - startmin;
    }

    /**
     * return the total number of committed
     * operations for the worker thread group.
     * @param testers
     * @return
     */
    static int
    getCommittedOps(TXListTester[] testers) {
        int committed = 0;
        for(TXListTester tester : testers)
            committed += tester.m_numcommits;
        return committed;
    }

    /**
     * console logging for verbose mode.
     * @param strFormat
     * @param args
     */
    protected void
    inform(
            String strFormat,
            Object... args
    )
    {
        if(m_verbose)
            System.out.format(strFormat, args);
    }

    /**
     * ctor
     * @param nId           worker id
     * @param startbarrier  barrier: all threads complete init phase before starting work
     * @param stopbarrier   barrier: all threads complete all work before benchmark timing stops
     * @param tcr           corfu runtime
     * @param v             field of lists over which to apply random reads/updates
     * @param nops          number of read or update ops to apply
     * @param nkeys         number of items per list
     * @param rwpct         ratio of reads to updates
     * @param generator     random element generator for list population
     */
    public
    TXListTester(
            int nId,
            CyclicBarrier startbarrier,
            CyclicBarrier stopbarrier,
            AbstractRuntime tcr,
            List<L> v,
            int nops,
            int nkeys,
            double rwpct,
            ElemGenerator<E> generator,
            boolean _verbose
            )
    {
        m_nId = nId;
        m_nOps = nops;
        m_nKeys = nkeys;
        m_v = v;
        m_rt = tcr;
        m_startbarrier = startbarrier;
        m_stopbarrier = stopbarrier;
        m_generator = generator;
        m_verbose = _verbose;
        m_numcommits =  0;
        m_naborts = 0;
        m_ntotalretries = 0;
        m_nattempts = 0;
        m_readWriteRatio = rwpct;
    }

    /**
     * selectRandomLists
     * @return a pair of lists to be used as
     * source and destination in a random move
     * or random get.
     */
    private Pair<L, L> selectRandomLists() {
        L src = null;
        L dst = null;
        ArrayList<L> lists = new ArrayList<L>();
        lists.addAll(m_v);
        while(lists.size() > 0 && (src == null || dst == null)) {
            int lidx = lists.size() == 1 ? 0 : (int) (Math.random() * lists.size());
            assert(lidx >= 0 && lidx < lists.size());
            L randlist = lists.remove(lidx);
            if(randlist.isEmpty()) {
                // if the size is zero, this can only be the destination
                // list...if we have a dst list already, prefer this one,
                // so that we can start filling it back up again! If we've already
                // got a dst, with a non-zero size, assign it to the source.
                if(dst != null && !dst.isEmpty()) {
                    src = dst;
                }
                dst = randlist;
            } else {
                // a non-empty list can be conditionally assigned to either
                // dst or src--we prefer to assign it to src first, since we need
                // it for src, and cannot predict whether we will see non-zero
                // candidates in the future
                if(src == null) {
                    src = randlist;
                } else if(dst == null) {
                    dst = randlist;
                }
            }
        }
        if(src == null || dst == null)
            throw new RuntimeException("failed to select non-empty src and (potentially empty) dst lists!");
        return new Pair(src, dst);
    }

    /**
     * getRandomElement
     * given a list, choose a random element if possible. There is some
     * subtlety here--depending on the list implementation, the size
     * method may swallow the whole list into the read set of
     * the transaction, which messes with the fine-grain conflict
     * detection we want to exercise and test. To avoid this, we slightly
     * change the specification of Java.Collections.List.get to return
     * null when an index is out of bounds and retry when this happens.
     * To select the initial range. we assume that the initial state of the lists
     * populates each list more or less evenly, and that subsequent updates
     * do not perturb the distribution much. Consequently, we can guess an
     * index in the range of the average list size, and keep retrying until the
     * get returns non-null.
     *
     * @param src
     */
    private Pair<Integer, E>
    getRandomElement(L src) {
        int range = m_nKeys/m_v.size();
        while(true) {
            int lidx = (int) Math.floor(Math.random() * range);
            E item = src.get(lidx);
            if (item != null) {
                return new Pair<Integer, E>(lidx, item);
            } else {
                // if we got a null return value, the index was out of
                // range. This can happen either because we literally guessed
                // too high, or because the list is empty (meaning we can never
                // make an in-range guess). However, if we've gotten a null return,
                // it means we've already traversed the whole list in the transaction
                // anyway (most likely, modulo the list implementation) so calling
                // size is not going to do any additional damage. So update our
                // guess of the list size. If it's zero bail. Otherwise, try again--
                // we're guaranteed to get something on the second try.
                range = src.size();
                inform("[T%d]   getRandomElem: idx:%d OOR for L%d, set range=%d\n", m_nId, lidx, src.oid, range);
                if(range == 0) {
                    inform("[T%d]   getRandomElem: L%d is empty!\n", m_nId, src.oid);
                    return null;
                }
            }
        }
    }

    /**
     * given an input list, read
     * a random item from that list
     */
    private Pair<L,L>
    readRandomItem() {
        L src = selectRandomList();
        Pair<Integer,E> pair = getRandomElement(src);
        inform("T[%d]   read L%d(%d)\n", m_nId, src.oid, pair.first);
        return new Pair<L, L>(src, null);
    }

    /**
     * moveRandomItem
     * given a pair of lists, randomly choose an element from the
     * source and move it to the destination list.
     */
    private Pair<L, L>
    moveRandomItem() {

        Pair<L, L> lists = selectRandomLists();
        L src = lists.first;
        L dst = lists.second;
        Pair<Integer, E> item = getRandomElement(src);
        if(item != null) {
            src.remove((int)item.first);
            dst.add(item.second);
            inform("[T%d]   move %d L%d[idx:%d]->L%d\n",
                    m_nId, item.second, src.oid, item.first, dst.oid);
            return new Pair<L,L>(src, dst);
        }
        inform("[T%d]   move failed to select random element from L%d\n", src.oid);
        return new Pair<L, L>(null, null);
    }

    /**
     * selectRandomList
     * @return a randomly selected list
     * to be used for a random read-only tx
     */
    private L selectRandomList() {
        L src = null;
        int lidx = m_v.size() == 1 ? 0 : (int) (Math.random() * m_v.size());
        assert(lidx >= 0 && lidx < m_v.size());
        return m_v.get(lidx);
    }

    /**
     * performRandomOp
     * According the distribution indicated by m_readWriteRatio,
     * perform a read-only operation (get an item from a list)
     * or a read-write tx (move a random item between lists)
     */
    private Pair<L,L>
    performRandomOp() {
        Pair<L, L> result;
        double diceRoll = Math.random();
        if(diceRoll < m_readWriteRatio) {
            result = readRandomItem();
        } else {
            result = moveRandomItem();
        }
        return result;
    }

    /**
     * populateLists
     * randomly distribute the key space over the
     * available lists in the field. This is not concurrent
     * code, so technically, the transactions are not necessary.
     * But using them teased out a large number of problems...
     */
    public void
    populateLists() {

        // only a single worker
        // should bother to populate
        // the field of lists.
        if(m_nId != 0)
            return;

        for(int i=0; i<m_nKeys; i++) {
            int lidx = (int) (Math.random() * m_v.size());
            L randlist = m_v.get(lidx);
            m_rt.BeginTX();
            if(!writeOnlyTxSupport)
                randlist.size();
            E elem = m_generator.randElem(i);
            randlist.add(elem);
            inform("...added item %d to list:%d\n", i, randlist.oid);
            m_rt.EndTX();
        }
    }

    /**
     * use the start barrier to wait for
     * all worker threads to initialize
     */
    private void awaitInit() {
        try {
            m_startbarrier.await();
        } catch(Exception bbe) {
            throw new RuntimeException(bbe);
        }
        inform("Entering run loop for tx list tester thread %d\n", m_nId);
        m_startwork = System.currentTimeMillis();
    }

    /**
     * use the start barrier to wait for
     * all worker threads to complete the tx loop
     */
    private void awaitComplete() {
        m_endwork = System.currentTimeMillis();
        try {
            m_stopbarrier.await();
        } catch(Exception bbe) {
            throw new RuntimeException(bbe);
        }
        inform("Leaving run loop for tx list tester thread %d\n", m_nId);
    }

    private static Object slock = new Object();

    /**
     * dump the state of the lists used in the last transaction attempt
     * as close to atomically as possible.
     * Which isn't really all that close. Option to use a transaction
     * or not--this is a debug utility, and we may want to see uncommitted
     * state as often as not.
     * @param pair
     * @param txid
     * @param attemptid
     * @param committed
     * @param usetx
     */
    private void
    dumpLists(
            Pair<L, L> pair,
            int txid,
            long attemptid,
            boolean committed,
            boolean usetx
        )
    {
        if(!extremeDebug) return;
        String strSrc = null;
        String strDst = null;
        String strOp = committed ? "post-cmt" : "post-abt";
        boolean done = false;

        while(!done) {
            boolean inTX = false;
            try {
                if(usetx) m_rt.BeginTX();
                inTX = usetx;
                strSrc = pair.first.print();
                if(pair.second != null)
                    strDst = pair.second.print();
                done = usetx ? m_rt.EndTX() : true;
                inTX = false;
            } catch (Exception e) {
                if(inTX) m_rt.AbortTX();
                inTX = false;
            }
        }

        synchronized (slock) {
            inform("[T%d]   %s[#%d, try:%d]->src:L%d=%s\n",
                    m_nId, strOp, txid, attemptid, pair.first.oid, strSrc);
            if(pair.second != null) {
                inform("       %s[#%d, try:%d]->dst:L%d=%s\n", strOp, txid, attemptid, pair.second.oid, strDst);
            }
        }
    }

    /**
     * run method (runnable)
     * wait for all worker threads to initialize
     * perform the specified number of random transactions
     * wait for all worker threads to complete the tx loop.
     */
    public void run()
    {
        populateLists();
        awaitInit();
        for(int i=0;i<m_nOps;i++)
        {
            long attempts = 0;
            long icretries = 0;
            boolean done = false;
            while(!done) {
                inform("[T%d] begintx[#%d, try:%d]\n", m_nId, i, attempts);
                boolean inTX = false;
                try {
                    attempts++;
                    m_nattempts++;
                    m_rt.BeginTX();
                    inTX = true;
                    Pair<L, L> res = performRandomOp();
                    done = m_rt.EndTX();
                    inTX = false;
                    inform("[T%d] endtx[#%d, try:%d]->%s\n", m_nId, i, attempts, (done?"COMMIT":"ABORT"));
                    m_numcommits += done ? 1 : 0;
                    m_naborts += done ? 0 : 1;
                    dumpLists(res, i, attempts, done, true);
                } catch (Exception e) {
                    inform("[T%d] force retry tx[%d, try%d] because of exception "+e+"\n", m_nId, i, attempts);
                    icretries++;
                    if(inTX) m_rt.AbortTX();
                }
            }
            m_ntotalretries += icretries;
        }
        awaitComplete();
        System.out.format("[T%d] done(%d ops): %d commits of %d attempts with %d retries for inconsistent views...\n",
                m_nId, m_nOps, m_numcommits, m_nattempts, m_ntotalretries);
    }

    /**
     * "random" integer generator--just returns the index supplied for now.
     */
    static class SeqIntGenerator implements ElemGenerator<Integer> {
        public Integer randElem(Object i) {
            return new Integer((Integer) i);
        }
    }


    /**
     * check whether the end state of the field
     * of lists is consistent with a serial order
     * of all the operations. Because the key space
     * is unique, (see the entirely not random random
     * generator above), each element must be present
     * in exactly one of the lists.
     * @param rt    runtime
     * @param v     list of lists
     * @param <E>   element type
     * @param <L>   list type
     * @return true if the state of the
     *      list is consistent
     */
    static <E, L extends CDBAbstractList<E>> boolean
    isConsistent(
            AbstractRuntime rt,
            List<L> v,
            int expectedKeys,
            StringBuilder strDetails
            )
    {
        int violations = 0;
        int totalElems = 0;
        boolean consistent = true;
        boolean failfast = strDetails == null;
        List<String> failures = failfast ? null : new LinkedList<String>();
        for(L l : v) {
            rt.BeginTX();
            totalElems += l.size();
            rt.EndTX();
        }
        for(L l : v) {
            rt.BeginTX();
            int siz = l.size();
            for (int i=0; i<siz; i++) {
                E e = l.get(i);
                for (L lB : v) {
                    if (lB != l && lB.contains(e)) {
                        consistent = false;
                        violations++;
                        if(failfast) break;
                        String failure = "" + e + " contained in L" + l.oid +
                                " and L" + lB.oid + "\n";
                        failures.add(failure);
                    }
                }
            }
            if(!rt.EndTX())
                throw new RuntimeException("Consistency check aborted...");
            if (!consistent && failfast)
                break;
        }

        if(totalElems != expectedKeys) {
            consistent = false;
            violations++;
            if(failures != null)
                failures.add(new String("expected "+expectedKeys+", found "+totalElems+"\n"));
            List<String> missingKeys = new LinkedList<String>();
            for(int i=0; i<expectedKeys; i++) {
                boolean foundi = false;
                for(L l: v) {
                    foundi = l.contains(i);
                    if(foundi)
                        break;
                }
                if(!foundi)
                    missingKeys.add("" + i);
            }
            failures.add("missing keys: [" + String.join(", ", missingKeys) + "]\n");
        }

        if(!consistent && strDetails != null) {
            strDetails.append("found " + violations + " violations:\n");
            for(String s : failures)
                strDetails.append(s);
        }
        return consistent;
    }

    /**
     * create a new CDB list object
     * @param strClass
     * @param TR
     * @param sf
     * @param oid
     * @param <E>
     * @param <L>
     * @return new list (empty) of the appropriate class
     */
    static <E, L extends CDBAbstractList<E>> L
    createList(
            String strClass,
            AbstractRuntime TR,
            IStreamFactory sf,
            long oid
        )
    {
        if(strClass.contains("CDBLinkedList"))
            return (L) new CDBLinkedList<E>(TR, sf, oid);
        if(strClass.contains("CDBDoublyLinkedList"))
            return (L) new CDBDoublyLinkedList<E>(TR, sf, oid);
        else if(strClass.contains("CDBLogicalList"))
            return (L) new CDBLogicalList<E>(TR, sf, oid);
        return null;
    }

    /**
     * run a tx list test.
     * @param TR
     * @param sf
     * @param numthreads
     * @param numlists
     * @param nOperations
     * @param numkeys
     * @param rwpct
     * @param strClass
     * @param verbose
     * @param <E>
     * @param <L>
     * @throws InterruptedException
     */
    public static <E, L extends CDBAbstractList<E>> void
    runListTest(
            AbstractRuntime TR,
            IStreamFactory sf,
            int numthreads,
            int numlists,
            int nOperations,
            int numkeys,
            double rwpct,
            String strClass,
            boolean verbose
        ) throws InterruptedException
    {
        ElemGenerator<E> generator = (ElemGenerator<E>) new SeqIntGenerator();
        ArrayList<L> lists = new ArrayList<L>();
        CyclicBarrier startbarrier = new CyclicBarrier(numthreads);
        CyclicBarrier stopbarrier = new CyclicBarrier(numthreads);

        for(int i=0; i<numlists; i++) {
            long oidlist = DirectoryService.getUniqueID(sf);
            L list = TXListTester.<E,L>createList(strClass, TR, sf, oidlist);
            lists.add(list);
        }

        Thread[] threads = new Thread[numthreads];
        TXListTester<E,L>[] testers = new TXListTester[numthreads];
        int perWorkerOps = nOperations / numthreads;
        for (int i = 0; i < numthreads; i++) {
            TXListTester<E, L> txl = new TXListTester<E, L>(
                    i, startbarrier, stopbarrier, TR, lists, perWorkerOps, numkeys, rwpct, generator, verbose);
            testers[i] = txl;
            threads[i] = new Thread(txl);
            threads[i].start();
        }
        for(int i=0;i<numthreads;i++)
            threads[i].join();

        StringBuilder strDetails = new StringBuilder();
        boolean success = isConsistent(TR, lists, numkeys, strDetails);
        long e2e = getEndToEndLatency(testers);
        int committedops = getCommittedOps(testers);
        double throughput = (1000.0 * (double)committedops) / (double)e2e;
        System.out.format("List consistency check %s!\n", success ? "PASSED" : "FAILED");
        System.out.format("Throughput: %d tx in %d msec -> %f tx/sec\n", committedops, e2e, throughput);
        System.out.format("tput: %s, %d, %d, %d, %.1f, %.3f\n", strClass, numthreads, numkeys, nOperations, rwpct, throughput);
        System.out.print(strDetails.toString());
        System.out.println(TR);
    }

}

