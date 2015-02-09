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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.corfudb.runtime.collections.CorfuDBList;


class TXListTester<E> implements Runnable {

    public static interface RandomElementProvider<E> {
        E randElem(Object i);
    };

    private static Logger dbglog = LoggerFactory.getLogger(TXListTester.class);

    TXRuntime m_rt;
    List<CorfuDBList<E>> m_v;
    CyclicBarrier m_startbarrier;
    CyclicBarrier m_stopbarrier;
    int m_nOps;
    int m_nKeys;
    int m_nId;
    RandomElementProvider<E> m_generator;

    public
    TXListTester(
            int nId,
            CyclicBarrier startbarrier,
            CyclicBarrier stopbarrier,
            TXRuntime tcr,
            List<CorfuDBList<E>> v,
            int nops,
            int nkeys,
            RandomElementProvider<E> generator
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
    }

    // this version checks the list invariant by enumerating the key space,
    // which has the advantage that it no has requirements around support for iterators
    // in a tx context, but has the disadvantage that it is horribly inefficient.
    public boolean
    isConsistentBF() {

        boolean consistent = true;
        m_rt.BeginTX();
        for(int i=0;i<m_nKeys && consistent;i++) {
            for(CorfuDBList<E> l : m_v) {
                if(l.contains(i)) {
                    for(CorfuDBList<E> lB : m_v) {
                        if (lB != l && lB.contains(i)) {
                            consistent = false;
                            break;
                        }
                    }
                }
                if(!consistent)
                    break;
            }
        }
        if(!m_rt.EndTX()) throw new RuntimeException("Consistency check aborted...");
        return consistent;
    }


    public boolean
    isConsistent() {

        boolean consistent = true;
        m_rt.BeginTX();
        for(CorfuDBList<E> l : m_v) {
            for (E e : l) {
                for (CorfuDBList<E> lB : m_v) {
                    if (lB != l && lB.contains(e)) {
                        consistent = false;
                        break;
                    }
                }
            }
            if (!consistent)
                break;
        }

        if(!m_rt.EndTX()) throw new RuntimeException("Consistency check aborted...");
        return consistent;
    }

    private Pair<CorfuDBList<E>, CorfuDBList<E>> selectLists() {
        CorfuDBList<E> src = null;
        CorfuDBList<E> dst = null;
        ArrayList<CorfuDBList<E>> lists = new ArrayList<CorfuDBList<E>>();
        lists.addAll(m_v);
        while(lists.size() > 0 && (src == null || dst == null)) {
            int lidx = (int) (Math.random() * lists.size());
            CorfuDBList<E> randlist = lists.remove(lidx);
            if(src == null && randlist.size() != 0)
                src = randlist;
            else if (src != null)
                dst = randlist;
        }
        if(src == null || dst == null)
            throw new RuntimeException("failed to select non-empty src and (potentially empty) dst lists!");
        return new Pair(src, dst);
    }

    private void
    moveRandomItem(
            CorfuDBList<E> src,
            CorfuDBList<E> dst
            ) {
        int lidx = (int) (Math.random() * src.size());
        // E item = src.remove(lidx);
        E item = src.get(lidx);
        dst.add(item);
    }

    public void
    populateLists() {

        m_rt.BeginTX();
        for(int i=0; i<m_nKeys; i++) {
            int lidx = (int) (Math.random() * m_v.size());
            CorfuDBList<E> randlist = m_v.get(lidx);
            randlist.add(m_generator.randElem(i));
        }
        m_rt.EndTX();
    }

    public void run()
    {
        int numcommits = 0;
        int naborts = 0;
        if(m_nId == 0)
            populateLists();
        System.out.println("starting tx list tester thread " + m_nId);
        try {
            m_startbarrier.await();
        } catch(Exception bbe) {
            throw new RuntimeException(bbe);
        }
        for(int i=0;i<m_nOps;i++)
        {
            long curtime = System.currentTimeMillis();
            dbglog.debug("Tx starting...");
            m_rt.BeginTX();
            Pair<CorfuDBList<E>, CorfuDBList<E>> pair = selectLists();
            moveRandomItem(pair.first, pair.second);
            if(m_rt.EndTX()) numcommits++;
            else naborts++;
            dbglog.debug("Tx took {}", (System.currentTimeMillis()-curtime));
        }
        try {
            m_stopbarrier.await();
        } catch(Exception bbe) {
            throw new RuntimeException(bbe);
        }
        System.out.println("Tester thread is done: " + numcommits + " commits out of " + m_nOps);
    }

}

