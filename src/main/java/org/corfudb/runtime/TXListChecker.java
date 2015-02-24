package org.corfudb.runtime;
import org.corfudb.runtime.TXRuntime;
import org.corfudb.runtime.collections.CorfuDBList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class TXListChecker<E, L extends CorfuDBList<E>> {

    private static Logger dbglog = LoggerFactory.getLogger(TXListChecker.class);

    AbstractRuntime m_rt;
    List<L> m_v;
    int m_nOps;
    int m_nKeys;
    int m_nId;

    public
    TXListChecker(
            AbstractRuntime tcr,
            List<L> v,
            int nops,
            int nkeys) {
        m_nOps = nops;
        m_nKeys = nkeys;
        m_v = v;
        m_rt = tcr;
    }

    // this version checks the list invariant by enumerating the key space,
    // which has the advantage that it no has requirements around support for iterators
    // in a tx context, but has the disadvantage that it is horribly inefficient.
    public boolean
    isConsistentBF() {

        boolean consistent = true;
        m_rt.BeginTX();
        for(int i=0;i<m_nKeys && consistent;i++) {
            for(L l : m_v) {
                if(l.contains(i)) {
                    for(L lB : m_v) {
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
        for(L l : m_v) {
            int siz = l.size();
            for (int i=0; i<siz; i++) {
                E e = l.get(i);
                for (L lB : m_v) {
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

}