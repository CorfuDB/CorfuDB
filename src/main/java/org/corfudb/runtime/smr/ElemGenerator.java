package org.corfudb.runtime.smr;

/**
 * Created by crossbach on 2/13/2015.
 */
public interface ElemGenerator<E> {
    E randElem(Object i);
}
