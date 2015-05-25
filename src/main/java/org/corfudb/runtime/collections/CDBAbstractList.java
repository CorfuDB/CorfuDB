package org.corfudb.runtime.collections;

import org.corfudb.runtime.smr.legacy.AbstractRuntime;
import org.corfudb.runtime.smr.legacy.CorfuDBObject;
import org.corfudb.runtime.smr.IStreamFactory;

import java.util.*;

/**
 *
 */
public abstract class CDBAbstractList<E> extends CorfuDBObject implements List<E> {

    public CDBAbstractList(AbstractRuntime tTR, IStreamFactory sf, UUID toid) {
        super(tTR, toid);
        TR = tTR;
        oid = toid;
    }

    protected boolean isTypeE(Object o) {
        try {
            E e = (E) o;
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    abstract public int sizeview();

    abstract public String print();

}


