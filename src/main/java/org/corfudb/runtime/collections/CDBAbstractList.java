package org.corfudb.runtime.collections;

import org.corfudb.runtime.AbstractRuntime;
import org.corfudb.runtime.CorfuDBObject;
import org.corfudb.runtime.StreamFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;
import java.util.function.UnaryOperator;

/**
 *
 */
public abstract class CDBAbstractList<E> extends CorfuDBObject implements List<E> {

    public CDBAbstractList(AbstractRuntime tTR, StreamFactory sf, long toid) {
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

}


