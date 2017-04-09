package org.corfudb.runtime.object;

import org.corfudb.annotations.Accessor;
import org.corfudb.annotations.CorfuObject;
import org.corfudb.annotations.Mutator;
import org.corfudb.annotations.MutatorAccessor;

/**
 * Created by dmalkhi on 12/2/16.
 */
@CorfuObject
public class CorfuSharedCounter {
    int value = 0;

    // currently, this is invisible to Corfu applications, cannot be used
    public CorfuSharedCounter(int initvalue) { value = initvalue; }

    // there must be an empty constructor;
    // because of the non-empty one above, we must explicitly provide an empty one as well
    public CorfuSharedCounter() { this(0); }

    @Accessor
    public int getValue() { return value; }

    @Mutator(name = "setValue")
    public void setValue(int newValue) { value = newValue; }

    @MutatorAccessor(name = "CAS")
    int CAS(int testValue, int newValue) {
        int curValue = value;
        if (curValue == testValue)
            value = newValue;
        return curValue;
    }
}
