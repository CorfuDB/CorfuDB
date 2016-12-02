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

    @Accessor
    int getValue() { return value; }

    @Mutator
    void setValue(int newValue) { value = newValue; }

    @MutatorAccessor
    int CAS(int testValue, int newValue) {
        int curValue = value;
        if (curValue == testValue)
            value = newValue;
        return curValue;
    }

}
