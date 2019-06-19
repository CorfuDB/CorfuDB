package org.corfudb.runtime.collections;

import org.corfudb.annotations.CorfuObject;
import org.corfudb.annotations.DontInstrument;
import org.corfudb.annotations.MutatorAccessor;

@CorfuObject
public class CorfuCounter {
    private long counter;

    public CorfuCounter() {
        counter = 0L;
    }

    @MutatorAccessor(name="next", undoFunction = "prev")
    public Long next() {
        counter = counter+1;
        return counter;
    }

    @DontInstrument
    protected Long prev(CorfuCounter corfuCounter) {
        corfuCounter.counter = corfuCounter.counter-1;
        return corfuCounter.counter;
    }
}
