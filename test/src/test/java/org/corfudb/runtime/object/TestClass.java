package org.corfudb.runtime.object;

import org.corfudb.annotations.CorfuObject;
import org.corfudb.annotations.Mutator;

/**
 * Created by mwei on 6/21/16.
 */
@CorfuObject
public class TestClass implements ICorfuSMR<TestClass> {
    int testInt;

    @Mutator(name="set")
    public void set(int toSet) {
        testInt = toSet;
    }

    public int get() {
        return testInt;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TestClass getContext(ICorfuExecutionContext.Context context) {
        return this;
    }
}
