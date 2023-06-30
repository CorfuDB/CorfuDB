package org.corfudb.runtime.object;

import org.corfudb.annotations.Accessor;
import org.corfudb.annotations.ConflictParameter;
import org.corfudb.annotations.CorfuObject;
import org.corfudb.annotations.Mutator;
import org.corfudb.annotations.MutatorAccessor;

@CorfuObject
public class ConflictParameterClass implements ICorfuSMR<ConflictParameterClass> {

    @Mutator(name = "mutatorTest")
    public void mutatorTest(int test1, @ConflictParameter int test2) {

    }

    @Accessor
    public int accessorTest(@ConflictParameter String test1, String test2) {
        return 0;
    }

    @MutatorAccessor(name = "mutatorAccessorTest")
    public Object mutatorAccessorTest(@ConflictParameter String test1, String test2) {
        return 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConflictParameterClass getContext(ICorfuExecutionContext.Context context) {
        return this;
    }
}
