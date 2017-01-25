package org.corfudb.runtime.object;

import org.corfudb.annotations.Accessor;
import org.corfudb.annotations.ConflictParameter;
import org.corfudb.annotations.CorfuObject;
import org.corfudb.annotations.Mutator;
import org.corfudb.annotations.MutatorAccessor;

/**
 * Created by mwei on 12/15/16.
 */
@CorfuObject
public class ConflictParameterClass {

    @Mutator
    void mutatorTest(int test1, @ConflictParameter int test2) {

    }

    @Accessor
    int accessorTest(@ConflictParameter String test1, String test2) {
        return 0;
    }

    @MutatorAccessor
    Object mutatorAccessorTest(@ConflictParameter String test1, String test2) {
        return 0;
    }
}
