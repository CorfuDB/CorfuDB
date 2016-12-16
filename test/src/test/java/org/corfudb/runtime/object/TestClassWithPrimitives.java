package org.corfudb.runtime.object;

import org.corfudb.annotations.Accessor;
import org.corfudb.annotations.Mutator;

/**
 * Created by mwei on 2/18/16.
 */
public class TestClassWithPrimitives {

    byte[] a;

    public TestClassWithPrimitives() {

    }

    @Accessor
    byte[] getPrimitive() {
        return a;
    }

    @Mutator
    void setPrimitive(byte[] a) {
        this.a = a;
    }
}
