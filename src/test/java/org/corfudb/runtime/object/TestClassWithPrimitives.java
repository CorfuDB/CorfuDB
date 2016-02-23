package org.corfudb.runtime.object;

/**
 * Created by mwei on 2/18/16.
 */
public class TestClassWithPrimitives {

    byte[] a;

    public TestClassWithPrimitives() {

    }

    @Mutator
    void setPrimitive(byte[] a) {
        this.a = a;
    }

    @Accessor
    byte[] getPrimitive() {
        return a;
    }
}
