package org.corfudb.runtime.object;

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
