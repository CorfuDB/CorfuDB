package org.corfudb.runtime.object;

/**
 * Created by mwei on 6/21/16.
 */
public class TestClass {
    int testInt;

    public void set(int toSet) {
        testInt = toSet;
    }

    public int get() {
        return testInt;
    }
}
