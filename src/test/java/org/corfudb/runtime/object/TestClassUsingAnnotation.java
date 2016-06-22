package org.corfudb.runtime.object;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by mwei on 6/22/16.
 */
@CorfuObject(objectType= ObjectType.SMR,
        constructorType= ConstructorType.PERSISTED,
        stateSource = StateSource.SELF
)
public class TestClassUsingAnnotation {

    AtomicInteger a1;

    public TestClassUsingAnnotation() {
        a1 = new AtomicInteger();
    }

    boolean testFn1() {
        return true;
    }

    boolean testIncrement() {
        return a1.incrementAndGet() != 0;
    }

    void reset() {
        a1.set(0);
    }
}
