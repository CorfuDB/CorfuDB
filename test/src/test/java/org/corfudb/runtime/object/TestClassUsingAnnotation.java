package org.corfudb.runtime.object;

import org.corfudb.annotations.ConstructorType;
import org.corfudb.annotations.CorfuObject;
import org.corfudb.annotations.ObjectType;
import org.corfudb.annotations.StateSource;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by mwei on 6/22/16.
 */
@CorfuObject(objectType = ObjectType.SMR,
        constructorType = ConstructorType.RUNTIME,
        stateSource = StateSource.SELF
)
public class TestClassUsingAnnotation {

    AtomicInteger a1;

    public TestClassUsingAnnotation() {
        a1 = new AtomicInteger();
    }

    public boolean testFn1() {
        return true;
    }

    public boolean testIncrement() {
        return a1.incrementAndGet() != 0;
    }

    public int getValue() {
        return a1.get();
    }

    public void reset() {
        a1.set(0);
    }
}
