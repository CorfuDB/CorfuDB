package org.corfudb.runtime.object;

import org.corfudb.annotations.*;

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

    @PassThrough
    public boolean testFn1() {
        return true;
    }

    @MutatorAccessor(name = "testIncrement")
    public boolean testIncrement() {
        return a1.incrementAndGet() != 0;
    }

    @Accessor
    public int getValue() {
        return a1.get();
    }

    @Mutator(name = "reset")
    public void reset() {
        a1.set(0);
    }
}
