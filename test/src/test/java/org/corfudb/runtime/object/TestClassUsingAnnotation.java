package org.corfudb.runtime.object;

import org.corfudb.annotations.*;

import java.util.concurrent.atomic.AtomicInteger;

@CorfuObject(objectType = ObjectType.SMR,
        constructorType = ConstructorType.RUNTIME,
        stateSource = StateSource.SELF
)
public class TestClassUsingAnnotation implements ICorfuSMR<TestClassUsingAnnotation> {

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

    /**
     * {@inheritDoc}
     */
    @Override
    public TestClassUsingAnnotation getContext(ICorfuExecutionContext.Context context) {
        return this;
    }
}
