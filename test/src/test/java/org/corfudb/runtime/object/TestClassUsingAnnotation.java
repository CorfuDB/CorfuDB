package org.corfudb.runtime.object;

import java.util.concurrent.atomic.AtomicInteger;
import org.corfudb.annotations.Accessor;
import org.corfudb.annotations.ConstructorType;
import org.corfudb.annotations.CorfuObject;
import org.corfudb.annotations.Mutator;
import org.corfudb.annotations.MutatorAccessor;
import org.corfudb.annotations.ObjectType;
import org.corfudb.annotations.PassThrough;
import org.corfudb.annotations.StateSource;

/** Created by mwei on 6/22/16. */
@CorfuObject(
    objectType = ObjectType.SMR,
    constructorType = ConstructorType.RUNTIME,
    stateSource = StateSource.SELF)
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

  /** {@inheritDoc} */
  @Override
  public TestClassUsingAnnotation getContext(ICorfuExecutionContext.Context context) {
    return this;
  }
}
