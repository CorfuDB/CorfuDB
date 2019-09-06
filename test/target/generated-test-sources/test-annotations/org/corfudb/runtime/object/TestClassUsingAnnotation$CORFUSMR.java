package org.corfudb.runtime.object;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.lang.Object;
import java.lang.Override;
import java.lang.String;
import java.util.Map;
import java.util.Set;
import org.corfudb.annotations.Accessor;
import org.corfudb.annotations.InstrumentedCorfuObject;
import org.corfudb.annotations.Mutator;
import org.corfudb.annotations.MutatorAccessor;
import org.corfudb.annotations.PassThrough;

@InstrumentedCorfuObject
public class TestClassUsingAnnotation$CORFUSMR extends TestClassUsingAnnotation implements ICorfuSMR<TestClassUsingAnnotation> {
  public ICorfuSMRProxy<TestClassUsingAnnotation> proxy_CORFUSMR;

  public final Map<String, ICorfuSMRUpcallTarget<TestClassUsingAnnotation>> upcallMap_CORFUSMR = new ImmutableMap.Builder<String, ICorfuSMRUpcallTarget<TestClassUsingAnnotation>>()
  .put("reset", (obj, args) -> { obj.reset();return null;})
  .put("testIncrement", (obj, args) -> { return obj.testIncrement();}).build();

  public final Map<String, IUndoRecordFunction<TestClassUsingAnnotation>> undoRecordMap_CORFUSMR = new ImmutableMap.Builder<String, IUndoRecordFunction<TestClassUsingAnnotation>>().build();

  public final Map<String, IUndoFunction<TestClassUsingAnnotation>> undoMap_CORFUSMR = new ImmutableMap.Builder<String, IUndoFunction<TestClassUsingAnnotation>>().build();

  public final Set<String> resetSet_CORFUSMR = new ImmutableSet.Builder<String>().build();

  public TestClassUsingAnnotation$CORFUSMR() {
    super();
  }

  public ICorfuSMRProxy<TestClassUsingAnnotation> getCorfuSMRProxy() {
    return proxy_CORFUSMR;
  }

  public void setCorfuSMRProxy(ICorfuSMRProxy<TestClassUsingAnnotation> proxy) {
    this.proxy_CORFUSMR = proxy;
  }

  @Override
  public boolean equals(Object arg0) {
    return proxy_CORFUSMR.access(o_CORFUSMR -> {return o_CORFUSMR.equals(arg0);},null);
  }

  @Override
  public String toString() {
    return proxy_CORFUSMR.access(o_CORFUSMR -> {return o_CORFUSMR.toString();},null);
  }

  @Override
  public int hashCode() {
    return proxy_CORFUSMR.access(o_CORFUSMR -> {return o_CORFUSMR.hashCode();},null);
  }

  @Override
  @Mutator(
      name = "reset"
  )
  public void reset() {
    proxy_CORFUSMR.logUpdate("reset",false,null);
  }

  @Override
  @MutatorAccessor(
      name = "testIncrement"
  )
  public boolean testIncrement() {
    long address_CORFUSMR = proxy_CORFUSMR.logUpdate("testIncrement",true,null);
    return (boolean) proxy_CORFUSMR.getUpcallResult(address_CORFUSMR, null);
  }

  @Override
  @PassThrough
  public boolean testFn1() {
    return  super.testFn1();
  }

  @Override
  @Accessor
  public int getValue() {
    return proxy_CORFUSMR.access(o_CORFUSMR -> {return o_CORFUSMR.getValue();},null);
  }

  public Map<String, ICorfuSMRUpcallTarget<TestClassUsingAnnotation>> getCorfuSMRUpcallMap() {
    return upcallMap_CORFUSMR;
  }

  public Map<String, IUndoRecordFunction<TestClassUsingAnnotation>> getCorfuUndoRecordMap() {
    return undoRecordMap_CORFUSMR;
  }

  public Map<String, IUndoFunction<TestClassUsingAnnotation>> getCorfuUndoMap() {
    return undoMap_CORFUSMR;
  }

  public Set<String> getCorfuResetSet() {
    return resetSet_CORFUSMR;
  }
}
