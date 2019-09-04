package org.corfudb.runtime.object;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.lang.Object;
import java.lang.Override;
import java.lang.String;
import java.util.Map;
import java.util.Set;
import org.corfudb.annotations.InstrumentedCorfuObject;
import org.corfudb.annotations.Mutator;

@InstrumentedCorfuObject
public class TestClass$CORFUSMR extends TestClass implements ICorfuSMR<TestClass> {
  public ICorfuSMRProxy<TestClass> proxy_CORFUSMR;

  public final Map<String, ICorfuSMRUpcallTarget<TestClass>> upcallMap_CORFUSMR = new ImmutableMap.Builder<String, ICorfuSMRUpcallTarget<TestClass>>()
  .put("set", (obj, args) -> { obj.set((int) args[0]);return null;}).build();

  public final Map<String, IUndoRecordFunction<TestClass>> undoRecordMap_CORFUSMR = new ImmutableMap.Builder<String, IUndoRecordFunction<TestClass>>().build();

  public final Map<String, IUndoFunction<TestClass>> undoMap_CORFUSMR = new ImmutableMap.Builder<String, IUndoFunction<TestClass>>().build();

  public final Set<String> resetSet_CORFUSMR = new ImmutableSet.Builder<String>().build();

  public TestClass$CORFUSMR() {
    super();
  }

  public ICorfuSMRProxy<TestClass> getCorfuSMRProxy() {
    return proxy_CORFUSMR;
  }

  public void setCorfuSMRProxy(ICorfuSMRProxy<TestClass> proxy) {
    this.proxy_CORFUSMR = proxy;
  }

  @Override
  public int hashCode() {
    return proxy_CORFUSMR.access(o_CORFUSMR -> {return o_CORFUSMR.hashCode();},null);
  }

  @Override
  public int get() {
    return proxy_CORFUSMR.access(o_CORFUSMR -> {return o_CORFUSMR.get();},null);
  }

  @Override
  @Mutator(
      name = "set"
  )
  public void set(int toSet) {
    proxy_CORFUSMR.logUpdate("set",false,null,toSet);
  }

  @Override
  public String toString() {
    return proxy_CORFUSMR.access(o_CORFUSMR -> {return o_CORFUSMR.toString();},null);
  }

  @Override
  public boolean equals(Object arg0) {
    return proxy_CORFUSMR.access(o_CORFUSMR -> {return o_CORFUSMR.equals(arg0);},null);
  }

  public Map<String, ICorfuSMRUpcallTarget<TestClass>> getCorfuSMRUpcallMap() {
    return upcallMap_CORFUSMR;
  }

  public Map<String, IUndoRecordFunction<TestClass>> getCorfuUndoRecordMap() {
    return undoRecordMap_CORFUSMR;
  }

  public Map<String, IUndoFunction<TestClass>> getCorfuUndoMap() {
    return undoMap_CORFUSMR;
  }

  public Set<String> getCorfuResetSet() {
    return resetSet_CORFUSMR;
  }
}
