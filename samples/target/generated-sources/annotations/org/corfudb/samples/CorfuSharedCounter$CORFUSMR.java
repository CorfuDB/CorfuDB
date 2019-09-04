package org.corfudb.samples;

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
import org.corfudb.runtime.object.ICorfuSMR;
import org.corfudb.runtime.object.ICorfuSMRProxy;
import org.corfudb.runtime.object.ICorfuSMRUpcallTarget;
import org.corfudb.runtime.object.IUndoFunction;
import org.corfudb.runtime.object.IUndoRecordFunction;

@InstrumentedCorfuObject
public class CorfuSharedCounter$CORFUSMR extends CorfuSharedCounter implements ICorfuSMR<CorfuSharedCounter> {
  public ICorfuSMRProxy<CorfuSharedCounter> proxy_CORFUSMR;

  public final Map<String, ICorfuSMRUpcallTarget<CorfuSharedCounter>> upcallMap_CORFUSMR = new ImmutableMap.Builder<String, ICorfuSMRUpcallTarget<CorfuSharedCounter>>()
  .put("Increment", (obj, args) -> { return obj.Increment();})
  .put("Set", (obj, args) -> { obj.Set((int) args[0]);return null;}).build();

  public final Map<String, IUndoRecordFunction<CorfuSharedCounter>> undoRecordMap_CORFUSMR = new ImmutableMap.Builder<String, IUndoRecordFunction<CorfuSharedCounter>>().build();

  public final Map<String, IUndoFunction<CorfuSharedCounter>> undoMap_CORFUSMR = new ImmutableMap.Builder<String, IUndoFunction<CorfuSharedCounter>>().build();

  public final Set<String> resetSet_CORFUSMR = new ImmutableSet.Builder<String>().build();

  public CorfuSharedCounter$CORFUSMR() {
    super();
  }

  public ICorfuSMRProxy<CorfuSharedCounter> getCorfuSMRProxy() {
    return proxy_CORFUSMR;
  }

  public void setCorfuSMRProxy(ICorfuSMRProxy<CorfuSharedCounter> proxy) {
    this.proxy_CORFUSMR = proxy;
  }

  @Override
  @MutatorAccessor(
      name = "Increment"
  )
  public int Increment() {
    long address_CORFUSMR = proxy_CORFUSMR.logUpdate("Increment",true,null);
    return (int) proxy_CORFUSMR.getUpcallResult(address_CORFUSMR, null);
  }

  @Override
  @Accessor
  public int Get() {
    return proxy_CORFUSMR.access(o_CORFUSMR -> {return o_CORFUSMR.Get();},null);
  }

  @Override
  public String toString() {
    return proxy_CORFUSMR.access(o_CORFUSMR -> {return o_CORFUSMR.toString();},null);
  }

  @Override
  @Mutator(
      name = "Set"
  )
  public void Set(int newvalue) {
    proxy_CORFUSMR.logUpdate("Set",false,null,newvalue);
  }

  @Override
  public int hashCode() {
    return proxy_CORFUSMR.access(o_CORFUSMR -> {return o_CORFUSMR.hashCode();},null);
  }

  @Override
  public boolean equals(Object arg0) {
    return proxy_CORFUSMR.access(o_CORFUSMR -> {return o_CORFUSMR.equals(arg0);},null);
  }

  public Map<String, ICorfuSMRUpcallTarget<CorfuSharedCounter>> getCorfuSMRUpcallMap() {
    return upcallMap_CORFUSMR;
  }

  public Map<String, IUndoRecordFunction<CorfuSharedCounter>> getCorfuUndoRecordMap() {
    return undoRecordMap_CORFUSMR;
  }

  public Map<String, IUndoFunction<CorfuSharedCounter>> getCorfuUndoMap() {
    return undoMap_CORFUSMR;
  }

  public Set<String> getCorfuResetSet() {
    return resetSet_CORFUSMR;
  }
}
