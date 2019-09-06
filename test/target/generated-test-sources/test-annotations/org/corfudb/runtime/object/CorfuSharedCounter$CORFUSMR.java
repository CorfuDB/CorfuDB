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

@InstrumentedCorfuObject
public class CorfuSharedCounter$CORFUSMR extends CorfuSharedCounter implements ICorfuSMR<CorfuSharedCounter> {
  public ICorfuSMRProxy<CorfuSharedCounter> proxy_CORFUSMR;

  public final Map<String, ICorfuSMRUpcallTarget<CorfuSharedCounter>> upcallMap_CORFUSMR = new ImmutableMap.Builder<String, ICorfuSMRUpcallTarget<CorfuSharedCounter>>()
  .put("CAS", (obj, args) -> { return obj.CAS((int) args[0], (int) args[1]);})
  .put("setValue", (obj, args) -> { obj.setValue((int) args[0]);return null;}).build();

  public final Map<String, IUndoRecordFunction<CorfuSharedCounter>> undoRecordMap_CORFUSMR = new ImmutableMap.Builder<String, IUndoRecordFunction<CorfuSharedCounter>>().build();

  public final Map<String, IUndoFunction<CorfuSharedCounter>> undoMap_CORFUSMR = new ImmutableMap.Builder<String, IUndoFunction<CorfuSharedCounter>>().build();

  public final Set<String> resetSet_CORFUSMR = new ImmutableSet.Builder<String>().build();

  public CorfuSharedCounter$CORFUSMR(int initvalue) {
    super(initvalue);
  }

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
  public boolean equals(Object arg0) {
    return proxy_CORFUSMR.access(o_CORFUSMR -> {return o_CORFUSMR.equals(arg0);},null);
  }

  @Override
  @Accessor
  public int getValue() {
    return proxy_CORFUSMR.access(o_CORFUSMR -> {return o_CORFUSMR.getValue();},null);
  }

  @Override
  public int hashCode() {
    return proxy_CORFUSMR.access(o_CORFUSMR -> {return o_CORFUSMR.hashCode();},null);
  }

  @Override
  @MutatorAccessor(
      name = "CAS"
  )
  int CAS(int testValue, int newValue) {
    long address_CORFUSMR = proxy_CORFUSMR.logUpdate("CAS",true,null,testValue, newValue);
    return (int) proxy_CORFUSMR.getUpcallResult(address_CORFUSMR, null);
  }

  @Override
  @Mutator(
      name = "setValue"
  )
  public void setValue(int newValue) {
    proxy_CORFUSMR.logUpdate("setValue",false,null,newValue);
  }

  @Override
  public String toString() {
    return proxy_CORFUSMR.access(o_CORFUSMR -> {return o_CORFUSMR.toString();},null);
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
