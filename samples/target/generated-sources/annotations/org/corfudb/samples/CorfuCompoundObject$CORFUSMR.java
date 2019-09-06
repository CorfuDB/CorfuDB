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
import org.corfudb.runtime.object.ICorfuSMR;
import org.corfudb.runtime.object.ICorfuSMRProxy;
import org.corfudb.runtime.object.ICorfuSMRUpcallTarget;
import org.corfudb.runtime.object.IUndoFunction;
import org.corfudb.runtime.object.IUndoRecordFunction;

@InstrumentedCorfuObject
public class CorfuCompoundObject$CORFUSMR extends CorfuCompoundObject implements ICorfuSMR<CorfuCompoundObject> {
  public ICorfuSMRProxy<CorfuCompoundObject> proxy_CORFUSMR;

  public final Map<String, ICorfuSMRUpcallTarget<CorfuCompoundObject>> upcallMap_CORFUSMR = new ImmutableMap.Builder<String, ICorfuSMRUpcallTarget<CorfuCompoundObject>>()
  .put("set", (obj, args) -> { obj.set((org.corfudb.samples.CorfuCompoundObject.Inner) args[0], (int) args[1]);return null;}).build();

  public final Map<String, IUndoRecordFunction<CorfuCompoundObject>> undoRecordMap_CORFUSMR = new ImmutableMap.Builder<String, IUndoRecordFunction<CorfuCompoundObject>>().build();

  public final Map<String, IUndoFunction<CorfuCompoundObject>> undoMap_CORFUSMR = new ImmutableMap.Builder<String, IUndoFunction<CorfuCompoundObject>>().build();

  public final Set<String> resetSet_CORFUSMR = new ImmutableSet.Builder<String>().build();

  public CorfuCompoundObject$CORFUSMR() {
    super();
  }

  public ICorfuSMRProxy<CorfuCompoundObject> getCorfuSMRProxy() {
    return proxy_CORFUSMR;
  }

  public void setCorfuSMRProxy(ICorfuSMRProxy<CorfuCompoundObject> proxy) {
    this.proxy_CORFUSMR = proxy;
  }

  @Override
  @Mutator(
      name = "set"
  )
  public void set(CorfuCompoundObject.Inner in, int id) {
    proxy_CORFUSMR.logUpdate("set",false,null,in, id);
  }

  @Override
  public String toString() {
    return proxy_CORFUSMR.access(o_CORFUSMR -> {return o_CORFUSMR.toString();},null);
  }

  @Override
  @Accessor
  public CorfuCompoundObject.Inner getUser() {
    return proxy_CORFUSMR.access(o_CORFUSMR -> {return o_CORFUSMR.getUser();},null);
  }

  @Override
  @Accessor
  public int getID() {
    return proxy_CORFUSMR.access(o_CORFUSMR -> {return o_CORFUSMR.getID();},null);
  }

  @Override
  public boolean equals(Object arg0) {
    return proxy_CORFUSMR.access(o_CORFUSMR -> {return o_CORFUSMR.equals(arg0);},null);
  }

  @Override
  public int hashCode() {
    return proxy_CORFUSMR.access(o_CORFUSMR -> {return o_CORFUSMR.hashCode();},null);
  }

  public Map<String, ICorfuSMRUpcallTarget<CorfuCompoundObject>> getCorfuSMRUpcallMap() {
    return upcallMap_CORFUSMR;
  }

  public Map<String, IUndoRecordFunction<CorfuCompoundObject>> getCorfuUndoRecordMap() {
    return undoRecordMap_CORFUSMR;
  }

  public Map<String, IUndoFunction<CorfuCompoundObject>> getCorfuUndoMap() {
    return undoMap_CORFUSMR;
  }

  public Set<String> getCorfuResetSet() {
    return resetSet_CORFUSMR;
  }
}
