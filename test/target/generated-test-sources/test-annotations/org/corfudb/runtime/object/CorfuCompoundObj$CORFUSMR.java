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

@InstrumentedCorfuObject
public class CorfuCompoundObj$CORFUSMR extends CorfuCompoundObj implements ICorfuSMR<CorfuCompoundObj> {
  public ICorfuSMRProxy<CorfuCompoundObj> proxy_CORFUSMR;

  public final Map<String, ICorfuSMRUpcallTarget<CorfuCompoundObj>> upcallMap_CORFUSMR = new ImmutableMap.Builder<String, ICorfuSMRUpcallTarget<CorfuCompoundObj>>()
  .put("set", (obj, args) -> { obj.set((org.corfudb.runtime.object.CorfuCompoundObj.Inner) args[0], (int) args[1]);return null;}).build();

  public final Map<String, IUndoRecordFunction<CorfuCompoundObj>> undoRecordMap_CORFUSMR = new ImmutableMap.Builder<String, IUndoRecordFunction<CorfuCompoundObj>>().build();

  public final Map<String, IUndoFunction<CorfuCompoundObj>> undoMap_CORFUSMR = new ImmutableMap.Builder<String, IUndoFunction<CorfuCompoundObj>>().build();

  public final Set<String> resetSet_CORFUSMR = new ImmutableSet.Builder<String>().build();

  public CorfuCompoundObj$CORFUSMR() {
    super();
  }

  public ICorfuSMRProxy<CorfuCompoundObj> getCorfuSMRProxy() {
    return proxy_CORFUSMR;
  }

  public void setCorfuSMRProxy(ICorfuSMRProxy<CorfuCompoundObj> proxy) {
    this.proxy_CORFUSMR = proxy;
  }

  @Override
  @Mutator(
      name = "set"
  )
  public void set(CorfuCompoundObj.Inner in, int id) {
    proxy_CORFUSMR.logUpdate("set",false,null,in, id);
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

  @Override
  public String toString() {
    return proxy_CORFUSMR.access(o_CORFUSMR -> {return o_CORFUSMR.toString();},null);
  }

  @Override
  @Accessor
  public CorfuCompoundObj.Inner getUser() {
    return proxy_CORFUSMR.access(o_CORFUSMR -> {return o_CORFUSMR.getUser();},null);
  }

  public Map<String, ICorfuSMRUpcallTarget<CorfuCompoundObj>> getCorfuSMRUpcallMap() {
    return upcallMap_CORFUSMR;
  }

  public Map<String, IUndoRecordFunction<CorfuCompoundObj>> getCorfuUndoRecordMap() {
    return undoRecordMap_CORFUSMR;
  }

  public Map<String, IUndoFunction<CorfuCompoundObj>> getCorfuUndoMap() {
    return undoMap_CORFUSMR;
  }

  public Set<String> getCorfuResetSet() {
    return resetSet_CORFUSMR;
  }
}
