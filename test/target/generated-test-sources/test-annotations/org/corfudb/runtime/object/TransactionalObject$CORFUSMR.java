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
import org.corfudb.annotations.TransactionalMethod;

@InstrumentedCorfuObject
public class TransactionalObject$CORFUSMR extends TransactionalObject implements ICorfuSMR<TransactionalObject> {
  public ICorfuSMRProxy<TransactionalObject> proxy_CORFUSMR;

  public final Map<String, ICorfuSMRUpcallTarget<TransactionalObject>> upcallMap_CORFUSMR = new ImmutableMap.Builder<String, ICorfuSMRUpcallTarget<TransactionalObject>>().build();

  public final Map<String, IUndoRecordFunction<TransactionalObject>> undoRecordMap_CORFUSMR = new ImmutableMap.Builder<String, IUndoRecordFunction<TransactionalObject>>().build();

  public final Map<String, IUndoFunction<TransactionalObject>> undoMap_CORFUSMR = new ImmutableMap.Builder<String, IUndoFunction<TransactionalObject>>().build();

  public final Set<String> resetSet_CORFUSMR = new ImmutableSet.Builder<String>().build();

  public TransactionalObject$CORFUSMR() {
    super();
  }

  public ICorfuSMRProxy<TransactionalObject> getCorfuSMRProxy() {
    return proxy_CORFUSMR;
  }

  public void setCorfuSMRProxy(ICorfuSMRProxy<TransactionalObject> proxy) {
    this.proxy_CORFUSMR = proxy;
  }

  @Override
  @TransactionalMethod
  @Accessor
  public boolean isInTransaction() {
    return proxy_CORFUSMR.TXExecute(() -> {return super.isInTransaction();
    });}

  @Override
  public String toString() {
    return proxy_CORFUSMR.access(o_CORFUSMR -> {return o_CORFUSMR.toString();},null);
  }

  @Override
  @TransactionalMethod
  @Accessor
  public boolean isInNestedTransaction() {
    return proxy_CORFUSMR.TXExecute(() -> {return super.isInNestedTransaction();
    });}

  @Override
  public boolean equals(Object arg0) {
    return proxy_CORFUSMR.access(o_CORFUSMR -> {return o_CORFUSMR.equals(arg0);},null);
  }

  @Override
  @TransactionalMethod
  @Accessor
  public void throwRuntimeException() {
    proxy_CORFUSMR.TXExecute(() -> {super.throwRuntimeException();
    return null; });}

  @Override
  public int hashCode() {
    return proxy_CORFUSMR.access(o_CORFUSMR -> {return o_CORFUSMR.hashCode();},null);
  }

  public Map<String, ICorfuSMRUpcallTarget<TransactionalObject>> getCorfuSMRUpcallMap() {
    return upcallMap_CORFUSMR;
  }

  public Map<String, IUndoRecordFunction<TransactionalObject>> getCorfuUndoRecordMap() {
    return undoRecordMap_CORFUSMR;
  }

  public Map<String, IUndoFunction<TransactionalObject>> getCorfuUndoMap() {
    return undoMap_CORFUSMR;
  }

  public Set<String> getCorfuResetSet() {
    return resetSet_CORFUSMR;
  }
}
