package org.corfudb.runtime.collections;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.lang.Object;
import java.lang.Override;
import java.lang.String;
import java.util.Map;
import java.util.Set;
import org.corfudb.annotations.Accessor;
import org.corfudb.annotations.InstrumentedCorfuObject;
import org.corfudb.annotations.MutatorAccessor;
import org.corfudb.runtime.object.ICorfuSMR;
import org.corfudb.runtime.object.ICorfuSMRProxy;
import org.corfudb.runtime.object.ICorfuSMRUpcallTarget;
import org.corfudb.runtime.object.IUndoFunction;
import org.corfudb.runtime.object.IUndoRecordFunction;

@InstrumentedCorfuObject
public class PutIfAbsentMapTest$PutIfAbsentMap$CORFUSMR<K, V> extends PutIfAbsentMapTest.PutIfAbsentMap<K, V> implements ICorfuSMR<PutIfAbsentMapTest.PutIfAbsentMap<K, V>> {
  public ICorfuSMRProxy<PutIfAbsentMapTest.PutIfAbsentMap<K, V>> proxy_CORFUSMR;

  public final Map<String, ICorfuSMRUpcallTarget<PutIfAbsentMapTest.PutIfAbsentMap<K, V>>> upcallMap_CORFUSMR = new ImmutableMap.Builder<String, ICorfuSMRUpcallTarget<PutIfAbsentMapTest.PutIfAbsentMap<K, V>>>()
  .put("put", (obj, args) -> { return obj.put((K) args[0], (V) args[1]);})
  .put("putIfAbsent", (obj, args) -> { return obj.putIfAbsent((K) args[0], (V) args[1]);}).build();

  public final Map<String, IUndoRecordFunction<PutIfAbsentMapTest.PutIfAbsentMap<K, V>>> undoRecordMap_CORFUSMR = new ImmutableMap.Builder<String, IUndoRecordFunction<PutIfAbsentMapTest.PutIfAbsentMap<K, V>>>().build();

  public final Map<String, IUndoFunction<PutIfAbsentMapTest.PutIfAbsentMap<K, V>>> undoMap_CORFUSMR = new ImmutableMap.Builder<String, IUndoFunction<PutIfAbsentMapTest.PutIfAbsentMap<K, V>>>().build();

  public final Set<String> resetSet_CORFUSMR = new ImmutableSet.Builder<String>().build();

  public PutIfAbsentMapTest$PutIfAbsentMap$CORFUSMR() {
    super();
  }

  public ICorfuSMRProxy<PutIfAbsentMapTest.PutIfAbsentMap<K, V>> getCorfuSMRProxy() {
    return proxy_CORFUSMR;
  }

  public void setCorfuSMRProxy(ICorfuSMRProxy<PutIfAbsentMapTest.PutIfAbsentMap<K, V>> proxy) {
    this.proxy_CORFUSMR = proxy;
  }

  @Override
  @Accessor
  public V get(K key) {
    return proxy_CORFUSMR.access(o_CORFUSMR -> {return o_CORFUSMR.get(key);},null);
  }

  @Override
  @MutatorAccessor(
      name = "put"
  )
  public V put(K key, V value) {
    long address_CORFUSMR = proxy_CORFUSMR.logUpdate("put",true,null,key, value);
    return (V) proxy_CORFUSMR.getUpcallResult(address_CORFUSMR, null);
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
  public boolean equals(Object arg0) {
    return proxy_CORFUSMR.access(o_CORFUSMR -> {return o_CORFUSMR.equals(arg0);},null);
  }

  @Override
  @MutatorAccessor(
      name = "putIfAbsent"
  )
  public boolean putIfAbsent(K key, V value) {
    long address_CORFUSMR = proxy_CORFUSMR.logUpdate("putIfAbsent",true,null,key, value);
    return (boolean) proxy_CORFUSMR.getUpcallResult(address_CORFUSMR, null);
  }

  public Map<String, ICorfuSMRUpcallTarget<PutIfAbsentMapTest.PutIfAbsentMap<K, V>>> getCorfuSMRUpcallMap() {
    return upcallMap_CORFUSMR;
  }

  public Map<String, IUndoRecordFunction<PutIfAbsentMapTest.PutIfAbsentMap<K, V>>> getCorfuUndoRecordMap() {
    return undoRecordMap_CORFUSMR;
  }

  public Map<String, IUndoFunction<PutIfAbsentMapTest.PutIfAbsentMap<K, V>>> getCorfuUndoMap() {
    return undoMap_CORFUSMR;
  }

  public Set<String> getCorfuResetSet() {
    return resetSet_CORFUSMR;
  }
}
