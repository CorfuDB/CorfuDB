package org.corfudb.runtime.collections;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.lang.Object;
import java.lang.Override;
import java.lang.String;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.corfudb.annotations.InstrumentedCorfuObject;
import org.corfudb.annotations.PassThrough;
import org.corfudb.annotations.TransactionalMethod;
import org.corfudb.runtime.object.ICorfuSMR;
import org.corfudb.runtime.object.ICorfuSMRProxy;
import org.corfudb.runtime.object.ICorfuSMRUpcallTarget;
import org.corfudb.runtime.object.IUndoFunction;
import org.corfudb.runtime.object.IUndoRecordFunction;

@InstrumentedCorfuObject
public class FGMap$CORFUSMR<K, V> extends FGMap<K, V> implements ICorfuSMR<FGMap<K, V>> {
  public ICorfuSMRProxy<FGMap<K, V>> proxy_CORFUSMR;

  public final Map<String, ICorfuSMRUpcallTarget<FGMap<K, V>>> upcallMap_CORFUSMR = new ImmutableMap.Builder<String, ICorfuSMRUpcallTarget<FGMap<K, V>>>().build();

  public final Map<String, IUndoRecordFunction<FGMap<K, V>>> undoRecordMap_CORFUSMR = new ImmutableMap.Builder<String, IUndoRecordFunction<FGMap<K, V>>>().build();

  public final Map<String, IUndoFunction<FGMap<K, V>>> undoMap_CORFUSMR = new ImmutableMap.Builder<String, IUndoFunction<FGMap<K, V>>>().build();

  public final Set<String> resetSet_CORFUSMR = new ImmutableSet.Builder<String>().build();

  public FGMap$CORFUSMR(int numBuckets) {
    super(numBuckets);
  }

  public FGMap$CORFUSMR() {
    super();
  }

  public ICorfuSMRProxy<FGMap<K, V>> getCorfuSMRProxy() {
    return proxy_CORFUSMR;
  }

  public void setCorfuSMRProxy(ICorfuSMRProxy<FGMap<K, V>> proxy) {
    this.proxy_CORFUSMR = proxy;
  }

  @Override
  @PassThrough
  int getPartitionNumber(Object key) {
    return  super.getPartitionNumber(key);
  }

  @Override
  @PassThrough
  public V get(Object key) {
    return  super.get(key);
  }

  @Override
  @PassThrough
  List<Map<K, V>> getAllPartitionMaps() {
    return  super.getAllPartitionMaps();
  }

  @Override
  @PassThrough
  UUID getStreamID(int partition) {
    return  super.getStreamID(partition);
  }

  @Override
  @TransactionalMethod(
      readOnly = true
  )
  public boolean containsValue(Object value) {
    return proxy_CORFUSMR.TXExecute(() -> {return super.containsValue(value);
    });}

  @Override
  @PassThrough
  public V remove(Object key) {
    return  super.remove(key);
  }

  @Override
  @TransactionalMethod(
      modifiedStreamsFunction = "putAllGetStreams"
  )
  public void putAll(Map<? extends K, ? extends V> m) {
    proxy_CORFUSMR.TXExecute(() -> {super.putAll(m);
    return null; });}

  @Override
  @TransactionalMethod(
      readOnly = true
  )
  public Collection<V> values() {
    return proxy_CORFUSMR.TXExecute(() -> {return super.values();
    });}

  @Override
  @TransactionalMethod(
      readOnly = true
  )
  public int size() {
    return proxy_CORFUSMR.TXExecute(() -> {return super.size();
    });}

  @Override
  @PassThrough
  public V put(K key, V value) {
    return  super.put(key, value);
  }

  @Override
  @TransactionalMethod(
      modifiedStreamsFunction = "getAllStreamIDs"
  )
  public void clear() {
    proxy_CORFUSMR.TXExecute(() -> {super.clear();
    return null; });}

  @Override
  public boolean equals(Object arg0) {
    return proxy_CORFUSMR.access(o_CORFUSMR -> {return o_CORFUSMR.equals(arg0);},null);
  }

  @Override
  @TransactionalMethod(
      readOnly = true
  )
  public boolean isEmpty() {
    return proxy_CORFUSMR.TXExecute(() -> {return super.isEmpty();
    });}

  @Override
  @PassThrough
  Map<K, V> getPartition(Object key) {
    return  super.getPartition(key);
  }

  @Override
  public String toString() {
    return proxy_CORFUSMR.access(o_CORFUSMR -> {return o_CORFUSMR.toString();},null);
  }

  @Override
  Set<UUID> putAllGetStreams(Map<? extends K, ? extends V> m) {
    return proxy_CORFUSMR.access(o_CORFUSMR -> {return o_CORFUSMR.putAllGetStreams(m);},null);
  }

  @Override
  public int hashCode() {
    return proxy_CORFUSMR.access(o_CORFUSMR -> {return o_CORFUSMR.hashCode();},null);
  }

  @Override
  @PassThrough
  public boolean containsKey(Object key) {
    return  super.containsKey(key);
  }

  @Override
  @PassThrough
  Set<UUID> getAllStreamIDs() {
    return  super.getAllStreamIDs();
  }

  @Override
  @TransactionalMethod(
      readOnly = true
  )
  public Set<K> keySet() {
    return proxy_CORFUSMR.TXExecute(() -> {return super.keySet();
    });}

  @Override
  @TransactionalMethod(
      readOnly = true
  )
  public Set<Map.Entry<K, V>> entrySet() {
    return proxy_CORFUSMR.TXExecute(() -> {return super.entrySet();
    });}

  @Override
  @PassThrough
  Map<K, V> getPartitionMap(int partition) {
    return  super.getPartitionMap(partition);
  }

  public Map<String, ICorfuSMRUpcallTarget<FGMap<K, V>>> getCorfuSMRUpcallMap() {
    return upcallMap_CORFUSMR;
  }

  public Map<String, IUndoRecordFunction<FGMap<K, V>>> getCorfuUndoRecordMap() {
    return undoRecordMap_CORFUSMR;
  }

  public Map<String, IUndoFunction<FGMap<K, V>>> getCorfuUndoMap() {
    return undoMap_CORFUSMR;
  }

  public Set<String> getCorfuResetSet() {
    return resetSet_CORFUSMR;
  }
}
