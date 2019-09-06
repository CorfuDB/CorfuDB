package org.corfudb.runtime.object;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.lang.Object;
import java.lang.Override;
import java.lang.String;
import java.util.Map;
import java.util.Set;
import org.corfudb.annotations.Accessor;
import org.corfudb.annotations.ConflictParameter;
import org.corfudb.annotations.InstrumentedCorfuObject;
import org.corfudb.annotations.Mutator;
import org.corfudb.annotations.MutatorAccessor;

@InstrumentedCorfuObject
public class ConflictParameterClass$CORFUSMR extends ConflictParameterClass implements ICorfuSMR<ConflictParameterClass> {
  public ICorfuSMRProxy<ConflictParameterClass> proxy_CORFUSMR;

  public final Map<String, ICorfuSMRUpcallTarget<ConflictParameterClass>> upcallMap_CORFUSMR = new ImmutableMap.Builder<String, ICorfuSMRUpcallTarget<ConflictParameterClass>>()
  .put("mutatorTest", (obj, args) -> { obj.mutatorTest((int) args[0], (int) args[1]);return null;})
  .put("mutatorAccessorTest", (obj, args) -> { return obj.mutatorAccessorTest((java.lang.String) args[0], (java.lang.String) args[1]);}).build();

  public final Map<String, IUndoRecordFunction<ConflictParameterClass>> undoRecordMap_CORFUSMR = new ImmutableMap.Builder<String, IUndoRecordFunction<ConflictParameterClass>>().build();

  public final Map<String, IUndoFunction<ConflictParameterClass>> undoMap_CORFUSMR = new ImmutableMap.Builder<String, IUndoFunction<ConflictParameterClass>>().build();

  public final Set<String> resetSet_CORFUSMR = new ImmutableSet.Builder<String>().build();

  public ConflictParameterClass$CORFUSMR() {
    super();
  }

  public ICorfuSMRProxy<ConflictParameterClass> getCorfuSMRProxy() {
    return proxy_CORFUSMR;
  }

  public void setCorfuSMRProxy(ICorfuSMRProxy<ConflictParameterClass> proxy) {
    this.proxy_CORFUSMR = proxy;
  }

  @Override
  @Accessor
  public int accessorTest(@ConflictParameter String test1, String test2) {
    Object[] conflictField_CORFUSMR = new Object[]{test1};
    return proxy_CORFUSMR.access(o_CORFUSMR -> {return o_CORFUSMR.accessorTest(test1, test2);},conflictField_CORFUSMR);
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
      name = "mutatorTest"
  )
  public void mutatorTest(int test1, @ConflictParameter int test2) {
    Object[] conflictField_CORFUSMR = new Object[]{test2};
    proxy_CORFUSMR.logUpdate("mutatorTest",false,conflictField_CORFUSMR,test1, test2);
  }

  @Override
  @MutatorAccessor(
      name = "mutatorAccessorTest"
  )
  public Object mutatorAccessorTest(@ConflictParameter String test1, String test2) {
    Object[] conflictField_CORFUSMR = new Object[]{test1};
    long address_CORFUSMR = proxy_CORFUSMR.logUpdate("mutatorAccessorTest",true,conflictField_CORFUSMR,test1, test2);
    return (java.lang.Object) proxy_CORFUSMR.getUpcallResult(address_CORFUSMR, conflictField_CORFUSMR);
  }

  @Override
  public boolean equals(Object arg0) {
    return proxy_CORFUSMR.access(o_CORFUSMR -> {return o_CORFUSMR.equals(arg0);},null);
  }

  public Map<String, ICorfuSMRUpcallTarget<ConflictParameterClass>> getCorfuSMRUpcallMap() {
    return upcallMap_CORFUSMR;
  }

  public Map<String, IUndoRecordFunction<ConflictParameterClass>> getCorfuUndoRecordMap() {
    return undoRecordMap_CORFUSMR;
  }

  public Map<String, IUndoFunction<ConflictParameterClass>> getCorfuUndoMap() {
    return undoMap_CORFUSMR;
  }

  public Set<String> getCorfuResetSet() {
    return resetSet_CORFUSMR;
  }
}
