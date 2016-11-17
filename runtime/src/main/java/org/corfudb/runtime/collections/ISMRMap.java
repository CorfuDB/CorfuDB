package org.corfudb.runtime.collections;

import com.google.common.collect.ImmutableSet;
import org.corfudb.runtime.object.ISMRInterface;

import java.util.Map;
import java.util.Set;

/**
 * Created by mwei on 1/9/16.
 */
public interface ISMRMap<K, V> extends ISMRInterface, Map<K, V> {

    Set<SMRMethod> SMRAccessors = ImmutableSet.<SMRMethod>builder()
            .add(new SMRMethod("size", new Class[0]))
            .add(new SMRMethod("isEmpty", new Class[0]))
            .add(new SMRMethod("containsKey", new Class[]{Object.class}))
            .add(new SMRMethod("containsValue", new Class[]{Object.class}))
            .add(new SMRMethod("get", new Class[]{Object.class}))
            .add(new SMRMethod("keySet", new Class[0]))
            .add(new SMRMethod("values", new Class[0]))
            .add(new SMRMethod("entrySet", new Class[0]))
            .add(new SMRMethod("remove", new Class[]{Object.class}))
            .add()
            .build();

    Set<SMRMethod> SMRMutators = ImmutableSet.<SMRMethod>builder()
            .add(new SMRMethod("clear", new Class[0], "clear"))
            .add(new SMRMethod("putAll", new Class[]{Map.class}))
            .build();

    Set<SMRMethod> SMRMutatorAccessors = ImmutableSet.<SMRMethod>builder()
            .add(new SMRMethod("put", new Class[]{Object.class, Object.class}))
            .build();

    @Override
    default Set<SMRMethod> getSMRAccessors() {
        return SMRAccessors;
    }

    @Override
    default Set<SMRMethod> getSMRMutators() {
        return SMRMutators;
    }

    @Override
    default Set<SMRMethod> getSMRMutatorAccessors() {
        return SMRMutatorAccessors;
    }
}
