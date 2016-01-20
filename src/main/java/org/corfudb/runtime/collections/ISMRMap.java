package org.corfudb.runtime.collections;

import com.google.common.collect.ImmutableSet;
import lombok.Getter;
import org.corfudb.runtime.object.ISMRInterface;

import java.util.Map;
import java.util.Set;

/**
 * Created by mwei on 1/9/16.
 */
public interface ISMRMap<K,V> extends ISMRInterface, Map<K,V> {

    Set<SMRMethod> SMRAccessors = ImmutableSet.<SMRMethod>builder()
            .add(new SMRMethod("size", new Class[0]))
            .add(new SMRMethod("isEmpty", new Class[0]))
            .add(new SMRMethod("containsKey", new Class[]{Object.class}))
            .add()
            .build();

    Set<SMRMethod> SMRMutators = ImmutableSet.<SMRMethod>builder()
            .add(new SMRMethod("clear", new Class[0], "clear"))
            .build();

    Set<SMRMethod> SMRMutatorAccessors = ImmutableSet.<SMRMethod>builder()
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
