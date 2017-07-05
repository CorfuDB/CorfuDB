package org.corfudb.runtime.object.transactions;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.corfudb.runtime.object.ICorfuSMRProxyInternal;

import lombok.Getter;

/**
 * This class captures information about objects accessed (read) during speculative
 * transaction execution.
 */
@Getter
class ReadSetInfo {
    // fine-grained conflict information regarding accessed-objects;
    // captures values passed using @conflict annotations in @corfuObject
    Map<UUID, Set<Object>> readSetConflicts = new HashMap<>();

    Map<UUID, ICorfuSMRProxyInternal> proxies = new HashMap<>();

    public void mergeInto(ReadSetInfo other) {
        other.getReadSetConflicts().forEach((streamId, cset) -> {
            getConflictSet(streamId).addAll(cset);
        });
    }

    public void addToReadSet(ICorfuSMRProxyInternal proxy, Object[] conflictObjects) {
        if (conflictObjects == null) {
            return;
        }

        Set<Object> streamConflicts = getConflictSet(proxy.getStreamID());
        Arrays.asList(conflictObjects).stream()
                .forEach(streamConflicts::add);

        proxies.put(proxy.getStreamID(), proxy);
    }

    public Set<Object> getConflictSet(UUID streamId) {
        return getReadSetConflicts().computeIfAbsent(streamId, u -> {
            return new HashSet<>();
        });
    }

}
