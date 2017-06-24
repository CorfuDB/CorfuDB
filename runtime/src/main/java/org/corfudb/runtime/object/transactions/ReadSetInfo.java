package org.corfudb.runtime.object.transactions;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import lombok.Getter;

/**
 * This class captures information about objects accessed (read) during speculative
 * transaction execution.
 */
@Getter
class ReadSetInfo {
    // fine-grained conflict information regarding accessed-objects;
    // captures values passed using @conflict annotations in @corfuObject
    Map<UUID, Set<Long>> readSetConflicts = new HashMap<>();

    public void mergeInto(ReadSetInfo other) {
        other.getReadSetConflicts().forEach((streamId, cset) -> {
            getConflictSet(streamId).addAll(cset);
        });
    }

    public void addToReadSet(UUID streamId, Object[] conflictObjects) {
        if (conflictObjects == null) {
            return;
        }

        Set<Long> streamConflicts = getConflictSet(streamId);
        Arrays.asList(conflictObjects).stream()
                .forEach(V -> streamConflicts.add(Long.valueOf(V.hashCode())));
    }

    public Set<Long> getConflictSet(UUID streamId) {
        return getReadSetConflicts().computeIfAbsent(streamId, u -> {
            return new HashSet<>();
        });
    }

}
