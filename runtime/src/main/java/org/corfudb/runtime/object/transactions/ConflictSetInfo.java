package org.corfudb.runtime.object.transactions;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.corfudb.util.Utils;

import lombok.Getter;

/**
 * This class captures information about objects accessed (read) during speculative
 * transaction execution.
 */
@Getter
class ConflictSetInfo {


    /** Set of objects this conflict set conflicts with. */
    protected Map<UUID, Set<Object>> conflicts = new HashMap<>();

    /** Get the conflict set for a specific stream.
     * @param streamId      The stream to obtain hashed conflicts for.
     * @return              A set of hashed conflicts for a given stream.
     */
    Set<Object> getConflictSetFor(UUID streamId) {
        return conflicts.computeIfAbsent(streamId, x -> new HashSet<>());
    }

    /** Get the hashed conflict set.
     * @return              The hashed conflict set.
     */
    Map<UUID, Set<byte[]>> getHashedConflictSet() {
        return conflicts.entrySet().stream()
                .collect(Collectors.toMap(
                        // Key = UUID
                        e -> e.getKey(),
                        // Value = big endian byte array of hashCode.
                        v -> v.getValue().stream()
                                .map(o -> o.hashCode())
                                .map(Utils::intToBigEndianByteArray)
                                .collect(Collectors.toSet())));
    }

    /** Merge a conflict set into this conflict set.
     * @param other         The conflict set to merge.
     */
    public void mergeInto(ConflictSetInfo other) {
        conflicts.putAll(other.conflicts);
    }

    /** Add an operation into this conflict set. */
    public void add(UUID streamId, Object[] conflictObjects) {
        if (conflictObjects == null) {
            return;
        }

        getConflictSetFor(streamId)
                // Convert conflict objects to stream
                .addAll(Arrays.asList(conflictObjects));
    }
}
