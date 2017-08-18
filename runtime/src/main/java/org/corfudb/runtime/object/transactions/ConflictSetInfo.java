package org.corfudb.runtime.object.transactions;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.corfudb.runtime.object.ICorfuSMRProxyInternal;

import lombok.Getter;

/**
 * This class captures information about objects accessed (read) during speculative
 * transaction execution.
 */
@Getter
public class ConflictSetInfo {


    /** Set of objects this conflict set conflicts with. */
    protected Map<ICorfuSMRProxyInternal, Set<Object>> conflicts = new HashMap<>();

    /** Get a hash for the object, given a proxy. */
    public static byte[] generateHashFromObject(ICorfuSMRProxyInternal p, Object o) {
        return p.getSerializer().hash(o);
    }

    /** Get the hashed conflict set.
     * @return              The hashed conflict set.
     */
    public Map<UUID, Set<byte[]>> getHashedConflictSet() {
        return conflicts.entrySet().stream()
                .collect(Collectors.toMap(
                        // Key = UUID
                        e -> e.getKey().getStreamID(),
                        // Value = Generated hash.
                        e -> e.getValue().stream()
                                .map(o -> ConflictSetInfo.generateHashFromObject(e.getKey(), o))
                                .collect(Collectors.toSet()),
                        // Merge function, in case key was already mapped
                        // (If two proxies have the same stream).
                        (v1,v2) -> {
                            // Combine the two sets.
                            v1.addAll(v2);
                            return v1;
                        }));
    }

    /** Merge a conflict set into this conflict set.
     * @param other         The conflict set to merge.
     */
    public void mergeInto(ConflictSetInfo other) {
        conflicts.putAll(other.conflicts);
    }

    /** Add an operation into this conflict set. */
    public void add(ICorfuSMRProxyInternal proxy, Object[] conflictObjects) {
        if (conflictObjects == null) {
            return;
        }

        // Add the conflict objects to the set for this proxy,
        // creating a new set if needed.
        conflicts.compute(proxy, (p, c) -> {
            // If key not previously mapped
            if (c == null) {
                c = new HashSet<>();
            }
            // Add conflicts to set.
            c.addAll(Arrays.asList(conflictObjects));
            return c;
        });
    }
}
