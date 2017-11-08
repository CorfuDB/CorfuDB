package org.corfudb.runtime.object.transactions;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import lombok.Getter;
import org.corfudb.runtime.object.IObjectManager;
import org.corfudb.runtime.view.ObjectBuilder;

/**
 * This class captures information about objects accessed (read) during speculative
 * transaction execution.
 */
@Getter
public class ConflictSet {


    /** Set of objects this conflict set conflicts with. */
    protected final Map<IObjectManager, Set<Object>> conflicts = new HashMap<>();


    Optional<IObjectManager> getWrapper(UUID stream) {
        return conflicts.keySet().stream()
                .filter(p -> p.getBuilder().getStreamId().equals(stream))
                .findFirst();
    }

    /** Get the hashed conflict set.
     * @return              The hashed conflict set.
     */
    Map<UUID, Set<byte[]>> getHashedConflictSet() {
        return conflicts.entrySet().stream()
                .collect(Collectors.toMap(
                        // Key = UUID
                        e -> e.getKey().getBuilder().getStreamId(),
                        // Value = Generated hash.
                        e -> e.getValue().stream()
                                .map(o ->
                                        ((ObjectBuilder)(e.getKey()).getBuilder()).getSerializer()
                                                .hash(o))
                                .collect(Collectors.toSet()),
                        // Merge function, in case key was already mapped
                        // (If two proxies have the same stream).
                        (v1,v2) -> {
                            // Combine the two sets.
                            v1.addAll(v2);
                            return v1;
                        }));
    }

    /** Add an operation into this conflict set. */
    void add(@Nonnull IObjectManager manager, @Nullable Object[] conflictObjects) {

        if (conflictObjects == null) {
            return;
        }

        // Add the conflict objects to the set for this proxy,
        // creating a new set if needed.
        conflicts.compute(manager, (p, c) -> {
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
