package org.corfudb.runtime.object.transactions;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import lombok.Getter;

import org.corfudb.protocols.logprotocol.MultiObjectSMREntry;
import org.corfudb.protocols.logprotocol.SMREntry;

import static org.corfudb.runtime.object.transactions.TransactionalContext.getRootContext;

/**
 * This class captures information about objects mutated (written) during speculative
 * transaction execution.
 */
@Getter
class WriteSetInfo {

    // fine-grained conflict information regarding mutated-objects;
    // captures values passed using @conflict annotations in @corfuObject
    Map<UUID, Set<Long>> writeSetConflicts = new HashMap<>();

    // the set of mutated objects
    Set<UUID> affectedStreams = new HashSet<>();

    // teh actual updates to mutated objects
    MultiObjectSMREntry writeSet = new MultiObjectSMREntry();

    // The set of poisoned streams. Poisoned streams contain an update
    // which conflicts with everything (some object inserted a conflict
    // set containing NULL), so the conflict set must be ignored.
    Set<UUID> poisonedStreams = new HashSet<>();

    Set<Long> getConflictSet(UUID streamId) {
        return getWriteSetConflicts().computeIfAbsent(streamId, u -> {
            return new HashSet<>();
        });
    }

    public void poisonStream(UUID streamId) {
        poisonedStreams.add(streamId);
    }

    public void mergeInto(WriteSetInfo other) {
        synchronized (getRootContext().getTransactionID()) {

            // copy all the conflict-params
            other.writeSetConflicts.forEach((streamId, cset) -> {
                getConflictSet(streamId).addAll(cset);
            });

            // copy all the writeSet SMR entries
            writeSet.mergeInto(other.getWriteSet());

            // copy the list of poisoned streams
            poisonedStreams.addAll(other.poisonedStreams);
        }
    }

    public long addToWriteSet(UUID streamId, SMREntry updateEntry, Object[]
            conflictObjects) {
        synchronized (getRootContext().getTransactionID()) {

            // add the SMRentry to the list of updates for this stream
            writeSet.addTo(streamId, updateEntry);

            // add all the conflict params to the conflict-params set for this stream
            if (conflictObjects != null) {
                Set<Long> streamConflicts = getConflictSet(streamId);
                Arrays.asList(conflictObjects).stream()
                        .forEach(V -> streamConflicts.add(Long.valueOf(V.hashCode())));
            } else {
                poisonStream(streamId);
            }

            return writeSet.getSMRUpdates(streamId).size() - 1;
        }
    }

}
