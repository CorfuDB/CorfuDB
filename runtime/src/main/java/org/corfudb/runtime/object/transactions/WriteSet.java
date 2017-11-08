package org.corfudb.runtime.object.transactions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import lombok.Getter;
import org.corfudb.protocols.logprotocol.MultiObjectSMREntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.object.IObjectManager;

/**
 * This class captures information about objects mutated (written) during speculative
 * transaction execution.
 */
@Getter
public class WriteSet extends ConflictSet {

    /** The actual updates to mutated objects. */
    final MultiObjectSMREntry writeSet = new MultiObjectSMREntry();

    /** Returns whether there are any writes present in the write set.
     *
     * @return  True, if there are writes present, false otherwise.
     */
    public boolean isEmpty() {
        return writeSet.entryMap.isEmpty();
    }

    <T> long add(@Nonnull IObjectManager<T> manager,
                 @Nonnull SMREntry updateEntry,
                 @Nullable Object[] conflictObjects) {
        super.add(manager, conflictObjects);
        writeSet.addTo(manager.getId(), updateEntry);
        return writeSet.getSMRUpdates(manager.getId()).size() - 1;
    }

}
