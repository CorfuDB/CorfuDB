package org.corfudb.runtime.object.transactions;

import java.util.HashSet;
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
class WriteSetInfo extends ConflictSetInfo {

    /** The set of mutated objects. */
    Set<UUID> affectedStreams = new HashSet<>();

    /** The actual updates to mutated objects. */
    MultiObjectSMREntry writeSet = new MultiObjectSMREntry();

    public long add(UUID streamId, SMREntry updateEntry, Object[] conflictObjects) {
        synchronized (getRootContext().getTransactionID()) {

            // add the SMRentry to the list of updates for this stream
            writeSet.addTo(streamId, updateEntry);

            super.add(streamId, conflictObjects);

            return writeSet.getSMRUpdates(streamId).size() - 1;
        }
    }

    @Override
    public void mergeInto(ConflictSetInfo other) {
        if (!(other instanceof WriteSetInfo)) {
            throw new UnsupportedOperationException("Merging write set with read set unsupported");
        }

        super.mergeInto(other);
        affectedStreams.addAll(((WriteSetInfo) other).affectedStreams);
        writeSet.mergeInto(((WriteSetInfo) other).writeSet);
    }
}
