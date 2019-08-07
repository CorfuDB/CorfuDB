package org.corfudb.runtime.object.transactions;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import lombok.Getter;

import org.corfudb.protocols.logprotocol.SMRLogEntry;
import org.corfudb.protocols.logprotocol.SMRRecord;
import org.corfudb.runtime.object.ICorfuSMRProxyInternal;

import static org.corfudb.runtime.object.transactions.TransactionalContext.getRootContext;

/**
 * This class captures information about objects mutated (written) during speculative
 * transaction execution.
 */
@Getter
public class WriteSetInfo extends ConflictSetInfo {

    /** The set of mutated objects. */
    Set<UUID> affectedStreams = new HashSet<>();

    /** The actual updates to mutated objects. */
    SMRLogEntry writeSet = new SMRLogEntry();

    public long add(ICorfuSMRProxyInternal proxy, SMRRecord updateRecord, Object[] conflictObjects) {

        synchronized (getRootContext().getTransactionID()) {

            // add the SMR update record to the list of updates for this stream.
            writeSet.addTo(proxy.getStreamID(), updateRecord);

            super.add(proxy, conflictObjects);

            return writeSet.getSMRUpdates(proxy.getStreamID()).size() - 1;
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
