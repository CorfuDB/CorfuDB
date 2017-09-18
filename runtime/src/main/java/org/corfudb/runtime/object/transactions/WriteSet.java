package org.corfudb.runtime.object.transactions;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import lombok.Getter;

import org.corfudb.protocols.logprotocol.MultiObjectSMREntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.object.ICorfuSMRProxyInternal;

/**
 * This class captures information about objects mutated (written) during speculative
 * transaction execution.
 */
@Getter
public class WriteSet extends ConflictSet {

    /** The actual updates to mutated objects. */
    final MultiObjectSMREntry writeSet = new MultiObjectSMREntry();


    public long add(ICorfuSMRProxyInternal proxy, SMREntry updateEntry, Object[] conflictObjects) {
        super.add(proxy, conflictObjects);
        writeSet.addTo(proxy.getStreamID(), updateEntry);
        return writeSet.getSMRUpdates(proxy.getStreamID()).size() - 1;
    }

}
