package org.corfudb.runtime.object.transactions;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import lombok.Getter;

import org.corfudb.protocols.logprotocol.MultiObjectSMREntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.object.ICorfuSMR;
import org.corfudb.runtime.object.ICorfuSMRProxyInternal;

import static org.corfudb.runtime.object.transactions.TransactionalContext.getRootContext;

/**
 * This class captures information about objects mutated (written) during speculative
 * transaction execution.
 */
@Getter
public class WriteSetInfo extends ConflictSetInfo {

    // The union of stream tags from all affected streams.
    Set<UUID> streamTags = new HashSet<>();

    // The actual updates to mutated objects.
    MultiObjectSMREntry writeSet = new MultiObjectSMREntry();

    public <T extends ICorfuSMR<T>> long add(ICorfuSMRProxyInternal<T> proxy,
                                             SMREntry updateEntry, Object[] conflictObjects) {
        synchronized (getRootContext().getTransactionID()) {

            // Add the SMREntry to the list of updates for this stream.
            writeSet.addTo(proxy.getStreamID(), updateEntry);
            streamTags.addAll(proxy.getStreamTags());
            super.add(proxy, conflictObjects);

            return writeSet.getSMRUpdates(proxy.getStreamID()).size() - 1;
        }
    }

    public void add(UUID streamId, SMREntry updateEntry) {
        synchronized (getRootContext().getTransactionID()) {
            // Add the SMREntry to the list of updates for this stream.
            writeSet.addTo(streamId, updateEntry);
        }
    }

    public void add(UUID streamId, List<SMREntry> updateEntries) {
        synchronized (getRootContext().getTransactionID()) {
            // add the SMRentry to the list of updates for this stream
            writeSet.addTo(streamId, updateEntries);
        }
    }

    @Override
    public void mergeInto(ConflictSetInfo other) {
        if (!(other instanceof WriteSetInfo)) {
            throw new UnsupportedOperationException("Merging write set with read set unsupported");
        }

        super.mergeInto(other);
        streamTags.addAll(((WriteSetInfo) other).streamTags);
        writeSet.mergeInto(((WriteSetInfo) other).writeSet);
    }
}
