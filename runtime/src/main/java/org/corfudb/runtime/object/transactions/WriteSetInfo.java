package org.corfudb.runtime.object.transactions;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import lombok.Getter;

import org.corfudb.protocols.logprotocol.MultiObjectSMREntry;
import org.corfudb.protocols.logprotocol.SMREntry;
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
    MultiObjectSMREntry writeSet = new MultiObjectSMREntry();

    public long add(ICorfuSMRProxyInternal proxy, SMREntry updateEntry, Object[] conflictObjects) {
        synchronized (getRootContext().getTransactionID()) {

            affectedStreams.add(proxy.getStreamID());

            // add the SMRentry to the list of updates for this stream
            writeSet.addTo(proxy.getStreamID(), updateEntry);

            super.add(proxy, conflictObjects);

            return writeSet.getSMRUpdates(proxy.getStreamID()).size() - 1;
        }
    }

    public void add(UUID stream) {
        affectedStreams.add(stream);
    }

    @Override
    public Map<UUID, Set<byte[]>> getHashedConflictSet() {
        Map<UUID, Set<byte[]>> conflicts =  super.getHashedConflictSet();

        // Include the affected streams that are missing in the conflict set.
        Set<UUID> diff = Sets.difference(affectedStreams, conflicts.keySet());
        ImmutableMap.Builder<UUID, Set<byte[]>> builder = ImmutableMap.builder();
        builder.putAll(conflicts);
        diff.forEach(id -> builder.put(id, Collections.emptySet()));

        return builder.build();
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
