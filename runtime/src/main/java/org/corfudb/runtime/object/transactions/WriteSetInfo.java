package org.corfudb.runtime.object.transactions;

import com.google.common.base.Preconditions;
import lombok.Getter;
import org.corfudb.protocols.logprotocol.MultiObjectSMREntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.object.MVOCorfuCompileProxy;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

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

    /**
     * Register a conflict on an object without writing anything to it. Used by no-op writes such
     * as TxnContext.touch(), which need to create a write-write conflict on a key but must not
     * write a payload or generate a stream tag notification.
     * <p>
     * The stream is registered with an empty update list rather than skipped entirely, for two
     * reasons:
     * If it isn't added to the writeSet, it is treated as a read-only transction and short circuits
     * before reaching the Sequencer (where global conflicts are detected).
     * Passing an empty list ensures that the actual chain replication for this transaction remains
     * cheap.
     *
     * @param proxy           the SMR object the conflict is registered on.
     * @param conflictObjects the fine-grained conflict information, which must be present.
     */
    public void addConflictOnly(MVOCorfuCompileProxy<?> proxy, Object[] conflictObjects) {
        // A null or empty conflict set is interpreted by the sequencer as a conflict against
        // every update on the stream, which would abort on any concurrent write to the table.
        Preconditions.checkArgument(conflictObjects != null && conflictObjects.length > 0,
                "addConflictOnly requires fine-grained conflict objects");

        synchronized (getRootContext().getTransactionID()) {
            // Register the stream carrying no updates, so no payload is written.
            writeSet.addTo(proxy.getStreamID(), Collections.emptyList());
            // Deliberately no streamTags.addAll(): a conflict-only update notifies no one.
            super.add(proxy, conflictObjects);
        }
    }

    public long add(MVOCorfuCompileProxy<?> proxy,
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

    public void add(UUID streamId, SMREntry updateEntry, List<UUID> streamTags) {
        synchronized (getRootContext().getTransactionID()) {
            this.streamTags.addAll(streamTags);
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
