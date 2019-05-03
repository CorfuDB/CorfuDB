package org.corfudb.runtime.view;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.exceptions.TrimmedException;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.UUID;

@Slf4j
@Data
/**
 * This class manages a stream's address space as a set of queues for the AbstractQueuedStreamView.
 * It manages the space of addresses of a stream based on three queues:
 *
 * - Resolved queue: a queue of known (already resolved) addresses for the underlying stream.
 * - Read queue: a queue of potential addresses. This queue contains addresses of the regular stream.
 * - Read Checkpoint queue: a queue of potential checkpoint records with state/updates to this stream.
 *
 * Once addresses in the read queue (regular stream) have been read and validated, they are added
 * to the resolved queue.
 */
public class QueueAddressManager {

    /** Stream ID for which the address space is managed. */
    private final UUID id;

    /** A queue of addresses which have already been resolved (in sorted order). */
    public final NavigableSet<Long> resolvedQueue = new TreeSet<>();

    /** A queue of potential addresses to be read from (sorted). */
    public final NavigableSet<Long> readQueue = new TreeSet<>();

    /** List of checkpoint records, if a successful checkpoint has been observed (sorted). */
    public final NavigableSet<Long> readCpQueue = new TreeSet<>();

    /** A pointer to the current global address. */
    private long globalPointer;

    /** The minimum global address which we have resolved this stream to. */
    private long minResolution = Address.NON_ADDRESS;

    /** The maximum global address which we have resolved this stream to. */
    private long maxResolution = Address.NON_ADDRESS;

    /**
     * The trim mark for Garbage Collection.
     *
     * This is based on the log trim mark, and is used to discard
     * data in the stream views before the trim mark.
     *
     * Note: Data cannot be discarded deliberately as there might be active transactions
     * still operating in this space, we need to ensure the object is synced beyond this threshold
     * before discarding data, or data might be temporarily lost or resets will slow performance.
     *
     */
    private long gcTrimMark = 0L;

    /** Info on checkpoint we used for initial stream replay. */

    /** Checkpoint identifier. */
    private UUID checkpointSuccessId = null;

    /** Checkpointed data start address, i.e.,
     * the greatest version of the regular stream
     * for which this checkpoint contains data. */
    private long checkpointSuccessStartAddr = Address.NEVER_READ;

    /** The address the current checkpoint snapshot was taken at.
     *  The checkpoint guarantees for this stream there are no entries
     *  between checkpointSuccessStartAddr and checkpointSnapshotAddress.
     */
    private long checkpointSnapshotAddress = Address.NEVER_READ;

    /** Create a stream's queue address manager.
     *
     * @param id  The ID of the stream to manage addresses for.
     */
    public QueueAddressManager(UUID id) {
        this.id = id;
        this.setGlobalPointer(Address.NON_ADDRESS);
    }

    /**
     * Set Global Pointer and validate that its position does not fall in the GC trim range.
     *
     * If it falls we should throw a Trim Exception as this data no longer
     * exists in the log and will be GC from all layers.
     *
     * @param globalPointer position to set the global pointer to.
     */
    public void setGlobalPointerCheckGCTrimMark(long globalPointer) {
        validateGlobalPointerPositionForGC(globalPointer);
        this.setGlobalPointer(globalPointer);
    }

    /**
     * Validate that Global Pointer position does not fall in the GC trim range.
     *
     * Note: we need to throw an exception whenever this is the case, as keeping active transactions
     * in this range can lead to 'temporal' data loss when GC cycles are started, i.e., sync to an old version
     * and trimming the resolved queue, before the updates between old version and trim mark are applied to the object.
     *
     * @param globalPointer position to set the global pointer to.
     */
    public void validateGlobalPointerPositionForGC(long globalPointer) {
        if (globalPointer < gcTrimMark && globalPointer > Address.NON_ADDRESS) {
            throw new TrimmedException(String.format("Global pointer position[%s] is in GC trim range. " +
                            "GC Trim mark: [%s]. This address is trimmed from the log.",
                    globalPointer, gcTrimMark));
        }
    }

    /** Add the given address to the resolved queue of the given context.
     *
     * @param globalAddress     The resolved global address.
     */
    public void addToResolvedQueue(long globalAddress) {
        resolvedQueue.add(globalAddress);

        if (maxResolution < globalAddress) {
            maxResolution = globalAddress;
        }
    }

    /**
     * Reset all queues and pointers for this address space.
     */
    public void reset() {
        globalPointer = Address.NON_ADDRESS;

        readCpQueue.clear();
        readQueue.clear();
        resolvedQueue.clear();
        minResolution = Address.NON_ADDRESS;
        maxResolution = Address.NON_ADDRESS;

        checkpointSuccessId = null;
        checkpointSuccessStartAddr = Address.NEVER_READ;
        checkpointSnapshotAddress = Address.NEVER_READ;
    }

    /** Seek to the requested global address. The next read will
     * begin at the given global address, inclusive.
     *
     * @param globalAddress Address to seek to.
     */
    public synchronized void seek(long globalAddress) {
        if (Address.nonAddress(globalAddress)) {
            throw new IllegalArgumentException("globalAddress must"
                    + " be >= Address.maxNonAddress()");
        }
        log.trace("Seek[{}]({}), min={} max={}", this,  globalAddress,
                minResolution, maxResolution);
        // Update minResolution if necessary
        if (globalAddress >= maxResolution) {
            log.trace("Set min resolution to {}" , globalAddress);
            minResolution = globalAddress;
        }
        // remove anything in the read queue LESS
        // than global address.
        readQueue.headSet(globalAddress).clear();
        // transfer from the resolved queue into
        // the read queue anything equal to or
        // greater than the global address
        readQueue.addAll(resolvedQueue.tailSet(globalAddress, true));

        // TODO: THIS IS WRONG THE GLOBAL POINTER IS NOT -1 THAT MIGHT NOT BE A VALID ADDRESS
        // by default we just need to update the pointer.
        // we subtract by one, since the NEXT read will
        // have to include globalAddress.
        // FIXME change this; what if globalAddress==0? somewhere down the line,
        // some code will compare this with NEVER_READ
        globalPointer = globalAddress - 1;
    }

    /**
     * Verifies that there are no pending reads, neither for the regular stream
     * or checkpoint records.
     *
     * Returns True, if no pending reads.
     * Returns False, otherwise.
     */
    public boolean isQueueEmpty() {
        return readQueue.isEmpty() && readCpQueue.isEmpty();
    }

    /**
     * Returns the queue to read from given the maxGlobal to read up to.
     *
     * @param maxGlobal maximum address to read up to.
     * @return read queue (regular or checkpoint)
     */
    public NavigableSet<Long> getReadQueue(long maxGlobal) {
        // If the lowest DATA element is greater than maxGlobal, there's nothing
        // to return.
        if (readCpQueue.isEmpty() && readQueue.first() > maxGlobal) {
            return null;
        }

        // If checkpoint data is available, get from readCpQueue first
        NavigableSet<Long> queueToRead;
        if (readCpQueue.size() > 0) {
            queueToRead = readCpQueue;
            // Note: this is a checkpoint, we do not need to verify it is before the trim mark, it actually should be
            // cause this is the last address of the trimmed range.
            setGlobalPointer(checkpointSuccessStartAddr);
        } else {
            queueToRead = readQueue;
        }

        return queueToRead;
    }

    /**
     * Get list of pending reads up to maxGlobal (from checkpoint and regular stream)
     *
     * @param maxGlobal maximum address to resolve up to.
     * @return list of all pending reads up to maxGlobal.
     */
    public List<Long> getAllReads(long maxGlobal) {
        List<Long> pendingReads = new ArrayList<>();
        // If we witnessed a checkpoint during our scan that
        // we should pay attention to, then start with them.
        pendingReads.addAll(readCpQueue);

        if (!readQueue.isEmpty() && readQueue.first() > maxGlobal) {
            // If the lowest element is greater than maxGlobal, there's nothing
            // more to return: readSet is ok as-is.
        } else {
            // Select everything in the read queue between
            // the start and maxGlobal
            pendingReads.addAll(readQueue.headSet(maxGlobal, true));
        }
        return pendingReads;
    }

    /**
     * Collect garbage for stream view, i.e., clear all addresses below the previous trim mark.
     *
     * @param trimMark point log has been trimmed.
     */
    public void gc(long trimMark) {
        // GC stream only if the pointer is ahead from the trim mark (last untrimmed address),
        // this guarantees that the data to be discarded is already applied to the stream and data will not be lost.
        // Note: if the pointer is behind, discarding the data immediately will incur in data
        // loss as checkpoints are only loaded on resets. We don't want to trigger resets as this slows
        // the runtime.
        if (globalPointer >= gcTrimMark) {
            log.debug("gc[{}]: start GC on stream {} for trim mark {}", this, id, gcTrimMark);
            // Remove all the entries that are strictly less than
            // the trim mark
            readCpQueue.headSet(gcTrimMark).clear();
            readQueue.headSet(gcTrimMark).clear();
            resolvedQueue.headSet(gcTrimMark).clear();

            if (!resolvedQueue.isEmpty()) {
                minResolution = resolvedQueue.first();
            }
        } else {
            log.debug("gc[{}]: GC not performed on stream {} for this cycle. Global pointer {} is below trim mark {}",
                    this, id, globalPointer, gcTrimMark);
        }

        // Set the trim mark for next GC Cycle
        gcTrimMark = trimMark;
    }

    /**
     * Clear regular stream read queue up to maxGlobal.
     *
     * @param maxGlobal maximum address to clear queue (inclusive).
     */
    public void clearReadQueue(long maxGlobal) {
       readQueue.headSet(maxGlobal, true).clear();
    }

    /**
     * Check if the current address space was reset.
     *
     * @return True, if address space is reset.
     *         False, otherwise.
     */
    public boolean isAddressSpaceReset() {
        return globalPointer == Address.NEVER_READ && checkpointSuccessId == null;
    }

    /**
     * Verify that a given address is resolved for the current address space.
     *
     * @param address global address.
     * @return True, if address is in the resolved queue.
     *         False, otherwise.
     */
    public boolean isAddressResolved(long address) {
        return checkpointSuccessId == null && resolvedQueue.contains(address);
    }

    /**
     * Verify that address is covered by a valid checkpoint.
     *
     * @param address global address.
     * @return True, if address is covered by checkpoint.
     *         False, otherwise.
     */
    public boolean isAddressCoveredByCheckpoint(long address) {
        return checkpointSuccessId != null && checkpointSuccessStartAddr >= address;
    }

    /**
     * Verify if the address space is resolved up to a given boundary. This address
     * does not need to belong to the stream.
     *
     * @param maxGlobal global address.
     * @return True, if address space is resolved up to this address (inclusive).
     *         False, otherwise.
     */
    public boolean isAddressSpaceResolvedUpTo(long maxGlobal) {
        return maxResolution >= maxGlobal;
    }

    /**
     * Get the maximum address resolved for this stream.
     *
     * @return
     */
    public long getMaxResolvedAddress() {
        // The max resolution should actually be the max between: checkpointSnapshotAddress &
        // maxResolution, but because maxResolution is not properly set (bug in current code),
        // this should do it for now.
        return Long.max(globalPointer, checkpointSnapshotAddress);
    }

    /**
     * Verify if any of the pending read queues is filled.
     *
     * @return True, if read queue or read checkpoint queue is filled.
     *         False, otherwise.
     */
    public boolean isAnyQueueFilled() {
        return !readCpQueue.isEmpty() || !readQueue.isEmpty();
    }
}
