package org.corfudb.runtime.view.stream;

import java.util.UUID;

import lombok.Getter;
import lombok.Setter;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.view.Address;
import org.corfudb.util.Utils;


/** A data class which keeps data for each stream context.
 * Stream contexts represent a copy-on-write context - for each source
 * stream a single context is used up until maxGlobalAddress, at which the new
 * *destination* stream is used by popping the context off the stream
 * context stack.
 * Created by mwei on 1/6/17.
 */
public abstract class AbstractStreamContext implements
        Comparable<AbstractStreamContext> {

    /**
     * The ID (stream ID) of this context.
     */
    final UUID id;

    /**
     * The maximum global address that we should follow to, or
     * Address.MAX, if this is the final context.
     */
    final long maxGlobalAddress;

    /**
     * The trim mark for Garbage Collection.
     *
     * This is based on the log trim mark,
     * and is used to discard data in the stream views before the trim mark.
     *
     * Note: Data cannot be discarded deliberately as there might be active transactions
     * still operating in this space, we need to ensure the object is synced beyond this threshold
     * before discarding data, or data might be temporarily loss or resets will slow performance.
     *
     */
    @Getter
    @Setter
    protected long gcTrimMark = 0;

    /**
     * A pointer to the current global address, which is the
     * global address of the most recently added entry.
     */
    @Getter
    protected long globalPointer;

    /**
     * Set Global Pointer and validate its position does not fall in the GC trim range.
     *
     * If it falls we should throw a Trim Exception as this data no longer
     * exists in the log and will be GC from all layers.
     *
     * @param globalPointer position to set the global pointer to.
     */
    protected void setGlobalPointerCheckGCTrimMark(long globalPointer) {
        validateGlobalPointerPosition(globalPointer);
        this.setGlobalPointer(globalPointer);
    }

    protected void setGlobalPointer(long globalPointer) {
        this.globalPointer = globalPointer;
    }

    /**
     * Validate that Global Pointer position does not fall in the GC trim range.
     *
     * Note: we need to throw an exception whenever this is the case, as keeping active transactions
     * in this range can lead to 'temporal' data loss when GC cycles ate started, i.e., sync to an old version
     * and trimming the resolved queue, before the updates between old version and trim mark are applied to the object.
     *
     * @param globalPointer position to set the global pointer to.
     */
    protected void validateGlobalPointerPosition(long globalPointer) {
        if (globalPointer < gcTrimMark && globalPointer > Address.NON_ADDRESS) {
            throw new TrimmedException(String.format("Stream[%s] global pointer position[%s] is in GC trim range. " +
                            "GC Trim mark: [%s]. This address is trimmed from the log.", Utils.toReadableId(id), globalPointer, gcTrimMark));
        }
    }

    /**
     * Generate a new stream context given the id of the stream and the
     * maximum address to read to.
     * @param id                  The id of the stream.
     * @param maxGlobalAddress    The maximum address to read up to.
     */
    public AbstractStreamContext(final UUID id,
                                 final long maxGlobalAddress) {
        this.id = id;
        this.maxGlobalAddress = maxGlobalAddress;
        this.setGlobalPointerCheckGCTrimMark(Address.NON_ADDRESS);
    }

    /** Reset the stream context. */
    void reset() {
        globalPointer = Address.NON_ADDRESS;
    }

    /** Move the pointer for the context to the given global address,
     * updating any structures if necessary.
     * @param globalAddress     The address to seek to.
     */
    void seek(long globalAddress) {
        // by default we just need to update the pointer.
        // we subtract by one, since the NEXT read will
        // have to include globalAddress.
        // FIXME change this; what if globalAddress==0? somewhere down the line,
        // some code will compare this with NEVER_READ
        globalPointer = globalAddress - 1;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int compareTo(AbstractStreamContext o) {
        return Long.compare(this.maxGlobalAddress, o.maxGlobalAddress);
    }
}
