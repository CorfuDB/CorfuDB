package org.corfudb.runtime.view.stream;

import java.util.UUID;
import org.corfudb.runtime.view.Address;


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
     * A pointer to the current global address, which is the
     * global address of the most recently added entry.
     */
    long globalPointer;

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
        this.globalPointer = Address.NON_ADDRESS;
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
