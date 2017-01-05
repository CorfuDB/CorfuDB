package org.corfudb.runtime.view.stream;

import org.corfudb.runtime.view.Address;

import java.util.UUID;

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
     * @param id            The id of the stream.
     * @param maxGlobalAddress    The maximum address to read up to.
     */
    public AbstractStreamContext(final UUID id,
                                 final long maxGlobalAddress) {
        this.id = id;
        this.maxGlobalAddress = maxGlobalAddress;
        this.globalPointer = Address.NEVER_READ;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int compareTo(AbstractStreamContext o) {
        return Long.compare(this.maxGlobalAddress, o.maxGlobalAddress);
    }
}
