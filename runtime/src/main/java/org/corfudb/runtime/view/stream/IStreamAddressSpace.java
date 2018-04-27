package org.corfudb.runtime.view.stream;

import java.util.List;
import java.util.function.Function;

import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.view.StreamOptions;

/**
 * <p>This interface represents the space of addresses of a stream. It also defines
 * how the space of addresses can be traversed and discovered, i.e.,
 * how to find the addresses belonging to updates to this stream.</p>
 *
 * <p>The space of addresses is tracked by a pointer which allows to move back and forth between
 * different versions (states) of the stream.</p>
 *
 * Created by amartinezman on 4/24/18.
 */
public interface IStreamAddressSpace {

    /**
     * Reset the address space, i.e., reset pointers.
     *
     */
    void reset() ;

    /**
     * Seeks for a specific address, and moves the pointer in the stream to this position.
     *
     */
    void seek(long address);

    /**
     * Get maximum resolved address.
     *
     * @return maximum address for this stream (address is global)
     */
    long getMax();

    /**
     * Get minimum resolved address.
     *
     * @return minimum address for this stream (address is global)
     */
    long getMin();

    /**
     * Returns the next address in the stream
     *
     * @return next address in the stream (address is global)
     */
    long next();

    /**
     * Returns the previous address in the stream
     *
     * @return previous address in the stream (address is global)
     */
    long previous();

    /**
     * Returns a list of addresses up to a given limit.
     *
     * @return addresses in the stream up to a limit (addresses are global)
     */
    List<Long> remainingUpTo(long limit);

    /**
     * Add list of addresses to the space of addresses of this stream.
     *
     * @param addresses global addresses to be added.
     */
    void addAddresses(List<Long> addresses);

    /**
     * Returns the address for the current pointer.
     *
     * @return A stream address corresponding to the current position of the pointer in the stream.
     */
    long getCurrentPointer();

    /**
     * Set Stream Options
     *
     * @param options options for this stream.
     */
    void setStreamOptions(StreamOptions options);

    /**
     * Retrieves the last global address to which this stream was synced.
     * This might differ from getMax() depending on the actual stream implementation. For instance,
     * in the space of addresses of a checkpoint, getMax() will yield the maximum position of the
     * checkpoint, while getLastAddressSynced() will return the last address synced from
     * the regular stream and represented (contained) in the checkpoint.
     *
     * @return
     */
    long getLastAddressSynced();

    /**
     * Determines if there is a valid address in the stream while traversing forward
     * from the current pointer.
     *
     * @return true if exists, false otherwise.
     */
    boolean hasNext

    ();

    /**
     * Determines if there is a valid address in the stream while traversing backwards
     * from the current pointer.
     *
     * @return true if exists, false otherwise.
     */
    boolean hasPrevious();

    /**
     * Removes all addresses below the given limit from this address space.
     *
     * @param upperBound upper limit of addresses to remove from this space.
     */
    void removeAddresses(long upperBound);

    /**
     * Sync/update the space of addresses between newTail and lowerBound.
     */
    void syncUpTo(long globalAddress, long newTail, long lowerBound,
                         Function<Long, ILogData> readFn);

    /**
     * Determines if a given address is contained in the space of addresses of this stream
     */
    boolean containsAddress(long globalAddress);

    /**
     * Returns the least (smallest) element in the space address that is greater than (not equal too)
     * the element passed as parameter.
     */
    long higher(long globalAddress, boolean forward);

    /**
     * Returns the greatest element in the space address that is lower than (not equal too)
     * the element passed as parameter.
     */
    long lower(long globalAddress, boolean forward);

    /**
     * Defines how the space of addresses is traversed to find new addresses in the range between
     * newTail and oldTail (non-exclusive).
     *
     * @return the number of addresses found for this stream.
     */
    int findAddresses(long oldTail, long newTail, Function<Long, ILogData> readFn);

    /**
     * Indicates if the space of addresses is empty for this stream.
     *
     * @return true if empty. false, otherwise.
     */
    boolean isEmpty();

    /**
     * Set Pointer of Stream to the position of 'address'
     *
     * @param address address where the stream space should be pointing to.
     */
    void setPointerToPosition(long address);
}
