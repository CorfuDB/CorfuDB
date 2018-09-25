package org.corfudb.runtime.view.stream.address;

import java.util.List;

import javax.annotation.Nonnull;

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
     * Reset the address space, i.e., reset pointers to the beginning of the stream.
     */
    void reset();

    /**
     * Seeks for a specific address, and moves the pointer in the stream to this position.
     * If the given address does not exist for this stream, it will 'seek' until the closest address
     * (i.e., the greatest address less than the specified).
     */
    void seek(long address);

    /**
     * Get maximum resolved address.
     *
     * @return maximum address for this stream
     */
    long getMax();

    /**
     * Get minimum resolved address.
     *
     * @return minimum address for this stream
     */
    long getMin();

    /**
     * Returns the next address in the stream
     *
     * @return next address in the stream
     */
    long next();

    /**
     * Returns the previous address in the stream
     *
     * @return previous address in the stream
     */
    long previous();

    /**
     * Returns a list of addresses up to a given limit (inclusive).
     *
     * @return addresses in the stream up to a limit
     */
    List<Long> remainingUpTo(long limit);

    /**
     * Add list of addresses to the space of addresses of this stream.
     *
     * @param addresses addresses to be added.
     */
    void addAddresses(@Nonnull List<Long> addresses);

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
    void setStreamOptions(@Nonnull StreamOptions options);

    /**
     * Retrieves the last global address to which this stream was synced.
     * This might differ from getMax() depending on the actual stream implementation.
     *
     * For instance, in the space of addresses of a checkpoint, getMax() will yield the maximum
     * position of the checkpoint, while getLastAddressSynced() will return the last address
     * synced from the regular stream and represented (contained) in the checkpoint.
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
    boolean hasNext();

    /**
     * Determines if there is a valid address in the stream while traversing backwards
     * from the current pointer.
     *
     * @return true if exists, false otherwise.
     */
    boolean hasPrevious();

    /**
     * Removes all addresses below the given limit (inclusive) from this address space.
     *
     * @param upperBound upper limit of addresses to remove from this space.
     */
    void removeAddresses(long upperBound);

    /**
     * Removes a specific address from this address space.
     *
     * @param address to remove from this space.
     */
    void removeAddress(long address);

    /**
     * Sync/update the space of addresses between newTail and lowerBound.
     */
    void syncUpTo(long globalAddress, long newTail, long lowerBound);

    /**
     * Determines if a given address is contained in the space of addresses of this stream
     */
    boolean containsAddress(long address);

    /**
     * Defines how the space of addresses is traversed to find new addresses in the range between
     * newTail and oldTail (non-exclusive).
     *
     */
    void findAddresses(long globalAddress, long oldTail, long newTail);

    /**
     * Indicates if the space of addresses is empty for this stream.
     *
     * @return true if empty. false, otherwise.
     */
    boolean isEmpty();

    /**
     * Read an address.
     *
     * @param address address to be read.
     * @return actual data read from specified address.
     */
    ILogData read(final long address);

    /**
     * Read a range of addresses.
     *
     * @param start address to start reading.
     * @param stop address to stop reading.
     * @return actual data read from specified range of addresses.
     */
    List<ILogData> rangeRead(final long start, final long stop);

    /**
     * Read a range of addresses.
     *
     * @param base address to start reading from. It will read until the end of the stream.
     * @return actual data read from the range of addresses.
     */
    List<ILogData> rangeRead(final long base);
}
