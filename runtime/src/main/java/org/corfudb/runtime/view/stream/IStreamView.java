package org.corfudb.runtime.view.stream;

import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.view.Address;


/** This interface represents a view on a stream. A stream is an ordered
 * set of log entries which can only be appended to and read in sequential
 * order.
 *
 * <p>Created by mwei on 1/5/17.
 */
public interface IStreamView extends
        Iterator<ILogData> {

    /** Return the ID of the stream this view is for.
     * @return  The ID of the stream.
     */
    UUID getId();

    /** Reset the state of this stream view, causing the next read to
     * start from the beginning of this stream.
     */
    void reset();

    /**
     * Garbage collect all the trimmed entries on this stream
     * @param trimMark start of the active log
     */
    void gc(long trimMark);

    /** Seek to the requested maxGlobal address. The next read will
     * begin at the given global address, inclusive.
     * @param globalAddress Address to seek to
     */
    void seek(long globalAddress);

    /** Append an object to the stream, returning the global address
     * it was written at.
     * <p>
     * Optionally, provide a method to be called when an address is acquired,
     * and also a method to be called when an address is released (due to
     * an unsuccessful append).
     * </p>
     * @param   object              The object to append.
     * @param   acquisitionCallback A function to call when an address is
     *                              acquired.
     *                              It should return true to continue with the
     *                              append.
     * @param   deacquisitionCallback A function to call when an address is
     *                                released. It should return true to retry
     *                                writing.
     * @return  The (global) address the object was written at.
     */
    long append(Object object,
                Function<TokenResponse, Boolean> acquisitionCallback,
                Function<TokenResponse, Boolean> deacquisitionCallback);

    /** Append an object to the stream, returning the global address it was
     * written at.
     * @param   object Object to append/write
     * @return  The (global) address the object was written at.
     */
    default long append(Object object) {
        return append(object, null, null);
    }

    /** Retrieve the next entry from this stream, up to the tail of the stream
     * If there are no entries present, this function will return NULL. If there
     * are holes present in the log, they will be filled.
     * @return  The next entry in the stream, or NULL, if no entries are
     *          available.
     */
    @Nullable
    default ILogData next() {
        return nextUpTo(Long.MAX_VALUE);
    }

    /** Retrieve the previous entry in the stream. If there are no previous entries,
     * NULL will be returned.
     * @return  The previous entry in the stream, or NULL, if no entries are
     *           available.
     */
    @Nullable
    ILogData previous();

    /** Retrieve the current entry in the stream, which was the entry previously
     * returned by a call to next() or previous(). If the stream was never read
     * from, NULL will be returned.
     *
     * @return The current entry in the stream.
     */
    @Nullable
    ILogData current();

    /** Retrieve the next entry from this stream, up to the address given or the
     *  tail of the stream. If there are no entries present, this function
     *  will return NULL. If there  are holes present in the log, they will
     *  be filled.
     * @param maxGlobal The maximum global address to read up to.
     * @return          The next entry in the stream, or NULL, if no entries
     *                  are available.
     */
    @Nullable
    ILogData nextUpTo(long maxGlobal);

    /** Retrieve all of the entries from this stream, up to the tail of this
     *  stream. If there are no entries present, this function
     *  will return an empty list. If there  are holes present in the log,
     *  they will be filled.
     *
     *  <p>Note: the default implementation is thread-safe only if the
     *  implementation of read is synchronized.
     *
     * @return          The next entries in the stream, or an empty list,
     *                  if no entries are available.
     */
    @Nonnull
    default List<ILogData> remaining() {
        return remainingUpTo(Address.MAX);
    }

    /** Retrieve all of the entries from this stream, up to the address given or
     *  the tail of the stream. If there are no entries present, this function
     *  will return an empty list. If there  are holes present in the log,
     *  they will be filled.
     *
     * @param maxGlobal The maximum global address to read up to.
     * @return          The next entries in the stream, or an empty list,
     *                  if no entries are available.
     */
    List<ILogData> remainingUpTo(long maxGlobal);

    /** Returns whether or not there are potentially more entries in this
     * stream - this function may return true even if there are no entries
     * remaining, as addresses may have been acquired by other clients
     * but not written yet, or the addresses were hole-filled, or just failed.
     * @return      True, if there are potentially more entries in the stream.
     */
    boolean hasNext();

    /** Get the current position of the pointer in this stream (global address).
     *
     * @return          The position of the pointer in this stream (global address),
     *                  or Address.NON_ADDRESS.
     */
    long getCurrentGlobalPosition();

    /** Get a spliterator for this stream view
     * @return  A spliterator for this stream view.
     */
    default Spliterator<ILogData> spliterator() {
        return new StreamSpliterator(this);
    }

    /** Get a spliterator for this stream view, limiting the return
     * values to a specified global address.
     * @param maxGlobal     The maximum global address to read to
     * @return              A spliterator for this stream view.
     */
    default Spliterator<ILogData> spliteratorUpTo(long maxGlobal) {
        return new StreamSpliterator(this, maxGlobal);
    }

    /** Get a Java stream from this stream view.
     * @return  A Java stream for this stream view.
     */
    default Stream<ILogData> stream() {
        return StreamSupport.stream(spliterator(), false);
    }

    /** Get a Java stream from this stream view, limiting
     * reads to a specified global address.
     * @param maxGlobal     The maximum global address to read to.
     * @return              A spliterator for this stream view.
     */
    default Stream<ILogData> streamUpTo(long maxGlobal) {
        return StreamSupport.stream(spliteratorUpTo(maxGlobal), false);
    }

    /**
     * Get total number of updates registered to this stream.
     *
     * @return total number of updates belonging to this stream.
     */
    long getTotalUpdates();
}
