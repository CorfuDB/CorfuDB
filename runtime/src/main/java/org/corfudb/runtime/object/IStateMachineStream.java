package org.corfudb.runtime.object;

import java.util.UUID;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.corfudb.runtime.view.Address;


/** An {@link Class} represents a stream (ordered, infinite sequence) of state machine operations.
 *
 *  <p>An instance of {@link Class} keeps a pointer within this infinite sequence. Consumers of
 *  a {@link Class} call {@link #sync(long, Object[])} to read updates from the stream, which moves
 *  the pointer (backwards or forward) to the position requested, returning a stream of state
 *  machine operations which should be applied to reach that position. These operations may
 *  contain undo/redo operations which must be applied to the object.
 *
 *  <p>A stream may have a parent stream, which means that stream is "based" of the parent
 *  stream. {@link #getParent()} and {@link #getRoot()} can be used to determine whether the
 *  stream has a parent stream, and what the root stream (parent without a child) of that hierarchy
 *  is. Child streams may contain a history of updates which differ from the parent stream's
 *  updates. In typical usage, these updates are "optimistic" updates. Calling
 *  {@link #sync(long, Object[])} with the special address
 *  {@link org.corfudb.runtime.view.Address.OPTIMISTIC} returns a stream of state machine updates so
 *  that the position of the stream matches that of its parent (effectively "undoing" all
 *  optimistic updates).
 *
 */
public interface IStateMachineStream {

    /** Returns a stream of state machine operations which should be applied to an object in order
     * to bring it to the position given by {@code pos}, and moves the pointer of the stream to
     * the new position. The caller can provide an optional array of {@code conflictObjects},
     * which provides fine-grained information on the objects being requested by this read.
     *
     * <p>Callers may provide two special values for {@code pos}:
     *
     * <p>{@link org.corfudb.runtime.view.Address.MAX} consumes the latest updates to the stream.
     * The definition of "latest" is implementation dependent.
     * For example, {@link LinearizableStateMachineStream} guarantees
     * that a call to {@link #sync(long)} will return (in wall-clock time) all completed calls
     * to {@link #append(String, Object[], Object[], boolean)} before it.
     *
     * <p>{@link org.corfudb.runtime.view.Address.OPTIMISTIC} provides a stream of operations
     * necessary to revert any operations previously returned so that the pointer of this stream
     * matches the pointer of the parent stream. It can be thought of as "undoing" all optimistic
     * updates.
     *
     * <p>{@link org.corfudb.runtime.view.Address.NEVER_READ} reverts the stream back to the
     * state it was before {@link #sync(long, Object[])} was ever called.
     *
     * <p>After the completion of this call, the updates are considered consumed and the pointer
     * as reflected by {@link #pos()} will reflect the updates returned by this call.
     *
     * <p>In certain implementations, the returned stream may be unordered (in which case, it
     * is guaranteed that the updates may be safely processed in any order).
     *
     * @param pos   The position to move the pointer to,
     *              {@link org.corfudb.runtime.view.Address.MAX} to consume all available
     *              updates, {@link org.corfudb.runtime.view.Address.OPTIMISTIC} to make the
     *              current stream pointer match the parent stream, or
     *              {@link org.corfudb.runtime.view.Address.NEVER_READ} to revert the stream back
     *              to the position where it was never read from.
     *
     * @return      A {@link Stream} of updates to apply to bring an object to {@code pos}.
     */
    @Nonnull Stream<IStateMachineOp> sync(long pos, @Nullable Object[] conflictObjects);

    /** Returns the current position of the stream. The current position of the stream reflects
     * the position of the last call to {@link #sync(long, Object[])}. The implementation may
     * return the special value {@link org.corfudb.runtime.view.Address.NEVER_READ}, which indicates
     * sync has not been called.
     *
     * @return      The current position of the stream.
     */
    long pos();

    /** Reset the stream, returning the stream position to
     * {@link org.corfudb.runtime.view.Address.NEVER_READ}.
     */
    void reset();

    /** Check if the stream is up to date. If it is, the special value
     * {@link org.corfudb.runtime.view.Address.UP_TO_DATE} is returned, otherwise, a value which
     * can be passed to {@link #sync(long, Object[])} which will bring the stream up to date is
     * returned.
     *
     * @return      {@link org.corfudb.runtime.view.Address.UP_TO_DATE} if the stream is up to date,
     *              otherwise a value to pass into {@link #sync(long, Object[])} to bring the stream
     *              up to date.
     */
    default long check() {
        return Address.MAX;
    }

    /** Move the stream pointer so that the current position reflects the value given in {@code
     * pos}. This means that the position given in {@code pos} effectively becomes the last
     * read entry.
     *
     * @param pos   The new position to set for the stream pointer.
     */
    default void seek(long pos) {
        throw new UnsupportedOperationException("Not supported by this stream type.");
    }

    /** Append a new state machine operation into the stream, providing optional conflict
     * information and whether or not to keep track of the entry for use in an upcall later in a
     * subsequent call to {@link #consumeEntry(long)}.
     *
     * @param smrMethod         The name of the state machine method being called
     * @param smrArguments      Arguments to the state machine method
     * @param conflictObjects   An optional array of objects which describes fine-grained
     *                          conflict information.
     * @param keepEntry         True, if we should keep the entry for so it can be retrieved in a
     *                          future call to {@link #consumeEntry(long)}.
     * @return                  The address that the entry was appended to. This address can be
     *                          passed to {@link #consumeEntry(long)} or
     *                          {{@link #sync(long, Object[])}}.
     */
    long append(@Nonnull String smrMethod,
                @Nonnull Object[] smrArguments,
                @Nullable Object[] conflictObjects,
                boolean keepEntry);

    /** Consumes a state machine entry, returning the state machine operation at the given address.
     * Typically used to obtain the result of an upcall. This address must have been provided by
     * a previous call to {@link #append(String, Object[], Object[], boolean)} with the
     * {@code keepEntry} flag set to true.
     *
     * @param address           An address provided by a previous call to
     *                          {@link #append(String, Object[], Object[], boolean)}
     * @return                  The state machine entry appended to the given address.
     */
    @Nullable IStateMachineOp consumeEntry(long address);

    /** Get the parent stream of this stream, if present, or null if the stream has no parent.
     *
     * @return  The parent stream of this stream, or null, if there is no parent.
     */
    default @Nullable IStateMachineStream getParent() {
        return null;
    }

    /** Get the root stream in the hierarchy.
     *
     * @return  The root stream in this hierarchy, which may be this stream itself.
     */
    default @Nonnull IStateMachineStream getRoot() {
        IStateMachineStream root = this;
        while (true) {
            IStateMachineStream child = root.getParent();
            if (child == null) {
                return root;
            }
            root = child;
        }
    }

    /**
     * Get the UUID for this stream.
     *
     * @return The UUID for this stream.
     */
    UUID getId();
}
