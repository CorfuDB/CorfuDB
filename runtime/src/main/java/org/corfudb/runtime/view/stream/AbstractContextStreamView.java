package org.corfudb.runtime.view.stream;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Address;
import org.corfudb.util.Utils;

import javax.annotation.concurrent.NotThreadSafe;


/** An abstract context stream view maintains contexts, which are used to
 * implement copy-on-write entries.
 *
 *  <p>This implementation uses "contexts" to properly deal with copy-on-write
 * streams. Every time a stream is copied, a new context is created which
 * redirects requests to the source stream for the copy - each context
 * contains its own queue and pointers. Implementers of fillReadQueue() and
 * readAndUpdatePointers should be careful to use the id of the context,
 * rather than that of the stream view itself.
 *
 * <p>Created by mwei on 1/6/17.
 */
@Slf4j
@NotThreadSafe
public abstract class AbstractContextStreamView implements IStreamView, AutoCloseable {

    /**
     * The ID of the stream.
     */
    @Getter
    final UUID id;

    /**
     * The runtime the stream view was created with.
     */
    final CorfuRuntime runtime;

    // Context of this stream
    @Getter
    protected StreamContext context;

    /** Create a new abstract context stream view.
     *
     * @param runtime               The runtime.
     * @param id                    The id of the stream.
     */
    public AbstractContextStreamView(final CorfuRuntime runtime,
                                     final UUID id) {
        this.id = id;
        this.runtime = runtime;
        this.context = new StreamContext(id, Address.MAX);
    }

    /**
     * {@inheritDoc}
     */
    public void reset() {
        context.reset();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void seek(long globalAddress) {
        // now request a seek on the context
        this.context.seek(globalAddress);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {}

    /**
     * {@inheritDoc}
     */
    @Override
    public final ILogData nextUpTo(final long maxGlobal) {
        // Don't do anything if we've already exceeded the global pointer.
        if (context.getGlobalPointer() > maxGlobal) {
            return null;
        }

        // Get the next entry from the underlying implementation.
        final ILogData entry = getNextEntry(getContext(), maxGlobal);

        if (entry != null) {
            // Update the pointer.
            updatePointer(entry);
        }

        // Return the entry.
        return entry;
    }

    /** {@inheritDoc}
     */
    @Override
    public final List<ILogData> remainingUpTo(long maxGlobal) {
        final List<ILogData> entries = getNextEntries(getContext(), maxGlobal);

        // Nothing read, nothing to process.
        if (entries.size() == 0) {
            // We've resolved up to maxGlobal, so remember it. (if it wasn't max)
            if (maxGlobal != Address.MAX) {
                // Set Global Pointer and check that it is not pointing to an address in the trimmed space.
                getContext().setGlobalPointerCheckGCTrimMark(maxGlobal);
            }
            return entries;
        }

        // Otherwise update the pointer
        if (maxGlobal != Address.MAX) {
            // Set Global Pointer and check that it is not pointing to an address in the trimmed space.
            getContext().setGlobalPointerCheckGCTrimMark(maxGlobal);
        } else {
            // Update pointer from log data and then validate final position of the pointer against GC trim mark.
            updatePointer(entries.get(entries.size() - 1));
            getContext().validateGlobalPointerPosition(getCurrentGlobalPosition());
        }

        // And return the entries.
        return entries;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasNext() {
        return getHasNext(getContext());
    }

    /** Return whether calling getNextEntry() may return more
     * entries, given the context.
     * @param context       The context to retrieve the next entry from.
     * @return              True, if getNextEntry() may return an entry.
     *                      False otherwise.
     */
    protected abstract boolean getHasNext(StreamContext context);

    /** Retrieve the next entry in the stream, given the context.
     *
     * @param context       The context to retrieve the next entry from.
     * @param maxGlobal     The maximum global address to read to.
     * @return              Next ILogData for this context
     */
    protected abstract ILogData getNextEntry(StreamContext context, long maxGlobal);

    /** Retrieve the next entries in the stream, given the context.
     *
     * <p>This function is designed to implement a bulk read. In a bulk read,
     * one of the entries may cause the context to change - the implementation
     * should check if the entry changes the context and stop reading
     * if this occurs, returning the entry that caused contextCheckFn to return
     * true.
     *
     * <p>The default implementation simply calls getNextEntry.
     *
     * @param context           The context to retrieve the next entry from.
     * @param maxGlobal         The maximum global address to read to.
     * @return                  A list of the next entries for this context
     */
    protected List<ILogData> getNextEntries(StreamContext context, long maxGlobal) {
        final List<ILogData> dataList = new ArrayList<>();
        ILogData thisData;

        while ((thisData = getNextEntry(context, maxGlobal)) != null) {
            // Add this read to the list of reads to return.
            dataList.add(thisData);

            // Update the pointer, because the underlying implementation
            // will expect it to be updated when we call getNextEntry() again.
            updatePointer(thisData);
        }

        return dataList;
    }

    /** Update the global pointer, given an entry.
     *
     * @param data  The entry to use to update the pointer.
     */
    private void updatePointer(final ILogData data) {
        // Update the global pointer, if it is non-checkpoint data.
        if (data.getType() == DataType.DATA && !data.hasCheckpointMetadata()) {
            // Note: here we only set the global pointer and do not validate its position with respect to the trim mark,
            // as the pointer is expected to be moving step by step (for instance when syncing a stream up to maxGlobal)
            // The validation is deferred to these methods which call it in advance based on the expected final position
            // of the pointer.
            getContext().setGlobalPointer(data.getGlobalAddress());
        }
    }

    @Override
    public String toString() {
        return Utils.toReadableId(context.id) + "@" + getContext().getGlobalPointer();
    }
}
