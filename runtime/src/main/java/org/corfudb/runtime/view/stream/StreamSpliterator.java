package org.corfudb.runtime.view.stream;

import java.util.Comparator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.view.Address;


/** A spliterator for streams, which supports limiting the
 * spliterator's scope to a maximum global address.
 *
 * <p>This spliterator is guaranteed to never read PAST the
 * maximum global address given. This is necessary because
 * reading from a stream MODIFIES the stream pointer, and
 * streams provide the guarantee that they will return
 * entries in order exactly once.
 *
 * <p>Concurrent modification of the stream during iteration
 * is NOT supported.
 *
 * <p>Created by mwei on 4/24/17.
 */
@Slf4j
public class StreamSpliterator extends Spliterators.AbstractSpliterator<ILogData> {

    /** The stream view which backs the spliterator. */
    final IStreamView streamView;

    /** The maximum global address the spliterator permits reading up to. */
    final long maxGlobal;

    /** Construct a stream spliterator with the given view and no limit.
     * @param view  The view to construct the spliterator with.
     */
    public StreamSpliterator(IStreamView view) {
        this(view, Address.MAX);
    }

    /** Construct a stream spliterator with the given view and limit
     * @param view          The view to construct the spliterator with.
     * @param maxGlobal     The maximum global address to limit reads up to.
     */
    public StreamSpliterator(IStreamView view, long maxGlobal) {
        super(maxGlobal - view.getCurrentGlobalPosition(),
                Spliterator.ORDERED | Spliterator.SORTED);
        streamView = view;
        this.maxGlobal = maxGlobal;
    }

    /**
     * {@inheritDoc}
     * */
    @Override
    public boolean tryAdvance(Consumer<? super ILogData> action) {
        // Get the next entry in the stream.
        ILogData next = streamView.nextUpTo(maxGlobal);
        // If null, end.
        if (next == null) {
            return false;
        }
        // Otherwise, apply
        action.accept(next);
        return true;
    }

    /**
     * {@inheritDoc}
     * */
    @Override
    public Comparator<? super ILogData> getComparator() {
        return ILogData::compareTo;
    }
}
