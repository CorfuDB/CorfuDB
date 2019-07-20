package org.corfudb.runtime.view.stream;

import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.view.Address;

import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.UUID;


@ToString
@Slf4j
class StreamContext extends AbstractStreamContext {

    /**
     * A queue of addresses which have already been resolved.
     */
    final NavigableSet<Long> resolvedQueue
            = new TreeSet<>();

    /**
     * The minimum global address which we have resolved this
     * stream to.
     */
    long minResolution = Address.NON_ADDRESS;

    /**
     * The maximum global address which we have resolved this
     * stream to.
     */
    long maxResolution = Address.NON_ADDRESS;

    /**
     * A priority queue of potential addresses to be read from.
     */
    final NavigableSet<Long> readQueue
            = new TreeSet<>();

    /**
     * List of checkpoint records, if a successful checkpoint has been observed.
     */
    final NavigableSet<Long> readCpQueue = new TreeSet<>();

    /**
     * Info on checkpoint we used for initial stream replay,
     * other checkpoint-related info & stats.
     */
    AbstractQueuedStreamView.StreamCheckpoint checkpoint = new AbstractQueuedStreamView.StreamCheckpoint();

    /**
     * Create a new stream context with the given ID and maximum address
     * to read to.
     *
     * @param id               The ID of the stream to read from
     * @param maxGlobalAddress The maximum address for the context.
     */
    public StreamContext(UUID id, long maxGlobalAddress) {
        super(id, maxGlobalAddress);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    void reset() {
        super.reset();
        readCpQueue.clear();
        readQueue.clear();
        resolvedQueue.clear();
        minResolution = Address.NON_ADDRESS;
        maxResolution = Address.NON_ADDRESS;

        checkpoint.reset();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    synchronized void seek(long globalAddress) {
        if (Address.nonAddress(globalAddress)) {
            throw new IllegalArgumentException("globalAddress must"
                    + " be >= Address.maxNonAddress()");
        }
        log.trace("Seek[{}]({}), min={} max={}", this, globalAddress,
                minResolution, maxResolution);
        // Update minResolution if necessary
        if (globalAddress >= maxResolution) {
            log.trace("set min res to {}", globalAddress);
            minResolution = globalAddress;
        }
        // remove anything in the read queue LESS
        // than global address.
        readQueue.headSet(globalAddress).clear();
        // transfer from the resolved queue into
        // the read queue anything equal to or
        // greater than the global address
        readQueue.addAll(resolvedQueue.tailSet(globalAddress, true));
        super.seek(globalAddress);
    }

}
