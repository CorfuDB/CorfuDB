package org.corfudb.runtime.view.stream;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.StreamOptions;

import javax.annotation.Nonnull;
import java.util.NavigableSet;
import java.util.UUID;
import java.util.function.Function;

/** A view of a stream implemented with backpointers.
 *
 * <p>In this implementation, all addresses are global (log) addresses.
 *
 * <p>All method calls of this class are thread-safe.
 *
 * <p>Created by mwei on 12/11/15.
 */
@Slf4j
public class BackpointerStreamView extends AbstractQueuedStreamView {

    /** Create a new backpointer stream view.
     *
     * @param runtime   The runtime to use for accessing the log.
     * @param streamId  The ID of the stream to view.
     */
    public BackpointerStreamView(final CorfuRuntime runtime,
                                 final UUID streamId,
                                 @Nonnull final StreamOptions options) {
        super(runtime, streamId, options);
    }

    public BackpointerStreamView(final CorfuRuntime runtime,
                                 final UUID streamId) {
        this(runtime, streamId, StreamOptions.DEFAULT);
    }

    @Override
    protected ILogData removeFromQueue(NavigableSet<Long> queue) {
        boolean readNext;
        Long thisRead;
        ILogData ld;

        do {
            if (queue.isEmpty()) {
                // nothing to read, return.
                readNext = false;
                continue;
            }

            thisRead = queue.pollFirst();
            try {
                ld = read(thisRead);
            } catch (TrimmedException te) {
                if (!getReadOptions().isIgnoreTrim()) {
                    throw te;
                }

                readNext = true;
                continue;
            }

            if (queue == getCurrentContext().readQueue && ld != null) {
                addToResolvedQueue(getCurrentContext(), thisRead, ld);
            }
            return ld;
        } while (readNext);

        return null;
    }

    private long backpointerCount = 0L;

    @Override
    public long getTotalUpdates() {
        return backpointerCount;
    }

    protected boolean discoverAddressSpace(final UUID streamId,
                                           final NavigableSet<Long> queue,
                                           final long startAddress,
                                           final long stopAddress,
                                           final Function<ILogData, Boolean> filter,
                                           final boolean checkpoint,
                                           final long maxGlobal) {
        // Now we start traversing backpointers, if they are available. We
        // start at the latest token and go backward, until we reach the
        // log pointer -or- the checkpoint snapshot address, because all
        // values from the beginning of the stream up to the snapshot address
        // should be reflected. For each address which is less than
        // maxGlobalAddress, we insert it into the read queue.

        log.trace("followBackpointers: streamId[{}], queue[{}], startAddress[{}], stopAddress[{}]," +
                "filter[{}]", streamId, queue, startAddress, stopAddress, filter);
        long readStartTime = System.currentTimeMillis();

        // The current address which we are reading from.
        long currentAddress = startAddress;

        boolean startingSingleStep = true;

        // Loop until we have reached the stop address.
        while (currentAddress > stopAddress  && Address.isAddress(currentAddress)) {
            backpointerCount++;

            // Read the current address
            ILogData d;
            try {
                log.trace("followBackpointers: readAddress[{}]", currentAddress);
                d = read(currentAddress, readStartTime);
            } catch (TrimmedException e) {
                if (getReadOptions().isIgnoreTrim()) {
                    log.warn("followBackpointers: Ignoring trimmed exception for address[{}]," +
                            " stream[{}]", currentAddress, id);
                    return !queue.isEmpty();
                } else {
                    throw e;
                }
            }

            // If it contains the stream we are interested in
            if (d.containsStream(streamId)) {
                log.trace("followBackpointers: address[{}] contains streamId[{}], apply filter", currentAddress,
                        streamId);

                // Check whether we should include the address
                filter.apply(d);

                if (!checkpoint) {
                    log.trace("followBackpointers: Adding backpointer to address[{}] to queue", currentAddress);
                    queue.add(currentAddress);
                }
            }

            boolean singleStep = true;
            // Now calculate the next address
            // Try using backpointers first

            log.trace("followBackpointers: calculate the next address");

            if (d.hasBackpointer(streamId)) {
                long tmp = d.getBackpointer(streamId);
                log.trace("followBackpointers: backpointer points to {}", tmp);
                // if backpointer is a valid log address or Address.NON_EXIST
                // (beginning of the stream), do not single step back on the log
                if (Address.isAddress(tmp) || tmp == Address.NON_EXIST) {
                    currentAddress = tmp;
                    singleStep = false;
                    if (!startingSingleStep) {
                        // A started single step period finishes here, refresh flag for next cycle.
                        log.info("followBackpointers[{}]: Found backpointer for this stream at address {}."
                                + "Stop single step downgrade.", this, currentAddress);
                        startingSingleStep = true;
                    }
                }
            }

            if (singleStep) {
                if (startingSingleStep) {
                    startingSingleStep = false;
                    log.info("followBackpointers[{}]: Found hole at address {}. Starting single step downgrade.",
                            this, currentAddress);
                }
                // backpointers failed, so we're
                // downgrading to a linear scan
                log.trace("followBackpointers[{}]: downgrading to single step, found hole at {}", this, currentAddress);
                currentAddress = currentAddress - 1;
            }
        }

        if (checkpoint) {
            queue.addAll(resolveCheckpoint(getCurrentContext()));
        }

        return !queue.isEmpty();
    }
}

