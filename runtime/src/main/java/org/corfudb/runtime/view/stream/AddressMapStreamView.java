package org.corfudb.runtime.view.stream;

import java.util.NavigableSet;
import java.util.UUID;
import java.util.function.Function;

import javax.annotation.Nonnull;

import lombok.extern.slf4j.Slf4j;

import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.StreamAddressRange;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.StreamOptions;
import org.roaringbitmap.longlong.Roaring64NavigableMap;


/** A view of a stream implemented with address maps.
 *
 * A stream's address space is discovered by requesting to the
 * sequencer, the map of all addresses belonging to this stream.
 *
 * Notice that, unlike the BackpointerStreamView, addresses are discovered
 * in a single call to the sequencer, therefore, entries are not read
 * in advance. For this reason, when iterating over the stream (getNextEntry)
 * we perform batch reads, which significantly reduces the number of RPCs to
 * Log Units, as opposed to reading one entry at a time. Also, single stepping
 * in the presence of holes is no longer a need as address space is determined
 * by the map provided by the sequencer.
 *
 * Created by annym on 04/25/19.
 */
@Slf4j
public class AddressMapStreamView extends AbstractQueuedStreamView {

    private static final int DIFF_CONSECUTIVE_ADDRESSES = 1;

    private long addressCount = 0L;

    /** Create a new address map stream view.
     *
     * @param runtime   The runtime to use for accessing the log.
     * @param streamId  The ID of the stream to view.
     */
    public AddressMapStreamView(final CorfuRuntime runtime,
                                final UUID streamId,
                                @Nonnull final StreamOptions options) {
        super(runtime, streamId, options);
    }

    public AddressMapStreamView(final CorfuRuntime runtime,
                                final UUID streamId) {
        this(runtime, streamId, StreamOptions.DEFAULT);
    }

    @Override
    protected ILogData removeFromQueue(NavigableSet<Long> queue, long snapshot) {
        boolean readNext;
        ILogData ld = null;
        Long currentRead;

        do {
            currentRead = queue.pollFirst();

            if (currentRead == null) {
                // no more entries, empty queue.
                return null;
            }

            // Because the discovery mechanism implemented by this class
            // does not require to read log entries in advance (only requests
            // the stream's full address map, without reading the actual data), entries can be read in
            // batches whenever we have a cache miss. This allows next reads
            // to be serviced immediately, rather than reading one entry at a time.
            ld = read(currentRead, queue, snapshot);

            if (queue == getCurrentContext().readQueue && ld != null) {
                // Validate that the data entry belongs to this stream, otherwise, skip.
                // This verification protects from sequencer regression (tokens assigned in an older epoch
                // that were never written to, and reassigned on a newer epoch)
                if (ld.containsStream(this.id) && ld.getType() == DataType.DATA) {
                    addToResolvedQueue(getCurrentContext(), currentRead);
                    readNext = false;
                } else if (ld.isCompacted() && ld.containsStream(this.id)) {
                    log.trace("getNextEntry[{}]: the data for address {} is compacted. Skip.", this, currentRead);
                    readNext = true;
                } else {
                    log.trace("getNextEntry[{}]: the data for address {} does not belong to this stream. Skip.",
                            this, currentRead);
                    // Invalid entry (does not belong to this stream). Read next.
                    readNext = true;
                }
            } else {
                readNext = false;
            }
        } while (readNext);

        return ld;
    }

    /**
     * Retrieve this stream's address map, i.e., a map of all addresses corresponding to this stream between
     * (stop address, start address] and return a boolean indicating if addresses were found in this range.
     *
     * @param streamId stream's ID, it is required because this same method can be used for
     *                 discovering addresses from the checkpoint stream.
     * @param queue    queue to insert discovered addresses in the given range
     * @param startAddress range start address (inclusive)
     * @param stopAddress  range end address (exclusive and lower than start address)
     * @param filter       function to filter entries
     * @param maxGlobal    maximum Address until which we want to sync (is not necessarily equal to start address)
     * @return
     */
    @Override
    protected boolean discoverAddressSpace(final UUID streamId,
                                           final NavigableSet<Long> queue,
                                           final long startAddress,
                                           final long stopAddress,
                                           final Function<ILogData, Boolean> filter,
                                           final long maxGlobal) {
        // Sanity check: startAddress must be a valid address and greater than stopAddress.
        // The startAddress refers to the max resolution we want to resolve this stream to,
        // while stopAddress refers to the lower boundary (locally resolved).
        // If startAddress is equal to stopAddress there is nothing to resolve.
        if (Address.isAddress(startAddress) && (startAddress > stopAddress)) {

            StreamAddressSpace streamAddressSpace = getStreamAddressMap(startAddress, stopAddress, streamId);

            // Transfer discovered addresses to queue. We must limit to maxGlobal,
            // as startAddress could be ahead of maxGlobal---in case it reflects
            // the tail of the stream.
            queue.addAll(streamAddressSpace.copyAddressesToSet(maxGlobal));

        }

        addressCount += queue.size();
        return !queue.isEmpty();
    }

    private StreamAddressSpace getStreamAddressMap(long startAddress, long stopAddress, UUID streamId) {
        // Case non-consecutive addresses
        if (startAddress - stopAddress > DIFF_CONSECUTIVE_ADDRESSES) {
            log.trace("getStreamAddressMap[{}] get addresses from {} to {}", this, startAddress, stopAddress);
            // This step is an optimization.
            // Attempt to read the last entry and verify its backpointer,
            // if this address is already resolved locally do not request the stream map to the sequencer as new
            // updates are not required to be synced (this benefits single runtime writers).
            if(isAddressToBackpointerResolved(startAddress, streamId)) {
                return new StreamAddressSpace(Roaring64NavigableMap.bitmapOf(startAddress));
            }

            log.trace("getStreamAddressMap[{}]: request stream address space between {} and {}.",
                        streamId, startAddress, stopAddress);
            return runtime.getSequencerView()
                    .getStreamAddressSpace(new StreamAddressRange(streamId, startAddress, stopAddress));
        }

        // Start and stop address are consecutive addresses, no need to request the address map for this stream,
        // the only address to include is startAddress (stopAddress is already resolved - not included in the lookup).
        return new StreamAddressSpace(Roaring64NavigableMap.bitmapOf(startAddress));
    }

    private boolean isAddressToBackpointerResolved(long startAddress, UUID streamId) {
        ILogData d = read(startAddress, Long.MAX_VALUE);

        if (d.hasBackpointer(streamId)) {
            long previousAddress = d.getBackpointer(streamId);
            log.trace("getStreamAddressMap[{}]: backpointer for {} points to {}",
                    streamId, startAddress, previousAddress);
            // if backpointer is a valid log address or Address.NON_EXIST (beginning of the stream)
            if ((Address.isAddress(previousAddress) &&
                    getCurrentContext().resolvedQueue.contains(previousAddress))
                    || previousAddress == Address.NON_EXIST) {
                log.trace("getStreamAddressMap[{}]: backpointer {} is locally present, do not request " +
                        "stream address map.", streamId, previousAddress);
                return true;
            }
        }

        return false;
    }

    @Override
    public long getTotalUpdates() {
        return addressCount;
    }
}

