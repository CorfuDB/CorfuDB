package org.corfudb.runtime.view.stream;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.UUID;
import java.util.function.Function;

import javax.annotation.Nonnull;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.StreamAddressRange;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.TrimmedException;
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

    private static final int COUNTER_INIT_VALUE = 0;

    private static final int DIFF_CONSECUTIVE_ADDRESSES = 1;

    private int batchSize;

    private long addressCount = 0L;

    final StreamOptions options;

    /** Create a new address map stream view.
     *
     * @param runtime   The runtime to use for accessing the log.
     * @param streamId  The ID of the stream to view.
     */
    public AddressMapStreamView(final CorfuRuntime runtime,
                                final UUID streamId,
                                @Nonnull final StreamOptions options) {
        super(runtime, streamId);
        this.options = options;
        batchSize = runtime.getParameters().getStreamBatchSize();
    }

    public AddressMapStreamView(final CorfuRuntime runtime,
                                final UUID streamId) {
        this(runtime, streamId, StreamOptions.DEFAULT);
    }

    @Override
    protected ILogData removeFromQueue(NavigableSet<Long> queue) {
        // Because the discovery mechanism implemented by this class
        // does not require to read log entries in advance (requests
        // full address map in a single call), entries can be read in
        // batches whenever we have a cache miss. This allows next reads
        // to be serviced immediately, rather than reading one entry at a time.
        Long thisRead = queue.pollFirst();

        if (thisRead != null) {
            // In the case that the immediately requested address (thisRead) is not available,
            // read it along with a batch of subsequent addresses.
            List<Long> batchRead = getBatch(thisRead, queue);
            ILogData ld = readRange(thisRead, batchRead);

            if (queue == getCurrentContext().readQueue) {
                // Validate that the data entry belongs to this stream, otherwise, skip.
                // This verification protects from sequencer regression (tokens assigned in an older epoch
                // that were never written to, and reassigned on a newer epoch)
                if (ld.containsStream(this.id)) {
                    addToResolvedQueue(getCurrentContext(), thisRead, ld);
                    return ld;
                } else {
                    log.trace("getNextEntry[{}]: the data for address {} does not belong to this stream. Skip.",
                            this, thisRead);
                    return removeFromQueue(queue);
                }
            }

            return ld;
        }

        return null;
    }

    /**
     * Prepare a batch of entries to be read, including the current address to retrieve.
     *
     * @param currentRead current address to retrieve.
     * @param queue queue to get entries from.
     * @return batch of entries.
     */
    private List<Long> getBatch(long currentRead, @NonNull NavigableSet<Long> queue) {
        int counter = COUNTER_INIT_VALUE;
        List<Long> batchRead = new ArrayList<>();
        batchRead.add(currentRead);
        counter++;

        Iterator<Long> it = queue.iterator();
        while (it.hasNext() && counter < batchSize) {
            batchRead.add(it.next());
            counter++;
        }

        return batchRead;
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
     * @param checkpoint   boolean indicating if it is a checkpoint stream
     * @param maxGlobal    maximum Address until which we want to sync (is not necessarily equal to start address)
     * @return
     */
    @Override
    protected boolean discoverAddressSpace(final UUID streamId,
                                           final NavigableSet<Long> queue,
                                           final long startAddress,
                                           final long stopAddress,
                                           final Function<ILogData, BackpointerOp> filter,
                                           final boolean checkpoint,
                                           final long maxGlobal) {
        // Sanity check: startAddress must be a valid address and greater than stopAddress.
        // The startAddress refers to the max resolution we want to resolve this stream to,
        // while stopAddress refers to the lower boundary (locally resolved).
        // If startAddress is equal to stopAddress there is nothing to resolve.
        if (Address.isAddress(startAddress) && (startAddress > stopAddress)) {

            StreamAddressSpace streamAddressSpace = getStreamAddressMap(startAddress, stopAddress, streamId);

            if (checkpoint) {
                processCheckpoint(streamAddressSpace, filter, queue);
            } else {
                // Transfer discovered addresses to queue. We must limit to maxGlobal,
                // as startAddress could be ahead of maxGlobal---in case it reflects
                // the tail of the stream.
                queue.addAll(streamAddressSpace.copyAddressesToSet(maxGlobal));

                long trimMark = streamAddressSpace.getTrimMark();
                // Address maps might have been trimmed, hence not reflecting all updates to the stream
                // For this reason, in the case of a valid trim mark, we must be sure this space is
                // already resolved or loaded by a checkpoint.
                if (Address.isAddress(trimMark) && !isTrimCoveredByCheckpointOrLocalView(trimMark)) {
                    if (options.ignoreTrimmed) {
                        log.warn("getStreamAddressMap: Ignoring trimmed exception for address[{}]," +
                                " stream[{}]", streamAddressSpace.getTrimMark(), id);
                    } else {
                        String message = String.format("getStreamAddressMap[{%s}] stream has been " +
                                        "trimmed at address %s and this space is not covered by the " +
                                        "loaded checkpoint with start address %s, while accessing the " +
                                        "stream at version %s", this, streamAddressSpace.getTrimMark(),
                                getCurrentContext().checkpointSuccessStartAddr, maxGlobal);
                        log.warn(message);
                        throw new TrimmedException(message);
                    }
                }
            }
        }

        addressCount += queue.size();

        return !queue.isEmpty();
    }

    private void processCheckpoint(StreamAddressSpace streamAddressSpace, Function<ILogData, BackpointerOp> filter,
                                   NavigableSet<Long> queue) {
        List<Long> checkpointAddresses = new ArrayList<>();
        List<ILogData> checkpointData = new ArrayList<>();
        streamAddressSpace.getAddressMap().forEach(checkpointAddresses::add);

        // Read all checkpoint entries in a batch read
        try {
            checkpointData = readAll(checkpointAddresses);
        } catch (TrimmedException te) {
            if (options.ignoreTrimmed) {
                log.warn("getStreamAddressMap[{}]: Ignoring trimmed exception for address[{}]," +
                        " stream[{}]", this, checkpointAddresses, id);
            } else {
                throw te;
            }
        }

        // Sort in reverse order (based on address) to process
        Collections.sort(checkpointData, Comparator.comparing(ILogData::getGlobalAddress).reversed());

        for (ILogData data: checkpointData) {
            BackpointerOp op = filter.apply(data);
            if (op == BackpointerOp.INCLUDE || op == BackpointerOp.INCLUDE_STOP) {
                log.trace("getStreamAddressMap[{}]: Adding checkpoint address[{}] to queue",
                        this, data.getGlobalAddress());
                queue.add(data.getGlobalAddress());
                // Check if we need to stop
                if (op == BackpointerOp.INCLUDE_STOP) {
                    break;
                }
            }
        }
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
                return new StreamAddressSpace(Address.NON_ADDRESS, Roaring64NavigableMap.bitmapOf(startAddress));
            }

            log.trace("getStreamAddressMap[{}]: request stream address space between {} and {}.",
                        streamId, startAddress, stopAddress);
            return runtime.getSequencerView()
                    .getStreamAddressSpace(new StreamAddressRange(streamId, startAddress, stopAddress));
        }

        // Start and stop address are consecutive addresses, no need to request the address map for this stream,
        // the only address to include is startAddress (stopAddress is already resolved - not included in the lookup).
        return new StreamAddressSpace(Address.NON_ADDRESS, Roaring64NavigableMap.bitmapOf(startAddress));
    }

    private boolean isAddressToBackpointerResolved(long startAddress, UUID streamId) {
        ILogData d;
        try {
            d = read(startAddress);
        } catch (TrimmedException e) {
            if (options.ignoreTrimmed) {
                log.warn("getStreamAddressMap[{}]: Ignoring trimmed exception for address[{}]," +
                        " stream[{}]", this, startAddress, streamId);
                return false;
            } else {
                throw e;
            }
        }

        if (!runtime.getParameters().isBackpointersDisabled() && d.hasBackpointer(streamId)) {
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

    /**
     * Verify that a trim is covered either by a loaded checkpoint or by the locally resolved addresses.
     *
     * Because address maps might have been trimmed, the trim mark is a 'marker' of addresses that were
     * removed from the map (historical) and that should be covered by a checkpoint.
     *
     * @param trimMark
     * @return TRUE, trim mark contained in checkpoint, FALSE, otherwise.
     */
    private boolean isTrimCoveredByCheckpointOrLocalView(long trimMark) {
        return isTrimResolvedLocally(trimMark) ||
                isTrimCoveredByCheckpoint(trimMark);
    }

    private boolean isTrimResolvedLocally(long trimMark) {
        return getCurrentContext().checkpointSuccessId == null
                && getCurrentContext().resolvedQueue.contains(trimMark);
    }

    private boolean isTrimCoveredByCheckpoint(long trimMark) {
        return getCurrentContext().checkpointSuccessId != null &&
                getCurrentContext().checkpointSuccessStartAddr >= trimMark;
    }

    @Override
    public long getTotalUpdates() {
        return addressCount;
    }
}

