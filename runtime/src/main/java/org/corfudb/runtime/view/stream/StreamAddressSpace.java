package org.corfudb.runtime.view.stream;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.StreamAddressRange;
import org.corfudb.runtime.view.Address;
import org.corfudb.util.Utils;
import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;

/**
 * This class represents the space of all addresses belonging to a stream.
 *
 * A stream's address space is defined by:
 *       1. The collection of all addresses that belong to this stream.
 *       2. The trim mark (last trimmed address, i.e., an address that is no longer present and that was subsumed by
 *       a checkpoint).
  *
 * Created by annym on 03/06/2019
 */
@Slf4j
public class StreamAddressSpace {

    private static final int NO_ADDRESSES = 0;

    // Holds the last trimmed address for this stream.
    // Note: keeping the last trimmed address is required in order to properly set the stream tail on sequencer resets
    // when a stream has been checkpointed and trimmed and there are no further updates to this stream.
    private long trimMark;

    // Holds the complete map of addresses for this stream.
    private final Roaring64NavigableMap addressMap;

    public StreamAddressSpace(long trimMark, Roaring64NavigableMap addressMap) {
        this.trimMark = trimMark;
        this.addressMap = addressMap;
    }

    public Roaring64NavigableMap getAddressMap() {
        return addressMap;
    }

    /**
     * Copy this stream's addresses to a set, under a given boundary (inclusive).
     *
     * @param maxGlobal maximum address (inclusive upper bound)
     */
    public NavigableSet<Long> copyAddressesToSet(final Long maxGlobal) {
        NavigableSet<Long> queue = new TreeSet<>();
        this.addressMap.forEach(address -> {
            if (address <= maxGlobal){
                queue.add(address);
            }
        });

        return queue;
    }

    /**
     * Get tail for this stream.
     *
     * @return last address belonging to this stream
     */
    public Long getTail() {
        // If no address is present for this stream, the tail is given by the trim mark (last trimmed address)
        if (addressMap.getLongCardinality() == NO_ADDRESSES) {
            return trimMark;
        }

        // The stream tail is the max address present in the stream's address map
        return addressMap.getReverseLongIterator().next();
    }

    /**
     * Add an address to this address space.
     *
     * @param address address to add.
     */
    public void addAddress(long address) {
        addressMap.addLong(address);
    }

    /**
     * Remove addresses from the stream's address map
     * and set the new trim mark (to the greatest of all addresses to remove).
     */
    public void removeAddresses(List<Long> addresses) {
        addresses.forEach(addressMap::removeLong);

        // Recover allocated but unused memory
        addressMap.trim();
        trimMark = Collections.max(addresses);

        log.trace("removeAddresses: new trim mark set to {}", trimMark);
    }

    /**
     * Trim all addresses lower or equal to trimMark and set new trim mark.
     *
     * @param trimMark upper limit of addresses to trim
     */
    public void trim(Long trimMark) {
        if (!Address.isAddress(trimMark)) {
            // If not valid address return and do not attempt to trim.
            return;
        }

        // Note: if a negative value is passed to this API the cardinality
        // of the bitmap is returned, which would be incorrect as we would
        // be removing all addresses upon an invalid trim mark.
        long numAddressesToTrim = addressMap.rankLong(trimMark);

        if (numAddressesToTrim <= NO_ADDRESSES) {
            return;
        }

        List<Long> addressesToTrim = new ArrayList<>();
        LongIterator it = addressMap.getLongIterator();
        for (int i = 0; i < numAddressesToTrim; i++) {
            addressesToTrim.add(it.next());
        }

        log.trace("trim: Remove {} addresses for trim mark {}", addressesToTrim.size(), trimMark);

        // Remove and set trim mark
        if (!addressesToTrim.isEmpty()) {
            removeAddresses(addressesToTrim);
        }
    }

    /**
     * Get addresses in range (end, start], where start > end.
     *
     * @return Bitmap with addresses in this range.
     */
    public Roaring64NavigableMap getAddressesInRange(StreamAddressRange range) {
        Roaring64NavigableMap addressesInRange = new Roaring64NavigableMap();
        if (range.getStart() > range.getEnd()) {
            addressMap.forEach(address -> {
                // Because our search is referenced to the stream's tail => (end < start]
                if (address > range.getEnd() && address <= range.getStart()) {
                    addressesInRange.add(address);
                }
            });
        }

        log.trace("getAddressesInRange[{}]: address map in range [{}-{}] has a total of {} addresses.",
                Utils.toReadableId(range.getStreamID()), range.getEnd(),
                range.getStart(), addressesInRange.getLongCardinality());

        return addressesInRange;
    }

    public void setTrimMark(long trimMark) {
        this.trimMark = trimMark;
    }

    public long getTrimMark() {
       return trimMark;
    }
    
    public long getLowestAddress() {
        if (addressMap.isEmpty()) {
            return Address.NON_EXIST;
        }

        return addressMap.iterator().next();
    }

    public long getHighestAddress() {
        if (addressMap.isEmpty()) {
            return Address.NON_EXIST;
        }

        return addressMap.getReverseLongIterator().next();
    }

    @Override
    public String toString() {
        return String.format("[%s, %s]@%s", getLowestAddress(), getHighestAddress(), trimMark);
    }
}