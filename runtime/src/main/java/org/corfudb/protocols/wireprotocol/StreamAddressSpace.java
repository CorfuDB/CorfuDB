package org.corfudb.protocols.wireprotocol;

import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NavigableSet;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class represents the space of addresses of a stream.
 *
 * A stream's address space is defined by:
 *       1. The collection of all addresses that belong to this stream.
 *       2. The trim mark (last trimmed address, i.e., an address that is no longer present and that was subsumed by
 *       a checkpoint).
  *
 * Created by annym on 03/06/2019
 */
public class StreamAddressSpace {

    // Holds the last trimmed address for this stream.
    // Note: keeping the last trimmed address is required in order to properly set the stream tail on sequencer resets
    // when a stream has been checkpointed and trimmed and there are no further updates to this stream.
    private AtomicLong trimMark;

    // Holds the complete map of addresses for this stream (bitmap).
    private Roaring64NavigableMap addressMap;

    public StreamAddressSpace(long trimMark, Roaring64NavigableMap addressMap) {
        this.trimMark = new AtomicLong(trimMark);
        this.addressMap = addressMap;
    }

    public synchronized Roaring64NavigableMap getAddressMap() {
        return addressMap;
    }

    /**
     * Copy this stream's addresses to a set, under a given boundary (inclusive).
     *
     * @param queue
     * @param maxGlobal maximum address (inclusive upper bound)
     */
    public synchronized void copyAddressesToSet(final NavigableSet<Long> queue, final Long maxGlobal) {
        this.addressMap.forEach(x -> {
            if (x <= maxGlobal){
                queue.add(x);
            }
        });
    }

    /**
     * Get number of addresses for this stream.
     *
     * @return number of addresses that belong to this stream.
     */
    public synchronized Long getAddressCount() {
       return this.addressMap.getLongCardinality();
    }

    /**
     * Get tail for this stream
     *
     * @return last address belonging to this stream
     */
    public synchronized Long getTail() {
        return this.addressMap.getReverseLongIterator().next();
    }

    /**
     * Add an address to this address space.
     *
     * @param address address to add.
     */
    public synchronized void addAddress(long address) {
        this.addressMap.addLong(address);
    }

    /**
     * Remove addresses from the stream's address map
     * and set the new trim mark (to the greatest of all addresses to remove).
     */
    public synchronized void removeAddresses(List<Long> addresses) {
        addresses.stream().forEach(v -> this.addressMap.removeLong(v));
        // Recover allocated but unused memory
        this.addressMap.trim();
        this.trimMark.set(Collections.max(addresses));
    }

    /**
     * Trim all addresses lower or equal to trimMark and set new trim mark.
     *
     * @param trimMark upper limit of addresses to trim
     */
    public synchronized void trim(Long trimMark) {
        long numAddressesLowerOrEqualTrimMark = this.addressMap.rankLong(trimMark);
        if (numAddressesLowerOrEqualTrimMark > 0) {
            LongIterator it = this.addressMap.getLongIterator();
            List<Long> valuesToRemove = new ArrayList<>();
            for (int i=0; i < numAddressesLowerOrEqualTrimMark; i++) {
                valuesToRemove.add(it.next());
            }
            if (!valuesToRemove.isEmpty()) {
                // Remove and set trim mark
                this.removeAddresses(valuesToRemove);
            }
        }
    }

    /**
     * Get addresses in range [start, end), where start > end.
     * @param start last address in the range (inclusive)
     * @param end first address in the range (exclusive)
     *
     * @return Bitmap with addresses in this range.
     */
    public synchronized Roaring64NavigableMap getAddressesInRange(long start, long end) {
        Roaring64NavigableMap addressesInRange = new Roaring64NavigableMap();
        if (start > end) {
            this.addressMap.forEach(address -> {
                // Because our search is referenced to the stream's tail => (end < start]
                if (address > end && address <= start) {
                    addressesInRange.add(address);
                }
            });
        }
        return addressesInRange;
    }

    public void setTrimMark(long trimMark) {
        this.trimMark.set(trimMark);
    }

    public long getTrimMark() {
       return this.trimMark.get();
    }
}