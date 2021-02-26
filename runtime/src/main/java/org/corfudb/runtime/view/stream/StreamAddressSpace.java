package org.corfudb.runtime.view.stream;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.StreamAddressRange;
import org.corfudb.runtime.view.Address;
import org.corfudb.util.Utils;
import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;

/**
 * This class represents the space of all addresses belonging to a stream.
 * <p>
 * A stream's address space is defined by:
 * 1. The collection of all addresses that belong to this stream.
 * 2. The trim mark (last trimmed address, i.e., an address that is no longer present and that was subsumed by
 * a checkpoint).
 * <p>
 * Created by annym on 03/06/2019
 */
@Slf4j
final public class StreamAddressSpace {

    // Holds the last trimmed address for this stream.
    // Note: keeping the last trimmed address is required in order to properly set the stream tail on sequencer resets
    // when a stream has been checkpointed and trimmed and there are no further updates to this stream.
    private long trimMark;

    // Holds the complete map of addresses for this stream.
    private final Roaring64NavigableMap bitmap;

    /**
     * This constructor is required to facilitate deserialization, keep it private.
     * The internal bitmap container shouldn't be exposed to external consumers.
     * @param trimMark Stream's trim mark
     * @param bitmap Stream's bitmap
     */
    private StreamAddressSpace(long trimMark, Roaring64NavigableMap bitmap) {
        this.trimMark = trimMark;
        this.bitmap = bitmap;
        this.trim(trimMark);
    }

    public StreamAddressSpace() {
        this(Address.NON_ADDRESS, Roaring64NavigableMap.bitmapOf());
    }

    public StreamAddressSpace(long trimMark, Set<Long> addresses) {
        this(trimMark, Roaring64NavigableMap.bitmapOf());
        addresses.forEach(bitmap::addLong);
        this.trim(trimMark);
    }

    public StreamAddressSpace(Set<Long> addresses) {
        this(Address.NON_ADDRESS, addresses);
    }

    /**
     * Membership test.
     * @param address checks if this map contains address
     * @return true if map contains address and false otherwise
     */
    public boolean contains(long address) {
        if (address < 0 || address <= trimMark) {
            return false;
        }
        return bitmap.contains(address);
    }

    /**
     * Merges b into a and returns a as the final result.
     *
     * @param a StreamAddressSpace to merge into
     * @param b StreamAddressSpace to merge
     * @return returns a as the merged StreamAddressSpace
     */
    public static StreamAddressSpace merge(StreamAddressSpace a, StreamAddressSpace b) {
        a.mergeFrom(b);
        return a;
    }

    // Merge another StreamAddressSpace into this instance
    private void mergeFrom(StreamAddressSpace other) {
        this.trimMark = Long.max(trimMark, other.getTrimMark());
        this.bitmap.or(other.bitmap);

        // Because the trim mark can increase after the merge another
        // trim needs to be issued
        this.trim(this.trimMark);
    }

    /**
     * Copy this stream's addresses to a set, under a given boundary (inclusive).
     *
     * @param maxGlobal maximum address (inclusive upper bound)
     */
    public NavigableSet<Long> copyAddressesToSet(final long maxGlobal) {
        NavigableSet<Long> queue = new TreeSet<>();
        this.bitmap.forEach(address -> {
            if (address <= maxGlobal) {
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
    public long getTail() {
        // If no address is present for this stream, the tail is given by the trim mark (last trimmed address)
        if (bitmap.isEmpty()) {
            return trimMark;
        }

        // The stream tail is the max address present in the stream's address map
        return bitmap.getReverseLongIterator().next();
    }

    /**
     * Add an address to this address space.
     *
     * @param address address to add.
     */
    public void addAddress(long address) {
        //TODO(Maithem): prevent adding an address smaller than the trim mark
        bitmap.addLong(address);
    }

    /**
     * Remove addresses from the stream's address map
     * and set the new trim mark (to the greatest of all addresses to remove).
     */
    private void removeAddresses(List<Long> addresses) {
        addresses.stream().forEach(bitmap::removeLong);

        // Recover allocated but unused memory
        bitmap.trim();
        // TODO(Maithem) if addresses is empty, the trim mark will not be updated
        trimMark = Collections.max(addresses);

        log.trace("removeAddresses: new trim mark set to {}", trimMark);
    }

    /**
     * Trim all addresses lower or equal to trimMark and set new trim mark.
     *
     * @param trimMark upper limit of addresses to trim
     */
    public void trim(long trimMark) {
        if (!Address.isAddress(trimMark)) {
            // If not valid address return and do not attempt to trim.
            if (log.isTraceEnabled()) {
                log.trace("trim: attempting to trim non-valid address {}", trimMark);
            }
            return;
        }

        // Note: if a negative value is passed to this API the cardinality
        // of the bitmap is returned, which would be incorrect as we would
        // be removing all addresses upon an invalid trim mark.
        long numAddressesToTrim = bitmap.rankLong(trimMark);

        if (numAddressesToTrim <= 0) {
            return;
        }

        List<Long> addressesToTrim = new ArrayList<>();
        LongIterator it = bitmap.getLongIterator();
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
    public StreamAddressSpace getAddressesInRange(StreamAddressRange range) {
        Roaring64NavigableMap addressesInRange = new Roaring64NavigableMap();
        if (range.getStart() > range.getEnd()) {
            bitmap.forEach(address -> {
                // Because our search is referenced to the stream's tail => (end < start]
                if (address > range.getEnd() && address <= range.getStart()) {
                    addressesInRange.add(address);
                }
            });
        }

        if (log.isTraceEnabled()) {
            log.trace("getAddressesInRange[{}]: address map in range [{}-{}] has a total of {} addresses.",
                    Utils.toReadableId(range.getStreamID()), range.getEnd(),
                    range.getStart(), addressesInRange.getLongCardinality());
        }

        return new StreamAddressSpace(this.trimMark, addressesInRange);
    }

    public void setTrimMark(long trimMark) {
        this.trimMark = trimMark;
    }

    public long getTrimMark() {
        return trimMark;
    }

    /**
     * Select the n - 1 largest element in this address space.
     */
    public long select(long n) {
        return bitmap.select(n);
    }

    public long size() {
        // The cardinality of the bitmap reflects the true size of the map only if all the addresses equal to and
        // less than the trim mark have been removed from the bitmap. Just setting the trim mark pointer is incorrect.
        // It will reflect a bigger set
        return bitmap.getLongCardinality();
    }

    /**
     * Creates a copy of this object
     * @return a new copy of StreamBitmap
     */
    public StreamAddressSpace copy() {
        StreamAddressSpace copy = new StreamAddressSpace();
        copy.trimMark = this.trimMark;
        this.bitmap.forEach(copy::addAddress);
        return copy;
    }

    public void serialize(DataOutput out) throws IOException {
        out.writeLong(trimMark);
        bitmap.serialize(out);
    }

    public static StreamAddressSpace deserialize(DataInputStream in) throws IOException {
        long trimMark = in.readLong();
        Roaring64NavigableMap map = new Roaring64NavigableMap();
        map.deserialize(in);
        return new StreamAddressSpace(trimMark, map);
    }

    /**
     * Converts this bitmap to an array of longs
     * @return a long array of all the non-trimmed addresses in this map
     */
    public long[] toArray() {
        return bitmap.toArray();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }

        if (this == o) {
            return true;
        }

        if (getClass() != o.getClass()) {
            return false;
        }

        StreamAddressSpace other = (StreamAddressSpace) o;
        return (this.trimMark == other.trimMark) && this.bitmap.equals(other.bitmap);
    }

    @Override
    public String toString() {
        long first = Address.NON_EXIST;
        long last = Address.NON_EXIST;
        if (!bitmap.isEmpty()) {
            first = bitmap.iterator().next();
            last =bitmap.getReverseLongIterator().next();
        }
        return String.format("[%s, %s]@%s size %s", first, last, trimMark, this.size());
    }
}