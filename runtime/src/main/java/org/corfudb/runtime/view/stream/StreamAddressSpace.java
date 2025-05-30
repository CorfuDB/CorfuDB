package org.corfudb.runtime.view.stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Longs;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.StreamAddressRange;
import org.corfudb.runtime.view.Address;
import org.corfudb.util.Utils;
import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;
import java.util.function.LongConsumer;

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
    private Roaring64NavigableMap bitmap;

    // True if caching should be used in internal StreamAddressSpace data structures.
    // Note: this parameter is not serialized, nor used to determine equality.
    private final boolean enableCaching;

    /**
     * This constructor is required to facilitate deserialization, keep it private.
     * The internal bitmap container shouldn't be exposed to external consumers.
     * @param trimMark Stream's trim mark
     * @param bitmap Stream's bitmap
     * @param enableCaching True if and only if caching is enabled in the stream's bitmap
     */
    private StreamAddressSpace(long trimMark, Roaring64NavigableMap bitmap, boolean enableCaching) {
        this.trimMark = trimMark;
        this.bitmap = bitmap;
        this.enableCaching = enableCaching;
    }

    public StreamAddressSpace() {
        // Note: caching of internal bitmap structures is disabled by default here.
        this(false);
    }

    public StreamAddressSpace(boolean enableCaching) {
        this(Address.NON_ADDRESS, new Roaring64NavigableMap(false, enableCaching), enableCaching);
    }

    public StreamAddressSpace(long trimMark, Set<Long> addresses) {
        this(trimMark, Roaring64NavigableMap.bitmapOf(Longs.toArray(addresses)), true);
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
        long newTrimMark = Long.max(a.getTrimMark(), b.getTrimMark());
        a.bitmap.or(b.bitmap);

        // Because the trim mark can increase after the merge another
        // trim needs to be issued
        a.trim(newTrimMark);
        // Since the trim method only sets the trimMark if an address was trimmed, we need to force
        // the new trim mark for cases where the trim mark increases, but there isn't any addresses
        // to trim.
        a.trimMark = newTrimMark;
        return a;
    }

    /**
     * Call a long consumer on range [trimMark + 1, max].
     *
     * @param max maximum address (inclusive upper bound)
     */
    public void forEachUpTo(final long max, LongConsumer consumer) {
        LongIterator iterator = this.bitmap.getLongIterator();
        while (iterator.hasNext()) {
            long current = iterator.next();
            if (current > trimMark && current <= max) {
                consumer.accept(current);
            } else {
                break;
            }
        }
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
     * Get the first address in this bitmap.
     * @return first address in the bitmap if its not empty, otherwise returns the trim mark
     */
    public long getFirst() {
        if (bitmap.isEmpty()) {
            return trimMark;
        }

        return bitmap.getLongIterator().next();
    }

    /**
     * Add an address to this address space.
     * By default, the 'initialize' flag is set to false.
     *
     * @param address address to add.
     */
    public void addAddress(long address) {
        addAddress(address, false);
    }

    /**
     * Add an address to this address space.
     *
     * @param address address to add.
     * @param initialize if it's during StreamAddressSpace initialization.
     */
    public void addAddress(long address, boolean initialize) {
        // Temporarily log error on trim mark comparison, as throwing an exception
        // unveils an underlying issue in the reset workflow (wipe data + data transfer in colibri)
        // During initialization all addresses below the latest checkpoint will be regarded as
        // trimmed, so don't log errors to avoid log spew
        if (!initialize && address <= this.trimMark) {
            log.error("IllegalArgumentException :: Address={}, TrimMark={}", address, this.trimMark);
        }

        if (Address.nonAddress(address)) {
            throw new IllegalArgumentException("Address=" + address + " TrimMark=" + this.trimMark);
        }
        bitmap.addLong(address);
    }

    /**
     * Trim all addresses lower or equal to trimMark and set new trim mark.
     *
     * @param newTrimMark upper limit of addresses to trim
     */
    public void trim(long newTrimMark) {
        if (Address.nonAddress(newTrimMark)) {
            // If not valid address return and do not attempt to trim.
            log.trace("trim: attempting to trim non-valid address {}", newTrimMark);
            return;
        }

        // If the new trim mark is greater than the current trim mark it needs to be
        // updated regardless whether it will result in addresses being removed or not.
        // For example, the bitmap can be empty, while the trim mark keeps increasing
        if (newTrimMark <= this.trimMark) {
            return;
        }

        // At this point point the trim mark is positive and we need to compute
        // the rank. The rank will indicate how many addresses need to be trimmed
        long rank = bitmap.rankLong(newTrimMark);

        if (rank <= 0) {
            return;
        }

        // Keep the same caching parameters as the parent Roaring64NavigableMap.
        Roaring64NavigableMap trimmedBitmap = new Roaring64NavigableMap(false, enableCaching);
        LongIterator iterator = this.bitmap.getReverseLongIterator();
        while (iterator.hasNext()) {
            long current = iterator.next();
            if (current <= newTrimMark) {
                this.trimMark = current;
                break;
            }

            trimmedBitmap.addLong(current);
        }

        this.bitmap = trimmedBitmap;

        if (log.isTraceEnabled()) {
            log.trace("trim: trimMark={} streamTrimMark {} size {} addresses={}", newTrimMark,
                    this.trimMark,
                    this.bitmap.getLongCardinality(),
                    this.bitmap.toArray());
        }
    }

    /**
     * Add range helper method that validates the range before adding it
     * to the bitmap, this is to protect against this bug: https://github.com/RoaringBitmap/RoaringBitmap/pull/445
     * @param toAdd bitmap to add the range to
     * @param start start address (inclusive)
     * @param end end address (exclusive)
     */

    /**
     * Get addresses in range (end, start], where start > end.
     *
     * @return Bitmap with addresses in this range.
     */
    public StreamAddressSpace getAddressesInRange(StreamAddressRange range) {
        // Keep the same caching parameters as the parent Roaring64NavigableMap.
        Roaring64NavigableMap addressesInRange = new Roaring64NavigableMap(false, enableCaching);

        if (range.getStart() <= range.getEnd()) {
            throw new IllegalArgumentException("Invalid range (" + range.getEnd() + ", " + range.getStart() + "]");
        }

        LongIterator iter = this.bitmap.getReverseLongIterator();
        while (iter.hasNext()) {
            long curr = iter.next();
            // (end, start]
            if (curr <= range.getEnd()) {
                break;
            } else if (curr <= range.getStart()) {
                addressesInRange.addLong(curr);
            }
        }

        if (log.isTraceEnabled()) {
            log.trace("getAddressesInRange[{}]: address map in range [{}-{}] has a total of {} addresses.",
                    Utils.toReadableId(range.getStreamID()), range.getEnd(),
                    range.getStart(), addressesInRange.getLongCardinality());
        }

        return new StreamAddressSpace(this.trimMark, addressesInRange, enableCaching);
    }

    public void setTrimMark(long trimMark) {
        this.trimMark = trimMark;
        // TODO(Maithem): This method should be removed. When the trimMark is set here, then trim is called
        // the trim call will return without trimming.
    }

    public long getTrimMark() {
        return trimMark;
    }

    /**
     * Select the n - 1 largest element in this address space.
     */
    public long select(long n) {
        if (n < 0) {
            throw new IllegalArgumentException("n=" + n + " size=" +  bitmap.getLongCardinality());
        }
        return bitmap.select(n);
    }

    public long size() {
        // The cardinality of the bitmap reflects the true size of the map only if all the addresses equal to and
        // less than the trim mark have been removed from the bitmap. Just setting the trim mark pointer is incorrect.
        // It will reflect a bigger set
        return bitmap.getLongCardinality();
    }

    /**
     * Given an address, computes the largest address A in the bitmap,
     * such that A is less than or equal to the given address.
     * @param address The address to perform the floor query on.
     * @return The largest address in the bitmap that is less than or
     * equal to the given address.
     */
    public long floor(long address) {
        final long rank = bitmap.rankLong(address);

        if (rank == 0) {
            return Address.NON_ADDRESS;
        }

        return bitmap.select(rank - 1);
    }

    /**
     * Creates a copy of this object
     * @return a new copy of StreamBitmap
     */
    public StreamAddressSpace copy() {
        // Keep the same caching parameters as the parent Roaring64NavigableMap.
        StreamAddressSpace copy = new StreamAddressSpace(enableCaching);
        copy.trimMark = this.trimMark;
        this.bitmap.forEach(copy::addAddress);
        return copy;
    }

    /**
     * Serialize this StreamAddressSpace into DataOutput
     * @param out DataOutput to serialize to
     * @throws IOException
     */
    public void serialize(DataOutput out) throws IOException {
        out.writeLong(trimMark);
        bitmap.serialize(out);
    }

    /**
     * Deserialize: create a new StreamAddressSpace from DataInputStream
     * @param in input stream to read from
     * @return StreamAddressSpace
     * @throws IOException
     */
    public static StreamAddressSpace deserialize(DataInputStream in) throws IOException {
        long trimMark = in.readLong();
        Roaring64NavigableMap map = new Roaring64NavigableMap(false, false);
        map.deserialize(in);
        return new StreamAddressSpace(trimMark, map, true);
    }

    /**
     * Converts this bitmap to an array of longs
     * @return a long array of all the non-trimmed addresses in this map
     */
    public long[] toArray() {
        return bitmap.toArray();
    }

    @VisibleForTesting
    Roaring64NavigableMap getBitmap() {
        return bitmap;
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
            first = getFirst();
            last = getTail();
        }
        return String.format("[%s, %s]@%s size %s", first, last, trimMark, this.size());
    }
}
