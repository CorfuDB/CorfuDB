package org.corfudb.runtime.view.stream;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.exceptions.SerializerException;
import org.corfudb.runtime.view.Address;
import org.roaringbitmap.longlong.LongConsumer;
import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64Bitmap;

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

    // Holds the last trimmed address for this stream.
    // Note: keeping the last trimmed address is required in order to properly set the stream tail on sequencer resets
    // when a stream has been checkpointed and trimmed and there are no further updates to this stream.
    private Long trimMark;

    // Holds the complete map of addresses for this stream.
    private final Roaring64Bitmap bitmap;

    /**
     * Constructs a StreamAddressSpace from a trim and some addresses
     * @param trimMark trim mark for this stream address space
     * @param addresses addresses to initialize this address space with
     */
    public StreamAddressSpace(Long trimMark, Long ... addresses) {
        this(new Roaring64Bitmap(), trimMark, addresses);
    }

    /**
     * Constructs an empty StreamAddressSpace
     */
    public StreamAddressSpace() {
        this(new Roaring64Bitmap(), Address.NON_ADDRESS);
    }

    private StreamAddressSpace(Roaring64Bitmap bitmap, Long trimMark, Long ... addresses) {
        this.trimMark = trimMark;
        this.bitmap = bitmap;

        for (long address : addresses) {
            if (address > trimMark) {
                bitmap.addLong(address);
            }
        }
    }

    /**
     * Returns the internal bitset of this StreamAddressSpace. Keep this
     * method private to prevent direct external usage.
     * @return
     */
    private Roaring64Bitmap getBitmap() {
        return bitmap;
    }

    /**
     * Creates a copy of this object
     * @return a new copy of StreamAddressSpace
     */
    public StreamAddressSpace copy() {
        Roaring64Bitmap copy = bitmap.clone();
        return new StreamAddressSpace(copy, this.trimMark);
    }

    /**
     * Returns the number of addresses in this StreamAddressSpace
     */
    public int size() {
        return Math.toIntExact(bitmap.getLongCardinality());
    }

    public void serialize(ByteBuf buf) {
        buf.writeLong(trimMark);
        try (ByteBufOutputStream outputStream = new ByteBufOutputStream(buf);
             DataOutputStream dataOutputStream =  new DataOutputStream(outputStream)){
            bitmap.serialize(dataOutputStream);
        } catch (IOException ioe) {
            throw new SerializerException("Unexpected error while serializing to a byte array");
        }
    }

    //TODO(Maithem) runoptimize on trim?

    public static StreamAddressSpace deserialize(ByteBuf buf) {
        long trimMark = buf.readLong();
        Roaring64Bitmap map = new Roaring64Bitmap();
        try (ByteBufInputStream inputStream = new ByteBufInputStream(buf)) {
            map.deserialize(inputStream);
            return new StreamAddressSpace(map, trimMark);
        } catch (IOException ioe) {
            throw new SerializerException("Exception when attempting to " +
                    "deserialize stream address space.", ioe);
        }
    }

    /**
     * Merges b into a and returns a as the final result.
     * @param a StreamAddressSpace to merge into
     * @param b StreamAddressSpace to merge
     * @return returns a as the merged StreamAddressSpace
     */
    public static StreamAddressSpace merge(StreamAddressSpace a, StreamAddressSpace b) {
        a.mergeFrom(b);
        return a;
    }

    /**
     * Merge another StreamAddressSpace into this instance
     * @param other the other StreamAddressSpace to merge into this
     */
    private void mergeFrom(StreamAddressSpace other) {
        this.trimMark = Long.max(trimMark, other.getTrimMark());
        this.bitmap.or(other.getBitmap());

        // Because the trim mark can increase after the merge another
        // trim needs to be issued
        this.trim(this.trimMark);
    }

    /**
     * Visit all values in the bitmap and pass them to the consumer.
     * @param longConsumer consumer
     */
    public void forEach(final LongConsumer longConsumer) {
        bitmap.forEach(longConsumer);
    }

    /**
     * Copy this stream's addresses to a set, under a given boundary (inclusive).
     *
     * @param maxGlobal maximum address (inclusive upper bound)
     */
    public NavigableSet<Long> getAddressesUpTo(final Long maxGlobal) {
        NavigableSet<Long> subset = new TreeSet<>();
        bitmap.forEach(address -> {
            if (address <= maxGlobal){
                subset.add(address);
            }
        });

        return subset;
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
    public void add(Long address) {
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
    public StreamAddressSpace getRange(final Long start, final Long end) {
        final StreamAddressSpace result = new StreamAddressSpace();
        result.trimMark = this.trimMark;
        if (start > end) {
            return result;
        }

        //TODO(Maithem): replace this with bitset logic
        bitmap.forEach(address -> {
            if (start <= address && address <= end) {
                result.add(start);
            }
        });


        return result;
    }

    public void setTrimMark(Long trimMark) {
        this.trimMark = trimMark;
    }

    public boolean contains(Long address) {
        return bitmap.contains(address);
    }

    public long getTrimMark() {
       return trimMark;
    }
    
    public long getLowestAddress() {
        if (bitmap.isEmpty()) {
            return Address.NON_EXIST;
        }

        return bitmap.iterator().next();
    }

    public long getHighestAddress() {
        if (bitmap.isEmpty()) {
            return Address.NON_EXIST;
        }

        return bitmap.getReverseLongIterator().next();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }

        StreamAddressSpace other = (StreamAddressSpace) o;

        return trimMark == other.trimMark && bitmap.equals(other.bitmap);
    }

    @Override
    public String toString() {
        return String.format("[%s, %s]@%s", getLowestAddress(), getHighestAddress(), trimMark);
    }
}