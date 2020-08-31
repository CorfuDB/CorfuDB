package org.corfudb.runtime.view.stream;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.NavigableSet;
import java.util.TreeSet;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.exceptions.SerializerException;
import org.corfudb.runtime.view.Address;
import org.roaringbitmap.longlong.LongConsumer;
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
public class StreamBitmap {

    // Holds the last trimmed address for this stream.
    // Note: keeping the last trimmed address is required in order to properly set the stream tail on sequencer resets
    // when a stream has been checkpointed and trimmed and there are no further updates to this stream.
    private long trimMark;

    // Lowest address in this bitmap
    private long lowestAddress;

    // Largest address in this bitmap
    private long highestAddress;

    // Holds the complete map of addresses for this stream.
    private Roaring64Bitmap bitmap;

    /**
     * Constructs a StreamBitmap from a trim and some addresses
     * @param trimMark trim mark for this stream address space
     * @param addresses addresses to initialize this address space with
     */
    public StreamBitmap(Long trimMark, Long ... addresses) {
        this(new Roaring64Bitmap(), trimMark, addresses);
    }

    /**
     * Constructs an empty StreamBitmap
     */
    public StreamBitmap() {
        this(new Roaring64Bitmap(), Address.NON_ADDRESS);
    }

    private StreamBitmap(Roaring64Bitmap bitmap, Long trimMark, Long ... addresses) {
        this.trimMark = trimMark;
        this.lowestAddress = Address.NON_ADDRESS;
        this.highestAddress = Address.NON_ADDRESS;
        this.bitmap = bitmap;

        for (long address : addresses) {
            if (address > trimMark) {
                bitmap.addLong(address);
            }
        }

        // Since bitmap might not be empty, the highestAddress/lowestAddress have to be updated
        // to reflect the final bitmap
        if (!bitmap.isEmpty()) {
            this.lowestAddress = bitmap.getLongIterator().next();
            this.highestAddress = bitmap.getReverseLongIterator().next();
        }
    }

    /**
     * Returns the internal bitset of this StreamBitmap. Keep this
     * method private to prevent direct external usage.
     * @return
     */
    private Roaring64Bitmap getBitmap() {
        return bitmap;
    }

    /**
     * Creates a copy of this object
     * @return a new copy of StreamBitmap
     */
    public StreamBitmap copy() {
        Roaring64Bitmap copy = bitmap.clone();
        return new StreamBitmap(copy, this.trimMark);
    }

    /**
     * Returns the number of addresses in this StreamBitmap
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

    public static StreamBitmap deserialize(ByteBuf buf) {
        long trimMark = buf.readLong();
        Roaring64Bitmap map = new Roaring64Bitmap();
        try (ByteBufInputStream inputStream = new ByteBufInputStream(buf)) {
            map.deserialize(inputStream);
            return new StreamBitmap(map, trimMark);
        } catch (IOException ioe) {
            throw new SerializerException("Exception when attempting to " +
                    "deserialize stream address space.", ioe);
        }
    }

    /**
     * Merges b into a and returns a as the final result.
     * @param a StreamBitmap to merge into
     * @param b StreamBitmap to merge
     * @return returns a as the merged StreamBitmap
     */
    public static StreamBitmap merge(StreamBitmap a, StreamBitmap b) {
        a.mergeFrom(b);
        return a;
    }

    /**
     * Merge another StreamBitmap into this instance
     * @param other the other StreamBitmap to merge into this
     */
    private void mergeFrom(StreamBitmap other) {
        this.trimMark = Long.max(trimMark, other.getTrimMark());
        this.lowestAddress = Long.min(lowestAddress, other.lowestAddress);
        this.highestAddress = Long.max(highestAddress, other.highestAddress);
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
        // We need to adjust the subRange querie's highestAddress because maxGlobal can be larger than
        // the actual highestAddress
        long adjustedMax = Math.min(maxGlobal, getHighestAddress());
        final Roaring64Bitmap subRange = getSubRange(getLowestAddress(), adjustedMax);
        NavigableSet<Long> result = new TreeSet<>();
        subRange.forEach(result::add);
        return result;
    }

    /**
     * Get tail for this stream.
     *
     * @return last address belonging to this stream
     */
    public long getTail() {
        return getHighestAddress();
    }

    /**
     * Add an address to this address space.
     *
     * @param address address to add.
     */
    public void add(Long address) {
        bitmap.addLong(address);
        highestAddress = Math.max(address, highestAddress);
    }

    /**
     * Trim all addresses lower or equal to trimMark and set new trim mark.
     *
     * @param trimMark upper limit of addresses to trim
     */
    public void trim(Long trimMark) {
        if (trimMark < lowestAddress) {
            // Nothing to trim
            return;
        }

        Roaring64Bitmap trimmedBitmap = new Roaring64Bitmap();
        trimmedBitmap.add(Math.addExact(trimMark, 1), Math.addExact(highestAddress, 1));
        trimmedBitmap.and(bitmap);
        bitmap = trimmedBitmap;
        // Update lowestAddress/highestAddress accordingly
    }

    /**
     * Creates a new Roaring64Bitmap with the subrange of this bitmap
     * @param start start of range
     * @param end end of range
     * @return a new Roaring64Bitmap that contains the range
     */
    private Roaring64Bitmap getSubRange(Long start, Long end) {
        Roaring64Bitmap subrange = new Roaring64Bitmap();
        subrange.add(start, end);
        subrange.and(bitmap);
        return subrange;
    }

    /**
     * Get addresses in range (end, start], where start > end.
     *
     * @return Bitmap with addresses in this range.
     */
    public StreamBitmap getRange(final Long start, final Long end) {
        if (start > end) {
            return new StreamBitmap();
        }
        return new StreamBitmap(getSubRange(start, end), trimMark);
    }

    public boolean contains(Long address) {
        return bitmap.contains(address);
    }

    public long getTrimMark() {
       return trimMark;
    }
    
    public long getLowestAddress() {
        return lowestAddress;
    }

    public long getHighestAddress() {
        return highestAddress;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }

        StreamBitmap other = (StreamBitmap) o;

        return trimMark == other.trimMark && bitmap.equals(other.bitmap);
    }

    @Override
    public String toString() {
        return String.format("[%s, %s]@%s", getLowestAddress(), getHighestAddress(), trimMark);
    }
}