package org.corfudb.runtime.view.stream;

import com.google.common.collect.ImmutableSet;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import org.corfudb.protocols.wireprotocol.StreamAddressRange;
import org.corfudb.runtime.view.Address;
import org.junit.Test;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.io.DataInputStream;
import java.io.DataOutput;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.LongStream;

import static org.assertj.core.api.Java6Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SuppressWarnings("checkstyle:magicnumber")
public class StreamAddressSpaceTest {

    @Test
    public void testStreamAddressSpaceMerge() {

        StreamAddressSpace streamA = new StreamAddressSpace();

        final int numStreamAEntries = 100;
        LongStream.range(0, numStreamAEntries).forEach(streamA::addAddress);

        assertThat(streamA.getTrimMark()).isEqualTo(Address.NON_ADDRESS);
        assertThat(streamA.getTail()).isEqualTo(numStreamAEntries - 1);

        // need to take higher trim mark?

        StreamAddressSpace streamB = new StreamAddressSpace();
        final int numStreamBEntries = 130;
        LongStream.range(0, numStreamBEntries).forEach(streamB::addAddress);
        final long streamBTrimMark = 40;
        streamB.trim(streamBTrimMark);

        assertThat(streamB.getTrimMark()).isEqualTo(streamBTrimMark);
        assertThat(streamB.getTail()).isEqualTo(numStreamBEntries - 1);

        // Merge steamB into streamA and verify that the highest trim mark is
        // adopted in streamA
        StreamAddressSpace.merge(streamA, streamB);

        assertThat(streamA.getTrimMark()).isEqualTo(streamBTrimMark);
        assertThat(streamA.getTail()).isEqualTo(numStreamBEntries - 1);

        LongStream.range(streamBTrimMark + 1, numStreamBEntries).forEach(address ->
                assertThat(streamA.contains(address)).isTrue()
        );
    }

    @Test
    public void testMergeDifferentTrimMarks() {
        StreamAddressSpace streamA = new StreamAddressSpace();
        streamA.addAddress(1L);
        streamA.trim(1L);
        StreamAddressSpace streamB = new StreamAddressSpace(6L, Collections.EMPTY_SET);

        assertThat(streamA.size()).isEqualTo(0);
        assertThat(streamA.getTrimMark()).isEqualTo(1L);
        assertThat(streamB.size()).isEqualTo(0);
        assertThat(streamB.getTrimMark()).isEqualTo(6L);

        // Merge streamB into streamA and verify that streamA adopted streamB's larger trim mark
        StreamAddressSpace.merge(streamA, streamB);
        assertThat(streamA.size()).isEqualTo(0);
        assertThat(streamA.getTrimMark()).isEqualTo(6L);
    }

    @Test
    public void testForEachUpTo() {
        StreamAddressSpace stream = new StreamAddressSpace();

        Set<Long> query1 = new HashSet<>();
        stream.forEachUpTo(Long.MAX_VALUE, query1::add);
        assertThat(query1).isEmpty();

        stream.addAddress(1L);
        stream.addAddress(5L);
        stream.addAddress(6L);

        Set<Long> query2 = new HashSet<>();
        stream.forEachUpTo(6L, query2::add);
        assertThat(query2).containsExactly(1L, 5L, 6L);

        stream.trim(5L);

        Set<Long> query3 = new HashSet<>();
        stream.forEachUpTo(6L, query3::add);
        assertThat(query3).containsExactly(6L);
    }

    @Test
    public void constructorTest() {
        StreamAddressSpace obj1 = new StreamAddressSpace(5L, Collections.emptySet());
        assertThat(obj1.getTrimMark()).isEqualTo(5L);
        assertThat(obj1.size()).isEqualTo(0L);

        StreamAddressSpace obj2 = new StreamAddressSpace(5L, Collections.singleton(6L));
        assertThat(obj2.getTrimMark()).isEqualTo(5L);
        assertThat(obj2.size()).isEqualTo(1L);
        assertThat(obj2.contains(6L)).isTrue();

        StreamAddressSpace obj3 = new StreamAddressSpace(Collections.singleton(6L));
        assertThat(obj3.getTrimMark()).isEqualTo(Address.NON_ADDRESS);
        assertThat(obj3.size()).isEqualTo(1L);
        assertThat(obj3.contains(6L)).isTrue();
    }

    @Test
    public void selectTest() {
        StreamAddressSpace obj1 = new StreamAddressSpace(5L, Collections.emptySet());
        assertThrows(IllegalArgumentException.class, () -> obj1.select(0));
        obj1.trim(5L);
        assertThrows(IllegalArgumentException.class, () -> obj1.select(0));
        obj1.addAddress(6L);
        assertThat(obj1.contains(6L)).isTrue();
        assertThat(obj1.select(0)).isEqualTo(6L);
        obj1.addAddress(8L);
        obj1.trim(6L);
        assertThat(obj1.select(0)).isEqualTo(8L);
        obj1.trim(8L);
        assertThrows(IllegalArgumentException.class, () -> obj1.select(0));
    }

    @Test
    public void testConstructorTrim() {
        StreamAddressSpace obj1 = new StreamAddressSpace(ImmutableSet.of(1L, 2L, 3L, 4L));
        assertThat(obj1.size()).isEqualTo(4);
        obj1.trim(2L);
        assertThat(obj1.size()).isEqualTo(2);
        assertThat(obj1.contains(1L)).isFalse();
        assertThat(obj1.contains(2L)).isFalse();
        assertThat(obj1.contains(3L)).isTrue();
        assertThat(obj1.contains(4L)).isTrue();
        assertThat(obj1.contains(-1L)).isFalse();
    }

    @Test
    public void testTrim() {
        StreamAddressSpace obj1 = new StreamAddressSpace(ImmutableSet.of(1L, 2L, 3L, 4L));
        assertThat(obj1.size()).isEqualTo(4);
        assertThat(obj1.contains(1L)).isTrue();
        assertThat(obj1.contains(2L)).isTrue();
        assertThat(obj1.contains(3L)).isTrue();
        assertThat(obj1.contains(4L)).isTrue();

        obj1.trim(2L);
        assertThat(obj1.size()).isEqualTo(2);
        assertThat(obj1.contains(1L)).isFalse();
        assertThat(obj1.contains(2L)).isFalse();
        assertThat(obj1.contains(3L)).isTrue();
        assertThat(obj1.contains(4L)).isTrue();

        obj1.trim(4L);
        assertThat(obj1.size()).isEqualTo(0);
        assertThat(obj1.contains(3L)).isFalse();
        assertThat(obj1.contains(4L)).isFalse();
    }

    @Test
    public void testTrimMarkUpdate() {
        StreamAddressSpace obj1 = new StreamAddressSpace(2L, ImmutableSet.of(3L, 4L, 6L));
        assertThat(obj1.getTrimMark()).isEqualTo(2L);
        obj1.trim(1L);
        // Verify that the trim mark doesn't regress
        assertThat(obj1.getTrimMark()).isEqualTo(2L);
        // Trim an address that belongs to the bitset
        obj1.trim(3L);
        assertThat(obj1.getTrimMark()).isEqualTo(3L);
        assertThat(obj1.contains(4L)).isTrue();
        assertThat(obj1.contains(6L)).isTrue();
        assertThat(obj1.size()).isEqualTo(2);
        // Trim an address that doesn't belongs to the bitset
        obj1.trim(5L);
        assertThat(obj1.getTrimMark()).isEqualTo(4L);
        assertThat(obj1.contains(6L)).isTrue();
        assertThat(obj1.size()).isEqualTo(1);
    }

    @Test
    public void testCopy() {
        StreamAddressSpace obj1 = new StreamAddressSpace();
        StreamAddressSpace obj1Copy = obj1.copy();
        assertThat(obj1Copy.getTrimMark()).isEqualTo(obj1.getTrimMark());
        assertThat(obj1Copy.size()).isEqualTo(obj1.size());
        assertNotSame(obj1Copy, obj1);

        StreamAddressSpace obj2 = new StreamAddressSpace(ImmutableSet.of(1L, 2L));
        StreamAddressSpace obj2Copy = obj2.copy();
        assertThat(obj2Copy.getTrimMark()).isEqualTo(obj2.getTrimMark());
        assertThat(obj2Copy.size()).isEqualTo(obj2.size());
        assertThat(obj2Copy.contains(1L)).isTrue();
        assertThat(obj2Copy.contains(2L)).isTrue();
        assertNotSame(obj2Copy, obj2);
    }

    @Test
    public void testToArray() {
        StreamAddressSpace obj1 = new StreamAddressSpace(ImmutableSet.of(1L, 2L, 3L, 4L, 5L));
        assertThat(obj1.size()).isEqualTo(5);
        obj1.trim(3L);
        assertThat(obj1.toArray()).containsExactly(4L, 5L);
        obj1.trim(5);
        assertThat(obj1.toArray()).isEmpty();
    }

    @Test
    @SuppressWarnings("ConstantConditions")
    public void testEquality() {
        assertThat(new StreamAddressSpace().equals(null)).isFalse();
        assertThat(new StreamAddressSpace().equals(new StreamAddressSpace())).isTrue();
        assertThat(new StreamAddressSpace(2L, Collections.emptySet()).equals(new StreamAddressSpace())).isFalse();
        assertThat(new StreamAddressSpace(2L, Collections.emptySet())
                .equals(new StreamAddressSpace(2L, Collections.emptySet()))).isTrue();
        // TODO(Maithem): Re-enable after this fix https://github.com/RoaringBitmap/RoaringBitmap/pull/451
        //assertThat(new StreamAddressSpace(2L, Collections.EMPTY_SET)
          //      .equals(new StreamAddressSpace(2L, ImmutableSet.of(1L, 2L)))).isTrue();
        assertThat(new StreamAddressSpace(ImmutableSet.of(1L, 2L))
                .equals(new StreamAddressSpace(ImmutableSet.of(1L, 2L)))).isTrue();
    }

    @Test
    public void testStreamSerialization() throws Exception {
        StreamAddressSpace obj1 = new StreamAddressSpace(3, ImmutableSet.of(1L, 2L, 3L, 4L, 5L));

        ByteBuf buf = Unpooled.buffer();
        DataOutput output = new ByteBufOutputStream(buf);
        obj1.serialize(output);
        DataInputStream inputStream = new DataInputStream(new ByteBufInputStream(buf));
        StreamAddressSpace deserialized = StreamAddressSpace.deserialize(inputStream);

        assertThat(obj1.getTrimMark()).isEqualTo(deserialized.getTrimMark());
        assertThat(obj1.size()).isEqualTo(deserialized.size());
        assertThat(obj1.toArray()).isEqualTo(deserialized.toArray());
    }

    @Test
    public void testGetAddressesInRange() {
        StreamAddressSpace obj1 = new StreamAddressSpace();
        // getAddressesInRange queries (end, start]

        StreamAddressRange invalidRange = new StreamAddressRange(UUID.randomUUID(), 1,  2);
        assertThrows(IllegalArgumentException.class,
                () -> obj1.getAddressesInRange(invalidRange),
                "Invalid range (2, 1]");

        StreamAddressRange range1 = new StreamAddressRange(UUID.randomUUID(), 2,  1);
        assertThat(obj1.getAddressesInRange(range1).size()).isEqualTo(0);
        assertThat(obj1.getAddressesInRange(range1).getTrimMark()).isEqualTo(Address.NON_ADDRESS);

        // Intersect a single point
        obj1.addAddress(5);

        StreamAddressRange range2 = new StreamAddressRange(UUID.randomUUID(), 5,  1);
        assertThat(obj1.getAddressesInRange(range2).size()).isEqualTo(1);
        assertThat(obj1.getAddressesInRange(range2).contains(5)).isTrue();
        assertThat(obj1.getAddressesInRange(range2).getTrimMark()).isEqualTo(Address.NON_ADDRESS);

        StreamAddressRange range3 = new StreamAddressRange(UUID.randomUUID(), 6,  5);
        assertThat(obj1.getAddressesInRange(range3).size()).isEqualTo(0);
        assertThat(obj1.getAddressesInRange(range3).contains(5)).isFalse();
        assertThat(obj1.getAddressesInRange(range3).getTrimMark()).isEqualTo(Address.NON_ADDRESS);

        obj1.addAddress(7);
        StreamAddressRange range4 = new StreamAddressRange(UUID.randomUUID(), 8,  4);
        assertThat(obj1.getAddressesInRange(range4).size()).isEqualTo(2);
        assertThat(obj1.getAddressesInRange(range4).contains(5)).isTrue();
        assertThat(obj1.getAddressesInRange(range4).contains(7)).isTrue();
        assertThat(obj1.getAddressesInRange(range4).getTrimMark()).isEqualTo(Address.NON_ADDRESS);

        obj1.trim(6);
        assertThat(obj1.getTrimMark()).isEqualTo(5L);
        assertThat(obj1.size()).isEqualTo(1);
        assertThat(obj1.contains(7)).isTrue();

        StreamAddressRange range5 = new StreamAddressRange(UUID.randomUUID(), 8,  4);
        assertThat(obj1.getAddressesInRange(range5).size()).isEqualTo(1);
        assertThat(obj1.getAddressesInRange(range5).contains(5)).isFalse();
        assertThat(obj1.getAddressesInRange(range5).contains(7)).isTrue();
        assertThat(obj1.getAddressesInRange(range5).getTrimMark()).isEqualTo(5L);
    }

    @Test
    public void testFloorNonExist() {
        StreamAddressSpace sas = new StreamAddressSpace();

        // Validate non-existing floor queries on an empty address space
        assertThat(sas.floor(0L)).isEqualTo(Address.NON_ADDRESS);
        assertThat(sas.floor(5L)).isEqualTo(Address.NON_ADDRESS);

        // Validate non-existing floor queries on a non-empty address space
        Arrays.asList(6L, 7L, 8L, 9L, 10L).forEach(sas::addAddress);
        assertThat(sas.floor(0L)).isEqualTo(Address.NON_ADDRESS);
        assertThat(sas.floor(5L)).isEqualTo(Address.NON_ADDRESS);
    }

    @Test
    public void testFloorExist() {
        StreamAddressSpace sas = new StreamAddressSpace();
        List<Long> addresses = Arrays.asList(6L, 7L, 8L, 9L, 10L);

        addresses.stream().filter(x -> x % 2 == 0).forEach(sas::addAddress);

        // Validate floor queries in the presence of gaps
        addresses.forEach(x -> {
            if (x % 2 == 0) {
                assertThat(sas.floor(x)).isEqualTo(x);
            } else {
                assertThat(sas.floor(x)).isEqualTo(x - 1L);
            }
        });

        // Validate floor queries for larger addresses than the largest in the address space
        final Long largest = addresses.get(addresses.size() - 1);
        assertThat(sas.floor(largest + 1L)).isEqualTo(largest);
        assertThat(sas.floor(10L * largest)).isEqualTo(largest);

        // Add address 2L * largest and revalidate the above queries.
        sas.addAddress(2L * largest);
        assertThat(sas.floor(largest + 1L)).isEqualTo(largest);
        assertThat(sas.floor(10L * largest)).isEqualTo(2L * largest);
    }

    @Test
    public void testFloorWithTrim() {
        StreamAddressSpace sas = new StreamAddressSpace();
        Arrays.asList(6L, 7L, 8L, 9L, 10L, 15L, 17L).forEach(sas::addAddress);

        // Trim up to an address existing in the address space and validate floor queries
        sas.trim(8L);
        assertThat(sas.floor(6L)).isEqualTo(Address.NON_ADDRESS);
        assertThat(sas.floor(8L)).isEqualTo(Address.NON_ADDRESS);
        assertThat(sas.floor(9L)).isEqualTo(9L);
        assertThat(sas.floor(12L)).isEqualTo(10L);
        assertThat(sas.floor(14L)).isEqualTo(10L);
        assertThat(sas.floor(15L)).isEqualTo(15L);
        assertThat(sas.floor(20L)).isEqualTo(17L);

        // Now trim up to an address not present in the address space, and revalidate the above
        sas.trim(12L);
        assertThat(sas.floor(6L)).isEqualTo(Address.NON_ADDRESS);
        assertThat(sas.floor(8L)).isEqualTo(Address.NON_ADDRESS);
        assertThat(sas.floor(9L)).isEqualTo(Address.NON_ADDRESS);
        assertThat(sas.floor(12L)).isEqualTo(Address.NON_ADDRESS);
        assertThat(sas.floor(14L)).isEqualTo(Address.NON_ADDRESS);
        assertThat(sas.floor(15L)).isEqualTo(15L);
        assertThat(sas.floor(20L)).isEqualTo(17L);

        // Now trim the remaining address space and revalidate the above
        sas.trim(30L);
        assertThat(sas.floor(6L)).isEqualTo(Address.NON_ADDRESS);
        assertThat(sas.floor(8L)).isEqualTo(Address.NON_ADDRESS);
        assertThat(sas.floor(9L)).isEqualTo(Address.NON_ADDRESS);
        assertThat(sas.floor(12L)).isEqualTo(Address.NON_ADDRESS);
        assertThat(sas.floor(14L)).isEqualTo(Address.NON_ADDRESS);
        assertThat(sas.floor(15L)).isEqualTo(Address.NON_ADDRESS);
        assertThat(sas.floor(20L)).isEqualTo(Address.NON_ADDRESS);
    }

    @Test
    public void testToString() {
        StreamAddressSpace sas = new StreamAddressSpace();
        assertThat(sas.toString()).isEqualTo("[-6, -6]@-1 size 0");
        sas.addAddress(4L);
        assertThat(sas.toString()).isEqualTo("[4, 4]@-1 size 1");
        sas.trim(4L);
        assertThat(sas.toString()).isEqualTo("[-6, -6]@4 size 0");
    }

    @Test
    public void testNoArgConstructor() {
        StreamAddressSpace sas = new StreamAddressSpace();
        assertThat(getDoCacheCardinalities(sas.getBitmap())).isFalse();

        sas = new StreamAddressSpace(true);
        assertThat(getDoCacheCardinalities(sas.getBitmap())).isTrue();
    }

    private boolean getDoCacheCardinalities(Roaring64NavigableMap map) {
        try {
            Field field = Roaring64NavigableMap.class.getDeclaredField("doCacheCardinalities");
            field.setAccessible(true);
            return field.getBoolean(map);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException("Failed to access doCacheCardinalities", e);
        }
    }
}
