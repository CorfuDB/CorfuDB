package org.corfudb.runtime.view.stream;

import static org.assertj.core.api.Java6Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.DataInputStream;
import java.io.DataOutput;
import java.util.Collections;
import java.util.stream.LongStream;

import com.google.common.collect.ImmutableSet;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import org.corfudb.runtime.view.Address;
import org.junit.Test;

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
        StreamAddressSpace obj1 = new StreamAddressSpace(2L, ImmutableSet.of(1L, 2L, 3L, 4L));
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
        StreamAddressSpace obj1 = new StreamAddressSpace(3, ImmutableSet.of(1L, 2L, 3L, 4L, 5L));
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
    public void testToString() {
        StreamAddressSpace sas = new StreamAddressSpace();
        assertThat(sas.toString()).isEqualTo("[-6, -6]@-1 size 0");
        sas.addAddress(4L);
        assertThat(sas.toString()).isEqualTo("[4, 4]@-1 size 1");
        sas.trim(4L);
        assertThat(sas.toString()).isEqualTo("[-6, -6]@4 size 0");
    }
}
