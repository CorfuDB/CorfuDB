package org.corfudb.runtime.view.stream;

import static org.assertj.core.api.Java6Assertions.assertThat;


import java.util.UUID;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import org.corfudb.protocols.wireprotocol.StreamAddressRange;
import org.corfudb.runtime.view.Address;
import org.junit.Test;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

public class StreamAddressSpaceTest {

    @Test
    public void testStreamAddressSpaceMerge() {

        StreamAddressSpace streamA = new StreamAddressSpace();

        final int numStreamAEntries = 100;
        IntStream.range(0, numStreamAEntries).forEach(streamA::addAddress);

        assertThat(streamA.getTrimMark()).isEqualTo(Address.NON_ADDRESS);
        assertThat(streamA.getTail()).isEqualTo(numStreamAEntries - 1);

        // need to take higher trim mark?

        StreamAddressSpace streamB = new StreamAddressSpace();
        final int numStreamBEntries = 130;
        IntStream.range(0, numStreamBEntries).forEach(streamB::addAddress);
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
                assertThat(streamA.getAddressMap().contains(address)).isTrue()
        );
    }

    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void testAddressesInRange() {
        StreamAddressSpace sas = new StreamAddressSpace();
        sas.addAddresses(1L, 3L, 5L, 7L, 9L);
        StreamAddressRange range = new StreamAddressRange(UUID.randomUUID(), 7, 3);
        Roaring64NavigableMap rangeResult = sas.getAddressesInRange(range);
        assertThat(rangeResult.getLongCardinality()).isEqualTo(2L);
        assertThat(rangeResult.toArray()).containsExactlyInAnyOrder(7L, 5L);
    }
}
