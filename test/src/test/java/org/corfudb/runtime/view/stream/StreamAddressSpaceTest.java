package org.corfudb.runtime.view.stream;

import static org.assertj.core.api.Java6Assertions.assertThat;


import java.util.stream.LongStream;
import org.corfudb.runtime.view.Address;
import org.junit.Test;

public class StreamAddressSpaceTest {

    @Test
    public void testStreamAddressSpaceMerge() {

        StreamAddressSpace streamA = new StreamAddressSpace();

        final int numStreamAEntries = 100;
        LongStream.range(0, numStreamAEntries).forEach(streamA::add);

        assertThat(streamA.getTrimMark()).isEqualTo(Address.NON_ADDRESS);
        assertThat(streamA.getTail()).isEqualTo(numStreamAEntries - 1);

        // need to take higher trim mark?

        StreamAddressSpace streamB = new StreamAddressSpace();
        final int numStreamBEntries = 130;
        LongStream.range(0, numStreamBEntries).forEach(streamB::add);
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
}
