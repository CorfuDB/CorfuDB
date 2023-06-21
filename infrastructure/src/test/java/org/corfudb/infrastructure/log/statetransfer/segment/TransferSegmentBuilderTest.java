package org.corfudb.infrastructure.log.statetransfer.segment;

import com.google.common.collect.ImmutableList;
import org.corfudb.infrastructure.log.statetransfer.segment.TransferSegment.TransferSegmentBuilder;
import org.corfudb.infrastructure.log.statetransfer.segment.TransferSegmentStatus.SegmentState;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertThrows;

class TransferSegmentBuilderTest {

    @Test
    public void testVerify() {
        assertThrows(NullPointerException.class, ()-> {
            getValidTransferSegmentBuilder()
                    .status(new TransferSegmentStatus(SegmentState.TRANSFERRED, Optional.empty()))
                    .build();
        });

        assertThrows(NullPointerException.class, ()-> {
            getValidTransferSegmentBuilder()
                    .logUnitServers(ImmutableList.of())
                    .build();
        });

        assertThrows(IllegalStateException.class, ()-> {
            final int invalidStartAddress = 3;
            final int invalidEndAddress = 2;
            TransferSegment.builder()
                    .startAddress(invalidStartAddress)
                    .endAddress(invalidEndAddress)
                    .status(TransferSegmentStatus.builder().segmentState(SegmentState.TRANSFERRED).build())
                    .logUnitServers(ImmutableList.of())
                    .build();
        });
    }

    private static TransferSegmentBuilder getValidTransferSegmentBuilder() {
        final int startAddress = 1;
        final int endAddress = 2;
        return TransferSegment.builder()
                .startAddress(startAddress)
                .endAddress(endAddress);
    }

}