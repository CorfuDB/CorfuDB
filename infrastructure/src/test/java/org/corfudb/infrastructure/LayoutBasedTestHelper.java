package org.corfudb.infrastructure;

import com.google.common.collect.ImmutableList;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.corfudb.infrastructure.log.statetransfer.segment.TransferSegment;
import org.corfudb.infrastructure.log.statetransfer.segment.TransferSegmentStatus;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.Layout.LayoutSegment;
import org.corfudb.runtime.view.Layout.LayoutStripe;
import org.corfudb.runtime.view.Layout.ReplicationMode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.corfudb.infrastructure.NodeNames.ABC_NODES;
import static org.corfudb.runtime.view.Layout.ReplicationMode.CHAIN_REPLICATION;

public class LayoutBasedTestHelper {


    public Layout createTestLayout(List<LayoutSegment> segments) {

        long epoch = 0L;
        UUID uuid = UUID.randomUUID();
        return new Layout(ABC_NODES, ABC_NODES, segments, new ArrayList<>(), new ArrayList<>(), epoch, uuid);
    }

    @AllArgsConstructor
    @EqualsAndHashCode
    @ToString
    public static class MockedSegment {
        public final long startAddress;
        public final long endAddress;
        public TransferSegmentStatus status;
    }

    public Layout buildSimpleLayout() {
        return buildSimpleLayout(0);
    }

    public Layout buildSimpleLayout(long epoch) {
        List<LayoutStripe> stripes = Collections.singletonList(new LayoutStripe(ABC_NODES));
        LayoutSegment segment = new LayoutSegment(ReplicationMode.CHAIN_REPLICATION, 0L, -1L, stripes);
        List<LayoutSegment> segments = Collections.singletonList(segment);
        return new Layout(ABC_NODES, ABC_NODES, segments, epoch, UUID.randomUUID());
    }

    public Layout createNonPresentLayout() {
        LayoutStripe stripe1 = new LayoutStripe(Arrays.asList("A", "B"));
        LayoutStripe stripe2 = new LayoutStripe(Arrays.asList("A", "B"));
        LayoutStripe stripe3 = new LayoutStripe(Arrays.asList("localhost", "A", "B"));

        LayoutSegment segment1 = new LayoutSegment(CHAIN_REPLICATION, 0L, 2L,
                Collections.singletonList(stripe1));

        LayoutSegment segment2 = new LayoutSegment(CHAIN_REPLICATION, 2L, 4L,
                Collections.singletonList(stripe2));

        LayoutSegment segment3 = new LayoutSegment(CHAIN_REPLICATION, 4L, -1L,
                Collections.singletonList(stripe3));

        return createTestLayout(Arrays.asList(segment1, segment2, segment3));
    }

    public Layout createPresentLayout() {

        Layout layout = createNonPresentLayout();
        LayoutStripe stripe2 = new LayoutStripe(Arrays.asList("localhost", "A", "B"));

        LayoutSegment segment2 = new LayoutSegment(CHAIN_REPLICATION, 2L, 4L,
                Collections.singletonList(stripe2));

        return createTestLayout(Arrays.asList(layout.getSegment(0L),
                segment2, layout.getSegment(4L)));
    }

    public MockedSegment transformToMock(TransferSegment segment) {
        return new MockedSegment(segment.getStartAddress(), segment.getEndAddress(), segment.getStatus());
    }

    public ImmutableList<MockedSegment> transformListToMock(List<TransferSegment> segments) {
        List<MockedSegment> mockedSegments = segments.stream()
                .map(this::transformToMock)
                .collect(Collectors.toList());

        return ImmutableList.copyOf(mockedSegments);
    }

}
