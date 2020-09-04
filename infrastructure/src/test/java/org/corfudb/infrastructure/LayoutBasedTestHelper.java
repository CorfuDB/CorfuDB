package org.corfudb.infrastructure;

import com.google.common.collect.ImmutableList;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.corfudb.infrastructure.log.statetransfer.segment.TransferSegment;
import org.corfudb.infrastructure.log.statetransfer.segment.TransferSegmentStatus;
import org.corfudb.runtime.view.Layout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.corfudb.runtime.view.Layout.ReplicationMode.CHAIN_REPLICATION;

public class LayoutBasedTestHelper {
    private static String servers = "A|B|C";

    public Layout createTestLayout(List<Layout.LayoutSegment> segments){

        List<String> s = Arrays.stream(servers.split("|")).collect(Collectors.toList());
        long epoch = 0L;
        UUID uuid = UUID.randomUUID();
        return new Layout(s, s, segments, new ArrayList<>(), epoch, uuid);
    }

    @AllArgsConstructor
    @EqualsAndHashCode
    @ToString
    public class MockedSegment {
        public final long startAddress;
        public final long endAddress;
        public TransferSegmentStatus status;
    }

    public Layout createNonPresentLayout() {
        Layout.LayoutStripe stripe1 = new Layout.LayoutStripe(Arrays.asList("A", "B"));
        Layout.LayoutStripe stripe2 = new Layout.LayoutStripe(Arrays.asList("A", "B"));
        Layout.LayoutStripe stripe3 = new Layout.LayoutStripe(Arrays.asList("localhost", "A", "B"));

        Layout.LayoutSegment segment1 = new Layout.LayoutSegment(CHAIN_REPLICATION, 0L, 2L,
                Collections.singletonList(stripe1));

        Layout.LayoutSegment segment2 = new Layout.LayoutSegment(CHAIN_REPLICATION, 2L, 4L,
                Collections.singletonList(stripe2));

        Layout.LayoutSegment segment3 = new Layout.LayoutSegment(CHAIN_REPLICATION, 4L, -1L,
                Collections.singletonList(stripe3));

        return createTestLayout(Arrays.asList(segment1, segment2, segment3));
    }

    public Layout createPresentLayout() {

        Layout layout = createNonPresentLayout();
        Layout.LayoutStripe stripe2 = new Layout.LayoutStripe(Arrays.asList("localhost", "A", "B"));

        Layout.LayoutSegment segment2 = new Layout.LayoutSegment(CHAIN_REPLICATION, 2L, 4L,
                Collections.singletonList(stripe2));

        return createTestLayout(Arrays.asList(layout.getSegment(0L),
                segment2, layout.getSegment(4L)));
    }

    public MockedSegment transformToMock(TransferSegment segment) {
        return new MockedSegment(segment.getStartAddress(), segment.getEndAddress(), segment.getStatus());
    }

    public ImmutableList<MockedSegment> transformListToMock(List<TransferSegment> segments) {
        return ImmutableList.copyOf(segments.stream().map(this::transformToMock).collect(Collectors.toList()));
    }

}
