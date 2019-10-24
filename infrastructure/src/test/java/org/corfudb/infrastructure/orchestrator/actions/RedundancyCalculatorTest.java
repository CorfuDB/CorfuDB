package org.corfudb.infrastructure.orchestrator.actions;

import com.google.common.collect.ImmutableList;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.corfudb.common.util.Tuple;
import org.corfudb.infrastructure.LayoutBasedTest;
import org.corfudb.infrastructure.log.statetransfer.StateTransferManager.CurrentTransferSegment;
import org.corfudb.infrastructure.log.statetransfer.StateTransferManager.CurrentTransferSegmentStatus;
import org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState;
import org.corfudb.infrastructure.log.statetransfer.TransferSegmentCreator;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.Layout.LayoutSegment;
import org.corfudb.util.Sleep;
import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;
import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.FAILED;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.NOT_TRANSFERRED;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.RESTORED;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.TRANSFERRED;
import static org.corfudb.runtime.view.Layout.LayoutStripe;
import static org.corfudb.runtime.view.Layout.ReplicationMode.CHAIN_REPLICATION;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

public class RedundancyCalculatorTest extends LayoutBasedTest implements TransferSegmentCreator {

    @Test
    public void testSegmentContainsServer() {


        LayoutStripe stripe1 = new LayoutStripe(Arrays.asList("localhost", "A", "B"));
        LayoutStripe stripe2 = new LayoutStripe(Collections.singletonList("C"));
        LayoutStripe stripe3 = new LayoutStripe(Collections.singletonList("D"));

        LayoutSegment segment = new LayoutSegment(CHAIN_REPLICATION, 0L, 1L,
                Arrays.asList(stripe1, stripe2, stripe3));

        RedundancyCalculator calculator = new RedundancyCalculator("localhost");

        Assert.assertTrue(calculator.segmentContainsServer(segment, "localhost"));

        stripe1 = new LayoutStripe(Arrays.asList("A", "B"));
        segment = new LayoutSegment(CHAIN_REPLICATION, 0L, 1L,
                Arrays.asList(stripe1, stripe2, stripe3));
        Assert.assertFalse(calculator.segmentContainsServer(segment, "localhost"));
    }

    @Test
    public void testRequiresRedundancyRestoration() {
        LayoutStripe stripe1 = new LayoutStripe(Arrays.asList("A", "localhost"));
        LayoutStripe stripe2 = new LayoutStripe(Arrays.asList("A", "B", "localhost"));
        LayoutStripe stripe3 = new LayoutStripe(Arrays.asList("A"));

        LayoutSegment segment = new LayoutSegment(CHAIN_REPLICATION, 0L, 2L,
                Collections.singletonList(stripe1));
        LayoutSegment segment2 = new LayoutSegment(CHAIN_REPLICATION, 3L, 6L,
                Collections.singletonList(stripe2));
        LayoutSegment segment3 = new LayoutSegment(CHAIN_REPLICATION, 3L, 6L,
                Collections.singletonList(stripe3));

        // Different sets
        Layout layout = createTestLayout(Arrays.asList(segment, segment2));
        assertThat(RedundancyCalculator.requiresRedundancyRestoration(layout, "localhost"))
                .isFalse();
        // Single segment
        layout = createTestLayout(Collections.singletonList(segment));
        assertThat(RedundancyCalculator.requiresRedundancyRestoration(layout, "localhost"))
                .isFalse();
        layout = createTestLayout(Arrays.asList(segment2, segment3));
        assertThat(RedundancyCalculator.requiresRedundancyRestoration(layout, "localhost"))
                .isTrue();


    }

    @Test
    public void testRequiresMerge() {
        LayoutStripe stripe1 = new LayoutStripe(Arrays.asList("A", "localhost"));
        LayoutStripe stripe2 = new LayoutStripe(Arrays.asList("A", "B", "localhost"));
        LayoutSegment segment = new LayoutSegment(CHAIN_REPLICATION, 0L, 2L,
                Collections.singletonList(stripe1));
        LayoutSegment segment2 = new LayoutSegment(CHAIN_REPLICATION, 3L, 6L,
                Collections.singletonList(stripe2));

        // Can't be merged
        Layout layout = createTestLayout(Arrays.asList(segment, segment2));
        assertThat(RedundancyCalculator.requiresMerge(layout, "localhost"))
                .isFalse();

        stripe1 = new LayoutStripe(Arrays.asList("A"));

        segment = new LayoutSegment(CHAIN_REPLICATION, 0L, 2L,
                Collections.singletonList(stripe1));
        segment2 = new LayoutSegment(CHAIN_REPLICATION, 3L, 6L,
                Collections.singletonList(stripe2));

        layout = createTestLayout(Arrays.asList(segment, segment2));
        assertThat(RedundancyCalculator.requiresMerge(layout, "localhost"))
                .isFalse();

        // Can be merged
        stripe1 = new LayoutStripe(Arrays.asList("A", "B"));
        stripe2 = new LayoutStripe(Arrays.asList("A", "B", "localhost"));
        segment = new LayoutSegment(CHAIN_REPLICATION, 0L, 2L,
                Collections.singletonList(stripe1));
        segment2 = new LayoutSegment(CHAIN_REPLICATION, 3L, 6L,
                Collections.singletonList(stripe2));
        layout = createTestLayout(Arrays.asList(segment, segment2));
        assertThat(RedundancyCalculator.requiresMerge(layout, "localhost"))
                .isTrue();

    }

    @Test
    public void testCreateStateMap() {

        LayoutStripe stripe1 = new LayoutStripe(Arrays.asList("localhost", "A", "B"));
        LayoutStripe stripe2 = new LayoutStripe(Collections.singletonList("C"));
        LayoutStripe stripe3 = new LayoutStripe(Collections.singletonList("D"));
        LayoutSegment segment1 = new LayoutSegment(CHAIN_REPLICATION, 0L, 21L,
                Arrays.asList(stripe1, stripe2, stripe3));

        LayoutStripe stripe11 = new LayoutStripe(Arrays.asList("C", "D"));
        LayoutStripe stripe22 = new LayoutStripe(Collections.singletonList("C"));
        LayoutStripe stripe33 = new LayoutStripe(Collections.singletonList("D"));
        LayoutSegment segment2 = new LayoutSegment(CHAIN_REPLICATION, 21L, 51L,
                Arrays.asList(stripe11, stripe22, stripe33));

        List<LayoutSegment> layoutSegments = Arrays.asList(segment1, segment2);

        Layout testLayout = createTestLayout(layoutSegments);

        CorfuRuntime corfuRuntime = mock(CorfuRuntime.class);

        PrefixTrimRedundancyCalculator calculator = new PrefixTrimRedundancyCalculator
                ("localhost", corfuRuntime);

        PrefixTrimRedundancyCalculator spy = spy(calculator);

        doReturn(-1L).when(spy).setTrimOnNewLogUnit(testLayout, corfuRuntime, "localhost");

        ImmutableList<CurrentTransferSegment> currentTransferSegments =
                spy.createStateList(testLayout);

        MockedSegment presentSegment = new MockedSegment(0L,
                20L,
                CurrentTransferSegmentStatus.builder().segmentState(RESTORED).totalTransferred(21L).build());


        MockedSegment nonPresentSegment = new MockedSegment(21L,
                50L, CurrentTransferSegmentStatus.builder().segmentState(NOT_TRANSFERRED).totalTransferred(0L).build());

        Assert.assertTrue(transformListToMock(currentTransferSegments).containsAll(Arrays.asList(presentSegment,
                nonPresentSegment)));

        CurrentTransferSegmentStatus presentSegmentStatus = presentSegment.status;
        assertThat(SegmentState.RESTORED)
                .isEqualTo(presentSegmentStatus.getSegmentState());

        CurrentTransferSegmentStatus nonPresentSegmentStatus = nonPresentSegment.status;

        assertThat(NOT_TRANSFERRED)
                .isEqualTo(nonPresentSegmentStatus.getSegmentState());
    }

    @Test
    public void testRestoreRedundancyForSegment() {
        CurrentTransferSegment segment = createTransferSegment(0L, 20L, RESTORED);

        LayoutStripe stripe1 = new LayoutStripe(Collections.singletonList("A"));
        LayoutStripe stripe2 = new LayoutStripe(Collections.singletonList("C"));
        LayoutStripe stripe3 = new LayoutStripe(Collections.singletonList("D"));
        LayoutSegment segment1 = new LayoutSegment(CHAIN_REPLICATION, 0L, 21L,
                Arrays.asList(stripe1, stripe2, stripe3));

        LayoutStripe stripe11 = new LayoutStripe(Arrays.asList("C", "D"));
        LayoutStripe stripe22 = new LayoutStripe(Collections.singletonList("C"));
        LayoutStripe stripe33 = new LayoutStripe(Collections.singletonList("D"));
        LayoutSegment segment2 = new LayoutSegment(CHAIN_REPLICATION, 21L, 51L,
                Arrays.asList(stripe11, stripe22, stripe33));

        List<LayoutSegment> layoutSegments = Arrays.asList(segment1, segment2);

        Layout testLayout = createTestLayout(layoutSegments);

        RedundancyCalculator calculator = new RedundancyCalculator("localhost");
        Layout layout = calculator.restoreRedundancyForSegment(segment, testLayout);
        assertThat(layout.getFirstSegment().getFirstStripe().getLogServers())
                .contains("localhost");

    }

    @Test
    public void testUpdateLayoutAfterRedundancyRestoration() {

        LayoutStripe stripe1 = new LayoutStripe(Collections.singletonList("A"));
        LayoutStripe stripe2 = new LayoutStripe(Collections.singletonList("C"));
        LayoutStripe stripe3 = new LayoutStripe(Collections.singletonList("D"));
        LayoutSegment segment1 = new LayoutSegment(CHAIN_REPLICATION, 0L, 21L,
                Arrays.asList(stripe1, stripe2, stripe3));

        LayoutStripe stripe11 = new LayoutStripe(Arrays.asList("C", "D"));
        LayoutStripe stripe22 = new LayoutStripe(Collections.singletonList("C"));
        LayoutStripe stripe33 = new LayoutStripe(Collections.singletonList("D"));
        LayoutSegment segment2 = new LayoutSegment(CHAIN_REPLICATION, 21L, 51L,
                Arrays.asList(stripe11, stripe22, stripe33));


        LayoutStripe stripe111 = new LayoutStripe(Arrays.asList("E", "F"));
        LayoutStripe stripe222 = new LayoutStripe(Collections.singletonList("G"));
        LayoutStripe stripe333 = new LayoutStripe(Collections.singletonList("H"));
        LayoutSegment segment3 = new LayoutSegment(CHAIN_REPLICATION, 51L, 61L,
                Arrays.asList(stripe111, stripe222, stripe333));

        List<LayoutSegment> layoutSegments = Arrays.asList(segment1, segment2, segment3);

        Layout testLayout = createTestLayout(layoutSegments);

        List<CurrentTransferSegment> currentTransferSegments = Arrays.asList
                (createTransferSegment(0L, 20L, RESTORED),
                        createTransferSegment(21L, 50L, RESTORED),
                        createTransferSegment(51L, 60L, RESTORED));


        RedundancyCalculator calculator = new RedundancyCalculator("localhost");

        Layout consolidatedLayout =
                calculator.updateLayoutAfterRedundancyRestoration(currentTransferSegments,
                        testLayout);

        assertThat(consolidatedLayout.getSegments().stream())
                .allMatch(segment -> segment.getFirstStripe().getLogServers().contains("localhost"));

    }


    @Test
    public void testMergeSegmentsMatchFailed() {
        RedundancyCalculator calculator = new RedundancyCalculator("localhost");

        // old segment had a failed transfer, segments match completely -> preserve
        CurrentTransferSegment oldSegment = createTransferSegment(0L, 5L, FAILED);

        CurrentTransferSegment newSegment = createTransferSegment(0L, 5L, NOT_TRANSFERRED);

        ImmutableList<CurrentTransferSegment> oldList = ImmutableList.of(oldSegment);
        ImmutableList<CurrentTransferSegment> newList = ImmutableList.of(newSegment);

        assertThat(calculator.mergeLists(oldList, newList).get(0).getStatus().join()
                .getSegmentState())
                .isEqualTo(FAILED);
    }


    @Test
    public void testMergeMapsMatchRestored() {

        RedundancyCalculator calculator = new RedundancyCalculator("localhost");

        CurrentTransferSegment oldSegment = createTransferSegment(0L, 5L, NOT_TRANSFERRED);

        CurrentTransferSegment newSegment = createTransferSegment(0L, 5L, RESTORED);

        ImmutableList<CurrentTransferSegment> oldList = ImmutableList.of(oldSegment);

        ImmutableList<CurrentTransferSegment> newList = ImmutableList.of(newSegment);


        assertThat(calculator.mergeLists(oldList, newList)
                .get(0)
                .getStatus()
                .join()
                .getSegmentState()).isEqualTo(RESTORED);

    }

    @Test
    public void testMergeMapsNonOverlap() {

        RedundancyCalculator calculator = new RedundancyCalculator("localhost");

        CurrentTransferSegment segment1 = createTransferSegment(0L, 5L, NOT_TRANSFERRED);
        CurrentTransferSegment segment2 = createTransferSegment(6L, 10L, NOT_TRANSFERRED);

        assertThat(calculator.mergeLists(ImmutableList.of(segment1), ImmutableList.of(segment2)))
                .contains(segment1).contains(segment2);

    }

    @Test
    public void testMergeMapsNewIsSubset() {
        RedundancyCalculator calculator = new RedundancyCalculator("localhost");

        CurrentTransferSegment oldSegment = createTransferSegment(0L, 5L, NOT_TRANSFERRED);
        CurrentTransferSegment newSegment = createTransferSegment(3L, 5L, RESTORED);

        assertThat(calculator.mergeLists(ImmutableList.of(oldSegment), ImmutableList.of(newSegment))
                .get(0)
                .getStatus()
                .join().getSegmentState()).isEqualTo(RESTORED);

        assertThat(calculator.mergeLists(ImmutableList.of(oldSegment), ImmutableList.of(newSegment))
                .get(0).getStartAddress()).isEqualTo(3L);

    }

    @Test
    public void testMergeMapsNewIsSuperSet() {
        RedundancyCalculator calculator = new RedundancyCalculator("localhost");
        // oldSegment is not transferred and new segment is restored
        CurrentTransferSegment oldSegment = createTransferSegment(0L, 5L, NOT_TRANSFERRED);
        CurrentTransferSegment newSegment = createTransferSegment(0L, 15L, RESTORED);

        assertThat(calculator.mergeLists(ImmutableList.of(oldSegment), ImmutableList.of(newSegment))
                .get(0)
                .getStatus()
                .join().getSegmentState()).isEqualTo(RESTORED);
    }

    @Test
    public void testMergeMapsNewIsPrefixedAndMerged() {
        RedundancyCalculator calculator = new RedundancyCalculator("localhost");

        // oldSegment is transferred and newsegment is restored
        CurrentTransferSegment oldSegment = createTransferSegment(0L, 5L, NOT_TRANSFERRED);
        CurrentTransferSegment newSegment = createTransferSegment(3L, 15L, RESTORED);

        assertThat(calculator.mergeLists(ImmutableList.of(oldSegment), ImmutableList.of(newSegment))
                .get(0)
                .getStatus()
                .join().getSegmentState()).isEqualTo(RESTORED);
    }

    @Test
    public void testMergeMapsNewMapIsEmpty() {
        RedundancyCalculator calculator = new RedundancyCalculator("localhost");

        // oldSegment is transferred and new segment is non existent

        CurrentTransferSegment oldSegment = createTransferSegment(0L, 5L, TRANSFERRED);

        assertThat(calculator.mergeLists(ImmutableList.of(oldSegment),
                ImmutableList.of()).get(0).getStatus()
                .join().getSegmentState()).isEqualTo(RESTORED);


    }

    @Builder
    @AllArgsConstructor
    @ToString
    @EqualsAndHashCode
    private static class SegmentData {
        public final Map.Entry<Long, Long> range;
        public boolean inProgress;
        public boolean failed;
        public SegmentState state;
    }

    @Test
    public void testMergeComplex() {
        // old list:
        // 0 -> 10 transferred, 11 -> 12 not transferred, 13 -> 16 failed, 18 -> 20 failed, 21 -> 25 restored
        // new list:
        // 5 -> 10 restored, 11 -> 12 not transferred, 13 -> 16 is not transferred, 18 -> 20 not transferred,
        // 21 -> 25 not transferred, 26 -> 30 restored
        // result:
        // 5 -> 10 restored, 11 -> 12 not transferred, 13 -> 16 failed, 18 -> 20 failed, 21 -> 25 restored, 26 -> 30 restored

        // Old list:
        List<CurrentTransferSegment> oldList = Arrays.asList(
                createTransferSegment(0L, 10L, TRANSFERRED),
                createTransferSegment(11L, 12L, NOT_TRANSFERRED),
                createTransferSegment(13L, 16L, FAILED),
                createTransferSegment(18L, 20L, FAILED),
                createTransferSegment(21L, 25L, RESTORED)
        );

        // New list:
        List<CurrentTransferSegment> newList = Arrays.asList(
                createTransferSegment(5L, 10L, RESTORED),
                createTransferSegment(11L, 12L, NOT_TRANSFERRED),
                createTransferSegment(13L, 16L, NOT_TRANSFERRED),
                createTransferSegment(18L, 20L, NOT_TRANSFERRED),
                createTransferSegment(21L, 25L, NOT_TRANSFERRED),
                createTransferSegment(26L, 30L, RESTORED)
        );

        RedundancyCalculator calculator = new RedundancyCalculator("localhost");
        ImmutableList<CurrentTransferSegment> resultList = calculator.mergeLists(oldList, newList);
        // Expected:
        List<CurrentTransferSegment> expected = Arrays.asList(
                createTransferSegment(5L, 10L, RESTORED),
                createTransferSegment(11L, 12L, NOT_TRANSFERRED),
                createTransferSegment(13L, 16L, FAILED),
                createTransferSegment(18L, 20L, FAILED),
                createTransferSegment(21L, 25L, RESTORED),
                createTransferSegment(26L, 30L, RESTORED)
        );

        assertThat(IntStream.range(0, resultList.size()).boxed().map(index ->
                new Tuple<>(resultList.get(index), expected.get(index)))
                .allMatch(tuple -> {
                    return tuple.first.getStartAddress() == tuple.second.getStartAddress() &&
                            tuple.first.getEndAddress() == tuple.second.getEndAddress() &&
                            tuple.first.getStatus().join().getSegmentState() == tuple.first.getStatus().join().getSegmentState();
                })).isTrue();

    }

    @Test
    public void testCanMergeSegmentsOneSegment() {
        LayoutSegment segment = new LayoutSegment(CHAIN_REPLICATION, 0L, 1L,
                Arrays.asList(new LayoutStripe(Arrays.asList("localhost"))));
        assertThat(RedundancyCalculator
                .canMergeSegments(createTestLayout(Arrays.asList(segment)), "localhost")).isFalse();
    }

    @Test
    public void testCanMergeSegmentsServerNonPresent() {
        LayoutSegment segment1 = new LayoutSegment(CHAIN_REPLICATION, 0L, 1L,
                Arrays.asList(new LayoutStripe(Arrays.asList("localhost", "B"))));

        LayoutSegment segment2 = new LayoutSegment(CHAIN_REPLICATION, 0L, 1L,
                Arrays.asList(new LayoutStripe(Arrays.asList("C", "A"))));
        assertThat(RedundancyCalculator
                .canMergeSegments(createTestLayout(Arrays.asList(segment1, segment2)), "localhost")).isFalse();
    }

    @Test
    public void testCanMergeEmptyNonEmptySetDifference() {
        LayoutSegment segment1 = new LayoutSegment(CHAIN_REPLICATION, 0L, 1L,
                Arrays.asList(new LayoutStripe(Arrays.asList("localhost", "B"))));

        LayoutSegment segment2 = new LayoutSegment(CHAIN_REPLICATION, 0L, 1L,
                Arrays.asList(new LayoutStripe(Arrays.asList("C", "B", "localhost"))));
        assertThat(RedundancyCalculator
                .canMergeSegments(createTestLayout(Arrays.asList(segment1, segment2)), "localhost")).isFalse();
    }

    @Test
    public void testCanMergeSegmentsDoMerge() {
        LayoutSegment segment1 = new LayoutSegment(CHAIN_REPLICATION, 0L, 1L,
                Arrays.asList(new LayoutStripe(Arrays.asList("localhost", "B"))));

        LayoutSegment segment2 = new LayoutSegment(CHAIN_REPLICATION, 0L, 1L,
                Arrays.asList(new LayoutStripe(Arrays.asList("B", "localhost"))));
        assertThat(RedundancyCalculator
                .canMergeSegments(createTestLayout(Arrays.asList(segment1, segment2)), "localhost")).isTrue();

    }

}
