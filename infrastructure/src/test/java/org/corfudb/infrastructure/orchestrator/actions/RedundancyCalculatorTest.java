package org.corfudb.infrastructure.orchestrator.actions;

import com.google.common.collect.ImmutableList;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.corfudb.infrastructure.LayoutBasedTest;
import org.corfudb.infrastructure.log.statetransfer.StateTransferManager.CurrentTransferSegment;
import org.corfudb.infrastructure.log.statetransfer.StateTransferManager.CurrentTransferSegmentStatus;
import org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState;
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
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.FAILED;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.NOT_TRANSFERRED;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.RESTORED;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.TRANSFERRED;
import static org.corfudb.runtime.view.Address.NON_ADDRESS;
import static org.corfudb.runtime.view.Layout.*;
import static org.corfudb.runtime.view.Layout.ReplicationMode.CHAIN_REPLICATION;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

public class RedundancyCalculatorTest extends LayoutBasedTest {

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
                new CurrentTransferSegmentStatus(RESTORED, 20L));


        MockedSegment nonPresentSegment = new MockedSegment(21L,
                50L, new CurrentTransferSegmentStatus(NOT_TRANSFERRED, NON_ADDRESS));

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
        CurrentTransferSegment segment = new CurrentTransferSegment(0L, 20L, null);

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
                (new CurrentTransferSegment(0L, 20L, null),
                        new CurrentTransferSegment(21L, 50L, null),
                        new CurrentTransferSegment(51L, 60L, null));


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

        CompletableFuture<CurrentTransferSegmentStatus> failed =
                CompletableFuture.completedFuture
                        (new CurrentTransferSegmentStatus(FAILED,
                                -1L));

        CompletableFuture<CurrentTransferSegmentStatus> notTransferred =
                CompletableFuture.completedFuture
                        (new CurrentTransferSegmentStatus(NOT_TRANSFERRED,
                                -1L));

        // old segment had a failed transfer, segments match completely -> preserve
        CurrentTransferSegment oldSegment = new CurrentTransferSegment(0L, 5L, failed);

        CurrentTransferSegment newSegment = new CurrentTransferSegment(0L, 5L, notTransferred);

        ImmutableList<CurrentTransferSegment> oldList = ImmutableList.of(oldSegment);
        ImmutableList<CurrentTransferSegment> newList = ImmutableList.of(newSegment);

        assertThat(calculator.mergeLists(oldList, newList).get(0).getStatus().join()
                .getSegmentState())
                .isEqualTo(FAILED);
    }

    @Test
    public void testMergeSegmentsMatchInProgress() {

        RedundancyCalculator calculator = new RedundancyCalculator("localhost");


        // old segment has a running transfer, segments match -> preserve
        CompletableFuture<CurrentTransferSegmentStatus> inProgress =
                CompletableFuture.supplyAsync(() -> {
                    Sleep.sleepUninterruptibly(Duration.ofMillis(2000));
                    return new CurrentTransferSegmentStatus(FAILED,
                            -1L);
                });

        CompletableFuture<CurrentTransferSegmentStatus> notTransferred =
                CompletableFuture.completedFuture
                        (new CurrentTransferSegmentStatus(NOT_TRANSFERRED,
                                -1L));

        CurrentTransferSegment oldSegment = new CurrentTransferSegment(0L,
                5L, inProgress);

        // new map is not transferred
        CurrentTransferSegment newSegment = new CurrentTransferSegment(0L,
                5L, notTransferred);

        ImmutableList<CurrentTransferSegment> oldList = ImmutableList.of(oldSegment);

        ImmutableList<CurrentTransferSegment> newList = ImmutableList.of(newSegment);

        assertThat(calculator.mergeLists(oldList, newList).get(0).getStatus().isDone()).isFalse();
    }

    @Test
    public void testMergeSegmentsMatchExceptional() {

        RedundancyCalculator calculator = new RedundancyCalculator("localhost");


        CompletableFuture<CurrentTransferSegmentStatus> notTransferred =
                CompletableFuture.completedFuture
                        (new CurrentTransferSegmentStatus(NOT_TRANSFERRED,
                                -1L));

        // old map had an exceptionally failed transfer, segments match -> preserve
        CompletableFuture<CurrentTransferSegmentStatus> exceptional =
                CompletableFuture.supplyAsync(() -> {
                    throw new RuntimeException();
                });

        CurrentTransferSegment oldSegment = new CurrentTransferSegment(0L, 5L, exceptional);

        CurrentTransferSegment newSegment = new CurrentTransferSegment(0L, 5L, notTransferred);

        ImmutableList<CurrentTransferSegment> oldList = ImmutableList.of(oldSegment);
        ImmutableList<CurrentTransferSegment> newList = ImmutableList.of(newSegment);

        Sleep.sleepUninterruptibly(Duration.ofMillis(200));
        assertThat(calculator.mergeLists(oldList, newList).get(0).getStatus().isCompletedExceptionally())
                .isTrue();
    }

    @Test
    public void testMergeMapsMatchRestored() {

        RedundancyCalculator calculator = new RedundancyCalculator("localhost");


        CompletableFuture<CurrentTransferSegmentStatus> notTransferred =
                CompletableFuture.completedFuture
                        (new CurrentTransferSegmentStatus(NOT_TRANSFERRED,
                                -1L));

        // new segment is restored, segments match -> update
        CompletableFuture<CurrentTransferSegmentStatus> restored =
                CompletableFuture.completedFuture
                        (new CurrentTransferSegmentStatus(RESTORED,
                                -1L));

        CurrentTransferSegment oldSegment = new CurrentTransferSegment(0L, 5L, notTransferred);

        CurrentTransferSegment newSegment = new CurrentTransferSegment(0L, 5L, restored);

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

        CompletableFuture<CurrentTransferSegmentStatus> notTransferred =
                CompletableFuture.completedFuture
                        (new CurrentTransferSegmentStatus(NOT_TRANSFERRED,
                                -1L));
        CurrentTransferSegment segment1 = new CurrentTransferSegment(0L, 5L, notTransferred);
        CurrentTransferSegment segment2 = new CurrentTransferSegment(6L, 10L, notTransferred);

        assertThat(calculator.mergeLists(ImmutableList.of(segment1), ImmutableList.of(segment2)))
                .contains(segment1).contains(segment2);

    }

    @Test
    public void testMergeMapsNewIsSubset() {
        RedundancyCalculator calculator = new RedundancyCalculator("localhost");


        CompletableFuture<CurrentTransferSegmentStatus> notTransferred =
                CompletableFuture.completedFuture
                        (new CurrentTransferSegmentStatus(NOT_TRANSFERRED,
                                -1L));

        CompletableFuture<CurrentTransferSegmentStatus> restored =
                CompletableFuture.completedFuture
                        (new CurrentTransferSegmentStatus(RESTORED,
                                -1L));

        CurrentTransferSegment oldSegment = new CurrentTransferSegment(0L, 5L, notTransferred);
        CurrentTransferSegment newSegment = new CurrentTransferSegment(3L, 5L, restored);

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
        CompletableFuture<CurrentTransferSegmentStatus> notTransferred =
                CompletableFuture.completedFuture
                        (new CurrentTransferSegmentStatus(NOT_TRANSFERRED,
                                -1L));

        CompletableFuture<CurrentTransferSegmentStatus> restored =
                CompletableFuture.completedFuture
                        (new CurrentTransferSegmentStatus(RESTORED,
                                -1L));

        CurrentTransferSegment oldSegment = new CurrentTransferSegment(0L, 5L, notTransferred);
        CurrentTransferSegment newSegment = new CurrentTransferSegment(0L, 15L, restored);

        assertThat(calculator.mergeLists(ImmutableList.of(oldSegment), ImmutableList.of(newSegment))
                .get(0)
                .getStatus()
                .join().getSegmentState()).isEqualTo(RESTORED);
    }

    @Test
    public void testMergeMapsNewIsPrefixedAndMerged() {
        RedundancyCalculator calculator = new RedundancyCalculator("localhost");

        // oldSegment is transferred and newsegment is restored
        CompletableFuture<CurrentTransferSegmentStatus> notTransferred =
                CompletableFuture.completedFuture
                        (new CurrentTransferSegmentStatus(NOT_TRANSFERRED,
                                -1L));

        CompletableFuture<CurrentTransferSegmentStatus> restored =
                CompletableFuture.completedFuture
                        (new CurrentTransferSegmentStatus(RESTORED,
                                -1L));

        CurrentTransferSegment oldSegment = new CurrentTransferSegment(0L, 5L, notTransferred);
        CurrentTransferSegment newSegment = new CurrentTransferSegment(3L, 15L, restored);

        assertThat(calculator.mergeLists(ImmutableList.of(oldSegment), ImmutableList.of(newSegment))
                .get(0)
                .getStatus()
                .join().getSegmentState()).isEqualTo(RESTORED);
    }

    @Test
    public void testMergeMapsNewMapIsEmpty() {
        RedundancyCalculator calculator = new RedundancyCalculator("localhost");

        // oldSegment is transferred and new segment is non existent
        CompletableFuture<CurrentTransferSegmentStatus> transferred =
                CompletableFuture.completedFuture
                        (new CurrentTransferSegmentStatus(TRANSFERRED,
                                -1L));

        CurrentTransferSegment oldSegment = new CurrentTransferSegment(0L, 5L, transferred);

        assertThat(calculator.mergeLists(ImmutableList.of(oldSegment),
                ImmutableList.of()).get(0).getStatus()
                .join().getSegmentState()).isEqualTo(RESTORED);


    }


    private CurrentTransferSegment dataToSegment(SegmentData data){
        long begin = data.range.getKey();
        long end = data.range.getValue();
        SegmentState state = data.state;
        boolean inProgress = data.inProgress;
        boolean failed = data.failed;

        if(inProgress){
            return new CurrentTransferSegment(begin, end, CompletableFuture.supplyAsync(() -> {
                Sleep.sleepUninterruptibly(Duration.ofMillis(5000));
                return null;
            }));
        }
        if(failed){
            return new CurrentTransferSegment(begin, end, CompletableFuture.supplyAsync(() -> {
                throw new RuntimeException("failed");
            }));
        }
        return new CurrentTransferSegment(begin, end, CompletableFuture.completedFuture(new CurrentTransferSegmentStatus(state, -1L)));
    }

    private SegmentData segmentToData(CurrentTransferSegment segment){
        if(!segment.getStatus().isDone()){
            return new SegmentData(new SimpleEntry<>(segment.getStartAddress(), segment.getEndAddress()), true, false, NOT_TRANSFERRED);
        }
        if(segment.getStatus().isCompletedExceptionally()){
            return new SegmentData(new SimpleEntry<>(segment.getStartAddress(), segment.getEndAddress()), false, true, NOT_TRANSFERRED);
        }
        return new SegmentData(new SimpleEntry<>(segment.getStartAddress(), segment.getEndAddress()), false, false, segment.getStatus().join().getSegmentState());
    }

    @Builder
    @AllArgsConstructor
    @ToString
    @EqualsAndHashCode
    private static class SegmentData{
        public final Map.Entry<Long, Long> range;
        public boolean inProgress;
        public boolean failed;
        public SegmentState state;
    }

    @Test
    public void testMergeComplex(){
        // old list:
        // 0 -> 10 transferred, 11 -> 12 is not done, 13 -> 16 completed exceptionally, 18 -> 20 failed, 21 -> 25 restored
        // new list:
        // 5 -> 10 restored, 11 -> 12 not transferred, 13 -> 16 is not transferred, 18 -> 20 not transferred, 21 -> 25 not transferred, 26 -> 30 restored
        // result:
        // 5 -> 10 restored, 11 -> 12 is not done, 13 -> 16 completed exceptionally, 18 -> 20 failed, 21 -> 25 restored, 26 -> 30 restored

        // Old list:
        List<CurrentTransferSegment> oldList = Arrays.asList(
                SegmentData.builder().range(new SimpleEntry<>(0L, 10L)).state(TRANSFERRED).build(),
                SegmentData.builder().range(new SimpleEntry<>(11L, 12L)).state(NOT_TRANSFERRED).inProgress(true).build(),
                SegmentData.builder().range(new SimpleEntry<>(13L, 16L)).state(NOT_TRANSFERRED).failed(true).build(),
                SegmentData.builder().range(new SimpleEntry<>(18L, 20L)).state(FAILED).build(),
                SegmentData.builder().range(new SimpleEntry<>(21L, 25L)).state(RESTORED).build()
                ).stream()
                .map(data -> dataToSegment(data)).collect(Collectors.toList());

        // New list:
        List<CurrentTransferSegment> newList = Arrays.asList(
                SegmentData.builder().range(new SimpleEntry<>(5L, 10L)).state(RESTORED).build(),
                SegmentData.builder().range(new SimpleEntry<>(11L, 12L)).state(NOT_TRANSFERRED).build(),
                SegmentData.builder().range(new SimpleEntry<>(13L, 16L)).state(NOT_TRANSFERRED).build(),
                SegmentData.builder().range(new SimpleEntry<>(18L, 20L)).state(NOT_TRANSFERRED).build(),
                SegmentData.builder().range(new SimpleEntry<>(21L, 25L)).state(NOT_TRANSFERRED).build(),
                SegmentData.builder().range(new SimpleEntry<>(26L, 30L)).state(RESTORED).build()
                ).stream()
                .map(data -> dataToSegment(data)).collect(Collectors.toList());

        RedundancyCalculator calculator = new RedundancyCalculator("localhost");
        ImmutableList<SegmentData> resultList = ImmutableList.copyOf(calculator.mergeLists(oldList, newList).stream()
                .map(segment -> segmentToData(segment)).collect(Collectors.toList()));

        // Expected:
        ImmutableList<SegmentData> expected = ImmutableList.copyOf(Arrays.asList(
                SegmentData.builder().range(new SimpleEntry<>(5L, 10L)).state(RESTORED).build(),
                SegmentData.builder().range(new SimpleEntry<>(11L, 12L)).state(NOT_TRANSFERRED).inProgress(true).build(),
                SegmentData.builder().range(new SimpleEntry<>(13L, 16L)).state(NOT_TRANSFERRED).failed(true).build(),
                SegmentData.builder().range(new SimpleEntry<>(18L, 20L)).state(FAILED).build(),
                SegmentData.builder().range(new SimpleEntry<>(21L, 25L)).state(RESTORED).build(),
                SegmentData.builder().range(new SimpleEntry<>(26L, 30L)).state(RESTORED).build()
        ));

        assertThat(resultList).isEqualTo(expected);

    }

    @Test
    public void testCanMergeSegmentsOneSegment(){
        LayoutSegment segment = new LayoutSegment(CHAIN_REPLICATION, 0L, 1L,
                Arrays.asList(new LayoutStripe(Arrays.asList("localhost"))));
        assertThat(RedundancyCalculator
                .canMergeSegments(createTestLayout(Arrays.asList(segment)), "localhost")).isFalse();
    }

    @Test
    public void testCanMergeSegmentsServerNonPresent(){
        LayoutSegment segment1 = new LayoutSegment(CHAIN_REPLICATION, 0L, 1L,
                Arrays.asList(new LayoutStripe(Arrays.asList("localhost", "B"))));

        LayoutSegment segment2 = new LayoutSegment(CHAIN_REPLICATION, 0L, 1L,
                Arrays.asList(new LayoutStripe(Arrays.asList("C", "A"))));
        assertThat(RedundancyCalculator
                .canMergeSegments(createTestLayout(Arrays.asList(segment1, segment2)), "localhost")).isFalse();
    }

    @Test
    public void testCanMergeEmptyNonEmptySetDifference(){
        LayoutSegment segment1 = new LayoutSegment(CHAIN_REPLICATION, 0L, 1L,
                Arrays.asList(new LayoutStripe(Arrays.asList("localhost", "B"))));

        LayoutSegment segment2 = new LayoutSegment(CHAIN_REPLICATION, 0L, 1L,
                Arrays.asList(new LayoutStripe(Arrays.asList("C", "B", "localhost"))));
        assertThat(RedundancyCalculator
                .canMergeSegments(createTestLayout(Arrays.asList(segment1, segment2)), "localhost")).isFalse();
    }

    @Test
    public void testCanMergeSegmentsDoMerge(){
        LayoutSegment segment1 = new LayoutSegment(CHAIN_REPLICATION, 0L, 1L,
                Arrays.asList(new LayoutStripe(Arrays.asList("localhost", "B"))));

        LayoutSegment segment2 = new LayoutSegment(CHAIN_REPLICATION, 0L, 1L,
                Arrays.asList(new LayoutStripe(Arrays.asList("B", "localhost"))));
        assertThat(RedundancyCalculator
                .canMergeSegments(createTestLayout(Arrays.asList(segment1, segment2)), "localhost")).isTrue();

    }

}
