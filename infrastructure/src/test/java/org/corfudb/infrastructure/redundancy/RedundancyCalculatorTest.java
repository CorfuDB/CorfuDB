package org.corfudb.infrastructure.redundancy;

import com.google.common.collect.ImmutableList;
import org.corfudb.infrastructure.LayoutBasedTestHelper;
import org.corfudb.infrastructure.log.statetransfer.segment.StateTransferType;
import org.corfudb.infrastructure.log.statetransfer.segment.TransferSegment;
import org.corfudb.infrastructure.log.statetransfer.segment.TransferSegmentRange;
import org.corfudb.infrastructure.log.statetransfer.segment.TransferSegmentRangeSingle;
import org.corfudb.infrastructure.log.statetransfer.segment.TransferSegmentRangeSplit;
import org.corfudb.infrastructure.log.statetransfer.segment.TransferSegmentStatus;
import org.corfudb.infrastructure.log.statetransfer.TransferSegmentCreator;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.Layout.LayoutSegment;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.corfudb.infrastructure.log.statetransfer.segment.TransferSegmentStatus.SegmentState.NOT_TRANSFERRED;
import static org.corfudb.infrastructure.log.statetransfer.segment.TransferSegmentStatus.SegmentState.RESTORED;
import static org.corfudb.infrastructure.log.statetransfer.segment.TransferSegmentStatus.SegmentState.TRANSFERRED;
import static org.corfudb.runtime.view.Address.NON_ADDRESS;
import static org.corfudb.runtime.view.Layout.LayoutStripe;
import static org.corfudb.runtime.view.Layout.ReplicationMode.CHAIN_REPLICATION;

public class RedundancyCalculatorTest extends LayoutBasedTestHelper implements TransferSegmentCreator {

    @Test
    public void testCreateStateListTrimMarkNotMoved() {
        RedundancyCalculator redundancyCalculator =
                new RedundancyCalculator("localhost");

        Layout layout = createNonPresentLayout();

        ImmutableList<LayoutBasedTestHelper.MockedSegment> expected = ImmutableList.of(
                new MockedSegment(0L, 1L,
                        createStatus(NOT_TRANSFERRED, Optional.empty())),
                new MockedSegment(2L, 3L,
                        createStatus(NOT_TRANSFERRED, Optional.empty())));

        ImmutableList<TransferSegment> result = redundancyCalculator
                .createStateList(layout, -1L);

        assertThat(transformListToMock(result)).isEqualTo(expected);

        layout = createPresentLayout();

        expected = ImmutableList.of(
                new MockedSegment(0L, 1L,
                        createStatus(NOT_TRANSFERRED, Optional.empty())),
                new MockedSegment(2L, 3L,
                        createStatus(RESTORED, Optional.empty())));

        result = redundancyCalculator.createStateList(layout, -1L);

        assertThat(transformListToMock(result))
                .isEqualTo(expected);
    }

    @Test
    public void testCreateStateListTrimMarkIntersectsSegment() {
        RedundancyCalculator redundancyCalculator =
                new RedundancyCalculator("localhost");

        Layout layout = createNonPresentLayout();

        // node is not present anywhere, trim mark starts from the middle of a second segment ->
        // transfer half of second and third segments
        ImmutableList<MockedSegment> expected =
                ImmutableList.of(
                        new MockedSegment(3L, 3L,
                                createStatus(NOT_TRANSFERRED, Optional.empty())));


        List<TransferSegment> result = redundancyCalculator.createStateList(layout, 3L);

        assertThat(transformListToMock(result)).isEqualTo(expected);

        layout = createPresentLayout();

        expected = ImmutableList.of(
                new MockedSegment(3L, 3L,
                        createStatus(RESTORED, Optional.empty())));

        result = redundancyCalculator.createStateList(layout, 3L);

        assertThat(transformListToMock(result)).isEqualTo(expected);

    }

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
        LayoutStripe stripe0 = new LayoutStripe(Collections.singletonList("A"));
        LayoutStripe stripe1 = new LayoutStripe(Arrays.asList("A", "B"));
        LayoutStripe stripe2 = new LayoutStripe(Arrays.asList("A", "B", "localhost"));
        LayoutSegment segment0 = new LayoutSegment(CHAIN_REPLICATION, 0L, 2L,
                Collections.singletonList(stripe0));
        LayoutSegment segment1 = new LayoutSegment(CHAIN_REPLICATION, 3L, 6L,
                Collections.singletonList(stripe1));
        LayoutSegment segment2 = new LayoutSegment(CHAIN_REPLICATION, 6L, 9L,
                Collections.singletonList(stripe2));

        // Not required, since A is present
        Layout layout = createTestLayout(Arrays.asList(segment1, segment2));
        assertThat(RedundancyCalculator.canRestoreRedundancy(layout, "A"))
                .isFalse();
        // Not required, since the layout consists of only one segment
        layout = createTestLayout(Collections.singletonList(segment1));
        assertThat(RedundancyCalculator.canRestoreRedundancy(layout, "A"))
                .isFalse();
        // Required, since node B is not present at the beginning
        layout = createTestLayout(Arrays.asList(segment0, segment1, segment2));
        assertThat(RedundancyCalculator.canRestoreRedundancy(layout, "B"))
                .isTrue();
        // Required, since localhost is not present in the second last segment
        layout = createTestLayout(Arrays.asList(segment0, segment2));
        assertThat(RedundancyCalculator.canRestoreRedundancy(layout, "localhost"))
                .isTrue();


    }

    @Test
    public void testCreateStateListComplex() {

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

        RedundancyCalculator calculator = new RedundancyCalculator("localhost");

        ImmutableList<TransferSegment> transferSegments =
                calculator.createStateList(testLayout, -1L);

        MockedSegment presentSegment = new MockedSegment(0L,
                20L,
                TransferSegmentStatus.builder().segmentState(RESTORED).build());


        MockedSegment nonPresentSegment = new MockedSegment(21L,
                50L, TransferSegmentStatus.builder().segmentState(NOT_TRANSFERRED).build());

        Assert.assertTrue(transformListToMock(transferSegments).containsAll(Arrays.asList(presentSegment,
                nonPresentSegment)));

        TransferSegmentStatus presentSegmentStatus = presentSegment.status;
        assertThat(RESTORED)
                .isEqualTo(presentSegmentStatus.getSegmentState());

        TransferSegmentStatus nonPresentSegmentStatus = nonPresentSegment.status;

        assertThat(NOT_TRANSFERRED)
                .isEqualTo(nonPresentSegmentStatus.getSegmentState());
    }

    @Test
    public void testRestoreRedundancyForSegment() {
        TransferSegment segment = createTransferSegment(0L, 20L, RESTORED);

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

        List<TransferSegment> transferSegments = Arrays.asList
                (createTransferSegment(0L, 20L, RESTORED),
                        createTransferSegment(21L, 50L, RESTORED),
                        createTransferSegment(51L, 60L, RESTORED));


        RedundancyCalculator calculator = new RedundancyCalculator("localhost");

        Layout consolidatedLayout =
                calculator.updateLayoutAfterRedundancyRestoration(transferSegments,
                        testLayout);

        assertThat(consolidatedLayout.getSegments().stream())
                .allMatch(segment -> segment.getFirstStripe().getLogServers().contains("localhost"));

    }

    @Test
    public void testCanMergeSegmentsOneSegment() {
        LayoutSegment segment = new LayoutSegment(CHAIN_REPLICATION, 0L, 1L,
                Arrays.asList(new LayoutStripe(Arrays.asList("localhost"))));
        assertThat(RedundancyCalculator
                .canMergeSegments(createTestLayout(Arrays.asList(segment)))).isFalse();
    }

    @Test
    public void testCanMergeSegmentsServersNonPresent() {
        LayoutSegment segment1 = new LayoutSegment(CHAIN_REPLICATION, 0L, 1L,
                Arrays.asList(new LayoutStripe(Arrays.asList("localhost", "B"))));

        LayoutSegment segment2 = new LayoutSegment(CHAIN_REPLICATION, 0L, 1L,
                Arrays.asList(new LayoutStripe(Arrays.asList("C", "A"))));
        assertThat(RedundancyCalculator
                .canMergeSegments(createTestLayout(Arrays.asList(segment1, segment2)))).isFalse();
    }

    @Test
    public void testCanMergeSegmentsServerNotPresent() {
        LayoutSegment segment1 = new LayoutSegment(CHAIN_REPLICATION, 0L, 1L,
                Arrays.asList(new LayoutStripe(Arrays.asList("localhost", "B"))));

        LayoutSegment segment2 = new LayoutSegment(CHAIN_REPLICATION, 0L, 1L,
                Arrays.asList(new LayoutStripe(Arrays.asList("C", "B", "localhost"))));
        assertThat(RedundancyCalculator
                .canMergeSegments(createTestLayout(Arrays.asList(segment1, segment2)))).isFalse();
    }

    @Test
    public void testCanMergeSegmentsDoMerge() {
        LayoutSegment segment1 = new LayoutSegment(CHAIN_REPLICATION, 0L, 1L,
                Arrays.asList(new LayoutStripe(Arrays.asList("localhost", "B"))));

        LayoutSegment segment2 = new LayoutSegment(CHAIN_REPLICATION, 0L, 1L,
                Arrays.asList(new LayoutStripe(Arrays.asList("B", "localhost"))));
        assertThat(RedundancyCalculator
                .canMergeSegments(createTestLayout(Arrays.asList(segment1, segment2)))).isTrue();

    }

    @Test
    public void testSegmentVerification() {
        TransferSegmentStatus status = TransferSegmentStatus.builder().build();

        // start can't be < 0L.
        assertThatThrownBy(() ->
                TransferSegment.builder()
                        .startAddress(NON_ADDRESS)
                        .endAddress(0)
                        .status(status)
                        .build()).isInstanceOf(IllegalStateException.class);

        // end can't be < 0L.
        assertThatThrownBy(() ->
                TransferSegment.builder()
                        .startAddress(0)
                        .endAddress(NON_ADDRESS)
                        .status(status)
                        .build()).isInstanceOf(IllegalStateException.class);

        // start can't be greater than end.
        assertThatThrownBy(() ->
                TransferSegment.builder()
                        .startAddress(3)
                        .endAddress(2)
                        .status(status)
                        .build()).isInstanceOf(IllegalStateException.class);

        // status should be defined.
        assertThatThrownBy(() ->
                TransferSegment.builder()
                        .startAddress(0L)
                        .endAddress(1L)
                        .status(null)
                        .build()).isInstanceOf(IllegalStateException.class);


    }

    private ImmutableList<TransferSegment> getTestSegments() {
        TransferSegment firstSegment = TransferSegment
                .builder()
                .logUnitServers(ImmutableList.of("A", "B", "C"))
                .startAddress(150L)
                .endAddress(300L)
                .status(TransferSegmentStatus.builder().segmentState(RESTORED).build())
                .build();
        TransferSegment secondSegment = TransferSegment
                .builder()
                .logUnitServers(ImmutableList.of("A", "C"))
                .startAddress(301L)
                .endAddress(500L)
                .status(TransferSegmentStatus.builder().build())
                .build();
        return ImmutableList.of(firstSegment, secondSegment);
    }

    private boolean singleRangeIsCorrect(TransferSegmentRangeSingle range,
                                         Predicate<Long> startAddressTest,
                                         Predicate<Long> endAddressTest,
                                         Predicate<Optional<ImmutableList<String>>> availableServersTest,
                                         Predicate<StateTransferType> transferTypeTest,
                                         Predicate<TransferSegmentStatus> segmentStatusTest) {
        return startAddressTest.test(range.getStartAddress()) &&
                endAddressTest.test(range.getEndAddress()) &&
                availableServersTest.test(range.getAvailableServers()) &&
                transferTypeTest.test(range.getTypeOfTransfer()) &&
                segmentStatusTest.test(range.getStatus());
    }

    @Test
    public void testPrepareTransferWorkloadNoCommittedTail() {
        ImmutableList<TransferSegment> testSegments = getTestSegments();
        RedundancyCalculator calc = new RedundancyCalculator("test");
        // CT is not present.
        // Every segment is a non-committed single range.
        Optional<Long> tail = Optional.empty();
        ImmutableList<TransferSegmentRange> transferSegmentRanges =
                calc.prepareTransferWorkload(testSegments, tail);
        assertThat(transferSegmentRanges.size()).isEqualTo(2);
        for (int i = 0; i < transferSegmentRanges.size(); i++) {
            TransferSegmentRangeSingle transferSegmentRange = (TransferSegmentRangeSingle) transferSegmentRanges.get(i);
            final long startAddress = testSegments.get(i).getStartAddress();
            final long endAddress = testSegments.get(i).getEndAddress();
            final StateTransferType type = StateTransferType.PROTOCOL_READ;
            final TransferSegmentStatus status = testSegments.get(i).getStatus();

            assertThat(singleRangeIsCorrect(transferSegmentRange,
                    sa -> sa == startAddress,
                    ea -> ea == endAddress,
                    lus -> !lus.isPresent(),
                    t -> t == type,
                    s -> s.equals(status))).isTrue();
        }
    }

    @Test
    public void testPrepareTransferWorkloadCommittedTailBeforeStartAddress() {
        ImmutableList<TransferSegment> testSegments = getTestSegments();
        // Range starts at 150L but CT is at 149L.
        // Every segment is a non-committed range and none are split.
        Optional<Long> tail = Optional.of(149L);
        RedundancyCalculator calc = new RedundancyCalculator("test");
        ImmutableList<TransferSegmentRange> transferSegmentRanges =
                calc.prepareTransferWorkload(testSegments, tail);
        for (int i = 0; i < transferSegmentRanges.size(); i++) {
            TransferSegmentRangeSingle transferSegmentRange = (TransferSegmentRangeSingle) transferSegmentRanges.get(i);
            final long startAddress = testSegments.get(i).getStartAddress();
            final long endAddress = testSegments.get(i).getEndAddress();
            final StateTransferType type = StateTransferType.PROTOCOL_READ;
            final TransferSegmentStatus status = testSegments.get(i).getStatus();

            assertThat(singleRangeIsCorrect(transferSegmentRange,
                    sa -> sa == startAddress,
                    ea -> ea == endAddress,
                    lus -> !lus.isPresent(),
                    t -> t == type,
                    s -> s.equals(status))).isTrue();
        }
    }

    @Test
    public void testPrepareTransferWorkloadCommittedTailEqualsStartAddress() {
        ImmutableList<TransferSegment> testSegments = getTestSegments();
        // Range starts at 150L and CT starts at 150L ->
        // only one address is committed and first segment is split into two ranges.
        Optional<Long> tail = Optional.of(150L);
        RedundancyCalculator calc = new RedundancyCalculator("test");
        ImmutableList<TransferSegmentRange> transferSegmentRanges =
                calc.prepareTransferWorkload(testSegments, tail);
        assertThat(transferSegmentRanges.size()).isEqualTo(2);
        TransferSegmentRangeSplit splitRange1 = (TransferSegmentRangeSplit) transferSegmentRanges.get(0);
        // Check first range.
        assertThat(singleRangeIsCorrect(
                splitRange1.getSplitSegments().first,
                sa -> sa == 150L,
                ea -> ea == 150L,
                lus -> lus.get().equals(testSegments.get(0).getLogUnitServers()),
                t -> t == StateTransferType.CONSISTENT_READ,
                s -> s.equals(testSegments.get(0).getStatus()))).isTrue();
        // Check second range.
        assertThat(singleRangeIsCorrect(
                splitRange1.getSplitSegments().second,
                sa -> sa == 151L,
                ea -> ea == 300L,
                lus -> !lus.isPresent(),
                t -> t == StateTransferType.PROTOCOL_READ,
                s -> s.equals(testSegments.get(0).getStatus()))).isTrue();

        // Check last range.
        TransferSegmentRangeSingle range2 = (TransferSegmentRangeSingle) transferSegmentRanges.get(1);
        assertThat(singleRangeIsCorrect(
                range2,
                sa -> sa == testSegments.get(1).getStartAddress(),
                ea -> ea == testSegments.get(1).getEndAddress(),
                lus -> !lus.isPresent(),
                t -> t == StateTransferType.PROTOCOL_READ,
                s -> s.equals(testSegments.get(1).getStatus()))).isTrue();
    }

    @Test
    public void testPrepareTransferWorkloadCommittedTailAfterEndAddress() {
        ImmutableList<TransferSegment> testSegments = getTestSegments();
        // Ranges end at 500L and CT starts at 501L.
        // Every segment is a committed range and none are split.
        Optional<Long> tail = Optional.of(501L);
        RedundancyCalculator calc = new RedundancyCalculator("test");
        ImmutableList<TransferSegmentRange> transferSegmentRanges =
                calc.prepareTransferWorkload(testSegments, tail);
        assertThat(transferSegmentRanges.size()).isEqualTo(2);
        for (int i = 0; i < transferSegmentRanges.size(); i++) {
            TransferSegmentRangeSingle transferSegmentRange = (TransferSegmentRangeSingle) transferSegmentRanges.get(i);
            final long startAddress = testSegments.get(i).getStartAddress();
            final long endAddress = testSegments.get(i).getEndAddress();
            final StateTransferType type = StateTransferType.CONSISTENT_READ;
            final TransferSegmentStatus status = testSegments.get(i).getStatus();
            final ImmutableList<String> logUnitServers = testSegments.get(i).getLogUnitServers();
            assertThat(singleRangeIsCorrect(transferSegmentRange,
                    sa -> sa == startAddress,
                    ea -> ea == endAddress,
                    lus -> lus.get().equals(logUnitServers),
                    t -> t == type,
                    s -> s.equals(status))).isTrue();
        }
    }

    @Test
    public void testPrepareTransferWorkloadCommittedTailEqualEndAddress() {
        ImmutableList<TransferSegment> testSegments = getTestSegments();
        // Ranges end at 500L and CT starts at 500L.
        // Every segment is a committed range and none are split.
        Optional<Long> tail = Optional.of(500L);
        RedundancyCalculator calc = new RedundancyCalculator("test");
        ImmutableList<TransferSegmentRange> transferSegmentRanges =
                calc.prepareTransferWorkload(testSegments, tail);
        assertThat(transferSegmentRanges.size()).isEqualTo(2);
        for (int i = 0; i < transferSegmentRanges.size(); i++) {
            TransferSegmentRangeSingle transferSegmentRange = (TransferSegmentRangeSingle) transferSegmentRanges.get(i);
            final long startAddress = testSegments.get(i).getStartAddress();
            final long endAddress = testSegments.get(i).getEndAddress();
            final StateTransferType type = StateTransferType.CONSISTENT_READ;
            final TransferSegmentStatus status = testSegments.get(i).getStatus();
            final ImmutableList<String> logUnitServers = testSegments.get(i).getLogUnitServers();
            assertThat(singleRangeIsCorrect(transferSegmentRange,
                    sa -> sa == startAddress,
                    ea -> ea == endAddress,
                    lus -> lus.get().equals(logUnitServers),
                    t -> t == type,
                    s -> s.equals(status))).isTrue();
        }
    }

    @Test
    public void testPrepareTransferWorkloadCommittedTailSplitsSegment() {
        ImmutableList<TransferSegment> testSegments = getTestSegments();
        // Ranges end at 500L and CT starts at 400L.
        // (150, 300), (301, 500) ->
        // (150, 300, CONSISTENT_READ), (301, 400, CONSISTENT_READ), (401, 500, PROTOCOL_READ).
        Optional<Long> tail = Optional.of(400L);
        RedundancyCalculator calc = new RedundancyCalculator("test");
        ImmutableList<TransferSegmentRange> transferSegmentRanges =
                calc.prepareTransferWorkload(testSegments, tail);
        assertThat(transferSegmentRanges.size()).isEqualTo(2);
        TransferSegmentRangeSingle range1 = (TransferSegmentRangeSingle) transferSegmentRanges.get(0);
        // Check first range.
        assertThat(singleRangeIsCorrect(
                range1,
                sa -> sa == 150L,
                ea -> ea == 300L,
                lus -> lus.get().equals(testSegments.get(0).getLogUnitServers()),
                t -> t == StateTransferType.CONSISTENT_READ,
                s -> s.equals(testSegments.get(0).getStatus()))).isTrue();
        // Check second range.
        TransferSegmentRangeSplit splitRange2 = (TransferSegmentRangeSplit) transferSegmentRanges.get(1);

        assertThat(singleRangeIsCorrect(
                splitRange2.getSplitSegments().first,
                sa -> sa == testSegments.get(1).getStartAddress(),
                ea -> ea == 400L,
                lus -> lus.get().equals(testSegments.get(1).getLogUnitServers()),
                t -> t == StateTransferType.CONSISTENT_READ,
                s -> s.equals(testSegments.get(1).getStatus()))).isTrue();

        // Check last range.
        assertThat(singleRangeIsCorrect(
                splitRange2.getSplitSegments().second,
                sa -> sa == 401L,
                ea -> ea == testSegments.get(1).getEndAddress(),
                lus -> !lus.isPresent(),
                t -> t == StateTransferType.PROTOCOL_READ,
                s -> s.equals(testSegments.get(1).getStatus()))).isTrue();
    }

    @Test
    public void testGetTrimmedNotRestoredSegments() {
        Layout.ReplicationMode replicationMode = CHAIN_REPLICATION;
        LayoutStripe layoutStripe1 = new LayoutStripe(ImmutableList.of("A"));
        LayoutSegment segment1 = new LayoutSegment(replicationMode, 0L, 100L,
                ImmutableList.of(layoutStripe1));
        LayoutStripe layoutStripe2 = new LayoutStripe(ImmutableList.of("B", "C"));
        LayoutSegment segment2 = new LayoutSegment(replicationMode, 100L, 150L,
                ImmutableList.of(layoutStripe2));
        List<String> allServers = ImmutableList.of("A", "B", "C");
        Layout layout = new Layout(allServers, allServers,
                ImmutableList.of(segment1, segment2), ImmutableList.of(), 1L,
                UUID.randomUUID());
        long trimMark = 101L;

        ImmutableList<TransferSegment> trimmedNotRestoredSegmentsForA =
                new RedundancyCalculator("A")
                        .getTrimmedNotRestoredSegments(layout, trimMark);

        assertThat(trimmedNotRestoredSegmentsForA).isEmpty();

        ImmutableList<TransferSegment> trimmedNotRestoredSegmentsForB =
                new RedundancyCalculator("B")
                .getTrimmedNotRestoredSegments(layout, trimMark);

        ImmutableList<TransferSegment> trimmedNotRestoredSegmentsForC =
                new RedundancyCalculator("C")
                        .getTrimmedNotRestoredSegments(layout, trimMark);

        TransferSegmentStatus transferredStatus =
                TransferSegmentStatus.builder().segmentState(TRANSFERRED).build();

        ImmutableList<TransferSegment> expectedList = ImmutableList.of(TransferSegment.builder()
                .startAddress(0L)
                .endAddress(99L)
                .logUnitServers(ImmutableList.copyOf(segment1.getAllLogServers()))
                .status(transferredStatus).build());

        assertThat(trimmedNotRestoredSegmentsForB).isEqualTo(expectedList);

        assertThat(trimmedNotRestoredSegmentsForC).isEqualTo(expectedList);
    }

}
