package org.corfudb.infrastructure.redundancy;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import org.corfudb.infrastructure.log.statetransfer.segment.TransferSegment;
import org.corfudb.infrastructure.log.statetransfer.segment.TransferSegmentRange;
import org.corfudb.infrastructure.log.statetransfer.segment.TransferSegmentStatus;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.Layout.LayoutSegment;
import org.corfudb.runtime.view.Layout.LayoutStripe;
import org.corfudb.runtime.view.LayoutBuilder;

import java.util.List;
import java.util.Optional;
import java.util.function.IntPredicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.corfudb.infrastructure.log.statetransfer.segment.TransferSegmentStatus.SegmentState.NOT_TRANSFERRED;
import static org.corfudb.infrastructure.log.statetransfer.segment.TransferSegmentStatus.SegmentState.RESTORED;
import static org.corfudb.infrastructure.log.statetransfer.segment.TransferSegmentStatus.SegmentState.TRANSFERRED;
import static org.corfudb.runtime.view.Address.NON_ADDRESS;

/**
 * A class used to compute the transfer segments, as well as the layout
 * after the redundancy restoration.
 */
@AllArgsConstructor
public class RedundancyCalculator {

    @NonNull
    @Getter
    private final String server;

    @VisibleForTesting
    boolean segmentContainsServer(LayoutSegment segment, String server) {
        return segment.getFirstStripe().getLogServers().contains(server);
    }


    /**
     * Given a current layout and a restored transfer segment, create a new layout, that
     * contains a restored node.
     *
     * @param transferSegment A restored transfer segment.
     * @param layout          A current layout.
     * @return A new, updated layout.
     */
    @VisibleForTesting
    Layout restoreRedundancyForSegment(TransferSegment transferSegment, Layout layout) {
        List<LayoutSegment> segments = layout.getSegments().stream().map(layoutSegment -> {
            if (layoutSegment.getEnd() == transferSegment.getEndAddress() + 1L) {

                List<LayoutStripe> newStripes = layoutSegment.getStripes()
                        .stream()
                        .map(stripe -> {

                            if (layoutSegment.getFirstStripe().equals(stripe)) {
                                ImmutableList<String> servers = new ImmutableList
                                        .Builder<String>()
                                        .addAll(stripe.getLogServers())
                                        .add(server)
                                        .build();

                                return new LayoutStripe(servers);
                            }

                            return stripe;

                        })
                        .collect(Collectors.toList());

                return new LayoutSegment(
                        layoutSegment.getReplicationMode(), layoutSegment.getStart(),
                        layoutSegment.getEnd(), newStripes);
            } else {
                return new LayoutSegment(
                        layoutSegment.getReplicationMode(), layoutSegment.getStart(),
                        layoutSegment.getEnd(), layoutSegment.getStripes());
            }
        }).collect(Collectors.toList());
        Layout newLayout = new Layout(layout);
        LayoutBuilder builder = new LayoutBuilder(newLayout);
        return builder.setSegments(segments).build();
    }

    /**
     * Updates the initial layout given the restored transferred segments.
     *
     * @param segments   A list of restored transferred segments.
     * @param initLayout Initial layout.
     * @return A new, updated layout.
     */
    public Layout updateLayoutAfterRedundancyRestoration(
            List<TransferSegment> segments, Layout initLayout) {
        return segments
                .stream()
                .reduce(initLayout,
                        (layout, segment) -> restoreRedundancyForSegment(segment, layout),
                        (oldLayout, newLayout) -> newLayout);
    }

    /**
     * Returns true if a given server can restore redundancy for this layout.
     * A particular server can restore redundancy for this layout if the segment is split
     * and a set difference of log servers between any two adjacent segments contains this node.
     * This ensures that the absence of a node in a layout is detected in any
     * multiple split segment scenarios which results in the current node being absent
     * closer to the beginning or to the end of a layout segment list.
     *
     * @param layout Current layout.
     * @param server Current server.
     * @return True, if the server is not present in any segment.
     */
    public static boolean canRestoreRedundancy(Layout layout, String server) {
        if (layout.getSegments().size() == 1) {
            // Single segment present in the layout. No segments to merge.
            return false;
        } else {

            IntPredicate nodeNotPresent = currentIndex -> Sets.difference(
                    layout.getSegments().get(currentIndex).getAllLogServers(),
                    layout.getSegments().get(currentIndex - 1).getAllLogServers()
            ).contains(server);

            return IntStream.range(1, layout.getSegments().size())
                    .boxed()
                    .reduce(false,
                            (nodePresentSoFar, currentIndex) ->
                                    nodePresentSoFar || nodeNotPresent.test(currentIndex),
                            (nodePresentBefore, nodePresentNow) ->
                                    nodePresentBefore || nodePresentNow);
        }
    }

    /**
     * Returns true if any server can merge the segments of this layout.
     * Any server can merge the segments of this layout if the segment is split
     * and a set difference of log servers between the first two adjacent segments is empty.
     *
     * @param layout Current layout.
     * @return True if the segments can be merged.
     */
    public static boolean canMergeSegments(Layout layout) {
        if (layout.getSegments().size() == 1) {
            return false;
        } else {
            final int firstSegmentIndex = 0;
            final int secondSegmentIndex = 1;
            return Sets.difference(
                    layout.getSegments().get(secondSegmentIndex).getAllLogServers(),
                    layout.getSegments().get(firstSegmentIndex).getAllLogServers()).isEmpty();
        }
    }

    /**
     * Returns true if any layout restoration action is needed. A restoration action is needed
     * when any server can merge the segments or a current server can restore a layout redundancy.
     *
     * @param layout Current layout.
     * @param server Current server.
     * @return True, if restoration action is needed.
     */
    public static boolean canRestoreRedundancyOrMergeSegments(Layout layout, String server) {
        return RedundancyCalculator.canRestoreRedundancy(layout, server) ||
                RedundancyCalculator.canMergeSegments(layout);
    }

    /**
     * Given a layout, and a global trim mark, creates an initial list
     * of non-empty and bounded transfer segments.
     *
     * @param layout   A current layout.
     * @param trimMark A current global trim mark.
     * @return A list of transfer segments.
     */
    public ImmutableList<TransferSegment> createStateList(Layout layout, long trimMark) {
        return layout.getSegments()
                .stream()
                // Keep all the segments after the trim mark, except the open one.
                .filter(segment -> segment.getEnd() != NON_ADDRESS && segment.getEnd() > trimMark)
                .map(segment -> {
                    // The transfer segment's start is the layout segment's start or a trim mark,
                    // whichever is greater.
                    long segmentStart = Math.max(segment.getStart(), trimMark);
                    // The transfer segment's end should be inclusive.
                    // It is the last address to transfer.
                    long segmentEnd = segment.getEnd() - 1L;

                    if (segmentContainsServer(segment, getServer())) {
                        TransferSegmentStatus restored = TransferSegmentStatus
                                .builder()
                                .segmentState(RESTORED)
                                .build();

                        return TransferSegment
                                .builder()
                                .startAddress(segmentStart)
                                .endAddress(segmentEnd)
                                .status(restored)
                                .logUnitServers(ImmutableList.copyOf(segment.getAllLogServers()))
                                .build();
                    } else {
                        TransferSegmentStatus notTransferred = TransferSegmentStatus
                                .builder()
                                .segmentState(NOT_TRANSFERRED)
                                .build();

                        return TransferSegment
                                .builder()
                                .startAddress(segmentStart)
                                .endAddress(segmentEnd)
                                .status(notTransferred)
                                .logUnitServers(ImmutableList.copyOf(segment.getAllLogServers()))
                                .build();
                    }

                })
                .collect(ImmutableList.toImmutableList());
    }

    /**
     * Get all the layout segments that were trimmed and that do not contain a current server, and
     * return them as TRANSFERRED segments. Since these segments were trimmed, they
     * contain zero transferable addresses, and hence are not eligible for state transfer.
     * However, since they also do not contain a current server, they should be considered
     * TRANSFERRED, eligible for the layout redundancy restoration.
     *
     * @param layout   A current layout.
     * @param trimMark A current global trim mark.
     * @return A list of transfer segments.
     */
    public ImmutableList<TransferSegment> getTrimmedNotRestoredSegments(Layout layout, long trimMark) {
        return layout.getSegments()
                .stream()
                .filter(segment -> segment.getEnd() <= trimMark && !segmentContainsServer(segment, getServer()))
                .map(segment -> {
                    TransferSegmentStatus transferred = TransferSegmentStatus
                            .builder()
                            .segmentState(TRANSFERRED)
                            .build();

                    return TransferSegment
                            .builder()
                            .startAddress(segment.getStart())
                            .endAddress(segment.getEnd() - 1L)
                            .status(transferred)
                            .logUnitServers(ImmutableList.copyOf(segment.getAllLogServers()))
                            .build();
                })
                .collect(ImmutableList.toImmutableList());
    }

    /**
     * Creates the list of transfer segment ranges
     * given the transfer segment list and the committed tail.
     * The ranges are consumed by the state transfer manager
     * in order to execute the state transfer.
     *
     * @param transferSegmentList A list of transfer segments.
     * @param committedTail       An optional committed tail, retrieved earlier.
     * @return A list of transfer segment ranges.
     */
    public ImmutableList<TransferSegmentRange> prepareTransferWorkload(
            ImmutableList<TransferSegment> transferSegmentList,
            Optional<Long> committedTail) {
        return transferSegmentList.stream()
                .map(segment -> segment.toTransferSegmentRange(committedTail))
                .collect(ImmutableList.toImmutableList());
    }

}
