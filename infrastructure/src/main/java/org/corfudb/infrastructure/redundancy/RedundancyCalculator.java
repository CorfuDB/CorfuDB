package org.corfudb.infrastructure.redundancy;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import org.corfudb.infrastructure.log.statetransfer.StateTransferManager.CurrentTransferSegment;
import org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.Layout.LayoutSegment;
import org.corfudb.runtime.view.Layout.LayoutStripe;
import org.corfudb.runtime.view.LayoutBuilder;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.CurrentTransferSegmentStatus;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.FAILED;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.RESTORED;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.TRANSFERRED;
import static org.corfudb.infrastructure.redundancy.RedundancyCalculator.SegmentAge.CURRENT_EPOCH;
import static org.corfudb.infrastructure.redundancy.RedundancyCalculator.SegmentAge.PREVIOUS_EPOCH;

/**
 * A class used to compute the transfer segments, as well as the layout
 * after the redundancy restoration.
 */
@AllArgsConstructor
public class RedundancyCalculator {

    /**
     * A status that tells whether a transfer segment belongs to the current or a previous epoch.
     */
    public enum SegmentAge {
        PREVIOUS_EPOCH,
        CURRENT_EPOCH
    }

    /**
     * A transfer segment that also contains an information about its age.
     */
    @AllArgsConstructor
    @Getter
    @ToString
    @EqualsAndHashCode
    public static class AgedSegment implements Comparable<AgedSegment> {
        private final CurrentTransferSegment segment;
        private final SegmentAge age;

        @Override
        public int compareTo(AgedSegment other) {
            return segment.compareTo(other.segment);
        }
    }


    @NonNull
    @Getter
    private final String server;

    static boolean segmentContainsServer(LayoutSegment segment, String server) {
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
    Layout restoreRedundancyForSegment(CurrentTransferSegment transferSegment, Layout layout) {
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
            List<CurrentTransferSegment> segments, Layout initLayout) {
        return segments
                .stream()
                .reduce(initLayout,
                        (layout, segment) -> restoreRedundancyForSegment(segment, layout),
                        (oldLayout, newLayout) -> newLayout);
    }

    /**
     * Returns true if a given layout requires redundancy restoration.
     *
     * @param layout Current layout.
     * @param server Current server.
     * @return True, if requires.
     */
    public static boolean requiresRedundancyRestoration(Layout layout, String server) {
        if (layout.getSegments().size() == 1) {
            return false;
        } else {
            return layout.getSegments().stream().anyMatch(segment -> !segment.getAllLogServers()
                    .contains(server));
        }
    }

    /**
     * Returns true if after adding a server to the first segment, the segments can be merged.
     *
     * @param layout Current layout.
     * @param server The current node.
     * @return True is there is a redundancy restoration is needed.
     */
    public static boolean requiresMerge(Layout layout, String server) {
        if (layout.getSegments().size() == 1) {
            return false;
        } else {
            int firstSegmentIndex = 0;
            int secondSegmentIndex = 1;
            Layout copy = new Layout(layout);
            LayoutStripe firstStripe = copy.getFirstSegment().getFirstStripe();
            firstStripe.getLogServers().add(server);
            return Sets.difference(
                    copy.getSegments().get(secondSegmentIndex).getAllLogServers(),
                    copy.getSegments().get(firstSegmentIndex).getAllLogServers()).isEmpty();
        }
    }

    /**
     * Returns true if the first and the second segments
     * of this layout on this server can be merged into one.
     *
     * @param layout Current layout.
     * @param server The current node.
     * @return True if the segments can be merged.
     */
    public static boolean canMergeSegments(Layout layout, String server) {
        if (layout.getSegments().size() == 1) {
            return false;
        } else {
            int firstSegmentIndex = 0;
            int secondSegmentIndex = 1;
            boolean serverPresent = IntStream
                    .range(firstSegmentIndex, secondSegmentIndex + 1)
                    .boxed()
                    .allMatch(index -> layout
                            .getSegments()
                            .get(index)
                            .getFirstStripe()
                            .getLogServers()
                            .contains(server));

            return serverPresent && Sets.difference(
                    layout.getSegments().get(secondSegmentIndex).getAllLogServers(),
                    layout.getSegments().get(firstSegmentIndex).getAllLogServers()).isEmpty();
        }
    }


    /**
     * Given a sorted accumulated list of segments belonging to two epochs,
     * merges the next segment into the last added segment and returns a new list.
     *
     * @param nextSegment     A next segment to merge into the list.
     * @param accumulatedList An accumulated list of sorted and merged segments.
     * @return A new list.
     */
    ImmutableList<AgedSegment> mergeSegmentToAccumulatedList
    (AgedSegment nextSegment, ImmutableList<AgedSegment> accumulatedList) {
        if (accumulatedList.isEmpty()) {
            // If it's a first segment, add it to the list.
            return ImmutableList.of(nextSegment);
        } else {
            // Get the last added segment.
            int size = accumulatedList.size();
            AgedSegment lastAddedSegment = accumulatedList.get(size - 1);
            ImmutableList<AgedSegment> accumulatedListNoLastSegment = accumulatedList
                    .stream()
                    .filter(segment -> !segment.equals(lastAddedSegment))
                    .collect(ImmutableList.toImmutableList());
            // If the segments overlap:
            if (lastAddedSegment.segment.overlapsWith(nextSegment.segment)) {
                AgedSegment oldSegment;
                AgedSegment newSegment;
                AgedSegment mergedSegment;
                // Find the old and the new segment.
                if (lastAddedSegment.age.equals(PREVIOUS_EPOCH)) {
                    oldSegment = lastAddedSegment;
                    newSegment = nextSegment;
                } else {
                    oldSegment = nextSegment;
                    newSegment = lastAddedSegment;
                }

                CurrentTransferSegmentStatus oldSegmentStatus = oldSegment.segment.getStatus();

                mergedSegment = newSegment;
                // If the old segment is not done transferring, or restored -> update the segment
                // with a new range and the old status.
                SegmentState segmentState = oldSegmentStatus.getSegmentState();
                if (segmentState == FAILED || segmentState == RESTORED) {
                    CurrentTransferSegment segment = CurrentTransferSegment
                            .builder()
                            .startAddress(newSegment.segment.getStartAddress())
                            .endAddress(newSegment.segment.getEndAddress())
                            .status(oldSegmentStatus)
                            .build();
                    mergedSegment = new AgedSegment(segment, CURRENT_EPOCH);
                }


                return new ImmutableList.Builder<AgedSegment>()
                        .addAll(accumulatedListNoLastSegment)
                        .add(mergedSegment)
                        .build();

            }
            // No overlap -> add both segments.
            else {
                return new ImmutableList.Builder<AgedSegment>()
                        .addAll(accumulatedListNoLastSegment)
                        .add(lastAddedSegment)
                        .add(nextSegment)
                        .build();
            }
        }
    }

    /**
     * Merges two segment lists from the old and the new epochs.
     * If a new list is empty, restores all the transferred segments and creates a new list.
     * Otherwise, adds all the segments to the list,
     * sorts it and then merges the segments if they overlap.
     * Non overlapping segments are both added. If the segments overlap a range
     * of a new segment is taken and the status of an old segment is taken only
     * if its still transferring, failed or restored.
     *
     * @param oldList A list of segments before the epoch change.
     * @param newList A list of segments after the epoch change.
     * @return A new list, that reflects correctly the status of the transfer segments.
     */
    public ImmutableList<CurrentTransferSegment> mergeLists(
            List<CurrentTransferSegment> oldList, List<CurrentTransferSegment> newList) {

        // If a new list is empty ->
        // Nothing to transfer. Update all the TRANSFERRED to RESTORED and return.
        if (newList.isEmpty()) {
            return oldList.stream().map(segment -> {
                CurrentTransferSegmentStatus oldStatus = segment.getStatus();

                CurrentTransferSegmentStatus newStatus = oldStatus;

                if (oldStatus.getSegmentState() == TRANSFERRED) {
                    newStatus = CurrentTransferSegmentStatus
                            .builder()
                            .segmentState(RESTORED)
                            .totalTransferred(segment.computeTotalTransferred())
                            .build();
                }

                return CurrentTransferSegment
                        .builder()
                        .startAddress(segment.getStartAddress())
                        .endAddress(segment.getEndAddress())
                        .status(newStatus)
                        .build();

            }).collect(ImmutableList.toImmutableList());
        }

        // Group segments by age and sort them.
        ImmutableList<AgedSegment> oldSegments = oldList
                .stream()
                .map(segment -> new AgedSegment(segment, PREVIOUS_EPOCH))
                .collect(ImmutableList.toImmutableList());

        ImmutableList<AgedSegment> newSegments = newList
                .stream()
                .map(segment -> new AgedSegment(segment, CURRENT_EPOCH))
                .collect(ImmutableList.toImmutableList());

        ImmutableList<AgedSegment> allSegments = new ImmutableList.Builder<AgedSegment>()
                .addAll(oldSegments)
                .addAll(newSegments)
                .build()
                .stream()
                .sorted()
                .collect(ImmutableList.toImmutableList());

        ImmutableList<AgedSegment> initList = ImmutableList.of();

        // Merge the old and the new segments and return the new list.
        return allSegments.stream()
                .reduce(initList,
                        (accumulatedList, nextSegment) ->
                                mergeSegmentToAccumulatedList(nextSegment, accumulatedList),
                        (list1, list2) -> list2)
                .stream()
                .map(agedSegment -> agedSegment.segment)
                .collect(ImmutableList.toImmutableList());
    }
}
