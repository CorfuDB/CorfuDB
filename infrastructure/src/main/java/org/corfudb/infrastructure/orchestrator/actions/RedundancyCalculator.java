package org.corfudb.infrastructure.orchestrator.actions;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import org.apache.commons.collections.ListUtils;
import org.corfudb.infrastructure.log.statetransfer.StateTransferManager.CurrentTransferSegment;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.Layout.LayoutSegment;
import org.corfudb.runtime.view.Layout.LayoutStripe;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.Map.*;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.*;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.FAILED;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.NOT_TRANSFERRED;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.RESTORED;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.TRANSFERRED;
import static org.corfudb.infrastructure.orchestrator.actions.RedundancyCalculator.SegmentAge.*;
import static org.corfudb.runtime.view.Address.NON_ADDRESS;

@AllArgsConstructor
public class RedundancyCalculator {

    public enum SegmentAge {
        OLD_SEGMENT,
        NEW_SEGMENT
    }

    @AllArgsConstructor
    @Getter
    @ToString
    @EqualsAndHashCode
    public static class AgedSegment implements Comparable<AgedSegment> {
        private final CurrentTransferSegment segment;
        private final SegmentAge age;

        @Override
        public int compareTo(AgedSegment other) {
            return (int) (this.getSegment().getStartAddress() -
                    other.getSegment().getStartAddress());
        }

        boolean overlapsWith(AgedSegment other) {
            return other.getSegment().getStartAddress() <= this.getSegment().getEndAddress();
        }
    }


    @NonNull
    @Getter
    private final String server;

    static boolean segmentContainsServer(LayoutSegment segment, String server) {
        return segment.getFirstStripe().getLogServers().contains(server);
    }

    long retrieveGlobalCommittedOffset(Layout layout, CorfuRuntime runtime){
        return NON_ADDRESS;
    }

    Layout restoreRedundancyForSegment(CurrentTransferSegment transferSegment, Layout layout) {
        List<LayoutSegment> segments = layout.getSegments().stream().map(layoutSegment -> {
            if (layoutSegment.getEnd() == transferSegment.getEndAddress() + 1) {

                List<LayoutStripe> newStripes = layoutSegment.getStripes().stream().map(stripe -> {

                    if (layoutSegment.getFirstStripe().equals(stripe)) {
                        ArrayList<String> servers = new ArrayList<>(stripe.getLogServers());
                        servers.add(server);
                        return new LayoutStripe(servers);
                    }

                    return stripe;

                }).collect(Collectors.toList());

                return new LayoutSegment(layoutSegment.getReplicationMode(),
                        layoutSegment.getStart(), layoutSegment.getEnd(), newStripes);
            } else {
                return new LayoutSegment(layoutSegment.getReplicationMode(),
                        layoutSegment.getStart(), layoutSegment.getEnd(), layoutSegment.getStripes());
            }
        }).collect(Collectors.toList());
        Layout newLayout = new Layout(layout);
        newLayout.setSegments(segments);
        return newLayout;
    }

    public Layout updateLayoutAfterRedundancyRestoration(List<CurrentTransferSegment> segments, Layout initLayout) {
        return segments
                .stream()
                .reduce(initLayout,
                        (layout, segment) -> restoreRedundancyForSegment(segment, layout),
                        (oldLayout, newLayout) -> newLayout);
    }

    public static boolean requiresRedundancyRestoration(Layout layout, String server) {
        if (layout.getSegments().size() == 1) {
            return false;
        } else {
            return layout.getSegments().stream().anyMatch(segment -> !segment.getAllLogServers()
                    .contains(server));
        }
    }

    /**
     * Returns true if after adding itself to the first segment, the segments can be merged.
     *
     * @param layout Current layout.
     * @param server The current node.
     * @return True is there is the redundancy restoration is needed.
     */
    public static boolean requiresMerge(Layout layout, String server) {
        if (layout.getSegments().size() == 1) {
            return false;
        } else {
            int firstSegmentIndex = 0;
            int secondSegmentIndex = 1;
            Layout copy = new Layout(layout);
            LayoutStripe firstStripe = copy.getSegments().get(0).getFirstStripe();
            firstStripe.getLogServers().add(server);
            return Sets.difference(
                    copy.getSegments().get(secondSegmentIndex).getAllLogServers(),
                    copy.getSegments().get(firstSegmentIndex).getAllLogServers()).isEmpty();
        }
    }

    public static boolean canMergeSegments(Layout layout, String server) {
        if (layout.getSegments().size() == 1) {
            return false;
        } else {
            int firstSegmentIndex = 0;
            int secondSegmentIndex = 1;
            boolean serverPresent = IntStream
                    .range(firstSegmentIndex, secondSegmentIndex + 1).boxed().allMatch(index ->
                            layout.getSegments().get(index).getFirstStripe().getLogServers().contains(server));
            return serverPresent && Sets.difference(
                    layout.getSegments().get(secondSegmentIndex).getAllLogServers(),
                    layout.getSegments().get(firstSegmentIndex).getAllLogServers()).isEmpty();
        }
    }


    private ArrayList<AgedSegment> mergeSegmentToAccumulatedList(AgedSegment nextSegment,
                                                                 ArrayList<AgedSegment> accumulatedList) {
        if (accumulatedList.isEmpty()) {
            // if it's a first segment, add it to the list.

            return new ArrayList<>(Collections.singletonList(nextSegment));
        } else {
            // if it's not, get the last added segment.
            int size = accumulatedList.size();
            AgedSegment lastAddedSegment = accumulatedList.remove(size - 1);
            // if segments overlap
            if (lastAddedSegment.overlapsWith(nextSegment)) {
                AgedSegment oldSegment;
                AgedSegment newSegment;
                AgedSegment mergedSegment;
                // find the old and the new segment
                if (lastAddedSegment.age.equals(OLD_SEGMENT)) {
                    oldSegment = lastAddedSegment;
                    newSegment = nextSegment;
                } else {
                    oldSegment = nextSegment;
                    newSegment = lastAddedSegment;
                }

                CompletableFuture<CurrentTransferSegmentStatus> oldSegmentStatus =
                        oldSegment.segment.getStatus();

                // if the old segment completed with error, not done, failed or restored -> update the segment
                // with a new range and the old status.
                if (oldSegmentStatus.isCompletedExceptionally() ||
                        !oldSegmentStatus.isDone() ||
                        oldSegmentStatus.join().getSegmentState().equals(FAILED) ||
                                oldSegmentStatus.join().getSegmentState().equals(RESTORED)) {
                    mergedSegment = new AgedSegment(new CurrentTransferSegment(newSegment.segment
                            .getStartAddress(),
                            newSegment.segment.getEndAddress(), oldSegmentStatus), NEW_SEGMENT);
                }
                // otherwise discard the old segment, merged segment is the new segment.
                else {
                    mergedSegment = newSegment;
                }

                accumulatedList.add(mergedSegment);
                return new ArrayList<>(accumulatedList);

            }
            // no overlap -> add them both.
            else {
                accumulatedList.add(lastAddedSegment);
                accumulatedList.add(nextSegment);
                return new ArrayList<>(accumulatedList);
            }
        }
    }

    public ImmutableList<CurrentTransferSegment> mergeLists(List<CurrentTransferSegment> oldList,
                                                            List<CurrentTransferSegment> newList) {

        // If a new list is empty -> Nothing to transfer. Update all the TRANSFERRED to RESTORED and return.
        if (newList.isEmpty()) {
            return ImmutableList.copyOf(oldList.stream().map(segment -> {
                CompletableFuture<CurrentTransferSegmentStatus> status = segment.getStatus();

                CompletableFuture<CurrentTransferSegmentStatus> newStatus =
                        status.thenApply(oldStatus -> {
                            if (oldStatus.getSegmentState().equals(TRANSFERRED)) {
                                return new CurrentTransferSegmentStatus(RESTORED, segment.getEndAddress());
                            } else {
                                return oldStatus;
                            }
                        });
                return new CurrentTransferSegment(segment.getStartAddress(),
                        segment.getEndAddress(), newStatus);

            }).collect(Collectors.toList()));
        }

        // Group segments by age and sort them.
        List<AgedSegment> oldSegments =
                oldList.stream().map(segment -> new AgedSegment(segment, OLD_SEGMENT))
                        .collect(Collectors.toList());

        List<AgedSegment> newSegments =
                newList.stream().map(segment -> new AgedSegment(segment, NEW_SEGMENT))
                        .collect(Collectors.toList());

        List<AgedSegment> allSegments = new ArrayList<>();

        allSegments.addAll(oldSegments);

        allSegments.addAll(newSegments);

        Collections.sort(allSegments);

        ArrayList<AgedSegment> initList = new ArrayList<>();

        // Merge the overlapping old and new segments and return the new status list.
        ArrayList<CurrentTransferSegment> result = allSegments.stream()
                .reduce(initList,
                        (accumulatedList, nextSegment) ->
                                mergeSegmentToAccumulatedList(nextSegment, accumulatedList),
                        (list1, list2) -> list2)
                .stream()
                .map(agedSegment -> agedSegment.segment)
                .collect(Collectors.toCollection(ArrayList::new));
        return ImmutableList.copyOf(result);
    }
}
