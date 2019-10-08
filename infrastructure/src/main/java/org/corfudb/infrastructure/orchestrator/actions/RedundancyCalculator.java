package org.corfudb.infrastructure.orchestrator.actions;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import org.corfudb.infrastructure.log.statetransfer.StateTransferManager.CurrentTransferSegment;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.Layout.LayoutSegment;
import org.corfudb.runtime.view.Layout.LayoutStripe;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Map.*;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.*;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.FAILED;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.NOT_TRANSFERRED;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.RESTORED;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.TRANSFERRED;
import static org.corfudb.infrastructure.orchestrator.actions.RedundancyCalculator.SegmentAge.*;

@AllArgsConstructor
public class RedundancyCalculator {

    @AllArgsConstructor
    @Getter
    @ToString
    public static class OverlappingSegments {
        private final Optional<AgedSegment> oldSegment;
        private final Optional<AgedSegment> newSegment;

    }

    public enum SegmentAge {
        OLD_SEGMENT,
        NEW_SEGMENT
    }

    @AllArgsConstructor
    @Getter
    @ToString
    public static class AgedSegment implements Comparable<AgedSegment> {
        private final CurrentTransferSegment segment;
        private final SegmentAge age;

        @Override
        public int compareTo(AgedSegment other) {
            return (int) (this.getSegment().getStartAddress() -
                    other.getSegment().getStartAddress());
        }

        public boolean overlapsWith(AgedSegment other) {
            return other.getSegment().getStartAddress() <= this.getSegment().getEndAddress();
        }
    }


    @NonNull
    @Getter
    private final String server;

    public ImmutableMap<CurrentTransferSegment, CompletableFuture<CurrentTransferSegmentStatus>>
    createStateMap(Layout layout) {
        Map<CurrentTransferSegment, CompletableFuture<CurrentTransferSegmentStatus>> map
                = layout.getSegments().stream().map(segment -> {
            CurrentTransferSegment statusSegment =
                    new CurrentTransferSegment(segment.getStart(), segment.getEnd() - 1);

            if (segmentContainsServer(segment, server)) {
                return new SimpleEntry<>(statusSegment,
                        CompletableFuture
                                .completedFuture(new
                                        CurrentTransferSegmentStatus(RESTORED,
                                        segment.getEnd() - 1)));
            } else {
                return new SimpleEntry<>(statusSegment,
                        CompletableFuture.completedFuture(new
                                CurrentTransferSegmentStatus(NOT_TRANSFERRED,
                                -1L)));
            }
        }).collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue));
        return ImmutableMap.copyOf(map);
    }


    static boolean segmentContainsServer(LayoutSegment segment, String server) {
        return segment.getFirstStripe().getLogServers().contains(server);
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

    /**
     * Check that the redundancy is restored.
     *
     * @param map The immutable map of segment statuses.
     * @return True if every segment is transferred and false otherwise.
     */
    public boolean redundancyIsRestored(Map<CurrentTransferSegment, CompletableFuture<CurrentTransferSegmentStatus>> map) {
        if (map.isEmpty()) {
            return true;
        }
        return map.values().stream().allMatch(state -> {
            if (!state.isDone()) {
                return false;
            } else if (state.isCompletedExceptionally()) {
                return false;
            } else {
                CurrentTransferSegmentStatus currentTransferSegmentStatus = state.join();
                return currentTransferSegmentStatus.getSegmentStateTransferState()
                        .equals(RESTORED);
            }
        });
    }

    /**
     * Returns true if the segments can be merged or if the node is not present in all the segments.
     * @param layout Current layout.
     * @param server The current node.
     * @return True is there is a need for redundancy restoration, merge of segments or both.
     */
    public static boolean requiresRedundancyRestoration(Layout layout, String server){
        return canMergeSegments(layout, server) ||
                !layout.getSegments().stream().allMatch(segment -> segmentContainsServer(segment,
                        server));
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


    public ImmutableMap<CurrentTransferSegment, CompletableFuture<CurrentTransferSegmentStatus>>
    mergeMaps(ImmutableMap<CurrentTransferSegment, CompletableFuture<CurrentTransferSegmentStatus>> oldMap,
              ImmutableMap<CurrentTransferSegment, CompletableFuture<CurrentTransferSegmentStatus>> newMap) {

       // If a new map is empty -> Nothing to transfer. Update all the TRANSFERRED to RESTORED and return.
       if(newMap.isEmpty()){
           return ImmutableMap.copyOf(oldMap.entrySet().stream().map(entry -> {
               CurrentTransferSegment segment = entry.getKey();
               CompletableFuture<CurrentTransferSegmentStatus> status = entry.getValue();

               if (status.isDone() && status.join().getSegmentStateTransferState().equals(TRANSFERRED)) {
                   return new SimpleEntry<>(segment,
                           CompletableFuture.completedFuture(new CurrentTransferSegmentStatus(RESTORED, segment.getEndAddress())));
               } else {
                   return entry;
               }

           }).collect(Collectors.toMap(Entry::getKey, Entry::getValue)));
       }
        // Create the lists of old and new segments
        List<AgedSegment> newSegments =
                newMap.keySet().stream().map(segment -> new AgedSegment(segment, NEW_SEGMENT))
                        .collect(Collectors.toList());

        List<AgedSegment> oldSegments =
                oldMap.keySet().stream().map(segment -> new AgedSegment(segment, OLD_SEGMENT))
                        .collect(Collectors.toList());

        List<AgedSegment> allSegments = new ArrayList<>();

        allSegments.addAll(newSegments);

        allSegments.addAll(oldSegments);

        // Sort the list
        Collections.sort(allSegments);

        // Get the list of possibly overlapping segments
        List<OverlappingSegments> overlappingSegments = new ArrayList<>();
        Iterator<AgedSegment> iterator = allSegments.iterator();

        // Classify the segments based on what map they came from and group them.

        while(iterator.hasNext()){
            AgedSegment current = iterator.next();
            AgedSegment next;
            if(iterator.hasNext()){
                next = iterator.next();
                if (current.overlapsWith(next)) {
                    if (current.age.equals(OLD_SEGMENT)) {
                        overlappingSegments.add
                                (new OverlappingSegments(Optional.of(current), Optional.of(next)));
                    } else {
                        overlappingSegments.add
                                (new OverlappingSegments(Optional.of(next), Optional.of(current)));
                    }

                }
                else{
                    if (current.age.equals(OLD_SEGMENT)) {
                        overlappingSegments.add
                                (new OverlappingSegments(Optional.of(current), Optional.empty()));
                        overlappingSegments.add(new OverlappingSegments(Optional.empty(), Optional.of(next)));
                    } else {
                        overlappingSegments.add(new OverlappingSegments(Optional.empty(),
                                Optional.of(current)));
                        overlappingSegments.add(new OverlappingSegments(Optional.of(next), Optional.empty()));
                    }
                }
            }
            else{
                if (current.age.equals(OLD_SEGMENT)) {
                    overlappingSegments.add
                            (new OverlappingSegments(Optional.of(current), Optional.empty()));
                } else {
                    overlappingSegments.add
                            (new OverlappingSegments(Optional.empty(), Optional.of(current)));
                }
            }

        }

        // Create a new map based on what map the segments came from and whether they overlap or not.
        return ImmutableMap.copyOf(overlappingSegments.stream().map(segmentPair -> {
            if (segmentPair.newSegment.isPresent() && segmentPair.oldSegment.isPresent()) {
                CurrentTransferSegment oldSegment = segmentPair.oldSegment.get().segment;
                CurrentTransferSegment newSegment = segmentPair.newSegment.get().segment;
                if (oldMap.get(oldSegment).isCompletedExceptionally() ||
                        !oldMap.get(oldSegment).isDone() ||
                        oldMap.get(oldSegment).join().getSegmentStateTransferState().equals(FAILED)) {
                    return new SimpleEntry<>(newSegment, oldMap.get(oldSegment));
                }
                else{
                    return new SimpleEntry<>(newSegment, newMap.get(newSegment));
                }
            } else if(segmentPair.newSegment.isPresent()){
                CurrentTransferSegment newSegment = segmentPair.newSegment.get().segment;
                return new SimpleEntry<>(newSegment, newMap.get(newSegment));
            }
            else{
                CurrentTransferSegment oldSegment = segmentPair.oldSegment.get().segment;
                return new SimpleEntry<>(oldSegment, oldMap.get(oldSegment));
            }
        }).collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue)));

    }
}
