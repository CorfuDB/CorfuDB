package org.corfudb.infrastructure.orchestrator.actions;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import org.corfudb.infrastructure.log.statetransfer.StateTransferManager.CurrentTransferSegment;
import org.corfudb.runtime.view.Layout;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.*;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.NOT_TRANSFERRED;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.RESTORED;

@AllArgsConstructor
public class RedundancyCalculator {

    @NonNull
    @Getter
    private final String server;

    public ImmutableMap<CurrentTransferSegment, CompletableFuture<CurrentTransferSegmentStatus>>
    createStateMap(Layout layout) {
        Map<CurrentTransferSegment, CompletableFuture<CurrentTransferSegmentStatus>> map
                = layout.getSegments().stream().map(segment -> {
            CurrentTransferSegment statusSegment =
                    new CurrentTransferSegment(segment.getStart(), segment.getEnd() - 1);

            if (segmentContainsServer(segment)) {
                return new SimpleEntry<>(statusSegment,
                        CompletableFuture
                                .completedFuture(new
                                        CurrentTransferSegmentStatus(RESTORED,
                                        segment.getEnd())));
            } else {
                return new SimpleEntry<>(statusSegment,
                        CompletableFuture.completedFuture(new
                                CurrentTransferSegmentStatus(NOT_TRANSFERRED,
                                segment.getStart())));
            }
        }).collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue));
        return ImmutableMap.copyOf(map);
    }


    public boolean segmentContainsServer(Layout.LayoutSegment segment) {
        // Because we restore the node to the first stripe.
        return segment.getFirstStripe().getLogServers().contains(server);
    }

    private Layout restoreRedundancyForSegment(CurrentTransferSegment mapSegment, Layout layout) {
        List<Layout.LayoutSegment> segments = layout.getSegments().stream().map(layoutSegment -> {
            if (layoutSegment.getStart() == mapSegment.getStartAddress() &&
                    layoutSegment.getEnd() == mapSegment.getEndAddress()) {
                List<Layout.LayoutStripe> newStripes = layoutSegment.getStripes().stream().map(stripe -> {
                    List<String> logServers = new ArrayList<>(stripe.getLogServers());
                    logServers.add(getServer());
                    return new Layout.LayoutStripe(logServers);
                }).collect(Collectors.toList());

                return new Layout.LayoutSegment(layoutSegment.getReplicationMode(),
                        layoutSegment.getStart(), layoutSegment.getEnd(), newStripes);
            } else {
                return new Layout.LayoutSegment(layoutSegment.getReplicationMode(),
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
        return map.values().stream().allMatch(state -> {
            if (!state.isDone()) {
                return false;
            } else {
                CurrentTransferSegmentStatus currentTransferSegmentStatus = state.join();
                return currentTransferSegmentStatus.getSegmentStateTransferState()
                        .equals(RESTORED);
            }
        });
    }

    public static boolean canMergeSegments(Layout layout) {
        if (layout.getSegments().size() == 1) {
            return false;
        } else {
            return Sets.difference(
                    layout.getSegments().get(1).getAllLogServers(),
                    layout.getSegments().get(0).getAllLogServers()).isEmpty();
        }
    }

    public ImmutableMap<CurrentTransferSegment, CompletableFuture<CurrentTransferSegmentStatus>>
    mergeMaps(ImmutableMap<CurrentTransferSegment, CompletableFuture<CurrentTransferSegmentStatus>> oldMap,
              ImmutableMap<CurrentTransferSegment, CompletableFuture<CurrentTransferSegmentStatus>> newMap) {
        Map<CurrentTransferSegment, CompletableFuture<CurrentTransferSegmentStatus>> resultMap =
                newMap.keySet().stream().map(newMapKey -> {
                    if (oldMap.containsKey(newMapKey)) {
                        return new SimpleEntry<>(newMapKey, oldMap.get(newMapKey));
                    } else {
                        return new SimpleEntry<>(newMapKey, newMap.get(newMapKey));
                    }
                }).collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue));

        return ImmutableMap.copyOf(resultMap);
    }
}
