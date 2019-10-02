package org.corfudb.infrastructure.orchestrator.actions;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.corfudb.infrastructure.LayoutBasedTest;
import org.corfudb.infrastructure.log.statetransfer.StateTransferManager.CurrentTransferSegment;
import org.corfudb.infrastructure.log.statetransfer.StateTransferManager.CurrentTransferSegmentStatus;
import org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.Layout.LayoutSegment;
import org.corfudb.util.Sleep;
import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.FAILED;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.NOT_TRANSFERRED;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.RESTORED;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.TRANSFERRED;
import static org.corfudb.runtime.view.Layout.*;
import static org.corfudb.runtime.view.Layout.ReplicationMode.CHAIN_REPLICATION;

public class RedundancyCalculatorTest extends LayoutBasedTest {

    @Test
    public void testSegmentContainsServer(){


        LayoutStripe stripe1 = new LayoutStripe(Arrays.asList("localhost", "A", "B"));
        LayoutStripe stripe2 = new LayoutStripe(Collections.singletonList("C"));
        LayoutStripe stripe3 = new LayoutStripe(Collections.singletonList("D"));

        LayoutSegment segment = new LayoutSegment(CHAIN_REPLICATION, 0L, 1L,
                Arrays.asList(stripe1, stripe2, stripe3));

        RedundancyCalculator calculator = new RedundancyCalculator("localhost");

        Assert.assertTrue(calculator.segmentContainsServer(segment));

        stripe1 = new LayoutStripe(Arrays.asList("A", "B"));
        segment = new LayoutSegment(CHAIN_REPLICATION, 0L, 1L,
                Arrays.asList(stripe1, stripe2, stripe3));
        Assert.assertFalse(calculator.segmentContainsServer(segment));
    }

    @Test
    public void testCreateStateMap(){

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
        ImmutableMap<CurrentTransferSegment, CompletableFuture<CurrentTransferSegmentStatus>> stateMap
                = calculator.createStateMap(testLayout);

        ImmutableSet<CurrentTransferSegment> currentTransferSegments = stateMap.keySet();

        CurrentTransferSegment presentSegment = new CurrentTransferSegment(0L, 20L);
        CurrentTransferSegment nonPresentSegment = new CurrentTransferSegment(21L, 50L);
        Assert.assertTrue(currentTransferSegments.containsAll(Arrays.asList(presentSegment,
                nonPresentSegment)));

        CurrentTransferSegmentStatus presentSegmentStatus = stateMap.get(presentSegment).join();
        assertThat(SegmentState.RESTORED)
                .isEqualTo(presentSegmentStatus.getSegmentStateTransferState());

        CurrentTransferSegmentStatus nonPresentSegmentStatus = stateMap.get(nonPresentSegment)
                .join();

        assertThat(NOT_TRANSFERRED)
                .isEqualTo(nonPresentSegmentStatus.getSegmentStateTransferState());
    }

    @Test
    public void testRestoreRedundancyForSegment(){
        CurrentTransferSegment segment = new CurrentTransferSegment(0L, 20L);

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
    public void testUpdateLayoutAfterRedundancyRestoration(){

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
                (new CurrentTransferSegment(0L, 20L),
                new CurrentTransferSegment(21L, 50L),
                new CurrentTransferSegment(51L, 60L));


        RedundancyCalculator calculator = new RedundancyCalculator("localhost");

        Layout consolidatedLayout =
                calculator.updateLayoutAfterRedundancyRestoration(currentTransferSegments,
                        testLayout);

        assertThat(consolidatedLayout.getSegments().stream())
                .allMatch(segment -> segment.getFirstStripe().getLogServers().contains("localhost"));

    }

    @Test
    public void testRedundancyIsRestored(){
        CurrentTransferSegment notTransferred = new CurrentTransferSegment(0L, 5L);
        CurrentTransferSegment transferred = new CurrentTransferSegment(6L, 10L);
        CurrentTransferSegment restored = new CurrentTransferSegment(11L, 20L);
        CurrentTransferSegment failed = new CurrentTransferSegment(11L, 20L);
        CurrentTransferSegment inProgress = new CurrentTransferSegment(11L, 20L);

        Map<CurrentTransferSegment, CompletableFuture<CurrentTransferSegmentStatus>> map
                = new HashMap<>();

        map.put(restored, CompletableFuture.completedFuture(
                new CurrentTransferSegmentStatus(RESTORED, 20L)));

        map.put(inProgress, CompletableFuture.supplyAsync(() -> {
            Sleep.sleepUninterruptibly(Duration.ofMillis(3000)); // simulate in progress task
            return new CurrentTransferSegmentStatus(NOT_TRANSFERRED, 15L);
        }));

        RedundancyCalculator calculator = new RedundancyCalculator("localhost");

        // one task is in progress, so will return false
        assertThat(calculator.redundancyIsRestored(map)).isFalse();

        // remove in progress task, should return true
        map.remove(inProgress);

        assertThat(calculator.redundancyIsRestored(map)).isTrue();

        // add segments with other statuses, should return false
        map.put(failed, CompletableFuture.completedFuture(
                new CurrentTransferSegmentStatus(FAILED, 12L)));

        map.put(notTransferred, CompletableFuture.completedFuture(
                new CurrentTransferSegmentStatus(NOT_TRANSFERRED, 0L)));

        map.put(transferred, CompletableFuture.completedFuture(
                new CurrentTransferSegmentStatus(TRANSFERRED, 10L)));

        assertThat(calculator.redundancyIsRestored(map)).isFalse();

        // if map is empty redundancy is restored
        assertThat(calculator.redundancyIsRestored(new HashMap<>())).isTrue();

    }

    @Test
    public void testMergeMaps(){


        RedundancyCalculator calculator = new RedundancyCalculator("localhost");

        HashMap<CurrentTransferSegment, CompletableFuture<CurrentTransferSegmentStatus>> oldMap
                = new HashMap<>();

        oldMap.put(new CurrentTransferSegment(0L, 5L),
                CompletableFuture.completedFuture(
                        new CurrentTransferSegmentStatus(FAILED, 0L)));

        ImmutableMap<CurrentTransferSegment, CompletableFuture<CurrentTransferSegmentStatus>> immutableOldMap
                = ImmutableMap.copyOf(oldMap);

        HashMap<CurrentTransferSegment, CompletableFuture<CurrentTransferSegmentStatus>> newMap
                = new HashMap<>();

        newMap.put(new CurrentTransferSegment(0L, 5L),
                CompletableFuture.completedFuture(
                        new CurrentTransferSegmentStatus(NOT_TRANSFERRED, 0L)));

        newMap.put(new CurrentTransferSegment(6L, 10L),
                CompletableFuture.completedFuture(
                        new CurrentTransferSegmentStatus(NOT_TRANSFERRED, 6L)));


        ImmutableMap<CurrentTransferSegment, CompletableFuture<CurrentTransferSegmentStatus>> immutableNewMap
                = ImmutableMap.copyOf(newMap);

        // old segments should be preserved, new segments should be added
        ImmutableMap<CurrentTransferSegment, CompletableFuture<CurrentTransferSegmentStatus>> mergedMap
                = calculator.mergeMaps(immutableOldMap, immutableNewMap);

        assertThat(mergedMap.get(new CurrentTransferSegment(0L, 5L)).join())
                .isEqualTo(new CurrentTransferSegmentStatus(FAILED, 0L));

        assertThat(mergedMap.get(new CurrentTransferSegment(6L, 10L)).join())
                .isEqualTo(new CurrentTransferSegmentStatus(NOT_TRANSFERRED, 6L));

    }


}
