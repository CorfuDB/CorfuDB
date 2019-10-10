package org.corfudb.infrastructure.orchestrator.actions;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import lombok.ToString;
import org.corfudb.infrastructure.LayoutBasedTest;
import org.corfudb.infrastructure.log.statetransfer.StateTransferManager.CurrentTransferSegment;
import org.corfudb.infrastructure.log.statetransfer.StateTransferManager.CurrentTransferSegmentStatus;
import org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState;
import org.corfudb.protocols.wireprotocol.IMetadata;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.Layout.LayoutSegment;
import org.corfudb.util.Sleep;
import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

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

        Assert.assertTrue(calculator.segmentContainsServer(segment, "localhost"));

        stripe1 = new LayoutStripe(Arrays.asList("A", "B"));
        segment = new LayoutSegment(CHAIN_REPLICATION, 0L, 1L,
                Arrays.asList(stripe1, stripe2, stripe3));
        Assert.assertFalse(calculator.segmentContainsServer(segment, "localhost"));
    }

    @Test
    public void testRequiresRedundancyRestoration(){
        LayoutStripe stripe1 = new LayoutStripe(Arrays.asList("A", "localhost"));
        LayoutStripe stripe2 = new LayoutStripe(Arrays.asList("A", "B", "localhost"));

        LayoutSegment segment = new LayoutSegment(CHAIN_REPLICATION, 0L, 2L,
                Collections.singletonList(stripe1));
        LayoutSegment segment2 = new LayoutSegment(CHAIN_REPLICATION, 3L, 6L,
                Collections.singletonList(stripe2));

        // Different sets
        Layout layout = createTestLayout(Arrays.asList(segment, segment2));
        assertThat(RedundancyCalculator.requiresRedundancyRestoration(layout, "localhost"))
                .isFalse();
        // Single segment
        layout = createTestLayout(Collections.singletonList(segment));
        assertThat(RedundancyCalculator.requiresRedundancyRestoration(layout, "localhost"))
                .isFalse();
        // Restoring server is not present
        stripe1 = new LayoutStripe(Arrays.asList("A", "B"));
        stripe2 = new LayoutStripe(Arrays.asList("A", "B"));
        segment = new LayoutSegment(CHAIN_REPLICATION, 0L, 2L,
                Collections.singletonList(stripe1));
        segment2 = new LayoutSegment(CHAIN_REPLICATION, 3L, 6L,
                Collections.singletonList(stripe2));
        layout = createTestLayout(Arrays.asList(segment, segment2));

        assertThat(RedundancyCalculator.requiresRedundancyRestoration(layout, "localhost"))
                .isTrue();

        List<String> serverList = Arrays.asList("test:0", "test:1", "test:2");

        stripe1 = new LayoutStripe(Arrays.asList("test:0", "test:2"));
        stripe2 = new LayoutStripe(Arrays.asList("test:0",  "test:1", "test:2"));
        LayoutStripe stripe3 = new LayoutStripe(Arrays.asList("test:0",  "test:1", "test:2"));
        LayoutStripe stripe4 = new LayoutStripe(Arrays.asList("test:0",  "test:1", "test:2"));

        segment = new LayoutSegment(CHAIN_REPLICATION, 0L, 3L,
                Collections.singletonList(stripe1));
        segment2 = new LayoutSegment(CHAIN_REPLICATION, 3L, 6L,
                Collections.singletonList(stripe2));
        LayoutSegment segment3 = new LayoutSegment(CHAIN_REPLICATION, 6L, 9L,
                Collections.singletonList(stripe3));
        LayoutSegment segment4 = new LayoutSegment(CHAIN_REPLICATION, 9L, -1L,
                Collections.singletonList(stripe4));


        layout = new Layout(serverList,
                serverList,
                Arrays.asList(segment, segment2, segment3, segment4),
                0L, UUID.randomUUID());
        assertThat(RedundancyCalculator.requiresRedundancyRestoration(layout, "test:2"))
                .isFalse();


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
    public void testMergeSegmentsMatchFailed(){
        RedundancyCalculator calculator = new RedundancyCalculator("localhost");

        // old map had a failed transfer, segments match completely -> preserve
        CurrentTransferSegment segment = new CurrentTransferSegment(0L, 5L);

        CompletableFuture<CurrentTransferSegmentStatus> failed =
                CompletableFuture.completedFuture
                        (new CurrentTransferSegmentStatus(FAILED,
                                -1L));

        CompletableFuture<CurrentTransferSegmentStatus> notTransferred =
                CompletableFuture.completedFuture
                        (new CurrentTransferSegmentStatus(NOT_TRANSFERRED,
                                -1L));

        ImmutableMap<CurrentTransferSegment, CompletableFuture<CurrentTransferSegmentStatus>> oldMap =
                ImmutableMap.of(segment, failed);

        ImmutableMap<CurrentTransferSegment, CompletableFuture<CurrentTransferSegmentStatus>> newMap =
                ImmutableMap.of(segment, notTransferred);

        assertThat(calculator.mergeMaps(oldMap, newMap).get(segment).join().getSegmentStateTransferState())
                .isEqualTo(FAILED);
    }

    @Test
    public void testMergeSegmentsMatchInProgress(){

        RedundancyCalculator calculator = new RedundancyCalculator("localhost");

        // old map had a failed transfer, segments match completely -> preserve
        CurrentTransferSegment segment = new CurrentTransferSegment(0L, 5L);


        // old map had a running transfer, segments match -> preserve
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

        ImmutableMap<CurrentTransferSegment, CompletableFuture<CurrentTransferSegmentStatus>> newMap =
                ImmutableMap.of(segment, notTransferred);

        ImmutableMap<CurrentTransferSegment, CompletableFuture<CurrentTransferSegmentStatus>>oldMap
                = ImmutableMap.of(segment, inProgress);

        assertThat(calculator.mergeMaps(oldMap, newMap).get(segment).isDone()).isFalse();
    }

    @Test
    public void testMergeSegmentsMatchExceptional(){

        RedundancyCalculator calculator = new RedundancyCalculator("localhost");

        // old map had a failed transfer, segments match completely -> preserve
        CurrentTransferSegment segment = new CurrentTransferSegment(0L, 5L);


        CompletableFuture<CurrentTransferSegmentStatus> notTransferred =
                CompletableFuture.completedFuture
                        (new CurrentTransferSegmentStatus(NOT_TRANSFERRED,
                                -1L));

        // old map had an exceptionally failed transfer, segments match -> preserve
        CompletableFuture<CurrentTransferSegmentStatus> exceptional =
                CompletableFuture.supplyAsync(() -> {
                    throw new RuntimeException();
                });

        ImmutableMap<CurrentTransferSegment, CompletableFuture<CurrentTransferSegmentStatus>> newMap =
                ImmutableMap.of(segment, notTransferred);

        ImmutableMap<CurrentTransferSegment, CompletableFuture<CurrentTransferSegmentStatus>>
                oldMap = ImmutableMap.of(segment, exceptional);

        Sleep.sleepUninterruptibly(Duration.ofMillis(200));
        assertThat(calculator.mergeMaps(oldMap, newMap).get(segment).isCompletedExceptionally())
                .isTrue();
    }

    @Test
    public void testMergeMapsMatchRestored(){

        RedundancyCalculator calculator = new RedundancyCalculator("localhost");

        CurrentTransferSegment segment = new CurrentTransferSegment(0L, 5L);

        CompletableFuture<CurrentTransferSegmentStatus> notTransferred =
                CompletableFuture.completedFuture
                        (new CurrentTransferSegmentStatus(NOT_TRANSFERRED,
                                -1L));

        // new segment is restored, segments match -> update
        CompletableFuture<CurrentTransferSegmentStatus> restored =
                CompletableFuture.completedFuture
                        (new CurrentTransferSegmentStatus(RESTORED,
                                -1L));

        ImmutableMap<CurrentTransferSegment, CompletableFuture<CurrentTransferSegmentStatus>>
                oldMap = ImmutableMap.of(segment, notTransferred);
        ImmutableMap<CurrentTransferSegment, CompletableFuture<CurrentTransferSegmentStatus>>
                newMap = ImmutableMap.of(segment, restored);

        assertThat(calculator.mergeMaps(oldMap, newMap)
                .get(segment)
                .join()
                .getSegmentStateTransferState()).isEqualTo(RESTORED);

    }

    @Test
    public void testMergeMapsNonOverlap(){
        RedundancyCalculator calculator = new RedundancyCalculator("localhost");
        CurrentTransferSegment segment1 = new CurrentTransferSegment(0L, 5L);
        CurrentTransferSegment segment2 = new CurrentTransferSegment(6L, 10L);
        CompletableFuture<CurrentTransferSegmentStatus> notTransferred =
                CompletableFuture.completedFuture
                        (new CurrentTransferSegmentStatus(NOT_TRANSFERRED,
                                -1L));
        ImmutableMap<CurrentTransferSegment, CompletableFuture<CurrentTransferSegmentStatus>>
                oldMap = ImmutableMap.of(segment1, notTransferred);
        ImmutableMap<CurrentTransferSegment, CompletableFuture<CurrentTransferSegmentStatus>>
                newMap = ImmutableMap.of(segment2, notTransferred);

        assertThat(calculator.mergeMaps(oldMap, newMap)).containsKey(segment1);
        assertThat(calculator.mergeMaps(oldMap, newMap)).containsKey(segment2);

    }

    @Test
    public void testMergeMapsNewIsSubset(){
        RedundancyCalculator calculator = new RedundancyCalculator("localhost");
        CurrentTransferSegment oldSegment = new CurrentTransferSegment(0L, 5L);
        CurrentTransferSegment newSegment = new CurrentTransferSegment(3L, 5L);

        CompletableFuture<CurrentTransferSegmentStatus> notTransferred =
                CompletableFuture.completedFuture
                        (new CurrentTransferSegmentStatus(NOT_TRANSFERRED,
                                -1L));

        CompletableFuture<CurrentTransferSegmentStatus> restored =
                CompletableFuture.completedFuture
                        (new CurrentTransferSegmentStatus(RESTORED,
                                -1L));

        ImmutableMap<CurrentTransferSegment, CompletableFuture<CurrentTransferSegmentStatus>>
                oldMap = ImmutableMap.of(oldSegment, notTransferred);

        ImmutableMap<CurrentTransferSegment, CompletableFuture<CurrentTransferSegmentStatus>>
                newMap = ImmutableMap.of(newSegment, restored);

        assertThat(calculator.mergeMaps(oldMap, newMap).get(newSegment)
                .join().getSegmentStateTransferState()).isEqualTo(RESTORED);

    }

    @Test
    public void testMergeMapsNewIsSuperSet(){
        RedundancyCalculator calculator = new RedundancyCalculator("localhost");
        CurrentTransferSegment oldSegment = new CurrentTransferSegment(0L, 5L);
        CurrentTransferSegment newSegment = new CurrentTransferSegment(0L, 15L);
        // oldSegment is transferred and newsegment is restored
        CompletableFuture<CurrentTransferSegmentStatus> notTransferred =
                CompletableFuture.completedFuture
                        (new CurrentTransferSegmentStatus(NOT_TRANSFERRED,
                                -1L));

        CompletableFuture<CurrentTransferSegmentStatus> restored =
                CompletableFuture.completedFuture
                        (new CurrentTransferSegmentStatus(RESTORED,
                                -1L));

        ImmutableMap<CurrentTransferSegment, CompletableFuture<CurrentTransferSegmentStatus>>
                oldMap = ImmutableMap.of(oldSegment, notTransferred);

        ImmutableMap<CurrentTransferSegment, CompletableFuture<CurrentTransferSegmentStatus>>
                newMap = ImmutableMap.of(newSegment, restored);

        assertThat(calculator.mergeMaps(oldMap, newMap).get(newSegment)
                .join().getSegmentStateTransferState()).isEqualTo(RESTORED);
    }

    @Test
    public void testMergeMapsNewIsPrefixedAndMerged(){
        RedundancyCalculator calculator = new RedundancyCalculator("localhost");
        CurrentTransferSegment oldSegment = new CurrentTransferSegment(0L, 5L);
        CurrentTransferSegment newSegment = new CurrentTransferSegment(3L, 15L);
        // oldSegment is transferred and newsegment is restored
        CompletableFuture<CurrentTransferSegmentStatus> notTransferred =
                CompletableFuture.completedFuture
                        (new CurrentTransferSegmentStatus(NOT_TRANSFERRED,
                                -1L));

        CompletableFuture<CurrentTransferSegmentStatus> restored =
                CompletableFuture.completedFuture
                        (new CurrentTransferSegmentStatus(RESTORED,
                                -1L));

        ImmutableMap<CurrentTransferSegment, CompletableFuture<CurrentTransferSegmentStatus>>
                oldMap = ImmutableMap.of(oldSegment, notTransferred);

        ImmutableMap<CurrentTransferSegment, CompletableFuture<CurrentTransferSegmentStatus>>
                newMap = ImmutableMap.of(newSegment, restored);

        assertThat(calculator.mergeMaps(oldMap, newMap).get(newSegment)
                .join().getSegmentStateTransferState()).isEqualTo(RESTORED);
    }

    @Test
    public void testMergeMapsNewMapIsEmpty(){
        RedundancyCalculator calculator = new RedundancyCalculator("localhost");
        CurrentTransferSegment oldSegment = new CurrentTransferSegment(0L, 5L);
        // oldSegment is transferred and newsegment is restored
        CompletableFuture<CurrentTransferSegmentStatus> transferred =
                CompletableFuture.completedFuture
                        (new CurrentTransferSegmentStatus(TRANSFERRED,
                                -1L));

        ImmutableMap<CurrentTransferSegment, CompletableFuture<CurrentTransferSegmentStatus>>
                oldMap = ImmutableMap.of(oldSegment, transferred);

        ImmutableMap<CurrentTransferSegment, CompletableFuture<CurrentTransferSegmentStatus>>
                newMap = ImmutableMap.of();

        assertThat(calculator.mergeMaps(oldMap, newMap).get(oldSegment)
                .join().getSegmentStateTransferState()).isEqualTo(RESTORED);



    }


}
