package org.corfudb.infrastructure.orchestrator.actions;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.corfudb.infrastructure.LayoutBasedTest;
import org.corfudb.infrastructure.log.statetransfer.StateTransferManager.CurrentTransferSegment;
import org.corfudb.infrastructure.log.statetransfer.StateTransferManager.CurrentTransferSegmentStatus;
import org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.Layout.LayoutSegment;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
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

        assertThat(SegmentState.NOT_TRANSFERRED)
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



}
