package org.corfudb.infrastructure.orchestrator.actions;

import com.google.common.collect.ImmutableMap;
import org.corfudb.infrastructure.LayoutBasedTest;
import org.corfudb.infrastructure.log.statetransfer.StateTransferManager;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.Layout.LayoutSegment;
import org.junit.Test;
import org.mockito.Mockito;
import static org.assertj.core.api.Assertions.assertThat;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.*;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.*;
import static org.corfudb.runtime.view.Layout.*;
import static org.corfudb.runtime.view.Layout.ReplicationMode.CHAIN_REPLICATION;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

public class PrefixTrimRedundancyCalculatorTest extends LayoutBasedTest {

    private Layout createNonPresentLayout(){
        LayoutStripe stripe1 = new LayoutStripe(Arrays.asList("A", "B"));
        LayoutStripe stripe2 = new LayoutStripe(Collections.singletonList("B"));
        LayoutStripe stripe3 = new LayoutStripe(Collections.singletonList("A"));

        LayoutSegment segment1 = new LayoutSegment(CHAIN_REPLICATION, 0L, 2L,
                Collections.singletonList(stripe1));

        LayoutSegment segment2 = new LayoutSegment(CHAIN_REPLICATION, 2L, 4L,
                Collections.singletonList(stripe2));

        LayoutSegment segment3 = new LayoutSegment(CHAIN_REPLICATION, 4L, 6L,
                Collections.singletonList(stripe3));

        return createTestLayout(Arrays.asList(segment1, segment2, segment3));
    }

    private Layout createPresentLayout(){

        Layout layout = createNonPresentLayout();
        LayoutStripe stripe2 = new LayoutStripe(Arrays.asList("localhost", "B"));

        LayoutSegment segment2 = new LayoutSegment(CHAIN_REPLICATION, 2L, 4L,
                Collections.singletonList(stripe2));

        return createTestLayout(Arrays.asList(layout.getSegment(0L),
                segment2, layout.getSegment(4L)));
    }

    @Test
    public void testCreateStateMapTrimMarkNotMoved(){
        CorfuRuntime runtime = Mockito.mock(CorfuRuntime.class);

        PrefixTrimRedundancyCalculator redundancyCalculator =
                new PrefixTrimRedundancyCalculator("localhost", runtime);

        PrefixTrimRedundancyCalculator spy = spy(redundancyCalculator);


        Layout layout = createNonPresentLayout();

        // node is not present anywhere -> all segments should be scheduled.
        doReturn(0L).when(spy)
                .setTrimOnNewLogUnit(layout, runtime, "localhost");


        ImmutableMap<CurrentTransferSegment, CurrentTransferSegmentStatus> expected = ImmutableMap.of(
                new CurrentTransferSegment(0L, 1L),
                new CurrentTransferSegmentStatus(NOT_TRANSFERRED, -1L),
                new CurrentTransferSegment(2L, 3L),
                new CurrentTransferSegmentStatus(NOT_TRANSFERRED, -1L),
                new CurrentTransferSegment(4L, 5L),
                new CurrentTransferSegmentStatus(NOT_TRANSFERRED, -1L));

        Map<CurrentTransferSegment, CurrentTransferSegmentStatus> result = spy
                .createStateMap(layout)
                .entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().join()));

        assertThat(result).isEqualTo(expected);

        layout = createPresentLayout();

        expected = ImmutableMap.of(
                new CurrentTransferSegment(0L, 1L),
                new CurrentTransferSegmentStatus(NOT_TRANSFERRED, -1L),
                new CurrentTransferSegment(2L, 3L),
                new CurrentTransferSegmentStatus(RESTORED, 3L),
                new CurrentTransferSegment(4L, 5L),
                new CurrentTransferSegmentStatus(NOT_TRANSFERRED, -1L));

        doReturn(0L).when(spy)
                .setTrimOnNewLogUnit(layout, runtime, "localhost");

        result = spy
                .createStateMap(layout)
                .entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().join()));

        assertThat(result).isEqualTo(expected);
    }

    @Test
    public void testCreateStateMapTrimMarkIntersectsSegment(){
        CorfuRuntime runtime = Mockito.mock(CorfuRuntime.class);

        PrefixTrimRedundancyCalculator redundancyCalculator =
                new PrefixTrimRedundancyCalculator("localhost", runtime);

        PrefixTrimRedundancyCalculator spy = spy(redundancyCalculator);

        Layout layout = createNonPresentLayout();

        // node is not present anywhere, trim mark starts from the middle of a second segment ->
        // transfer half of second and third segments
        doReturn(3L).when(spy)
                .setTrimOnNewLogUnit(layout, runtime, "localhost");

        ImmutableMap<CurrentTransferSegment, CurrentTransferSegmentStatus> expected = ImmutableMap.of(
                new CurrentTransferSegment(3L, 3L),
                new CurrentTransferSegmentStatus(NOT_TRANSFERRED, -1L),
                new CurrentTransferSegment(4L, 5L),
                new CurrentTransferSegmentStatus(NOT_TRANSFERRED, -1L));


        Map<CurrentTransferSegment, CurrentTransferSegmentStatus> result = spy
                .createStateMap(layout)
                .entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().join()));

        assertThat(result).isEqualTo(expected);

        layout = createPresentLayout();

        expected = ImmutableMap.of(
                new CurrentTransferSegment(3L, 3L),
                new CurrentTransferSegmentStatus(RESTORED, 3L),
                new CurrentTransferSegment(4L, 5L),
                new CurrentTransferSegmentStatus(NOT_TRANSFERRED, -1L));

        doReturn(3L).when(spy)
                .setTrimOnNewLogUnit(layout, runtime, "localhost");

        result = spy
                .createStateMap(layout)
                .entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().join()));

        assertThat(result).isEqualTo(expected);

    }
}
