package org.corfudb.infrastructure.orchestrator.actions;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.ToString;
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
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.*;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.*;
import static org.corfudb.runtime.view.Layout.*;
import static org.corfudb.runtime.view.Layout.ReplicationMode.CHAIN_REPLICATION;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

public class PrefixTrimRedundancyCalculatorTest extends LayoutBasedTest {


    @Test
    public void testCreateStateMapTrimMarkNotMoved() {
        CorfuRuntime runtime = Mockito.mock(CorfuRuntime.class);

        PrefixTrimRedundancyCalculator redundancyCalculator =
                new PrefixTrimRedundancyCalculator("localhost", runtime);

        PrefixTrimRedundancyCalculator spy = spy(redundancyCalculator);


        Layout layout = createNonPresentLayout();

        // node is not present anywhere -> all segments should be scheduled.
        doReturn(-1L).when(spy)
                .setTrimOnNewLogUnit(layout, runtime, "localhost");


        ImmutableList<MockedSegment> expected = ImmutableList.of(
                new MockedSegment(0L, 1L,
                        new CurrentTransferSegmentStatus(NOT_TRANSFERRED, 0L)),
                new MockedSegment(2L, 3L,
                        new CurrentTransferSegmentStatus(NOT_TRANSFERRED, 0L)));

        ImmutableList<CurrentTransferSegment> result = spy
                .createStateList(layout);

        assertThat(transformListToMock(result)).isEqualTo(expected);

        layout = createPresentLayout();

        expected = ImmutableList.of(
                new MockedSegment(0L, 1L,
                        new CurrentTransferSegmentStatus(NOT_TRANSFERRED, 0L)),
                new MockedSegment(2L, 3L,
                        new CurrentTransferSegmentStatus(RESTORED, 2L)));

        doReturn(-1L).when(spy)
                .setTrimOnNewLogUnit(layout, runtime, "localhost");

        result = spy
                .createStateList(layout);

        assertThat(transformListToMock(result))
                .isEqualTo(expected);
    }

    @Test
    public void testCreateStateMapTrimMarkIntersectsSegment() {
        CorfuRuntime runtime = Mockito.mock(CorfuRuntime.class);

        PrefixTrimRedundancyCalculator redundancyCalculator =
                new PrefixTrimRedundancyCalculator("localhost", runtime);

        PrefixTrimRedundancyCalculator spy = spy(redundancyCalculator);

        Layout layout = createNonPresentLayout();

        // node is not present anywhere, trim mark starts from the middle of a second segment ->
        // transfer half of second and third segments
        doReturn(3L).when(spy)
                .setTrimOnNewLogUnit(layout, runtime, "localhost");

        ImmutableList<MockedSegment> expected =
                ImmutableList.of(
                        new MockedSegment(3L, 3L,
                                new CurrentTransferSegmentStatus(NOT_TRANSFERRED,
                                        0L)));


        List<CurrentTransferSegment> result = spy
                .createStateList(layout);

        assertThat(transformListToMock(result)).isEqualTo(expected);

        layout = createPresentLayout();

        expected = ImmutableList.of(
                new MockedSegment(3L, 3L,
                        new CurrentTransferSegmentStatus(RESTORED, 1L)));

        doReturn(3L).when(spy)
                .setTrimOnNewLogUnit(layout, runtime, "localhost");

        result = spy.createStateList(layout);

        assertThat(transformListToMock(result)).isEqualTo(expected);

    }
}
