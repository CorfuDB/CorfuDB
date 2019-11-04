package org.corfudb.infrastructure.redundancy;

import com.google.common.collect.ImmutableList;
import org.corfudb.infrastructure.LayoutBasedTestHelper;
import org.corfudb.infrastructure.log.statetransfer.TransferSegmentCreator;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Layout;
import org.junit.Test;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Optional;

import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.*;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.*;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

public class PrefixTrimRedundancyCalculatorTest extends LayoutBasedTestHelper implements TransferSegmentCreator {


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
                        createStatus(NOT_TRANSFERRED, 0L, Optional.empty())),
                new MockedSegment(2L, 3L,
                        createStatus(NOT_TRANSFERRED, 0L, Optional.empty())));

        ImmutableList<TransferSegment> result = spy
                .createStateList(layout);

        assertThat(transformListToMock(result)).isEqualTo(expected);

        layout = createPresentLayout();

        expected = ImmutableList.of(
                new MockedSegment(0L, 1L,
                        createStatus(NOT_TRANSFERRED, 0L, Optional.empty())),
                new MockedSegment(2L, 3L,
                        createStatus(RESTORED, 2L, Optional.empty())));

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
                        new MockedSegment(3L, 3L, createStatus(NOT_TRANSFERRED, 0L, Optional.empty())));


        List<TransferSegment> result = spy
                .createStateList(layout);

        assertThat(transformListToMock(result)).isEqualTo(expected);

        layout = createPresentLayout();

        expected = ImmutableList.of(
                new MockedSegment(3L, 3L, createStatus(RESTORED, 1L, Optional.empty())));

        doReturn(3L).when(spy)
                .setTrimOnNewLogUnit(layout, runtime, "localhost");

        result = spy.createStateList(layout);

        assertThat(transformListToMock(result)).isEqualTo(expected);

    }
}
