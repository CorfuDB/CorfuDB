package org.corfudb.infrastructure.orchestrator.actions;

import org.corfudb.infrastructure.LayoutBasedTestHelper;
import org.corfudb.infrastructure.log.statetransfer.TransferSegmentCreator;
import org.corfudb.infrastructure.log.statetransfer.streamprocessor.StreamProcessFailure;
import org.corfudb.infrastructure.redundancy.RedundancyCalculator;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.LayoutManagementView;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.CurrentTransferSegment;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.FAILED;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.NOT_TRANSFERRED;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.TRANSFERRED;
import static org.corfudb.infrastructure.orchestrator.actions.RestoreRedundancyMergeSegments.RestoreStatus.NOT_RESTORED;
import static org.corfudb.infrastructure.orchestrator.actions.RestoreRedundancyMergeSegments.RestoreStatus.RESTORED;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;

public class RestoreRedundancyAndMergeSegmentsTest extends LayoutBasedTestHelper implements TransferSegmentCreator {

    // empty list
    @Test
    public void testTryRestoreRedundancyEmptyList() {
        Layout.LayoutStripe stripe = new Layout.LayoutStripe(Arrays.asList("localhost"));
        Layout.LayoutSegment segment =
                new Layout.LayoutSegment(Layout.ReplicationMode.CHAIN_REPLICATION, 0L, 10L, Arrays.asList(stripe));
        Layout testLayout = createTestLayout(Arrays.asList(segment));
        ArrayList<CurrentTransferSegment> emptyList = new ArrayList<>();
        LayoutManagementView layoutManagementView = mock(LayoutManagementView.class);

        RestoreRedundancyMergeSegments action = new RestoreRedundancyMergeSegments("localhost");
        RestoreRedundancyMergeSegments.RestoreStatus status =
                action.tryRestoreRedundancyAndMergeSegments(emptyList, testLayout, layoutManagementView);
        assertThat(status).isEqualTo(NOT_RESTORED);

    }

    // list with transfer that failed
    @Test
    public void testTryRestoreFailedTransfer() {
        Layout.LayoutStripe stripe = new Layout.LayoutStripe(Arrays.asList("localhost"));
        Layout.LayoutSegment segment =
                new Layout.LayoutSegment(Layout.ReplicationMode.CHAIN_REPLICATION, 0L, 10L, Arrays.asList(stripe));
        Layout testLayout = createTestLayout(Arrays.asList(segment));
        List<CurrentTransferSegment> failed = Arrays.asList(
                createTransferSegment(0L, 5L, FAILED));

        LayoutManagementView layoutManagementView = mock(LayoutManagementView.class);

        RestoreRedundancyMergeSegments action = new RestoreRedundancyMergeSegments("localhost");
        assertThatThrownBy(() ->
                action.tryRestoreRedundancyAndMergeSegments(failed, testLayout, layoutManagementView))
                .isInstanceOf(StreamProcessFailure.class);
    }

    // list with non transferred entities
    @Test
    public void testTryRestoreNotTransferred() {
        Layout.LayoutStripe stripe = new Layout.LayoutStripe(Arrays.asList("localhost"));
        Layout.LayoutSegment segment =
                new Layout.LayoutSegment(Layout.ReplicationMode.CHAIN_REPLICATION, 0L, 10L, Arrays.asList(stripe));
        Layout testLayout = createTestLayout(Arrays.asList(segment));
        List<CurrentTransferSegment> notTransferred = Arrays.asList( createTransferSegment(0L, 5L, NOT_TRANSFERRED));

        LayoutManagementView layoutManagementView = mock(LayoutManagementView.class);

        RestoreRedundancyMergeSegments action = new RestoreRedundancyMergeSegments("localhost");
        RestoreRedundancyMergeSegments.RestoreStatus status =
                action.tryRestoreRedundancyAndMergeSegments(notTransferred, testLayout, layoutManagementView);
        assertThat(status).isEqualTo(NOT_RESTORED);
    }

    // list with restored segments that can be merged
    @Test
    public void testTryRestoreRestoredNotMerged() {
        Layout.LayoutStripe stripe = new Layout.LayoutStripe(Arrays.asList("localhost", "B"));
        Layout.LayoutSegment segment =
                new Layout.LayoutSegment(Layout.ReplicationMode.CHAIN_REPLICATION, 0L, 10L, Arrays.asList(stripe));

        Layout.LayoutStripe stripe2 = new Layout.LayoutStripe(Arrays.asList("localhost", "B"));
        Layout.LayoutSegment segment2 =
                new Layout.LayoutSegment(Layout.ReplicationMode.CHAIN_REPLICATION, 10L, 15L, Arrays.asList(stripe2));
        Layout testLayout = createTestLayout(Arrays.asList(segment, segment2));
        List<CurrentTransferSegment> restored = Arrays.asList( createTransferSegment(0L, 9L, SegmentState.RESTORED));

        LayoutManagementView layoutManagementView = mock(LayoutManagementView.class);

        Layout.LayoutSegment segment3 =
                new Layout.LayoutSegment(Layout.ReplicationMode.CHAIN_REPLICATION, 0L, 15L, Arrays.asList(stripe));

        doAnswer(invocation -> {
            Layout oldLayout = (Layout) invocation.getArguments()[0];
            oldLayout.setSegments(Arrays.asList(segment3));
            return null;
        }).when(layoutManagementView).mergeSegments(testLayout);

        RestoreRedundancyMergeSegments action = new RestoreRedundancyMergeSegments("localhost");
        RestoreRedundancyMergeSegments.RestoreStatus status =
                action.tryRestoreRedundancyAndMergeSegments(restored, testLayout, layoutManagementView);
        assertThat(status).isEqualTo(RESTORED);
        assertThat(testLayout.getSegments().get(0)).isEqualTo(segment3);
    }

    // list with transferred segments that can be merged
    @Test
    public void testTryRestoreTransferredCanMerge() {
        Layout.LayoutStripe stripe = new Layout.LayoutStripe(Arrays.asList("B"));
        Layout.LayoutSegment segment =
                new Layout.LayoutSegment(Layout.ReplicationMode.CHAIN_REPLICATION, 0L, 10L, Arrays.asList(stripe));

        Layout.LayoutStripe stripe2 = new Layout.LayoutStripe(Arrays.asList("localhost", "B"));
        Layout.LayoutSegment segment2 =
                new Layout.LayoutSegment(Layout.ReplicationMode.CHAIN_REPLICATION, 10L, 15L, Arrays.asList(stripe2));
        Layout testLayout = createTestLayout(Arrays.asList(segment, segment2));
        List<CurrentTransferSegment> transferred = Arrays.asList( createTransferSegment(0L, 9L, TRANSFERRED));

        LayoutManagementView layoutManagementView = mock(LayoutManagementView.class);

        Layout expectedLayout = new RedundancyCalculator("localhost").updateLayoutAfterRedundancyRestoration(transferred,
                testLayout);

        RestoreRedundancyMergeSegments action = new RestoreRedundancyMergeSegments("localhost");
        doNothing().when(layoutManagementView).mergeSegments(expectedLayout);
        RestoreRedundancyMergeSegments.RestoreStatus status =
                action.tryRestoreRedundancyAndMergeSegments(transferred, testLayout, layoutManagementView);

        assertThat(status).isEqualTo(RESTORED);
    }

    // list with transferred segments that can't be merged, but are still restored
    @Test
    public void testTryRestoreTrasferredCantMerge() {

        Layout.LayoutStripe stripe = new Layout.LayoutStripe(Arrays.asList("B"));
        Layout.LayoutSegment segment =
                new Layout.LayoutSegment(Layout.ReplicationMode.CHAIN_REPLICATION, 0L, 10L, Arrays.asList(stripe));

        Layout.LayoutStripe stripe2 = new Layout.LayoutStripe(Arrays.asList("localhost", "B", "C"));
        Layout.LayoutSegment segment2 =
                new Layout.LayoutSegment(Layout.ReplicationMode.CHAIN_REPLICATION, 10L, 15L, Arrays.asList(stripe2));
        Layout testLayout = createTestLayout(Arrays.asList(segment, segment2));
        List<CurrentTransferSegment> transferred = Arrays.asList( createTransferSegment(0L, 9L, TRANSFERRED));

        LayoutManagementView layoutManagementView = mock(LayoutManagementView.class);

        Layout expectedLayout = new RedundancyCalculator("localhost").updateLayoutAfterRedundancyRestoration(transferred,
                testLayout);

        RestoreRedundancyMergeSegments action = new RestoreRedundancyMergeSegments("localhost");
        doNothing().when(layoutManagementView).mergeSegments(expectedLayout);
        RestoreRedundancyMergeSegments.RestoreStatus status =
                action.tryRestoreRedundancyAndMergeSegments(transferred, testLayout, layoutManagementView);

        assertThat(status).isEqualTo(RESTORED);
    }

}
