package org.corfudb.infrastructure.orchestrator.actions;

import org.corfudb.infrastructure.LayoutBasedTest;
import org.corfudb.infrastructure.log.statetransfer.exceptions.StateTransferFailure;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.LayoutManagementView;
import org.corfudb.util.Sleep;
import org.junit.Test;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.*;
import static org.corfudb.infrastructure.orchestrator.actions.RestoreRedundancyMergeSegments.RestoreStatus.NOT_RESTORED;
import static org.corfudb.infrastructure.orchestrator.actions.RestoreRedundancyMergeSegments.RestoreStatus.RESTORED;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RestoreRedundancyAndMergeSegmentsTest extends LayoutBasedTest {

    // empty list
    @Test
    public void testTryRestoreRedundancyEmptyList(){
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
    // list with non completed transfers only
    @Test
    public void testTryRestoreNotDoneTransfers(){
        Layout.LayoutStripe stripe = new Layout.LayoutStripe(Arrays.asList("localhost"));
        Layout.LayoutSegment segment =
                new Layout.LayoutSegment(Layout.ReplicationMode.CHAIN_REPLICATION, 0L, 10L, Arrays.asList(stripe));
        Layout testLayout = createTestLayout(Arrays.asList(segment));
        List<CurrentTransferSegment> notDone = Arrays.asList(new CurrentTransferSegment(0L, 5L, CompletableFuture.supplyAsync(() -> {
            Sleep.sleepUninterruptibly(Duration.ofMillis(5000));
            return null;
        })));

        LayoutManagementView layoutManagementView = mock(LayoutManagementView.class);

        RestoreRedundancyMergeSegments action = new RestoreRedundancyMergeSegments("localhost");
        RestoreRedundancyMergeSegments.RestoreStatus status =
                action.tryRestoreRedundancyAndMergeSegments(notDone, testLayout, layoutManagementView);
        assertThat(status).isEqualTo(NOT_RESTORED);
    }

    // list with transfer that failed exceptionally
    @Test
    public void testTryRestoreExceptionalTransfers(){
        Layout.LayoutStripe stripe = new Layout.LayoutStripe(Arrays.asList("localhost"));
        Layout.LayoutSegment segment =
                new Layout.LayoutSegment(Layout.ReplicationMode.CHAIN_REPLICATION, 0L, 10L, Arrays.asList(stripe));
        Layout testLayout = createTestLayout(Arrays.asList(segment));
        List<CurrentTransferSegment> failed = Arrays.asList(new CurrentTransferSegment(0L, 5L, CompletableFuture.supplyAsync(() -> {
            throw new RuntimeException("Failure");
        })));

        LayoutManagementView layoutManagementView = mock(LayoutManagementView.class);

        RestoreRedundancyMergeSegments action = new RestoreRedundancyMergeSegments("localhost");
        assertThatThrownBy(() ->
                action.tryRestoreRedundancyAndMergeSegments(failed, testLayout, layoutManagementView))
                .isInstanceOf(StateTransferFailure.class)
                .hasRootCauseInstanceOf(RuntimeException.class);
    }


    // list with transfer that failed
    @Test
    public void testTryRestoreFailedTransfer(){
        Layout.LayoutStripe stripe = new Layout.LayoutStripe(Arrays.asList("localhost"));
        Layout.LayoutSegment segment =
                new Layout.LayoutSegment(Layout.ReplicationMode.CHAIN_REPLICATION, 0L, 10L, Arrays.asList(stripe));
        Layout testLayout = createTestLayout(Arrays.asList(segment));
        List<CurrentTransferSegment> failed = Arrays.asList(new CurrentTransferSegment(0L, 5L,
                CompletableFuture.completedFuture(new CurrentTransferSegmentStatus(SegmentState.FAILED, -1L,
                        new StateTransferFailure(new IllegalStateException())))));

        LayoutManagementView layoutManagementView = mock(LayoutManagementView.class);

        RestoreRedundancyMergeSegments action = new RestoreRedundancyMergeSegments("localhost");
        assertThatThrownBy(() ->
                action.tryRestoreRedundancyAndMergeSegments(failed, testLayout, layoutManagementView))
                .isInstanceOf(StateTransferFailure.class)
                .hasRootCauseInstanceOf(IllegalStateException.class);
    }

    // list with non transferred entities
    @Test
    public void testTryRestoreNotTransferred(){
        Layout.LayoutStripe stripe = new Layout.LayoutStripe(Arrays.asList("localhost"));
        Layout.LayoutSegment segment =
                new Layout.LayoutSegment(Layout.ReplicationMode.CHAIN_REPLICATION, 0L, 10L, Arrays.asList(stripe));
        Layout testLayout = createTestLayout(Arrays.asList(segment));
        List<CurrentTransferSegment> notTransferred = Arrays.asList(new CurrentTransferSegment(0L, 5L,
                CompletableFuture.completedFuture(new CurrentTransferSegmentStatus(SegmentState.NOT_TRANSFERRED, -1L))));

        LayoutManagementView layoutManagementView = mock(LayoutManagementView.class);

        RestoreRedundancyMergeSegments action = new RestoreRedundancyMergeSegments("localhost");
        RestoreRedundancyMergeSegments.RestoreStatus status =
                action.tryRestoreRedundancyAndMergeSegments(notTransferred, testLayout, layoutManagementView);
        assertThat(status).isEqualTo(NOT_RESTORED);
    }

    // list with restored segments that can be merged
    @Test
    public void testTryRestoreRestoredNotMerged(){
        Layout.LayoutStripe stripe = new Layout.LayoutStripe(Arrays.asList("localhost", "B"));
        Layout.LayoutSegment segment =
                new Layout.LayoutSegment(Layout.ReplicationMode.CHAIN_REPLICATION, 0L, 10L, Arrays.asList(stripe));

        Layout.LayoutStripe stripe2 = new Layout.LayoutStripe(Arrays.asList("localhost", "B"));
        Layout.LayoutSegment segment2 =
                new Layout.LayoutSegment(Layout.ReplicationMode.CHAIN_REPLICATION, 10L, 15L, Arrays.asList(stripe2));
        Layout testLayout = createTestLayout(Arrays.asList(segment, segment2));
        List<CurrentTransferSegment> restored = Arrays.asList(new CurrentTransferSegment(0L, 9L,
                CompletableFuture.completedFuture(new CurrentTransferSegmentStatus(SegmentState.RESTORED, 9L))));

        LayoutManagementView layoutManagementView = mock(LayoutManagementView.class);

        Layout.LayoutSegment segment3 =
                new Layout.LayoutSegment(Layout.ReplicationMode.CHAIN_REPLICATION, 0L, 15L, Arrays.asList(stripe));

        doAnswer(invocation -> {
            Layout oldLayout = (Layout)invocation.getArguments()[0];
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
    public void testTryRestoreTransferredCanMerge(){
        Layout.LayoutStripe stripe = new Layout.LayoutStripe(Arrays.asList("B"));
        Layout.LayoutSegment segment =
                new Layout.LayoutSegment(Layout.ReplicationMode.CHAIN_REPLICATION, 0L, 10L, Arrays.asList(stripe));

        Layout.LayoutStripe stripe2 = new Layout.LayoutStripe(Arrays.asList("localhost", "B"));
        Layout.LayoutSegment segment2 =
                new Layout.LayoutSegment(Layout.ReplicationMode.CHAIN_REPLICATION, 10L, 15L, Arrays.asList(stripe2));
        Layout testLayout = createTestLayout(Arrays.asList(segment, segment2));
        List<CurrentTransferSegment> transferred = Arrays.asList(new CurrentTransferSegment(0L, 9L,
                CompletableFuture.completedFuture(new CurrentTransferSegmentStatus(SegmentState.TRANSFERRED, 9L))));

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
    public void testTryRestoreTrasferredCantMerge(){

        Layout.LayoutStripe stripe = new Layout.LayoutStripe(Arrays.asList("B"));
        Layout.LayoutSegment segment =
                new Layout.LayoutSegment(Layout.ReplicationMode.CHAIN_REPLICATION, 0L, 10L, Arrays.asList(stripe));

        Layout.LayoutStripe stripe2 = new Layout.LayoutStripe(Arrays.asList("localhost", "B", "C"));
        Layout.LayoutSegment segment2 =
                new Layout.LayoutSegment(Layout.ReplicationMode.CHAIN_REPLICATION, 10L, 15L, Arrays.asList(stripe2));
        Layout testLayout = createTestLayout(Arrays.asList(segment, segment2));
        List<CurrentTransferSegment> transferred = Arrays.asList(new CurrentTransferSegment(0L, 9L,
                CompletableFuture.completedFuture(new CurrentTransferSegmentStatus(SegmentState.TRANSFERRED, 9L))));

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
