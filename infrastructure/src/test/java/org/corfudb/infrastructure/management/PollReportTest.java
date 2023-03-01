package org.corfudb.infrastructure.management;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.protocols.wireprotocol.NodeState;
import org.corfudb.runtime.view.Layout;
import org.junit.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.infrastructure.NodeNames.A;
import static org.corfudb.infrastructure.NodeNames.B;
import static org.corfudb.infrastructure.NodeNames.C;
import static org.corfudb.infrastructure.management.NodeStateTestUtil.nodeState;
import static org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity.ConnectionStatus.FAILED;
import static org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity.ConnectionStatus.OK;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PollReportTest {

    private final long epoch = 1;

    @Test
    public void testConnectionStatusesOneNode() {
        final String localEndpoint = A;

        ClusterState clusterState = ClusterState.buildClusterState(
                localEndpoint, ImmutableList.of(), nodeState(localEndpoint, epoch, OK)
        );

        final long epoch = 1;
        PollReport pollReport = PollReport.builder()
                .pingResponsiveServers(ImmutableList.of(localEndpoint))
                .wrongEpochs(ImmutableMap.of(localEndpoint, epoch))
                .clusterState(clusterState)
                .elapsedTime(Duration.ZERO)
                .build();

        assertThat(pollReport.getReachableNodes()).isEmpty();
        assertThat(pollReport.getAllReachableNodes()).containsExactly(localEndpoint);
    }

    @Test
    public void testConnectionStatuses() {
        final String localEndpoint = A;

        ClusterState clusterState = ClusterState.buildClusterState(
                localEndpoint,
                ImmutableList.of(),
                nodeState(localEndpoint, epoch, OK, FAILED, FAILED),
                NodeState.getUnavailableNodeState(B),
                NodeState.getUnavailableNodeState(C)
        );

        final long epoch = 1;
        PollReport pollReport = PollReport.builder()
                .pingResponsiveServers(ImmutableList.of(A, B, C))
                .wrongEpochs(ImmutableMap.of(B, epoch))
                .clusterState(clusterState)
                .elapsedTime(Duration.ZERO)
                .build();

        assertThat(pollReport.getReachableNodes()).containsExactly(localEndpoint);
        assertThat(pollReport.getAllReachableNodes()).containsExactly(localEndpoint, B);
        assertThat(pollReport.getFailedNodes()).containsExactly(C);
    }

    /**
     * Even if cluster state contains wrong NodeState (which is 'b')
     * it has not to change list of failed nodes.
     */
    @Test
    public void testInconsistencyBetweenClusterStateAndWrongEpochs() {
        final String localEndpoint = A;

        ClusterState clusterState = ClusterState.buildClusterState(
                localEndpoint,
                ImmutableList.of(),
                nodeState(localEndpoint, epoch, OK, FAILED, FAILED),
                nodeState(B, epoch, OK, OK, FAILED),
                NodeState.getUnavailableNodeState(C)
        );

        final long epoch = 1;
        PollReport pollReport = PollReport.builder()
                .pingResponsiveServers(ImmutableList.of(A, B, C))
                .wrongEpochs(ImmutableMap.of(B, epoch))
                .clusterState(clusterState)
                .elapsedTime(Duration.ZERO)
                .build();

        assertThat(pollReport.getReachableNodes()).containsExactly(localEndpoint);
        assertThat(pollReport.getAllReachableNodes()).containsExactly(localEndpoint, B);
        assertThat(pollReport.getFailedNodes()).containsExactly(C);
    }

    @Test
    public void testIsSlotUnfilled() {
        final String localEndpoint = A;

        ClusterState clusterState = ClusterState.buildClusterState(
                localEndpoint,
                ImmutableList.of(),
                nodeState(localEndpoint, epoch, OK, OK, FAILED),
                nodeState(B, epoch, OK, OK, FAILED),
                NodeState.getUnavailableNodeState(C)
        );

        final long epoch = 1;
        PollReport pollReport = PollReport.builder()
                .pingResponsiveServers(ImmutableList.of(localEndpoint, B, C))
                .wrongEpochs(ImmutableMap.of(localEndpoint, epoch, B, epoch, C, epoch))
                .clusterState(clusterState)
                .elapsedTime(Duration.ZERO)
                .build();

        Layout latestCommitted = mock(Layout.class);
        when(latestCommitted.getEpoch()).thenReturn(epoch - 1);
        assertThat(pollReport.getLayoutSlotUnFilled(latestCommitted)).isPresent();
    }
}
