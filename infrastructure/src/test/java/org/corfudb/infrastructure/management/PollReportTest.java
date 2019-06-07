package org.corfudb.infrastructure.management;


import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.infrastructure.management.NodeStateTestUtil.nodeState;
import static org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity.ConnectionStatus.FAILED;
import static org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity.ConnectionStatus.OK;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.protocols.wireprotocol.NodeState;
import org.junit.Test;

import java.time.Duration;

public class PollReportTest {

    private final long epoch = 1;

    @Test
    public void testConnectionStatusesOneNode() {
        final String localEndpoint = "a";

        ClusterState clusterState = ClusterState.buildClusterState(
                localEndpoint, nodeState("a", epoch, OK)
        );

        final long epoch = 1;
        PollReport pollReport = PollReport.builder()
                .responsiveServers(ImmutableList.of("a"))
                .wrongEpochs(ImmutableMap.of("a", epoch))
                .clusterState(clusterState)
                .elapsedTime(Duration.ZERO)
                .build();

        assertThat(pollReport.getReachableNodes()).isEmpty();
        assertThat(pollReport.getAllReachableNodes()).containsExactly("a");
    }

    @Test
    public void testConnectionStatuses() {
        final String localEndpoint = "a";

        ClusterState clusterState = ClusterState.buildClusterState(
                localEndpoint,
                nodeState("a", epoch, OK, FAILED, FAILED),
                NodeState.getUnavailableNodeState("b"),
                NodeState.getUnavailableNodeState("c")
        );

        final long epoch = 1;
        PollReport pollReport = PollReport.builder()
                .responsiveServers(ImmutableList.of("a", "b", "c"))
                .wrongEpochs(ImmutableMap.of("b", epoch))
                .clusterState(clusterState)
                .elapsedTime(Duration.ZERO)
                .build();

        assertThat(pollReport.getReachableNodes()).containsExactly("a");
        assertThat(pollReport.getAllReachableNodes()).containsExactly("a", "b");
        assertThat(pollReport.getFailedNodes()).containsExactly("c");
    }

    /**
     * Even if cluster state contains wrong NodeState (which is 'b')
     * it has not to change list of failed nodes.
     */
    @Test
    public void testInconsistencyBetweenClusterStateAndWrongEpochs() {
        final String localEndpoint = "a";

        ClusterState clusterState = ClusterState.buildClusterState(
                localEndpoint,
                nodeState("a", epoch, OK, FAILED, FAILED),
                nodeState("b", epoch, OK, OK, FAILED),
                NodeState.getUnavailableNodeState("c")
        );

        final long epoch = 1;
        PollReport pollReport = PollReport.builder()
                .responsiveServers(ImmutableList.of("a", "b", "c"))
                .wrongEpochs(ImmutableMap.of("b", epoch))
                .clusterState(clusterState)
                .elapsedTime(Duration.ZERO)
                .build();

        assertThat(pollReport.getReachableNodes()).containsExactly("a");
        assertThat(pollReport.getAllReachableNodes()).containsExactly("a", "b");
        assertThat(pollReport.getFailedNodes()).containsExactly("c");
    }

    @Test
    public void testIsSlotUnfilled() {
        final String localEndpoint = "a";

        ClusterState clusterState = ClusterState.buildClusterState(
                localEndpoint,
                nodeState("a", epoch, OK, OK, FAILED),
                nodeState("b", epoch, OK, OK, FAILED),
                NodeState.getUnavailableNodeState("c")
        );

        final long epoch = 1;

        PollReport pollReport = PollReport.builder()
                .responsiveServers(ImmutableList.of("a", "b"))
                .wrongEpochs(ImmutableMap.of("a", epoch, "b", epoch, "c", epoch))
                .clusterState(clusterState)
                .elapsedTime(Duration.ZERO)
                .build();

        assertThat(pollReport.isCurrentLayoutSlotUnFilled()).isTrue();
    }
}
