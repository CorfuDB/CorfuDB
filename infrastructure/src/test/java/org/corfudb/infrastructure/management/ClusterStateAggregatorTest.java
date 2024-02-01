package org.corfudb.infrastructure.management;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.protocols.wireprotocol.NodeState;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeRank;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.infrastructure.NodeNames.A;
import static org.corfudb.infrastructure.NodeNames.B;
import static org.corfudb.infrastructure.NodeNames.C;
import static org.corfudb.infrastructure.management.NodeStateTestUtil.nodeState;
import static org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity.ConnectionStatus.FAILED;
import static org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity.ConnectionStatus.OK;

public class ClusterStateAggregatorTest {

    private final long epoch = 1;

    @Test
    public void getAggregatedStateSingleNodeCluster() {
        final String localEndpoint = A;

        ClusterState clusterState = ClusterState.buildClusterState(
                localEndpoint,
                ImmutableList.of(),
                nodeState(A, epoch, OK)
        );

        List<ClusterState> clusterStates = Arrays.asList(clusterState, clusterState, clusterState);
        ClusterStateAggregator aggregator = ClusterStateAggregator.builder()
                .localEndpoint(localEndpoint)
                .clusterStates(clusterStates)
                .unresponsiveNodes(ImmutableList.of())
                .build();

        NodeState expectedNodeState = clusterState.getNode(localEndpoint).get();
        NodeState actualNodeState = aggregator.getAggregatedState().getNode(localEndpoint).get();
        assertThat(actualNodeState).isEqualTo(expectedNodeState);
    }

    @Test
    public void getAggregatedState() {
        final String localEndpoint = A;

        ClusterState clusterState1 = ClusterState.buildClusterState(
                localEndpoint,
                ImmutableList.of(),
                nodeState(localEndpoint, epoch, OK, FAILED, FAILED),
                NodeState.getUnavailableNodeState(B),
                NodeState.getUnavailableNodeState(C)
        );

        ClusterState clusterState2 = ClusterState.buildClusterState(
                localEndpoint,
                ImmutableList.of(),
                nodeState(A, epoch, OK, OK, FAILED),
                nodeState(B, epoch, OK, OK, FAILED),
                NodeState.getUnavailableNodeState(C)
        );

        ClusterState clusterState3 = ClusterState.buildClusterState(
                localEndpoint,
                ImmutableList.of(),
                nodeState(localEndpoint, epoch, OK, FAILED, FAILED),
                NodeState.getUnavailableNodeState(B),
                NodeState.getNotReadyNodeState(C)
        );

        List<ClusterState> clusterStates = Arrays.asList(
                clusterState1, clusterState2, clusterState3
        );

        ClusterStateAggregator aggregator = ClusterStateAggregator.builder()
                .localEndpoint(localEndpoint)
                .clusterStates(clusterStates)
                .unresponsiveNodes(ImmutableList.of())
                .build();

        //check [CONNECTED, CONNECTED, CONNECTED]
        NodeState expectedLocalNodeState = nodeState(localEndpoint, epoch, OK, OK, FAILED);
        NodeState localNodeState = aggregator.getAggregatedState().getNode(localEndpoint).get();
        assertThat(localNodeState.isConnected()).isTrue();
        assertThat(localNodeState).isEqualTo(expectedLocalNodeState);

        //check [UNAVAILABLE, CONNECTED, UNAVAILABLE]
        NodeState expectedNodeBState = clusterState2.getNode(B).get();
        NodeState nodeBState = aggregator.getAggregatedState().getNode(B).get();
        assertThat(nodeBState.isConnected()).isTrue();
        assertThat(nodeBState).isEqualTo(expectedNodeBState);

        //check [UNAVAILABLE, UNAVAILABLE, NOT_READY]
        NodeState expectedNodeCState = clusterState3.getNode(C).get();
        NodeState nodeCState = aggregator.getAggregatedState().getNode(C).get();
        assertThat(nodeCState.isConnected()).isFalse();
        assertThat(nodeCState).isEqualTo(expectedNodeCState);
    }

    @Test
    public void testAggregation() {

        ClusterState cs1 = ClusterState.buildClusterState(
                A,
                ImmutableList.of(),
                nodeState(A, epoch, OK, OK, OK),
                nodeState(B, epoch, OK, OK, OK),
                nodeState(C, epoch, OK, OK, OK)
        );

        ClusterState cs2 = ClusterState.buildClusterState(
                A,
                ImmutableList.of(),
                nodeState(A, epoch, OK, OK, OK),
                nodeState(B, epoch, OK, OK, OK),
                nodeState(C, epoch, OK, OK, OK)
        );

        ClusterState cs3 = ClusterState.buildClusterState(
                A,
                ImmutableList.of(),
                nodeState(A, epoch, OK, OK, FAILED),
                nodeState(B, epoch, OK, OK, OK),
                nodeState(C, epoch, OK, OK, OK)
        );

        PollReport pollReport1 = PollReport.builder()
                .pingResponsiveServers(ImmutableList.of(A, B, C))
                .wrongEpochs(ImmutableMap.of())
                .clusterState(cs1)
                .elapsedTime(Duration.ZERO)
                .build();

        PollReport pollReport2 = PollReport.builder()
                .pingResponsiveServers(ImmutableList.of(A, B, C))
                .wrongEpochs(ImmutableMap.of())
                .clusterState(cs2)
                .elapsedTime(Duration.ZERO)
                .build();

        PollReport pollReport3 = PollReport.builder()
                .pingResponsiveServers(ImmutableList.of(A, B))
                .wrongEpochs(ImmutableMap.of())
                .clusterState(cs3)
                .elapsedTime(Duration.ZERO)
                .build();

        List<PollReport> reports = new ArrayList<>();
        reports.add(pollReport1);
        reports.add(pollReport2);
        reports.add(pollReport3);

        PollReport pr = aggregatePollReports(epoch, reports, ImmutableList.<String>builder().build());

        CompleteGraphAdvisor advisor = new CompleteGraphAdvisor();
        Optional<NodeRank> ret = advisor.failedServer(pr.getClusterState());
        assertThat(ret).isEmpty();
    }

    private PollReport aggregatePollReports(long epoch, List<PollReport> reports,
                                            ImmutableList<String> layoutUnresponsiveNodes) {
        Map<String, Long> wrongEpochsAggregated = new HashMap<>();

        reports.forEach(report -> {
            //Calculate wrong epochs
            wrongEpochsAggregated.putAll(report.getWrongEpochs());
            report.getReachableNodes().forEach(wrongEpochsAggregated::remove);
        });

        List<ClusterState> clusterStates = reports.stream()
                .map(PollReport::getClusterState)
                .collect(Collectors.toList());

        ClusterStateAggregator aggregator = ClusterStateAggregator.builder()
                .localEndpoint(A)
                .clusterStates(clusterStates)
                .unresponsiveNodes(layoutUnresponsiveNodes)
                .build();

        Duration totalElapsedTime = reports.stream()
                .map(PollReport::getElapsedTime)
                .reduce(Duration.ZERO, Duration::plus);

        final ClusterState aggregatedClusterState = aggregator.getAggregatedState();
        return PollReport.builder()
                .pollEpoch(epoch)
                .elapsedTime(totalElapsedTime)
                .pingResponsiveServers(aggregatedClusterState.getPingResponsiveNodes())
                .wrongEpochs(ImmutableMap.copyOf(wrongEpochsAggregated))
                .clusterState(aggregatedClusterState)
                .build();
    }
}
