package org.corfudb.infrastructure.management;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.protocols.wireprotocol.NodeState;
import org.corfudb.protocols.wireprotocol.SequencerMetrics;
import org.junit.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.infrastructure.management.NodeStateTestUtil.A;
import static org.corfudb.infrastructure.management.NodeStateTestUtil.B;
import static org.corfudb.infrastructure.management.NodeStateTestUtil.C;
import static org.corfudb.infrastructure.management.NodeStateTestUtil.nodeState;
import static org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity.ConnectionStatus.OK;
import static org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity.connectivity;
import static org.junit.Assert.assertEquals;

public class ClusterStateTest {

    @Test
    public void isReady() {
        final String localEndpoint = A;
        final long epoch1 = 1;
        final long epoch2 = 2;

        ClusterState invalidClusterState = ClusterState.buildClusterState(
                localEndpoint,
                ImmutableList.of(),
                nodeState(localEndpoint, epoch1, OK),
                nodeState(B, epoch2, OK)
        );
        assertThat(invalidClusterState.isReady()).isFalse();

        ClusterState validClusterState = ClusterState.buildClusterState(
                localEndpoint,
                ImmutableList.of(),
                nodeState(localEndpoint, epoch1, OK),
                nodeState(B, epoch1, OK)
        );
        assertThat(validClusterState.isReady()).isTrue();
    }

    /**
     * Ensure that {@link ClusterState#getPingResponsiveNodes()} returns all nodes that have
     * responded to our ping, despite the face that all nodes are marked as unresponsive by the
     * current layout and one of the nodes responded with WrongEpochException.
     */
    @Test
    public void testTransformation() {
        NodeState a = NodeState.builder()
                .sequencerMetrics(SequencerMetrics.READY)
                .connectivity(connectivity(A, ImmutableMap.of(A, OK, B, OK, C, OK)))
                .fileSystem(Optional.empty())
                .build();

        NodeState b = NodeState.builder()
                .sequencerMetrics(SequencerMetrics.READY)
                .connectivity(connectivity(B, ImmutableMap.of(A, OK, B, OK, C, OK)))
                .fileSystem(Optional.empty())
                .build();

        NodeState c = NodeState.builder()
                .sequencerMetrics(SequencerMetrics.READY)
                .connectivity(connectivity(B, ImmutableMap.of(A, OK, B, OK, C, OK)))
                .fileSystem(Optional.empty())
                .build();

        ImmutableMap<String, NodeState> nodes = ImmutableMap.of(A, a, B, b, C, c);
        ClusterState clusterState = ClusterState.builder()
                .localEndpoint(A)
                .nodes(nodes)
                .unresponsiveNodes(ImmutableList.copyOf(nodes.keySet()))
                .build();

        assertEquals(clusterState.getPingResponsiveNodes().size(), nodes.size());
    }
}
