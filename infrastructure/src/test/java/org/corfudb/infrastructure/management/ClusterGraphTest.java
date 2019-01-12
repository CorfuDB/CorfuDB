package org.corfudb.infrastructure.management;

import static org.junit.Assert.fail;

import com.google.common.collect.ImmutableMap;
import org.corfudb.infrastructure.management.ClusterGraph.NodeRank;
import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.protocols.wireprotocol.NodeState;
import org.corfudb.protocols.wireprotocol.NodeState.HeartbeatTimestamp;
import org.corfudb.protocols.wireprotocol.NodeState.NodeConnectivity;
import org.corfudb.protocols.wireprotocol.NodeState.NodeConnectivityState;
import org.corfudb.protocols.wireprotocol.SequencerMetrics;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;
import java.util.SortedSet;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ClusterGraphTest {

    @Test
    public void testTransformation() {
        NodeState a = NodeState.builder()
                .sequencerMetrics(SequencerMetrics.READY)
                .heartbeat(new HeartbeatTimestamp(0, 0))
                .connectivity(connectivity("a", ImmutableMap.of("a", true, "b", true, "c", false)))
                .build();

        NodeState b = NodeState.builder()
                .sequencerMetrics(SequencerMetrics.READY)
                .heartbeat(new HeartbeatTimestamp(0, 0))
                .connectivity(connectivity("b", ImmutableMap.of("a", true, "b", true, "c", false)))
                .build();

        NodeState c = NodeState.getDefaultNodeState("c");

        ImmutableMap<String, NodeState> nodes = ImmutableMap.of("a", a, "b", b, "c", c);
        ClusterState clusterState = ClusterState.builder()
                .nodes(nodes)
                .build();

        ClusterGraph graph = ClusterGraph.transform(clusterState);
        fail("check graph transformation");
    }

    @Test
    public void testToSymmetricForTwoNodes() {
        NodeConnectivity a = connectivity("a", ImmutableMap.of("a", true, "b", false));
        NodeConnectivity b = connectivity("b", ImmutableMap.of("a", true, "b", true));

        ClusterGraph graph = cluster(a, b);

        System.out.println(graph.toSymmetric().toString());
        fail("check symmetric");
    }

    @Test
    public void testToSymmetricForThreeNodes() {
        NodeConnectivity a = connectivity("a", ImmutableMap.of("a", true, "b", false, "c", false));
        NodeConnectivity b = connectivity("b", ImmutableMap.of("a", true, "b", true, "c", true));
        NodeConnectivity c = connectivity("c", ImmutableMap.of("a", true, "b", false, "c", true));

        ClusterGraph graph = cluster(a, b, c);

        System.out.println(graph.toSymmetric().toString());
        fail("check symmetric");
    }

    @Test
    public void testToSymmetricForThreeNodesWithUnavailableNode() {
        NodeConnectivity a = connectivity("a", ImmutableMap.of("a", true, "b", false, "c", false));
        NodeConnectivity b = unavailable("b");
        NodeConnectivity c = connectivity("c", ImmutableMap.of("a", true, "b", false, "c", true));

        ClusterGraph graph = cluster(a, b, c);

        System.out.println(graph.toSymmetric().toString());
        fail("check symmetric");
    }

    @Test
    public void testQuorumNodes() {
        NodeConnectivity a = connectivity("a", ImmutableMap.of("a", true, "b", true, "c", true));
        NodeConnectivity b = connectivity("b", ImmutableMap.of("a", false, "b", true, "c", true));
        NodeConnectivity c = connectivity("a", ImmutableMap.of("a", true, "b", false, "c", true));

        ClusterGraph graph = cluster(a, b, c);
        SortedSet<NodeRank> quorumNodes = graph.toSymmetric().getQuorumNodes();

        fail("check quorum nodes");
    }

    private ClusterGraph cluster(NodeConnectivity... nodes) {
        Map<String, NodeConnectivity> graph = Arrays.stream(nodes)
                .collect(Collectors.toMap(NodeConnectivity::getEndpoint, Function.identity()));

        return ClusterGraph.builder()
                .graph(ImmutableMap.copyOf(graph))
                .build();
    }

    private NodeConnectivity connectivity(String endpoint, ImmutableMap<String, Boolean> state) {
        return NodeConnectivity.builder()
                .endpoint(endpoint)
                .type(NodeConnectivityState.CONNECTED)
                .connectivity(state)
                .build();
    }

    private NodeConnectivity unavailable(String endpoint) {
        return NodeConnectivity.builder()
                .endpoint(endpoint)
                .type(NodeConnectivityState.UNAVAILABLE)
                .connectivity(ImmutableMap.of())
                .build();
    }
}