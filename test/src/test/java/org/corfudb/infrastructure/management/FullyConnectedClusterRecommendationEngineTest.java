package org.corfudb.infrastructure.management;

import static org.assertj.core.api.Assertions.assertThat;

import org.corfudb.infrastructure.TestLayoutBuilder;
import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.protocols.wireprotocol.NodeState;
import org.corfudb.protocols.wireprotocol.SequencerMetrics;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.NodeLocator;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class test an instance {@link ClusterRecommendationEngine} with a FULLY_CONNECTED_CLUSTER
 * as the concrete underlying {@link ClusterRecommendationStrategy}.
 *
 * Created by Sam Behnam on 11/13/18.
 */
public class FullyConnectedClusterRecommendationEngineTest extends AbstractViewTest {

    private ClusterRecommendationEngine fullyConnectedClusterRecommendationEngine;


    @Before
    public void setUp() {
        fullyConnectedClusterRecommendationEngine =
                ClusterRecommendationEngineFactory
                        .createForStrategy(ClusterRecommendationStrategy.FULLY_CONNECTED_CLUSTER);
    }

    /**
     * This test ensures that the correct corresponding strategy is returned.
     */
    @Test
    public void getClusterRecommendationStrategyName() {
        final ClusterRecommendationStrategy strategy =
                fullyConnectedClusterRecommendationEngine.getClusterRecommendationStrategy();
        assertThat(strategy).isEqualTo(ClusterRecommendationStrategy.FULLY_CONNECTED_CLUSTER);
    }

    /**
     * In a three node cluster with all responsive nodes, when the latest cluster state
     * includes one symmetric links failure between two responsive nodes in the the cluster, it
     * is expected that:
     * Recommended Failed Node: expected to include the node with smaller name (String comparison
     * on the name).
     * Recommended Healed Node: expected to be empty.
     */
    @Test
    public void findFailedHealed_3Servers_Remove1LinkFailedOf3Nodes() {

        // Build a layout that includes all responsive servers
        Layout layout = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(SERVERS.PORT_0)
                .addLayoutServer(SERVERS.PORT_1)
                .addLayoutServer(SERVERS.PORT_2)
                .addSequencer(SERVERS.PORT_0)
                .buildSegment()
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addToSegment()
                .addToLayout()
                .build();

        // Build a cluster state representing a symmetric link failure between responsive nodes
        // ENDPOINT0 and ENDPOINT1 nodes.
        NodeState nodeStateEndpoint0 = new NodeState(NodeLocator.parseString(SERVERS.ENDPOINT_0),
                layout.getEpoch(),
                1,
                SequencerMetrics.UNKNOWN,
                Collections.singletonMap(SERVERS.ENDPOINT_1, false));
        NodeState nodeStateEndpoint1 = new NodeState(NodeLocator.parseString(SERVERS.ENDPOINT_1),
                layout.getEpoch(),
                1,
                SequencerMetrics.UNKNOWN,
                Collections.singletonMap(SERVERS.ENDPOINT_0, false));
        NodeState nodeStateEndpoint2 = new NodeState(NodeLocator.parseString(SERVERS.ENDPOINT_2),
                layout.getEpoch(),
                1,
                SequencerMetrics.UNKNOWN,
                Collections.emptyMap());

        Map<String, NodeState> nodeToNodeStatesMap = new HashMap<>();
        nodeToNodeStatesMap.put(SERVERS.ENDPOINT_0, nodeStateEndpoint0);
        nodeToNodeStatesMap.put(SERVERS.ENDPOINT_1, nodeStateEndpoint1);
        nodeToNodeStatesMap.put(SERVERS.ENDPOINT_2, nodeStateEndpoint2);

        ClusterState clusterState = new ClusterState(nodeToNodeStatesMap);

        // Assert that recommended failed nodes are the same as expected
        final List<String> failedSuspectsActual = fullyConnectedClusterRecommendationEngine
                .failedServers(clusterState, layout);
        final List<String> failedSuspectsExpected = Collections.singletonList(SERVERS.ENDPOINT_1);
        assertThat(failedSuspectsActual).isEqualTo(failedSuspectsExpected);

        // Assert that recommended healed nodes are the same as expected
        final List<String> healedSuspectActual = fullyConnectedClusterRecommendationEngine
                .healedServers(clusterState, layout);
        assertThat(healedSuspectActual).isEmpty();
    }

    /**
     * In a three node cluster with all responsive nodes, when the latest cluster state
     * includes one asymmetric links failure between two responsive nodes in the the cluster, it
     * is expected that:
     * Recommended Failed Node: expected to include the node with smaller name (String comparison
     * on the name).
     * Recommended Healed Node: expected to be empty.
     */
    @Test
    public void findFailedHealed_3Servers_SingleAsymmetricLinkFailure() {

        // Build a layout that includes all responsive servers
        Layout layout = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(SERVERS.PORT_0)
                .addLayoutServer(SERVERS.PORT_1)
                .addLayoutServer(SERVERS.PORT_2)
                .addSequencer(SERVERS.PORT_0)
                .buildSegment()
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addToSegment()
                .addToLayout()
                .build();

        // Build a cluster state representing existence of an asymmetric link failure between
        // ENDPOINT0 and ENDPOINT1 nodes.
        NodeState nodeStateEndpoint0 = new NodeState(NodeLocator.parseString(SERVERS.ENDPOINT_0),
                layout.getEpoch(),
                1,
                SequencerMetrics.UNKNOWN,
                Collections.singletonMap(SERVERS.ENDPOINT_1, false));

        Map<String, NodeState> nodeToNodeStatesMap = new HashMap<>();

        // Entry for ENDPOINT1 doesn't exist representing that ENDPOINT1 doesn't observe a link
        // failure
        nodeToNodeStatesMap.put(SERVERS.ENDPOINT_0, nodeStateEndpoint0);

        ClusterState clusterState = new ClusterState(nodeToNodeStatesMap);

        // Assert that recommended failed nodes are the same as expected
        final List<String> failedSuspectsActual =
                fullyConnectedClusterRecommendationEngine.failedServers(clusterState, layout);
        final List<String> failedSuspectsExpected = Arrays.asList(SERVERS.ENDPOINT_1);
        assertThat(failedSuspectsActual).isEqualTo(failedSuspectsExpected);

        // Assert that recommended healed nodes are the same as expected
        final List<String> healedSuspectActual = fullyConnectedClusterRecommendationEngine
                .healedServers(clusterState, layout);
        assertThat(healedSuspectActual).isEmpty();
    }


    /**
     * In a five node cluster with all responsive nodes, when the latest cluster state
     * includes three asymmetric link failure for two of the responsive nodes in the the cluster, it
     * is expected that:
     * Recommended Failed Node: expected to include the two nodes which with their removal a
     * fully connected cluster can be formed with larger names first (String comparison on the
     * name).
     * Recommended Healed Node: expected to be empty.
     */
    @Test
    public void findFailedHealed_5Servers_AsymmetricLinkFailuresFor2OutOf5Nodes() {

        // Build a layout that includes five responsive servers
        Layout layout = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(SERVERS.PORT_0)
                .addLayoutServer(SERVERS.PORT_1)
                .addLayoutServer(SERVERS.PORT_2)
                .addLayoutServer(SERVERS.PORT_3)
                .addLayoutServer(SERVERS.PORT_4)
                .addSequencer(SERVERS.PORT_0)
                .buildSegment()
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addToSegment()
                .addToLayout()
                .build();

        // Build a cluster state representing existence of one symmetric link failure between
        // ENDPOINT0 and ENDPOINT4. As well as two asymmetric link failures between
        // ENDPOINT0 and ENDPOINT2, ENDPOINT3. Similarly, two asymmetric link failures between
        // ENDPOINT4 and ENDPOINT1, ENDPOINT2.
        Map<String, Boolean> endpoint0Map = new HashMap<>();
        endpoint0Map.put(SERVERS.ENDPOINT_2, false);
        endpoint0Map.put(SERVERS.ENDPOINT_3, false);
        endpoint0Map.put(SERVERS.ENDPOINT_4, false);
        Map<String, Boolean> endpoint4Map = new HashMap<>();
        endpoint4Map.put(SERVERS.ENDPOINT_0, false);
        endpoint4Map.put(SERVERS.ENDPOINT_1, false);
        endpoint4Map.put(SERVERS.ENDPOINT_2, false);

        NodeState nodeStateEndpoint0 = new NodeState(NodeLocator.parseString(SERVERS.ENDPOINT_0),
                layout.getEpoch(),
                1,
                SequencerMetrics.UNKNOWN,
                endpoint0Map);
        NodeState nodeStateEndPoint4 = new NodeState(NodeLocator.parseString(SERVERS.ENDPOINT_4),
                layout.getEpoch(),
                1,
                SequencerMetrics.UNKNOWN,
                endpoint4Map);

        Map<String, NodeState> nodeToNodeStatesMap = new HashMap<>();

        // No entries for ENDPOINT1, ENDPOINT2, ENDPOINT3 exist which highlights the asymmetric
        // nature of link failures
        nodeToNodeStatesMap.put(SERVERS.ENDPOINT_0, nodeStateEndpoint0);
        nodeToNodeStatesMap.put(SERVERS.ENDPOINT_4, nodeStateEndPoint4);

        ClusterState clusterState = new ClusterState(nodeToNodeStatesMap);

        // Assert that recommended failed nodes are the same as expected
        final List<String> failedSuspectsActual = fullyConnectedClusterRecommendationEngine
                .failedServers(clusterState, layout);
        final List<String> failedSuspectsExpected = Arrays.asList(SERVERS.ENDPOINT_4,
                                                                  SERVERS.ENDPOINT_0);
        assertThat(failedSuspectsActual).isEqualTo(failedSuspectsExpected);

        // Assert that recommended healed nodes are the same as expected
        final List<String> healedSuspectActual = fullyConnectedClusterRecommendationEngine
                .healedServers(clusterState, layout);
        assertThat(healedSuspectActual).isEmpty();
    }

    /**
     * In a three node cluster with one unresponsive node, the latest cluster state
     * includes a crashed responsive node represented with a link failure between the
     * node and the other node in the responsive set AND a link failures between the node and the
     * node in the unresponsive set. It is expected that:
     * Recommended Failed Node: expected to be the crashed node even if it has a greater name
     * (String comparison of names).
     * Recommended Healed Node: expected to be empty.
     */
    @Test
    public void findFailedHealed_3Servers_RemoveCrashedResponsive() {

        // Build a layout that includes one unresponsive servers
        Layout layout = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(SERVERS.PORT_0)
                .addLayoutServer(SERVERS.PORT_1)
                .addLayoutServer(SERVERS.PORT_2)
                .addSequencer(SERVERS.PORT_0)
                .buildSegment()
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addToSegment()
                .addToLayout()
                .addUnresponsiveServer(SERVERS.PORT_2)
                .build();

        // Build a cluster state representing a crashed responsive node (ENDPOINT0). In such
        // cluster state there are link failures observed by both responsive ENDPOINT1 and
        // unresponsive ENDPOINT2.
        NodeState nodeStateEndpoint1 = new NodeState(NodeLocator.parseString(SERVERS.ENDPOINT_1),
                layout.getEpoch(),
                1,
                SequencerMetrics.UNKNOWN,
                Collections.singletonMap(SERVERS.ENDPOINT_0, false));
        NodeState nodeStateEndpoint2 = new NodeState(NodeLocator.parseString(SERVERS.ENDPOINT_2),
                layout.getEpoch(),
                1,
                SequencerMetrics.UNKNOWN,
                Collections.singletonMap(SERVERS.ENDPOINT_0, false));

        Map<String, NodeState> nodeToNodeStatesMap = new HashMap<>();
        nodeToNodeStatesMap.put(SERVERS.ENDPOINT_1, nodeStateEndpoint1);
        nodeToNodeStatesMap.put(SERVERS.ENDPOINT_2, nodeStateEndpoint2);

        ClusterState clusterState = new ClusterState(nodeToNodeStatesMap);

        // Assert that recommended failed nodes are the same as expected
        final List<String> failedSuspectsActual =
                fullyConnectedClusterRecommendationEngine.failedServers(clusterState, layout);
        final List<String> failedSuspectsExpected = Arrays.asList(SERVERS.ENDPOINT_0);
        assertThat(failedSuspectsActual).isEqualTo(failedSuspectsExpected);

        // Assert that recommended healed nodes are the same as expected
        final List<String> healedSuspectActual =
                fullyConnectedClusterRecommendationEngine.healedServers(clusterState, layout);
        assertThat(healedSuspectActual).isEmpty();
    }

    /**
     * In a three node cluster with one unresponsive node, when the latest cluster state
     * includes no link failure between any of the nodes in the cluster, it is expected
     * that:
     * Recommended Failed Node: expected to be empty.
     * Recommended Healed Node: expected to include the unresponsive node with active links to the
     * entire cluster.
     */
    @Test
    public void findFailedHealed_3Servers_LinkFailuresHealed() {

        // Build a layout that includes one unresponsive servers
        Layout layout = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(SERVERS.PORT_0)
                .addLayoutServer(SERVERS.PORT_1)
                .addLayoutServer(SERVERS.PORT_2)
                .addSequencer(SERVERS.PORT_0)
                .buildSegment()
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addToSegment()
                .addToLayout()
                .addUnresponsiveServer(SERVERS.PORT_0)
                .build();

        // Build a cluster state representing no link failure between an unresponsive
        // ENDPOINT0 and responsive ENDPOINT1, and ENDPOINT2 nodes.
        NodeState nodeStateEndpoint0 = new NodeState(NodeLocator.parseString(SERVERS.ENDPOINT_0),
                layout.getEpoch(),
                1,
                SequencerMetrics.UNKNOWN,
                Collections.emptyMap());
        NodeState nodeStateEndpoint1 = new NodeState(NodeLocator.parseString(SERVERS.ENDPOINT_1),
                layout.getEpoch(),
                1,
                SequencerMetrics.UNKNOWN,
                Collections.emptyMap());
        NodeState nodeStateEndpoint2 = new NodeState(NodeLocator.parseString(SERVERS.ENDPOINT_2),
                layout.getEpoch(),
                1,
                SequencerMetrics.UNKNOWN,
                Collections.emptyMap());

        Map<String, NodeState> nodeToNodeStatesMap = new HashMap<>();
        nodeToNodeStatesMap.put(SERVERS.ENDPOINT_0, nodeStateEndpoint0);
        nodeToNodeStatesMap.put(SERVERS.ENDPOINT_1, nodeStateEndpoint1);
        nodeToNodeStatesMap.put(SERVERS.ENDPOINT_2, nodeStateEndpoint2);

        ClusterState clusterState = new ClusterState(nodeToNodeStatesMap);

        // Assert that recommended failed nodes are the same as expected
        final List<String> failedSuspectsActual =
                fullyConnectedClusterRecommendationEngine.failedServers(clusterState, layout);
        assertThat(failedSuspectsActual).isEmpty();

        // Assert that recommended healed nodes are the same as expected
        final List<String> healedSuspectActual =
                fullyConnectedClusterRecommendationEngine.healedServers(clusterState, layout);
        final List<String> healedSuspectsExpected = Arrays.asList(SERVERS.ENDPOINT_0);
        assertThat(healedSuspectActual).isEqualTo(healedSuspectsExpected);
    }

    /**
     * In a three node cluster with one unresponsive node, when there is a link failure between
     * the unresponsive node and a responsive node it is expected that:
     * Recommended Failed Node: expected to be empty.
     * Recommended Healed Node: expected to be empty.
     */
    @Test
    public void findFailedHealed_3Servers_UnresponsiveNodeLinkToResponsiveFailed() {

        // Build a layout that includes one unresponsive server
        Layout layout = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(SERVERS.PORT_0)
                .addLayoutServer(SERVERS.PORT_1)
                .addLayoutServer(SERVERS.PORT_2)
                .addSequencer(SERVERS.PORT_0)
                .buildSegment()
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addToSegment()
                .addToLayout()
                .addUnresponsiveServer(SERVERS.PORT_0)
                .build();
        
        // Build a cluster state representing a symmetric failure of links between ENDPOINT0 
        // and ENDPOINT1 nodes.
        NodeState nodeStateEndpoint0 = new NodeState(NodeLocator.parseString(SERVERS.ENDPOINT_0),
                layout.getEpoch(),
                1,
                SequencerMetrics.UNKNOWN,
                Collections.singletonMap(SERVERS.ENDPOINT_1, false));
        NodeState nodeStateEndpoint1 = new NodeState(NodeLocator.parseString(SERVERS.ENDPOINT_1),
                layout.getEpoch(),
                1,
                SequencerMetrics.UNKNOWN,
                Collections.singletonMap(SERVERS.ENDPOINT_0, false));
        NodeState nodeStateEndpoint2 = new NodeState(NodeLocator.parseString(SERVERS.ENDPOINT_2),
                layout.getEpoch(),
                1,
                SequencerMetrics.UNKNOWN,
                Collections.emptyMap());

        Map<String, NodeState> nodeToNodeStatesMap = new HashMap<>();
        nodeToNodeStatesMap.put(SERVERS.ENDPOINT_0, nodeStateEndpoint0);
        nodeToNodeStatesMap.put(SERVERS.ENDPOINT_1, nodeStateEndpoint1);
        nodeToNodeStatesMap.put(SERVERS.ENDPOINT_2, nodeStateEndpoint2);

        ClusterState clusterState = new ClusterState(nodeToNodeStatesMap);

        // Assert that recommended failed nodes are the same as expected
        final List<String> failedSuspectsActual = fullyConnectedClusterRecommendationEngine
                .failedServers(clusterState, layout);
        assertThat(failedSuspectsActual).isEmpty();

        // Assert that recommended healed nodes are the same as expected
        final List<String> healedSuspectActual = fullyConnectedClusterRecommendationEngine
                .healedServers(clusterState, layout);
        assertThat(healedSuspectActual).isEmpty();
    }

    /**
     * In a three node cluster with one unresponsive node, when a responsive node is fully
     * partitioned. it is
     * expected that:
     * Recommended Failed Node: expected to include one node with greater number of failures.
     * Recommended Healed Node: expected to be empty.
     */
    @Test
    public void findFailedHealed_3Servers_AsymmetricPartitionOfResponsiveNode() {

        // Build a layout that includes one unresponsive server
        Layout layout = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(SERVERS.PORT_0)
                .addLayoutServer(SERVERS.PORT_1)
                .addLayoutServer(SERVERS.PORT_2)
                .addSequencer(SERVERS.PORT_0)
                .buildSegment()
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addToSegment()
                .addToLayout()
                .addUnresponsiveServer(SERVERS.PORT_0)
                .build();

        // Build a cluster state representing the asymmetric capture of failures in the cluster
        // where both ENDPOINT0 and ENDPOINT1 see ENDPOINT2 as failures
        NodeState nodeStateEndpoint0 = new NodeState(NodeLocator.parseString(SERVERS.ENDPOINT_0),
                layout.getEpoch(),
                1,
                SequencerMetrics.UNKNOWN,
                Collections.singletonMap(SERVERS.ENDPOINT_2, false));
        NodeState nodeStateEndpoint1 = new NodeState(NodeLocator.parseString(SERVERS.ENDPOINT_1),
                layout.getEpoch(),
                1,
                SequencerMetrics.UNKNOWN,
                Collections.singletonMap(SERVERS.ENDPOINT_2, false));

        Map<String, NodeState> nodeToNodeStatesMap = new HashMap<>();
        nodeToNodeStatesMap.put(SERVERS.ENDPOINT_0, nodeStateEndpoint0);
        nodeToNodeStatesMap.put(SERVERS.ENDPOINT_1, nodeStateEndpoint1);

        ClusterState clusterState = new ClusterState(nodeToNodeStatesMap);

        // Assert that recommended failed nodes are the same as expected
        final List<String> failedSuspectsActual = fullyConnectedClusterRecommendationEngine
                .failedServers(clusterState, layout);
        final List<String> failedSuspectExpected =
                Collections.singletonList(SERVERS.ENDPOINT_2);
        assertThat(failedSuspectsActual).isEqualTo(failedSuspectExpected);

        // Assert that recommended healed nodes are the same as expected
        final List<String> healedSuspectActual = fullyConnectedClusterRecommendationEngine
                .healedServers(clusterState, layout);
        assertThat(healedSuspectActual).isEmpty();
    }

    /**
     * In a three node cluster with two unresponsive nodes, when the latest cluster state only
     * includes a single symmetric link failure between the two unresponsive nodes it is expected
     * that:
     * Recommended Failed Node: expected to be empty.
     * Recommended Healed Node: expected to include the unresponsive node with smaller name
     * (String comparison on the name).
     */
    @Test
    public void findFailedHealed_3Servers_LinkFailureBetween2UnresponsiveNodes() {

        // Build a layout that includes one unresponsive server
        Layout layout = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(SERVERS.PORT_0)
                .addLayoutServer(SERVERS.PORT_1)
                .addLayoutServer(SERVERS.PORT_2)
                .addSequencer(SERVERS.PORT_0)
                .buildSegment()
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addToSegment()
                .addToLayout()
                .addUnresponsiveServer(SERVERS.PORT_0)
                .addUnresponsiveServer(SERVERS.PORT_1)
                .build();

        // Build a cluster state representing a symmetric failure of links between unresponsive
        // ENDPOINT0 and ENDPOINT1 nodes.
        NodeState nodeStateEndpoint0 = new NodeState(NodeLocator.parseString(SERVERS.ENDPOINT_0),
                layout.getEpoch(),
                1,
                SequencerMetrics.UNKNOWN,
                Collections.singletonMap(SERVERS.ENDPOINT_1, false));
        NodeState nodeStateEndpoint1 = new NodeState(NodeLocator.parseString(SERVERS.ENDPOINT_1),
                layout.getEpoch(),
                1,
                SequencerMetrics.UNKNOWN,
                Collections.singletonMap(SERVERS.ENDPOINT_0, false));
        NodeState nodeStateEndpoint2 = new NodeState(NodeLocator.parseString(SERVERS.ENDPOINT_2),
                layout.getEpoch(),
                1,
                SequencerMetrics.UNKNOWN,
                Collections.emptyMap());

        Map<String, NodeState> nodeToNodeStatesMap = new HashMap<>();
        nodeToNodeStatesMap.put(SERVERS.ENDPOINT_0, nodeStateEndpoint0);
        nodeToNodeStatesMap.put(SERVERS.ENDPOINT_1, nodeStateEndpoint1);
        nodeToNodeStatesMap.put(SERVERS.ENDPOINT_2, nodeStateEndpoint2);

        ClusterState clusterState = new ClusterState(nodeToNodeStatesMap);

        // Assert that recommended failed nodes are the same as expected
        final List<String> failedSuspectsActual = fullyConnectedClusterRecommendationEngine
                .failedServers(clusterState, layout);
        assertThat(failedSuspectsActual).isEmpty();

        // Assert that recommended healed nodes are the same as expected
        final List<String> healedSuspectExpected = Collections.singletonList(SERVERS.ENDPOINT_0);
        final List<String> healedSuspectActual = fullyConnectedClusterRecommendationEngine
                .healedServers(clusterState, layout);
        assertThat(healedSuspectActual).isEqualTo(healedSuspectExpected);
    }

    /**
     * In a three node cluster without unresponsive nodes, when all links are failed it is
     * expected that:
     * Recommended Failed Node: expected to include two nodes with greater node names.
     * Recommended Healed Node: expected to be empty.
     */
    @Test
    public void findFailedHealed_3Servers_AllLinksHaveFailed() {

        // Build a layout with three responsive servers
        Layout layout = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(SERVERS.PORT_0)
                .addLayoutServer(SERVERS.PORT_1)
                .addLayoutServer(SERVERS.PORT_2)
                .addSequencer(SERVERS.PORT_0)
                .buildSegment()
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addToSegment()
                .addToLayout()
                .build();

        // Build a cluster state representing that all the links have failed
        Map<String, Boolean> connectivityMapEndpoint0 = new HashMap<>();
        connectivityMapEndpoint0.put(SERVERS.ENDPOINT_1, false);
        connectivityMapEndpoint0.put(SERVERS.ENDPOINT_2, false);
        Map<String, Boolean> connectivityMapEndpoint1 = new HashMap<>();
        connectivityMapEndpoint1.put(SERVERS.ENDPOINT_0, false);
        connectivityMapEndpoint1.put(SERVERS.ENDPOINT_2, false);
        Map<String, Boolean> connectivityMapEndpoint2 = new HashMap<>();
        connectivityMapEndpoint2.put(SERVERS.ENDPOINT_0, false);
        connectivityMapEndpoint2.put(SERVERS.ENDPOINT_1, false);

        NodeState nodeStateEndpoint0 = new NodeState(NodeLocator.parseString(SERVERS.ENDPOINT_0),
                layout.getEpoch(),
                1,
                SequencerMetrics.UNKNOWN,
                connectivityMapEndpoint0);
        NodeState nodeStateEndpoint1 = new NodeState(NodeLocator.parseString(SERVERS.ENDPOINT_1),
                layout.getEpoch(),
                1,
                SequencerMetrics.UNKNOWN,
                connectivityMapEndpoint1);
        NodeState nodeStateEndPoint2 = new NodeState(NodeLocator.parseString(SERVERS.ENDPOINT_2),
                layout.getEpoch(),
                1,
                SequencerMetrics.UNKNOWN,
                connectivityMapEndpoint2);

        Map<String, NodeState> nodeToNodeStatesMap = new HashMap<>();
        nodeToNodeStatesMap.put(SERVERS.ENDPOINT_0, nodeStateEndpoint0);
        nodeToNodeStatesMap.put(SERVERS.ENDPOINT_1, nodeStateEndpoint1);
        nodeToNodeStatesMap.put(SERVERS.ENDPOINT_2, nodeStateEndPoint2);
        ClusterState clusterState = new ClusterState(nodeToNodeStatesMap);

        // Assert that recommended failed nodes are the same as expected
        final List<String> failedSuspectsActual = fullyConnectedClusterRecommendationEngine
                .failedServers(clusterState, layout);
        final List<String> failedSuspectExpected =
                Arrays.asList(SERVERS.ENDPOINT_2,
                              SERVERS.ENDPOINT_1);
        assertThat(failedSuspectsActual).isEqualTo(failedSuspectExpected);

        // Assert that recommended healed nodes are the same as expected
        final List<String> healedSuspectActual = fullyConnectedClusterRecommendationEngine
                .healedServers(clusterState, layout);
        assertThat(healedSuspectActual).isEmpty();
    }

    /**
     * In a three node cluster with two unresponsive nodes, when there is a link failure between
     * the unresponsive nodes and a responsive node it is expected that:
     * Recommended Failed Node: expected to be empty.
     * Recommended Healed Node: expected to be empty.
     */
    @Test
    public void findFailedHealed_3Servers_AsymmetricPartitionOfUnresponsiveNodes() {

        // Build a layout that includes two unresponsive servers
        Layout layout = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(SERVERS.PORT_0)
                .addLayoutServer(SERVERS.PORT_1)
                .addLayoutServer(SERVERS.PORT_2)
                .addSequencer(SERVERS.PORT_0)
                .buildSegment()
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addToSegment()
                .addToLayout()
                .addUnresponsiveServer(SERVERS.PORT_0)
                .addUnresponsiveServer(SERVERS.PORT_1)
                .build();

        // Build a cluster state representing a symmetric failure of links between both ENDPOINT0
        // and ENDPOINT1 nodes with ENDPOINT2.
        NodeState nodeStateEndpoint0 = new NodeState(NodeLocator.parseString(SERVERS.ENDPOINT_0),
                layout.getEpoch(),
                1,
                SequencerMetrics.UNKNOWN,
                Collections.singletonMap(SERVERS.ENDPOINT_2, false));
        NodeState nodeStateEndpoint1 = new NodeState(NodeLocator.parseString(SERVERS.ENDPOINT_1),
                layout.getEpoch(),
                1,
                SequencerMetrics.UNKNOWN,
                Collections.singletonMap(SERVERS.ENDPOINT_2, false));


        Map<String, NodeState> nodeToNodeStatesMap = new HashMap<>();
        nodeToNodeStatesMap.put(SERVERS.ENDPOINT_0, nodeStateEndpoint0);
        nodeToNodeStatesMap.put(SERVERS.ENDPOINT_1, nodeStateEndpoint1);

        ClusterState clusterState = new ClusterState(nodeToNodeStatesMap);

        // Assert that recommended failed nodes are the same as expected
        final List<String> failedSuspectsActual = fullyConnectedClusterRecommendationEngine
                .failedServers(clusterState, layout);
        assertThat(failedSuspectsActual).isEmpty();

        // Assert that recommended healed nodes are the same as expected
        final List<String> healedSuspectActual = fullyConnectedClusterRecommendationEngine
                .healedServers(clusterState, layout);
        assertThat(healedSuspectActual).isEmpty();
    }

    /**
     * In a three node cluster with two unresponsive nodes, when the latest cluster state only
     * includes a single symmetric link failure between the one of the unresponsive nodes it is
     * expected that:
     * Recommended Failed Node: expected to be empty.
     * Recommended Healed Node: expected to include the unresponsive node with active link to the
     * responsive server in the layout.
     */
    @Test
    public void findFailedHealed_3Servers_LinkFailureBetweenOneUnresponsiveAndActive() {

        // Build a layout that includes two unresponsive servers
        Layout layout = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(SERVERS.PORT_0)
                .addLayoutServer(SERVERS.PORT_1)
                .addLayoutServer(SERVERS.PORT_2)
                .addSequencer(SERVERS.PORT_0)
                .buildSegment()
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addToSegment()
                .addToLayout()
                .addUnresponsiveServer(SERVERS.PORT_0)
                .addUnresponsiveServer(SERVERS.PORT_1)
                .build();

        // Build a cluster state representing a symmetric failure of links between an unresponsive
        // ENDPOINT0 and responsive ENDPOINT2 nodes.
        Map<String, NodeState> nodeToNodeStatesMap = new HashMap<>();

        NodeState nodeStateEndpoint0 = new NodeState(NodeLocator.parseString(SERVERS.ENDPOINT_0),
                layout.getEpoch(),
                1,
                SequencerMetrics.UNKNOWN,
                Collections.singletonMap(SERVERS.ENDPOINT_2, false));
        NodeState nodeStateEndpoint1 = new NodeState(NodeLocator.parseString(SERVERS.ENDPOINT_1),
                layout.getEpoch(),
                1,
                SequencerMetrics.UNKNOWN,
                Collections.emptyMap());
        NodeState nodeStateEndpoint2 = new NodeState(NodeLocator.parseString(SERVERS.ENDPOINT_2),
                layout.getEpoch(),
                1,
                SequencerMetrics.UNKNOWN,
                Collections.singletonMap(SERVERS.ENDPOINT_0, false));


        nodeToNodeStatesMap.put(SERVERS.ENDPOINT_0, nodeStateEndpoint0);
        nodeToNodeStatesMap.put(SERVERS.ENDPOINT_1, nodeStateEndpoint1);
        nodeToNodeStatesMap.put(SERVERS.ENDPOINT_2, nodeStateEndpoint2);

        ClusterState clusterState = new ClusterState(nodeToNodeStatesMap);

        // Assert that recommended failed nodes are the same as expected
        final List<String> failedSuspectsActual = fullyConnectedClusterRecommendationEngine
                .failedServers(clusterState, layout);
        assertThat(failedSuspectsActual).isEmpty();

        // Assert that recommended healed nodes are the same as expected
        final List<String> healedSuspectActual = fullyConnectedClusterRecommendationEngine
                .healedServers(clusterState, layout);
        final List<String> healedSuspectExpected =
                Collections.singletonList(SERVERS.ENDPOINT_1);
        assertThat(healedSuspectActual).isEqualTo(healedSuspectExpected);
    }


    /**
     * In a three node cluster with two unresponsive nodes, when the latest cluster state
     * includes symmetric links failure between the one of the unresponsive nodes and the rest of
     * the cluster, it is expected that:
     * Recommended Failed Node: expected to be empty.
     * Recommended Healed Node: expected to include the unresponsive node with active link to the
     * responsive server in the layout.
     */
    @Test
    public void findFailedHealed_3Servers_LinkFailureBetweenUnresponsiveAndRestOfCluster() {

        // Build a layout that includes two unresponsive servers
        Layout layout = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(SERVERS.PORT_0)
                .addLayoutServer(SERVERS.PORT_1)
                .addLayoutServer(SERVERS.PORT_2)
                .addSequencer(SERVERS.PORT_0)
                .buildSegment()
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addToSegment()
                .addToLayout()
                .addUnresponsiveServer(SERVERS.PORT_0)
                .addUnresponsiveServer(SERVERS.PORT_1)
                .build();

        // Build a cluster state representing two symmetric link failures between an unresponsive
        // ENDPOINT0 and both ENDPOINT1 and ENDPOINT2 nodes.
        Map<String, Boolean> endpoint0Map = new HashMap<>();
        endpoint0Map.put(SERVERS.ENDPOINT_1, false);
        endpoint0Map.put(SERVERS.ENDPOINT_2, false);
        NodeState nodeStateEndpoint0 = new NodeState(NodeLocator.parseString(SERVERS.ENDPOINT_0),
                layout.getEpoch(),
                1,
                SequencerMetrics.UNKNOWN,
                endpoint0Map);
        NodeState nodeStateEndpoint1 = new NodeState(NodeLocator.parseString(SERVERS.ENDPOINT_1),
                layout.getEpoch(),
                1,
                SequencerMetrics.UNKNOWN,
                Collections.singletonMap(SERVERS.ENDPOINT_0, false));
        NodeState nodeStateEndpoint2 = new NodeState(NodeLocator.parseString(SERVERS.ENDPOINT_2),
                layout.getEpoch(),
                1,
                SequencerMetrics.UNKNOWN,
                Collections.singletonMap(SERVERS.ENDPOINT_0, false));


        Map<String, NodeState> nodeToNodeStatesMap = new HashMap<>();
        nodeToNodeStatesMap.put(SERVERS.ENDPOINT_0, nodeStateEndpoint0);
        nodeToNodeStatesMap.put(SERVERS.ENDPOINT_1, nodeStateEndpoint1);
        nodeToNodeStatesMap.put(SERVERS.ENDPOINT_2, nodeStateEndpoint2);

        ClusterState clusterState = new ClusterState(nodeToNodeStatesMap);

        // Assert that recommended failed nodes are the same as expected
        final List<String> failedSuspectsActual =
                fullyConnectedClusterRecommendationEngine.failedServers(clusterState, layout);
        assertThat(failedSuspectsActual).isEmpty();

        // Assert that recommended healed nodes are the same as expected
        final List<String> healedSuspectActual =
                fullyConnectedClusterRecommendationEngine.healedServers(clusterState, layout);
        final List<String> healedSuspectExpected =
                Collections.singletonList(SERVERS.ENDPOINT_1);
        assertThat(healedSuspectActual).isEqualTo(healedSuspectExpected);
    }

    /**
     * In a three node cluster with one unresponsive nodes, when the latest cluster state only
     * includes asymmetric link failures to the unresponsive node including that of the
     * unresponsive node, it is expected that:
     * Recommended Failed Node: expected to be empty.
     * Recommended Healed Node: expected to be empty.
     */
    @Test
    public void findFailedHealed_3Servers_AsymmetricFailureOfOneNodeToRespond() {

        // Build a layout that includes one unresponsive server
        Layout layout = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(SERVERS.PORT_0)
                .addLayoutServer(SERVERS.PORT_1)
                .addLayoutServer(SERVERS.PORT_2)
                .addSequencer(SERVERS.PORT_0)
                .buildSegment()
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addToSegment()
                .addToLayout()
                .addUnresponsiveServer(SERVERS.PORT_0)
                .build();

        // Build a cluster state representing asymmetric failures of links between an unresponsive
        // ENDPOINT0 and the entire cluster including ENDPOINT0.
        NodeState nodeStateEndpoint0 = new NodeState(NodeLocator.parseString(SERVERS.ENDPOINT_0),
                layout.getEpoch(),
                1,
                SequencerMetrics.UNKNOWN,
                Collections.singletonMap(SERVERS.ENDPOINT_0, false));
        NodeState nodeStateEndpoint1 = new NodeState(NodeLocator.parseString(SERVERS.ENDPOINT_1),
                layout.getEpoch(),
                1,
                SequencerMetrics.UNKNOWN,
                Collections.singletonMap(SERVERS.ENDPOINT_0, false));
        NodeState nodeStateEndpoint2 = new NodeState(NodeLocator.parseString(SERVERS.ENDPOINT_2),
                layout.getEpoch(),
                1,
                SequencerMetrics.UNKNOWN,
                Collections.singletonMap(SERVERS.ENDPOINT_0, false));


        Map<String, NodeState> nodeToNodeStatesMap = new HashMap<>();
        nodeToNodeStatesMap.put(SERVERS.ENDPOINT_0, nodeStateEndpoint0);
        nodeToNodeStatesMap.put(SERVERS.ENDPOINT_1, nodeStateEndpoint1);
        nodeToNodeStatesMap.put(SERVERS.ENDPOINT_2, nodeStateEndpoint2);

        ClusterState clusterState = new ClusterState(nodeToNodeStatesMap);

        // Assert that recommended failed nodes are the same as expected
        final List<String> failedSuspectsActual =
                fullyConnectedClusterRecommendationEngine.failedServers(clusterState, layout);
        assertThat(failedSuspectsActual).isEmpty();

        // Assert that recommended healed nodes are the same as expected
        final List<String> healedSuspectActual =
                fullyConnectedClusterRecommendationEngine.healedServers(clusterState, layout);
        assertThat(healedSuspectActual).isEmpty();
    }

    /**
     * In a five node cluster with three responsive nodes and two unresponsive ones, when the
     * latest cluster state includes three link failures for one of the unresponsive nodes while
     * the other has active connection the responsive nodes in the cluster, it is expected that:
     * Recommended Failed Node: expected to be empty.
     * Recommended Healed Node: expected to include the unresponsive node with active links to
     * the responsive nodes in the cluster.
     */
    @Test
    public void findFailedHealed_5Servers_AsymmetricLinkFailuresFor1Healed1Of5Nodes() {

        // Build a layout that includes five nodes from which three are responsive servers and
        // two are unresponsive ones.
        Layout layout = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(SERVERS.PORT_0)
                .addLayoutServer(SERVERS.PORT_1)
                .addLayoutServer(SERVERS.PORT_2)
                .addLayoutServer(SERVERS.PORT_3)
                .addLayoutServer(SERVERS.PORT_4)
                .addSequencer(SERVERS.PORT_0)
                .buildSegment()
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addToSegment()
                .addToLayout()
                .addUnresponsiveServer(SERVERS.PORT_0)
                .addUnresponsiveServer(SERVERS.PORT_4)
                .build();

        // Build a cluster state representing existence of one symmetric link failure between
        // ENDPOINT0 and ENDPOINT4. As well as two asymmetric link failures between ENDPOINT4
        // and ENDPOINT1, ENDPOINT2.
        Map<String, Boolean> endpoint0Map = new HashMap<>();
        endpoint0Map.put(SERVERS.ENDPOINT_4, false);
        Map<String, Boolean> endpoint4Map = new HashMap<>();
        endpoint4Map.put(SERVERS.ENDPOINT_0, false);
        endpoint4Map.put(SERVERS.ENDPOINT_1, false);
        endpoint4Map.put(SERVERS.ENDPOINT_2, false);

        NodeState nodeStateEndpoint0 = new NodeState(NodeLocator.parseString(SERVERS.ENDPOINT_0),
                layout.getEpoch(),
                1,
                SequencerMetrics.UNKNOWN,
                endpoint0Map);
        NodeState nodeStateEndpoint4 = new NodeState(NodeLocator.parseString(SERVERS.ENDPOINT_4),
                layout.getEpoch(),
                1,
                SequencerMetrics.UNKNOWN,
                endpoint4Map);

        Map<String, NodeState> nodeToNodeStatesMap = new HashMap<>();

        // No entries for ENDPOINT1, ENDPOINT2, ENDPOINT3 exists which highlights the asymmetric
        // nature of the link failures
        nodeToNodeStatesMap.put(SERVERS.ENDPOINT_0, nodeStateEndpoint0);
        nodeToNodeStatesMap.put(SERVERS.ENDPOINT_4, nodeStateEndpoint4);

        ClusterState clusterState = new ClusterState(nodeToNodeStatesMap);

        // Assert that recommended failed nodes are the same as expected
        final List<String> failedSuspectsActual = fullyConnectedClusterRecommendationEngine
                .failedServers(clusterState, layout);
        assertThat(failedSuspectsActual).isEmpty();

        // Assert that recommended healed nodes are the same as expected
        final List<String> healedSuspectActual = fullyConnectedClusterRecommendationEngine
                .healedServers(clusterState, layout);
        final List<String> healedSuspectsExpected = Collections.singletonList(SERVERS.ENDPOINT_0);
        assertThat(healedSuspectActual).isEqualTo(healedSuspectsExpected);
    }

    /**
     * In a five node cluster with all responsive nodes, when the latest cluster state includes
     * four symmetric link failures that can lead to multiple 3 node clusters, it is expected
     * that:
     * Recommended Failed Node: expected to include the nodes with highest number of failure
     * first and then in case of equal number of failures to responsive nodes, the one with the
     * least number link failures to unresponsive nodes
     * Recommended Healed Node: expected to be empty.
     */
    @Test
    public void findFailedHealed_5Servers_SymmetricLinkFailuresWithMultiple3NodePossibilities() {

        // Build a layout that includes five responsive servers
        Layout layout = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(SERVERS.PORT_0)
                .addLayoutServer(SERVERS.PORT_1)
                .addLayoutServer(SERVERS.PORT_2)
                .addLayoutServer(SERVERS.PORT_3)
                .addLayoutServer(SERVERS.PORT_4)
                .addSequencer(SERVERS.PORT_0)
                .buildSegment()
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addToSegment()
                .addToLayout()
                .build();

        // Build a cluster state representing existence of four symmetric link failures between
        // ENDPOINT0 - ENDPOINT1, ENDPOINT0 - ENDPOINT2, ENDPOINT2 - ENDPOINT3, ENDPOINT2 -
        // ENDPOINT4.
        Map<String, Boolean> endpoint0Map = new HashMap<>();
        endpoint0Map.put(SERVERS.ENDPOINT_1, false);
        endpoint0Map.put(SERVERS.ENDPOINT_2, false);
        Map<String, Boolean> endpoint1Map = new HashMap<>();
        endpoint1Map.put(SERVERS.ENDPOINT_0, false);
        Map<String, Boolean> endpoint2Map = new HashMap<>();
        endpoint2Map.put(SERVERS.ENDPOINT_0, false);
        endpoint2Map.put(SERVERS.ENDPOINT_3, false);
        endpoint2Map.put(SERVERS.ENDPOINT_4, false);
        Map<String, Boolean> endpoint3Map = new HashMap<>();
        endpoint3Map.put(SERVERS.ENDPOINT_2, false);
        Map<String, Boolean> endpoint4Map = new HashMap<>();
        endpoint4Map.put(SERVERS.ENDPOINT_2, false);

        NodeState nodeStateEndpoint0 = new NodeState(NodeLocator.parseString(SERVERS.ENDPOINT_0),
                layout.getEpoch(),
                1,
                SequencerMetrics.UNKNOWN,
                endpoint0Map);
        NodeState nodeStateEndpoint1 = new NodeState(NodeLocator.parseString(SERVERS.ENDPOINT_0),
                layout.getEpoch(),
                1,
                SequencerMetrics.UNKNOWN,
                endpoint1Map);
        NodeState nodeStateEndpoint2 = new NodeState(NodeLocator.parseString(SERVERS.ENDPOINT_0),
                layout.getEpoch(),
                1,
                SequencerMetrics.UNKNOWN,
                endpoint2Map);
        NodeState nodeStateEndpoint3 = new NodeState(NodeLocator.parseString(SERVERS.ENDPOINT_0),
                layout.getEpoch(),
                1,
                SequencerMetrics.UNKNOWN,
                endpoint3Map);
        NodeState nodeStateEndpoint4 = new NodeState(NodeLocator.parseString(SERVERS.ENDPOINT_4),
                layout.getEpoch(),
                1,
                SequencerMetrics.UNKNOWN,
                endpoint4Map);

        Map<String, NodeState> nodeToNodeStatesMap = new HashMap<>();

        nodeToNodeStatesMap.put(SERVERS.ENDPOINT_0, nodeStateEndpoint0);
        nodeToNodeStatesMap.put(SERVERS.ENDPOINT_1, nodeStateEndpoint1);
        nodeToNodeStatesMap.put(SERVERS.ENDPOINT_2, nodeStateEndpoint2);
        nodeToNodeStatesMap.put(SERVERS.ENDPOINT_3, nodeStateEndpoint3);
        nodeToNodeStatesMap.put(SERVERS.ENDPOINT_4, nodeStateEndpoint4);

        ClusterState clusterState = new ClusterState(nodeToNodeStatesMap);

        // Assert that recommended failed nodes are the same as expected
        final List<String> failedSuspectsActual = fullyConnectedClusterRecommendationEngine
                .failedServers(clusterState, layout);
        final List<String> failedSuspectsExpected =
                Arrays.asList(SERVERS.ENDPOINT_2,SERVERS.ENDPOINT_0);
        assertThat(failedSuspectsActual).isEqualTo(failedSuspectsExpected);

        // Assert that recommended healed nodes are the same as expected
        final List<String> healedSuspectActual = fullyConnectedClusterRecommendationEngine
                .healedServers(clusterState, layout);
        assertThat(healedSuspectActual).isEmpty();
    }
}
