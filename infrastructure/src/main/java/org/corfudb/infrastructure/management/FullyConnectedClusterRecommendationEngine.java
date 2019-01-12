package org.corfudb.infrastructure.management;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.management.ClusterGraph.NodeRank;
import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.runtime.view.Layout;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.stream.Stream;

/**
 * This class is an implementation of {@link ClusterRecommendationStrategy} where the ideal state
 * of the Corfu cluster is a fully connected network (i.e. complete graph) in which there is an
 * active link amongst all nodes in the cluster. Failed and Healed nodes are recommended so that
 * the cluster remains fully connected.
 * <p>
 * Created by Sam Behnam on 10/27/18.
 */
@Slf4j
public class FullyConnectedClusterRecommendationEngine implements ClusterRecommendationEngine {

    private static final ClusterRecommendationStrategy STRATEGY = ClusterRecommendationStrategy.FULLY_CONNECTED_CLUSTER;

    @Override
    public ClusterRecommendationStrategy getClusterRecommendationStrategy() {
        return STRATEGY;
    }

    /**
     * Provides list of servers from a given layout(epoch) that this implementation of
     * FULLY_CONNECTED_CLUSTER algorithm has determined as failed. The implementation of the
     * algorithm in this method is a greedy approach by executing the following steps:
     * <p>
     * a) Collect all nodes with link failures to the responsive nodes in the cluster
     * b) While possible, add the nodes with maximum number of failed links to set of
     * failed nodes
     * <p>
     * The result set of failed nodes is the recommendation of this strategy which their removal
     * from cluster will lead to a fully connected cluster.
     * <p>
     * The following represents the underlying implementation of one algorithm to achieve the
     * above goal however the clients of this strategy must only rely on the guarantee that removal
     * of the returned failed nodes is a recommendation for arriving at a fully connected
     * cluster and must not rely on the implementation details of the algorithm.
     * <p>
     * Find Failed Nodes algorithm:
     * <p>
     * // Create the super set of failed nodes
     * for (Node in Responsive Nodes Set):
     * disconnectedLinksMap.put(Node, set of active peers whose their heartbeat response
     * didn't get received by the Node)
     * <p>
     * Proposed Failed Set = {}
     * <p>
     * // Greedily find and remove the nodes with highest number of link failures until
     * // all the remaining nodes form a fully connected corfu cluster
     * while (disconnectedLinksMap is not empty):
     * <p>
     * // Descending sort of the nodes based on number of failed links
     * sortedDisconnectedLinksMap <- descending sort by size of Node Set of
     * disconnectedLinksMap entries
     * <p>
     * <p>
     * // Collect Failed Nodes
     * Fail Candidate entry <- remove the entry with highest number of link failure
     * add Node of Fail Candidate entry to Proposed Failed Set
     * disconnectedLinksMap.remove(Node of Fail Candidate entry)
     * for (Node in Disconnected Node set of Fail Candidate entry):
     * remove Fail Candidate Node from disconnectedLinksMap.get(Node)
     * remove the entry for the Node from disconnectedLinksMap if its Disconnected
     * Node set is empty
     * <p>
     * return Proposed Healed Set
     *
     * @param clusterState represents the state of connectivity amongst the Corfu cluster
     *                     nodes from a node's perspective.
     * @param layout       expected layout of the cluster.
     * @return a {@link List} of servers considered as failed according to the underlying
     * {@link ClusterRecommendationStrategy}.
     */
    @Override
    public List<String> failedServers(ClusterState clusterState, Layout layout, String localEndpoint) {
        log.trace("Detecting the failed nodes for: ClusterState= {} Layout= {}", clusterState, layout);

        if (clusterState.size() != layout.getAllServers().size()) {
            log.error("Cluster representation is different than layout. Cluster: {}, layout: {}", clusterState, layout);
            return Collections.emptyList();
        }

        ClusterGraph symmetric = ClusterGraph.transform(clusterState).toSymmetric();
        SortedSet<NodeRank> quorumNodes = symmetric.getQuorumNodes();

        if (quorumNodes.isEmpty()) {
            log.error("No quorum to detect failed servers. Graph: {}", symmetric);
            return Collections.emptyList();
        }

        if (!quorumNodes.first().is(localEndpoint)) {
           log.debug("The node is not a decision maker, skip operation. Decision maker is: {}", quorumNodes.first());
           return Collections.emptyList();
        }

        List<String> proposedFailedNodes = new ArrayList<>();

        log.debug("Proposed failed nodes: {}", proposedFailedNodes);
        if (!proposedFailedNodes.isEmpty()) {
            log.info("Proposed failed node: {} are decided based on the ClusterState: {} And Layout: {}",
                    proposedFailedNodes, clusterState, layout
            );
        }

        return proposedFailedNodes;
    }

    /**
     * Provide a list of servers considered to have healed in the Corfu cluster according to
     * the FULLY_CONNECTED_CLUSTER implementation of algorithm for
     * {@link ClusterRecommendationStrategy}. The implementation of the algorithm in this method
     * is a greedy implementation through the execution of the following steps:
     * <p>
     * a) Add all unresponsive nodes with active links to the entire set of responsive nodes in the
     * cluster will be collected
     * b) Greedily add the nodes with minimum number of failed links which are are fully
     * connected to the set of active nodes as well as to the proposed set of healed nodes
     * <p>
     * Output of the above steps recommends the healed nodes which their addition will increase
     * the number of active members of the Corfu cluster while keeping it as a fully connected
     * cluster.
     * <p>
     * The following represents the underlying implementation of one algorithm to achieve the
     * above goal however the clients of this strategy must only rely on the guarantee that addition
     * of returned healed nodes is a recommendation for increasing responsive servers in
     * cluster while keeping the responsive cluster as a fully connected corfu cluster. The
     * clients must not rely on the implementation details of the algorithm as it might change in
     * the future releases.
     * <p>
     * Find Healed Nodes algorithm:
     * <p>
     * // Create the super set of healed nodes
     * for (Node in Unresponsive Nodes Set):
     * reEstablishedLinksMap.put(Node, set of peers that sent successful heartbeat to
     * Node)
     * <p>
     * // Collect super set of healed nodes
     * for (entry in reEstablishedLinksMap):
     * if (Responsive Nodes Set is NOT subset of Nodes Set of entry):
     * reEstablishedLinksMap.remove(Node of entry)
     * <p>
     * // Descending sort of the nodes based on number of reestablished links
     * sortedReEstablishedLinksMap <- descending sort by size of Node Set of
     * reEstablishedLinksMap
     * <p>
     * // Greedily add fully connected nodes with highest number of established links while
     * // keeping the invariant of Complete Graph
     * Proposed Healed Set = {}
     * <p>
     * for (entry in sortedReEstablishedLinksMap):
     * if ((Responsive Node Set union with Proposed Healed Set) is subset of Node Set of
     * entry):
     * add Node of entry to Proposed Healed Set
     * <p>
     * return Proposed Healed Set
     *
     * @param clusterState represents the state of connectivity amongst the Corfu cluster
     *                     nodes from a node's perspective.
     * @param layout       expected layout of the cluster.
     * @return a {@link List} of servers considered as healed according to the underlying
     * {@link ClusterRecommendationStrategy}.
     */
    @Override
    public List<String> healedServers(ClusterState clusterState, Layout layout) {

        log.trace("Detecting the healed nodes for: ClusterState: {} Layout: {}", clusterState, layout);

        // Remove asymmetry by converting all asymmetric link failures to symmetric failures
        Map<String, Set<String>> nodeFailedNeighborMap = convertAsymmetricToSymmetricFailures(clusterState);

        // Collect potential healed nodes
        Map<String, Set<String>> healedLinksNodeNeighborsMap = potentialHealedNodes(layout, nodeFailedNeighborMap);

        List<String> proposedHealedNodes = new ArrayList<>();

        // Greedily add the nodes with minimum number of failed neighbors
        while (!healedLinksNodeNeighborsMap.isEmpty()) {
            // Sort nodes based on ascending number of failed links and then ascending id of the
            // nodes
            Stream<Map.Entry<String, Set<String>>> sortedNodeNeighbors = sortHealedNodes(healedLinksNodeNeighborsMap);

            // Pick the fully connected nodes, a node with lowest number of link failures
            Map.Entry<String, Set<String>> healedNodeCandidate = sortedNodeNeighbors.findFirst().get();
            proposedHealedNodes.add(healedNodeCandidate.getKey());

            removeHealedCandidate(healedLinksNodeNeighborsMap, healedNodeCandidate);
        }

        log.debug("Proposed healed nodes: {}", proposedHealedNodes);
        if (!proposedHealedNodes.isEmpty()) {
            log.info("Proposed healed node: {} are decided based on the Cluster State: {} AND Layout: {}",
                    proposedHealedNodes, clusterState, layout
            );
        }

        return proposedHealedNodes;
    }

    /**
     * Remove from the provided {@param healedNodeNeighborsMap} all the entries of the
     * nodes that are not fully connected to the proposed healed candidate. This ensures that the
     * {@param healedNodeNeighborsMap} remains a collection of entries representing nodes
     * that are fully connected to both:
     * a) the entire set of responsive nodes in the layout and
     * b) all the proposed healed nodes.
     * <p>
     * Current implementation removes the candidate from the map and then iterates over the
     * {@param healedNodeNeighborsMap} and removes the entries that observe a failed link to the
     * {@param healedNodeCandidate}.
     *
     * @param healedNodeNeighborsMap represent a map of nodes that are potentially healed. This
     *                               map representing the edges from unresponsive nodes
     *                               in the layout to the corresponding neighbors that the
     *                               node sees as failed due to a link or node failure.
     * @param healedNodeCandidate    representing a healed node candidate that has been added to
     *                               proposed healed node set.
     */
    private void removeHealedCandidate(
            Map<String, Set<String>> healedNodeNeighborsMap,
            Map.Entry<String, Set<String>> healedNodeCandidate) {

        // Remove the entry for the healed node candidate
        healedNodeNeighborsMap.remove(healedNodeCandidate.getKey());

        // Remove the entries which observe a failed link to the healed node
        healedNodeNeighborsMap
                .entrySet()
                .removeIf(healedNodeNeighborsEntry ->
                        healedNodeNeighborsEntry
                                .getValue()
                                .contains(healedNodeCandidate.getKey()));
    }

    /**
     * Create a sorted stream of {@link Map.Entry} representing a node and its neighboring nodes
     * provided by {@param healedLinksNodeNeighborsMap}. The stream is built from a shallow copy
     * of {@param healedLinksNodeNeighborsMap}. It is first sorted in ascending manner on the
     * number of failures for each node and then on the name of node in ascending manner. In other
     * words, the nodes with lowest number of failures will be at the beginning of stream however in
     * case of two nodes having the same number of failures, the nodes will appear with the
     * natural order of the node names.
     *
     * @param healedLinksNodeNeighborsMap A map which each of its entries corresponds to a Corfu
     *                                    server and a set of neighboring nodes of that server. In
     *                                    other words, each entry can represents a collection of
     *                                    edges from the source captured as the key of that entry.
     * @return A stream of entries from a shallow copy of the provided map which is sorted based on
     * the ascending size of the failed links and then by the ascending order of node
     * names as the tie breaker.
     */
    private Stream<Map.Entry<String, Set<String>>> sortHealedNodes(
            Map<String, Set<String>> healedLinksNodeNeighborsMap) {

        Map<String, Set<String>> copyOfNodeNeighborsMap = new HashMap<>(healedLinksNodeNeighborsMap);

        Comparator<Map.Entry<String, Set<String>>> comparatorNumOfFailedLinksAscending =
                Comparator.comparingInt(o -> o.getValue().size());
        Comparator<Map.Entry<String, Set<String>>> comparatorNodeNameAscending = Comparator.comparing(Map.Entry::getKey);

        Comparator<Map.Entry<String, Set<String>>> comparatorFailureAscendingThenNodeNameAscending =
                comparatorNumOfFailedLinksAscending.thenComparing(comparatorNodeNameAscending);

        return copyOfNodeNeighborsMap
                .entrySet()
                .stream()
                .sorted(comparatorFailureAscendingThenNodeNameAscending);
    }

    /**
     * Create of map Corfu nodes which are super set of healed nodes along with the neighbors
     * for each node. Each of these nodes is considered as unresponsive the current layout but it
     * has an active connection to the entire set of responsive nodes in the provided
     * {@link Layout}.
     *
     * @param layout                 an instance {@link Layout} representing expected layout capturing amongst the
     *                               other things, information about responsive and unresponsive servers.
     * @param nodeFailedNeighborsMap A map which each of its entries corresponding to a Corfu
     *                               server and a set of neighboring nodes of the server. In
     *                               other words, each entry represents a collection of edges with
     *                               link failure from the source captured as the key of that
     *                               entry.
     * @return A map where the keys represent Corfu servers that are currently part of unresponsive
     * set AND either:
     * a) have no failed link to any node in the cluster
     * b) have no failed link to the responsive nodes
     * The value is a set of the neighbors of the node represented by the key where there
     * is a link failure between the key and those neighbors.
     */
    private Map<String, Set<String>> potentialHealedNodes(
            Layout layout,
            Map<String, Set<String>> nodeFailedNeighborsMap) {

        Map<String, Set<String>> healedNodeNeighborsMap = new HashMap<>();

        // Find all nodes that are currently unresponsive in layout with no failure to any node
        Set<String> fullyRecoveredUnresponsiveNodes = new HashSet<>(layout.getUnresponsiveServers());
        fullyRecoveredUnresponsiveNodes.removeAll(nodeFailedNeighborsMap.keySet());

        // Add previously unresponsive nodes which are fully connected in the current cluster state
        fullyRecoveredUnresponsiveNodes
                .stream()
                .forEach(fullyConnectedNode ->
                        healedNodeNeighborsMap.put(fullyConnectedNode, Collections.emptySet()));

        // Add unresponsive nodes with link failures which are fully connected to active servers
        nodeFailedNeighborsMap
                .entrySet()
                .stream()
                .filter(nodeNeighborsEntry -> layout.getUnresponsiveServers()
                        .contains(nodeNeighborsEntry.getKey()) &&
                        Collections.disjoint(nodeNeighborsEntry.getValue(),
                                layout.getAllActiveServers()))
                .forEach(healedNodeCandidate -> healedNodeNeighborsMap.put(
                        healedNodeCandidate.getKey(),
                        healedNodeCandidate.getValue()));

        return healedNodeNeighborsMap;
    }
}
