package org.corfudb.infrastructure.management;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.protocols.wireprotocol.NodeState;
import org.corfudb.runtime.view.Layout;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This class is an implementation of {@link ClusterRecommendationStrategy} where the ideal state
 * of the Corfu cluster is a fully connected network (i.e. complete graph) in which there is an
 * active link amongst all nodes in the cluster. Failed and Healed nodes are recommended so that
 * the cluster remains fully connected.
 *
 * Created by Sam Behnam on 10/27/18.
 */
@Slf4j
public class FullyConnectedClusterRecommendationEngine implements ClusterRecommendationEngine {

    private static final ClusterRecommendationStrategy STRATEGY =
            ClusterRecommendationStrategy.FULLY_CONNECTED_CLUSTER;
    private final Gson gson = new GsonBuilder().setPrettyPrinting().create();

    @Override
    public ClusterRecommendationStrategy getClusterRecommendationStrategy() {
        return STRATEGY;
    }

    /**
     * Provides list of servers from a given layout(epoch) that this implementation of
     * FULLY_CONNECTED_CLUSTER algorithm has determined as failed. The implementation of the
     * algorithm in this method is a greedy approach by executing the following steps:
     *
     * a) Collect all nodes with link failures to the responsive nodes in the cluster
     * b) While possible, add the nodes with maximum number of failed links to set of
     * failed nodes
     *
     * The result set of failed nodes is the recommendation of this strategy which their removal
     * from cluster will lead to a fully connected cluster.
     *
     * The following represents the underlying implementation of one algorithm to achieve the
     * above goal however the clients of this strategy must only rely on the guarantee that removal
     * of the returned failed nodes is a recommendation for arriving at a fully connected
     * cluster and must not rely on the implementation details of the algorithm.
     *
     *     Find Failed Nodes algorithm:
     *
     *         // Create the super set of failed nodes
     *         for (Node in Responsive Nodes Set):
     *             disconnectedLinksMap.put(Node, set of active peers whose their heartbeat response
     *             didn't get received by the Node)
     *
     *         Proposed Failed Set = {}
     *
     *         // Greedily find and remove the nodes with highest number of link failures until
     *         // all the remaining nodes form a fully connected corfu cluster
     *         while (disconnectedLinksMap is not empty):
     *
     *             // Descending sort of the nodes based on number of failed links
     *             sortedDisconnectedLinksMap <- descending sort by size of Node Set of
     *             disconnectedLinksMap entries
     *
     *
     *             // Collect Failed Nodes
     *             Fail Candidate entry <- remove the entry with highest number of link failure
     *             add Node of Fail Candidate entry to Proposed Failed Set
     *             disconnectedLinksMap.remove(Node of Fail Candidate entry)
     *             for (Node in Disconnected Node set of Fail Candidate entry):
     *                 remove Fail Candidate Node from disconnectedLinksMap.get(Node)
     *                 remove the entry for the Node from disconnectedLinksMap if its Disconnected
     *                 Node set is empty
     *
     *         return Proposed Healed Set
     *
     * @param clusterState represents the state of connectivity amongst the Corfu cluster
     *                     nodes from a node's perspective.
     * @param layout expected layout of the cluster.
     * @return a {@link List} of servers considered as failed according to the underlying
     *         {@link ClusterRecommendationStrategy}.
     */
    @Override
    public List<String> failedServers(ClusterState clusterState, Layout layout) {

        log.trace("Detecting the failed nodes for: \nClusterState= {} \nLayout= {}",
                gson.toJson(clusterState), gson.toJson(layout));

        // Remove asymmetry by converting all asymmetric link failures to symmetric failures
        final Map<String, Set<String>> nodeFailedNeighborMap =
                convertAsymmetricToSymmetricFailures(clusterState);

        // Collect potential failed nodes
        final Map<String, Set<String>> disconnectedLinksMap =
                potentialFailedNodes(layout, nodeFailedNeighborMap);

        final List<String> proposedFailedNodes = new ArrayList<>();

        // Greedily find and remove the nodes with highest number of link failures
        while (!disconnectedLinksMap.isEmpty()) {

            // Sort of the nodes based on descending number of failed links and then on descending
            // id of nodes
            final Stream<Map.Entry<String, Set<String>>> sortedNodeNeighbors =
                    sortFailedNodes(disconnectedLinksMap, clusterState);

            // Add the node with most link failures as a failed node candidate
            final Map.Entry<String, Set<String>> failedNodeCandidate =
                    sortedNodeNeighbors.findFirst().get();
            proposedFailedNodes.add(failedNodeCandidate.getKey());

            // Update the map representing of link failures
            removeFailedCandidate(disconnectedLinksMap, failedNodeCandidate);
        }

        log.debug("Proposed failed nodes: {}", proposedFailedNodes);
        if (!proposedFailedNodes.isEmpty()) {
            log.info("Proposed failed node: {} are decided based on the \nClusterState: {} \nAnd" +
                    " \nLayout: {}", proposedFailedNodes, gson.toJson(clusterState),
                    gson.toJson(layout));
        }

        return proposedFailedNodes;
    }

    /**
     * Take an instance of {@link ClusterState}, traverse it and create {@link Map} of nodes and
     * corresponding neighbors of those nodes which are observed to have a failure. The failure
     * could be the result of either the neighbor's failure or the link failure to neighbor.
     * Note that {@param clusterState} captures the link failures as observed by each node and
     * hence it might include asymmetric view of link failures.
     * For example, a given node can consider the link to neighboring node as failed while the
     * neighbor observes the link to the node as active. This method takes such asymmetric view of
     * the cluster and creates a symmetric view by aggressively marking any active link that has
     * observed a failed counterpart. For instance, in above example, the neighbor which had a
     * link failure to a given node will be added to the set of failed neighbors of that node.
     *
     * @param clusterState represents the status of a cluster.
     * @return A map which each of its entries corresponding to a Corfu server node and a set of
     *         neighboring nodes of that server to which a link failure exists. In other words,
     *         each entry represents a collection of edges from the source node captured as the key
     *         of that entry. The returned map represents a symmetric view of cluster so if node
     *         A considers node B as a failed neighbor then node B considers node A as a failed
     *         neighbor too.
     */
    private Map<String, Set<String>> convertAsymmetricToSymmetricFailures(ClusterState clusterState) {

        log.trace("Converting to symmetric view for the provided cluster view: {}",
                gson.toJson(clusterState.getNodeStatusMap()));

        Map<String, Set<String>> clusterMatrix = new HashMap<>();

        // Traverse a ClusterState and add reverse of observed failed links as failed links.
        for (Map.Entry<String, NodeState> nodeNeighborEntry : clusterState.getNodeStatusMap()
                                                                          .entrySet()) {
            final String currentNode = nodeNeighborEntry.getKey();

            final Map<String, Boolean> neighborsReachabilityMap =
                    nodeNeighborEntry
                            .getValue()
                            .getConnectivityStatus();

            // Collect nodes that are unreachable from currentNode
            final Set<String> unreachableNeighborsSet = neighborsReachabilityMap
                    .entrySet()
                    .stream()
                    .filter(neighborReachabilityEntry -> !neighborReachabilityEntry.getValue())
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toSet());

            // Capture link failures seen by currentNode
            clusterMatrix
                    .computeIfAbsent(currentNode, key -> new HashSet<>())
                    .addAll(unreachableNeighborsSet);

            // Capture symmetric link failures for the neighbors of currentNode
            unreachableNeighborsSet
                    .stream()
                    .forEach(unreachableNeighbor ->
                            clusterMatrix
                                    .computeIfAbsent(unreachableNeighbor, key -> new HashSet<>())
                                    .add(currentNode));
        }

        log.trace("Converted view of the cluster to a symmetric view of connectivity failures: {}",
                gson.toJson(clusterMatrix));

        return clusterMatrix;
    }

    /**
     * Create a map of Corfu nodes which is the superset of failed nodes along with the neighbors
     * for each node. Each of these nodes are currently amongst the active nodes in the provided
     * {@link Layout} and have at least one link failure with their neighboring Corfu servers.
     *
     * @param layout an instance of {@link Layout} representing expected layout capturing amongst
     *               the  other things, information about responsive and unresponsive servers.
     * @param nodeFailedNeighborsMap A map which each of its entries corresponds to a Corfu
     *                              server and a set of its neighboring nodes where a link failure
     *                              has been observed. In other word, each entry represents a
     *                              collection of edges from the source captured as the key of
     *                              that entry to its failed neighbors.
     * @return A map representing Corfu servers considered as responsive in the provided layout and
     *         that have failed links to at least another responsive servers from the provided
     *         layout. Mathematically the map represents a symmetric matrix which both columns
     *         and rows represent active nodes
     */
    private Map<String, Set<String>> potentialFailedNodes(Layout layout,
                                                          Map<String, Set<String>> nodeFailedNeighborsMap) {

        final Map<String, Set<String>> potentialFailedNodeMap = new HashMap<>();

        // Responsive nodes observing link failures will be collected along with the
        // responsive neighbors
        nodeFailedNeighborsMap
                .entrySet()
                .stream()
                .filter(nodeNeighborEntry -> layout.getAllActiveServers()
                                                   .contains(nodeNeighborEntry.getKey()))
                .filter(failedLinkEntry -> !failedLinkEntry.getValue().isEmpty())
                .forEach(nodeFailedNeighborsEntry -> {
                    // Remove the failed neighbors that are not part of layout's responsive servers
                    final Set<String> neighbors = nodeFailedNeighborsEntry.getValue();
                    neighbors.retainAll(layout.getAllActiveServers());
                    potentialFailedNodeMap.put(nodeFailedNeighborsEntry.getKey(), neighbors);
                });

        // Return the candidates which their failing neighbors are non-empty
        return potentialFailedNodeMap
                .entrySet()
                .stream()
                .filter(failedLinkEntry -> !failedLinkEntry.getValue().isEmpty())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /**
     * Update the provided {@param disconnectedLinksMap} by removing a node which is candidate
     * of being a failure. The update removes the failed candidate node from the remaining
     * nodes' neighbor set. If the updated set is empty, the entry for the server which has
     * observed link failure with its neighbors will be removed from the map. Note because of the
     * symmetry of graph represented by disconnectedLinksMap, get operation by neighborNodes in
     * the implementation of this method is safe.
     *
     * @param disconnectedLinksMap A map which each of its entries corresponding to a Corfu
     *                             server and a set of neighboring nodes of the server which
     *                             have a link failure.
     * @param failedNodeCandidate A map entry representing a node that is
     *                                           candidate of failure along with its neighbors.
     */
    private void removeFailedCandidate(
            Map<String, Set<String>> disconnectedLinksMap,
            Map.Entry<String, Set<String>> failedNodeCandidate) {

        for (String neighborNode : failedNodeCandidate.getValue()) {
            disconnectedLinksMap
                    .get(neighborNode)
                    .remove(failedNodeCandidate.getKey());
            if (disconnectedLinksMap.get(neighborNode).isEmpty()) {
                disconnectedLinksMap.remove(neighborNode);
            }
        }
        disconnectedLinksMap.remove(failedNodeCandidate.getKey());
    }

    /**
     * Provide a list of servers considered to have healed in the Corfu cluster according to
     * the FULLY_CONNECTED_CLUSTER implementation of algorithm for
     * {@link ClusterRecommendationStrategy}. The implementation of the algorithm in this method
     * is a greedy implementation through the execution of the following steps:
     *
     * a) Add all unresponsive nodes with active links to the entire set of responsive nodes in the
     *    cluster will be collected
     * b) Greedily add the nodes with minimum number of failed links which are are fully
     *    connected to the set of active nodes as well as to the proposed set of healed nodes
     *
     * Output of the above steps recommends the healed nodes which their addition will increase
     * the number of active members of the Corfu cluster while keeping it as a fully connected
     * cluster.
     *
     * The following represents the underlying implementation of one algorithm to achieve the
     * above goal however the clients of this strategy must only rely on the guarantee that addition
     * of returned healed nodes is a recommendation for increasing responsive servers in
     * cluster while keeping the responsive cluster as a fully connected corfu cluster. The
     * clients must not rely on the implementation details of the algorithm as it might change in
     * the future releases.
     *
     *     Find Healed Nodes algorithm:
     *
     *         // Create the super set of healed nodes
     *         for (Node in Unresponsive Nodes Set):
     *             reEstablishedLinksMap.put(Node, set of peers that sent successful heartbeat to
     *             Node)
     *
     *         // Collect super set of healed nodes
     *         for (entry in reEstablishedLinksMap):
     *             if (Responsive Nodes Set is NOT subset of Nodes Set of entry):
     *                 reEstablishedLinksMap.remove(Node of entry)
     *
     *         // Descending sort of the nodes based on number of reestablished links
     *         sortedReEstablishedLinksMap <- descending sort by size of Node Set of
     *                                        reEstablishedLinksMap
     *
     *         // Greedily add fully connected nodes with highest number of established links while
     *         // keeping the invariant of Complete Graph
     *         Proposed Healed Set = {}
     *
     *         for (entry in sortedReEstablishedLinksMap):
     *             if ((Responsive Node Set union with Proposed Healed Set) is subset of Node Set of
     *             entry):
     *                 add Node of entry to Proposed Healed Set
     *
     *         return Proposed Healed Set
     *
     * @param clusterState represents the state of connectivity amongst the Corfu cluster
     *                     nodes from a node's perspective.
     * @param layout expected layout of the cluster.
     * @return a {@link List} of servers considered as healed according to the underlying
     *         {@link ClusterRecommendationStrategy}.
     */
    @Override
    public List<String> healedServers(final ClusterState clusterState, final Layout layout) {

        log.trace("Detecting the healed nodes for: \nClusterState: {} \nLayout: {}",
                gson.toJson(clusterState), gson.toJson(layout));

        // Remove asymmetry by converting all asymmetric link failures to symmetric failures
        final Map<String, Set<String>> nodeFailedNeighborMap =
                convertAsymmetricToSymmetricFailures(clusterState);

        // Collect potential healed nodes
        final Map<String, Set<String>> healedLinksNodeNeighborsMap =
                potentialHealedNodes(layout, nodeFailedNeighborMap);

        final List<String> proposedHealedNodes = new ArrayList<>();

        // Greedily add the nodes with minimum number of failed neighbors
        while (!healedLinksNodeNeighborsMap.isEmpty()) {
            // Sort nodes based on ascending number of failed links and then ascending id of the
            // nodes
            final Stream<Map.Entry<String, Set<String>>> sortedNodeNeighbors =
                    sortHealedNodes(healedLinksNodeNeighborsMap);

            // Pick the fully connected nodes, a node with lowest number of link failures
            final Map.Entry<String, Set<String>> healedNodeCandidate =
                    sortedNodeNeighbors.findFirst().get();
            proposedHealedNodes.add(healedNodeCandidate.getKey());

            removeHealedCandidate(healedLinksNodeNeighborsMap, healedNodeCandidate);
        }

        log.debug("Proposed healed nodes: {}", proposedHealedNodes);
        if (!proposedHealedNodes.isEmpty()) {
            log.info("Proposed healed node: {} are decided based on the \nCluster State: {} \n" +
                    "AND\nLayout: {}", proposedHealedNodes, gson.toJson(clusterState),
                    gson.toJson(layout));
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
     *
     * Current implementation removes the candidate from the map and then iterates over the
     * {@param healedNodeNeighborsMap} and removes the entries that observe a failed link to the
     * {@param healedNodeCandidate}.
     *
     * @param healedNodeNeighborsMap represent a map of nodes that are potentially healed. This
     *                               map representing the edges from unresponsive nodes
     *                               in the layout to the corresponding neighbors that the
     *                               node sees as failed due to a link or node failure.
     * @param healedNodeCandidate representing a healed node candidate that has been added to
     *                            proposed healed node set.
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
     *         the ascending size of the failed links and then by the ascending order of node
     *         names as the tie breaker.
     */
    private Stream<Map.Entry<String, Set<String>>> sortHealedNodes(
            Map<String, Set<String>> healedLinksNodeNeighborsMap) {

        final Map<String, Set<String>> copyOfNodeNeighborsMap =
                new HashMap<>(healedLinksNodeNeighborsMap);

        final Comparator<Map.Entry<String, Set<String>>> comparatorNumOfFailedLinksAscending =
                Comparator.comparingInt(o -> o.getValue().size());
        final Comparator<Map.Entry<String, Set<String>>> comparatorNodeNameAscending =
                Comparator.comparing(Map.Entry::getKey);

        final Comparator<Map.Entry<String, Set<String>>> comparatorFailureAscendingThenNodeNameAscending =
                comparatorNumOfFailedLinksAscending.thenComparing(comparatorNodeNameAscending);

        return copyOfNodeNeighborsMap
                .entrySet()
                .stream()
                .sorted(comparatorFailureAscendingThenNodeNameAscending);
    }

    /**
     * Create a sorted stream of {@link Map.Entry} representing a node and its neighboring nodes
     * provided through {@param failedLinks}. The stream is built from a shallow copy
     * of {@param failedLinks}. It is first sorted in descending manner on the number of
     * failures for each node in the responsive set, then by descending number of failed links to
     * unresponsive nodes, and finally on the name of node in descending manner. In other words,
     * the nodes with highest number of link failures to responsive and unresponsive nodes will be
     * at the beginning of the stream however in case of two nodes observing the same number of
     * failed links, the nodes will appear with reverse natural order of the node names.
     *
     * @param failedLinks A map which each of its entries corresponding to a Corfu
     *                                   server and a set of neighboring nodes of the server. In
     *                                   other word, each entry can represents a collection of
     *                                   edges from the source captured as the key of that entry.
     * @param clusterState represents the status of a cluster.
     * @return A stream of entries from a shallow copy of the provided map which is sorted based on
     *         the descending size of the failed links and then by the descending order of
     *         node names as the tie breaker.
     */
    private Stream<Map.Entry<String, Set<String>>> sortFailedNodes(
            Map<String, Set<String>> failedLinks, ClusterState clusterState) {

        final Map<String, Set<String>> copyOfNodeNeighborsMap = new HashMap<>(failedLinks);
        final Map<String, Set<String>> nodeFailedNeighborsMap =
                convertAsymmetricToSymmetricFailures(clusterState);

        final Comparator<Map.Entry<String, Set<String>>> failedLinksDescending =
                Collections.reverseOrder(Comparator.comparingInt(o -> o.getValue().size()));
        final Comparator<Map.Entry<String, Set<String>>> failedLinksToUnresponsiveNodesDescending =
                Collections.reverseOrder(Comparator.comparingInt(o -> nodeFailedNeighborsMap.get(o.getKey()).size()));
        final Comparator<Map.Entry<String, Set<String>>> nodeNamesDescending =
                Collections.reverseOrder(Comparator.comparing(Map.Entry::getKey));

        final Comparator<Map.Entry<String, Set<String>>> failedLinksThenNodeNamesDescending =
                failedLinksDescending
                        .thenComparing(failedLinksToUnresponsiveNodesDescending)
                        .thenComparing(nodeNamesDescending);

        return copyOfNodeNeighborsMap
                .entrySet()
                .stream()
                .sorted(failedLinksThenNodeNamesDescending);
    }

    /**
     * Create of map Corfu nodes which are super set of healed nodes along with the neighbors
     * for each node. Each of these nodes is considered as unresponsive the current layout but it
     * has an active connection to the entire set of responsive nodes in the provided
     * {@link Layout}.
     *
     * @param layout an instance {@link Layout} representing expected layout capturing amongst the
     *               other things, information about responsive and unresponsive servers.
     * @param nodeFailedNeighborsMap A map which each of its entries corresponding to a Corfu
     *                               server and a set of neighboring nodes of the server. In
     *                               other words, each entry represents a collection of edges with
     *                               link failure from the source captured as the key of that
     *                               entry.
     * @return A map where the keys represent Corfu servers that are currently part of unresponsive
     *         set AND either:
     *         a) have no failed link to any node in the cluster
     *         b) have no failed link to the responsive nodes
     *         The value is a set of the neighbors of the node represented by the key where there
     *         is a link failure between the key and those neighbors.
     */
    private Map<String, Set<String>> potentialHealedNodes(
                                                Layout layout,
                                                Map<String, Set<String>> nodeFailedNeighborsMap) {

        final Map<String, Set<String>> healedNodeNeighborsMap = new HashMap<>();

        // Find all nodes that are currently unresponsive in layout with no failure to any node
        final Set<String> fullyRecoveredUnresponsiveNodes =
                new HashSet<>(layout.getUnresponsiveServers());
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
