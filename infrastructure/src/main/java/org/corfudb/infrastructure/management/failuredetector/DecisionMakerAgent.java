package org.corfudb.infrastructure.management.failuredetector;

import com.google.common.annotations.VisibleForTesting;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.management.ClusterAdvisor;
import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.protocols.wireprotocol.NodeState;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeRank;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

@AllArgsConstructor
@Slf4j
public class DecisionMakerAgent {
    private final ClusterState clusterState;
    private final ClusterAdvisor clusterAdvisor;

    /**
     * Find a node that is a decision maker according to the current cluster state
     *
     * @return a decision maker
     */
    public Optional<String> findDecisionMaker() {
        log.trace("Find a decision maker");

        if (!clusterState.getLocalNode().isPresent()) {
            return Optional.empty();
        }

        Set<String> healthyNodes = healthyNodes();

        return clusterAdvisor
                .findDecisionMaker(clusterState, healthyNodes)
                .map(NodeRank::getEndpoint)
                //filter out unhealthy nodes
                .filter(healthyNodes::contains)
                //give up if a decision maker is not a local node, then the decision maker not found
                .filter(decisionMaker -> {
                    boolean isDmALocalNode = decisionMaker.equals(clusterState.getLocalEndpoint());
                    if (!isDmALocalNode) {
                        String message = "The node can't be a decision maker, skip operation. Decision maker node is: {}";
                        log.trace(message, decisionMaker);
                    }

                    return isDmALocalNode;
                });
    }

    /**
     * Provides a set of healthy nodes in the cluster.
     * If any node has missing information about it's state (like file system stats),
     * we can't have that node in the list of healthy nodes
     * @return a set of healthy nodes
     */
    @VisibleForTesting
    Set<String> healthyNodes() {
        Set<String> healthyNodes = new HashSet<>();

        for (NodeState node : clusterState.getNodes().values()) {
            node.getFileSystem().ifPresent(fsStats -> {
                if (fsStats.isOk()) {
                    healthyNodes.add(node.getConnectivity().getEndpoint());
                }
            });
        }

        return healthyNodes;
    }
}
