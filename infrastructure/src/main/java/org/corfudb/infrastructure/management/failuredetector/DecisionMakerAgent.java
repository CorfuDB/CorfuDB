package org.corfudb.infrastructure.management.failuredetector;

import com.google.common.annotations.VisibleForTesting;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.management.ClusterAdvisor;
import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.protocols.wireprotocol.NodeState;
import org.corfudb.protocols.wireprotocol.failuredetector.FileSystemStats;
import org.corfudb.protocols.wireprotocol.failuredetector.FileSystemStats.PartitionAttributeStats;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeRank;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeRank.NodeRankByPartitionAttributes;

import java.util.NavigableSet;
import java.util.Optional;
import java.util.TreeSet;

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
        if (!clusterState.getLocalNode().isPresent()) {
            return Optional.empty();
        }

        Optional<String> partitionDm = findPartitionAttributesDecisionMaker()
                .map(NodeRankByPartitionAttributes::getEndpoint);

        Optional<String> clusterDm = clusterAdvisor.findDecisionMaker(clusterState)
                .map(NodeRank::getEndpoint);

        //Only completely healthy nodes can update cluster layout
        if (clusterDm.equals(partitionDm)) {
            return clusterDm;
        }

        log.trace("Decision maker not found");
        return Optional.empty();
    }

    @VisibleForTesting
    Optional<NodeRankByPartitionAttributes> findPartitionAttributesDecisionMaker() {
        NavigableSet<NodeRankByPartitionAttributes> set = new TreeSet<>();
        for (NodeState node : clusterState.getNodes().values()) {
            String nodeEndpoint = node.getConnectivity().getEndpoint();
            node.getFileSystem()
                    //get partition attributes
                    .map(FileSystemStats::getPartitionAttributeStats)
                    //nodes with read only partitions can't be decision makers
                    .filter(PartitionAttributeStats::isWritable)
                    .map(attr -> new NodeRankByPartitionAttributes(nodeEndpoint, attr))
                    .ifPresent(set::add);
        }

        Optional<NodeRankByPartitionAttributes> maybeDecisionMaker = Optional.ofNullable(set.pollFirst());

        return maybeDecisionMaker
                //give up if a decision maker is not a local node, then the decision maker not found
                .filter(decisionMaker -> {
                    String dmEndpoint = decisionMaker.getEndpoint();
                    boolean isDmALocalNode = dmEndpoint.equals(clusterState.getLocalEndpoint());
                    if (!isDmALocalNode) {
                        String message = "The node can't be a decision maker, skip operation. Decision maker node is: {}";
                        log.trace(message, decisionMaker);
                    }

                    return isDmALocalNode;
                });
    }
}
