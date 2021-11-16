package org.corfudb.infrastructure.management;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.protocols.wireprotocol.NodeState;
import org.corfudb.protocols.wireprotocol.failuredetector.FileSystemStats;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeRank;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeRank.NodeRankByPartitionAttributes;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeRank.NodeRankByResourceQuota;

import java.util.NavigableSet;
import java.util.Optional;
import java.util.TreeSet;

/**
 * Detects failures in the file system
 */
@Slf4j
public class FileSystemAdvisor {

    /**
     * Checks if a node is failed because of quota exceeded
     *
     * @param clusterState cluste state
     * @return node rank by quota
     */
    public Optional<NodeRankByResourceQuota> findFailedNodeByResourceQuota(ClusterState clusterState) {

        if (!clusterState.getLocalNode().isPresent()) {
            return Optional.empty();
        }

        NavigableSet<NodeRankByResourceQuota> set = new TreeSet<>();
        for (NodeState node : clusterState.getNodes().values()) {
            node.getFileSystem().ifPresent(fsStats -> {
                if (!fsStats.getResourceQuotaStats().isExceeded()) {
                    return;
                }

                NodeRankByResourceQuota quota = new NodeRankByResourceQuota(
                        node.getConnectivity().getEndpoint(),
                        fsStats.getResourceQuotaStats()
                );
                set.add(quota);
            });
        }

        Optional<NodeRankByResourceQuota> maybeDecisionMaker = Optional.ofNullable(set.pollFirst());

        if (!maybeDecisionMaker.isPresent()) {
            log.trace("Decision maker not found");
            return Optional.empty();
        }

        NodeRankByResourceQuota decisionMaker = maybeDecisionMaker.get();
        if (!decisionMaker.getEndpoint().equals(clusterState.getLocalEndpoint())) {
            String message = "The node can't be a decision maker, skip operation. Decision maker node is: {}";
            log.trace(message, decisionMaker);
            return Optional.empty();
        }

        Optional<NodeRankByResourceQuota> maybeFailedNode = Optional.ofNullable(set.pollLast());

        if (maybeFailedNode.isPresent()) {
            NodeRankByResourceQuota failedNode = maybeFailedNode.get();
            if (decisionMaker.getEndpoint().equals(failedNode.getEndpoint())) {
                log.trace("The Decision maker and a failed node are the same, no way to detect a failure");
                return Optional.empty();
            }
        }

        return maybeFailedNode;
    }

    public Optional<NodeRankByPartitionAttributes> findFailedNodeByPartitionAttributes(ClusterState clusterState) {

        NavigableSet<NodeRankByPartitionAttributes> set = new TreeSet<>();
        for (NodeState node : clusterState.getNodes().values()) {
            node.getFileSystem().ifPresent(fsStats -> {
                if (fsStats.getPartitionAttributeStats().isReadOnly()) {
                    NodeRankByPartitionAttributes quota = new NodeRankByPartitionAttributes(
                            node.getConnectivity().getEndpoint(),
                            fsStats.getPartitionAttributeStats()
                    );
                    set.add(quota);
                }
            });
        }

        Optional<NodeRankByPartitionAttributes> maybeFailedNode = Optional.ofNullable(set.pollLast());

        Optional<NodeRankByPartitionAttributes> maybeDecisionMaker = Optional.ofNullable(set.pollFirst());

        if (!maybeDecisionMaker.isPresent()) {
            log.trace("Decision maker not found");
            return Optional.empty();
        }

        NodeRankByPartitionAttributes decisionMaker = maybeDecisionMaker.get();
        if (!decisionMaker.getEndpoint().equals(clusterState.getLocalEndpoint())) {
            String message = "The node can't be a decision maker, skip operation. Decision maker node is: {}";
            log.trace(message, decisionMaker);
            return Optional.empty();
        }

        if (maybeFailedNode.isPresent()) {
            NodeRankByPartitionAttributes failedNode = maybeFailedNode.get();
            if (decisionMaker.getEndpoint().equals(failedNode.getEndpoint())) {
                log.trace("The Decision maker and a failed node are the same, no way to detect a failure");
                return Optional.empty();
            }
        }

        return maybeFailedNode;
    }

    public Optional<NodeRankByResourceQuota> healedServer(ClusterState clusterState) {

        Optional<FileSystemStats> maybeFileSystem = getFileSystemStats(clusterState);

        return maybeFileSystem
                .filter(fileSystem -> {
                    log.trace("No node to heal, quota is exceeded");
                    return fileSystem.getResourceQuotaStats().isNotExceeded();
                })
                .map(fileSystem -> new NodeRankByResourceQuota(
                        clusterState.getLocalEndpoint(),
                        fileSystem.getResourceQuotaStats()
                ));
    }

    private Optional<FileSystemStats> getFileSystemStats(ClusterState clusterState) {
        if (!clusterState.getLocalNode().isPresent()) {
            log.trace("Can't find a node to heal, cluster state doesn't contain local node");
            return Optional.empty();
        }

        NodeState localNodeState = clusterState.getLocalNode().get();

        if (!localNodeState.getFileSystem().isPresent()) {
            log.trace("Can't find a node to heal, missing file system stats");
            return Optional.empty();
        }

        return localNodeState.getFileSystem();
    }
}
