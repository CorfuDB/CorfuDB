package org.corfudb.infrastructure.management;

import com.google.common.collect.ImmutableList;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.protocols.wireprotocol.NodeState;
import org.corfudb.protocols.wireprotocol.failuredetector.FileSystemStats;
import org.corfudb.protocols.wireprotocol.failuredetector.FileSystemStats.PartitionAttributeStats;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeRank.NodeRankByPartitionAttributes;

import java.util.NavigableSet;
import java.util.Optional;
import java.util.TreeSet;

/**
 * Detects failures in the file system
 */
@Slf4j
public class FileSystemAdvisor {

    public Optional<NodeRankByPartitionAttributes> findFailedNodeByPartitionAttributes(ClusterState clusterState) {
        NavigableSet<NodeRankByPartitionAttributes> set = new TreeSet<>();

        for (NodeState node : clusterState.getNodes().values()) {
            String nodeEndpoint = node.getConnectivity().getEndpoint();

            ImmutableList<String> unresponsiveNodes = clusterState.getUnresponsiveNodes();
            if (unresponsiveNodes.contains(nodeEndpoint)) {
                log.trace("Failed node already in the list of unresponsive nodes: {}", unresponsiveNodes);
                continue;
            }

            node.getFileSystem()
                    .filter(FileSystemStats::hasError)
                    .map(attr -> new NodeRankByPartitionAttributes(nodeEndpoint, attr))
                    .ifPresent(set::add);
        }

        return Optional.ofNullable(set.pollLast());
    }

    public Optional<NodeRankByPartitionAttributes> healedServer(ClusterState clusterState) {

        ImmutableList<String> unresponsiveNodes = clusterState.getUnresponsiveNodes();
        if (!unresponsiveNodes.contains(clusterState.getLocalEndpoint())) {
            return Optional.empty();
        }

        Optional<FileSystemStats> maybeFileSystem = getFileSystemStats(clusterState);

        return maybeFileSystem
                .filter(FileSystemStats::isOk)
                .map(fileSystem -> new NodeRankByPartitionAttributes(clusterState.getLocalEndpoint(), fileSystem));
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
