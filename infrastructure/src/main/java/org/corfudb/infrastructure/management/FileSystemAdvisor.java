package org.corfudb.infrastructure.management;

import com.google.common.collect.ImmutableList;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.protocols.wireprotocol.NodeState;
import org.corfudb.protocols.wireprotocol.failuredetector.FileSystemStats;
import org.corfudb.protocols.wireprotocol.failuredetector.FileSystemStats.PartitionAttributeStats;
import org.corfudb.protocols.wireprotocol.failuredetector.FileSystemStats.ResourceQuotaStats;
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

        NavigableSet<NodeRankByResourceQuota> set = new TreeSet<>();

        for (NodeState node : clusterState.getNodes().values()) {
            String nodeEndpoint = node.getConnectivity().getEndpoint();

            ImmutableList<String> unresponsiveNodes = clusterState.getUnresponsiveNodes();
            if (unresponsiveNodes.contains(nodeEndpoint)) {
                log.trace("Failed node already in the list of unresponsive nodes: {}", unresponsiveNodes);
                continue;
            }

            node.getFileSystem()
                    .map(FileSystemStats::getResourceQuotaStats)
                    //check if the node has failed: exceeded quota
                    .filter(ResourceQuotaStats::isExceeded)
                    .map(quota -> new NodeRankByResourceQuota(nodeEndpoint, quota))
                    .ifPresent(set::add);
        }

        return Optional.ofNullable(set.pollLast());
    }

    public Optional<NodeRankByPartitionAttributes> findFailedNodeByPartitionAttributes(ClusterState clusterState) {
        NavigableSet<NodeRankByPartitionAttributes> set = new TreeSet<>();

        for (NodeState node : clusterState.getNodes().values()) {
            String localEndpoint = node.getConnectivity().getEndpoint();

            String nodeEndpoint = node.getConnectivity().getEndpoint();

            ImmutableList<String> unresponsiveNodes = clusterState.getUnresponsiveNodes();
            if (unresponsiveNodes.contains(nodeEndpoint)) {
                log.trace("Failed node already in the list of unresponsive nodes: {}", unresponsiveNodes);
                continue;
            }

            node.getFileSystem()
                    .map(FileSystemStats::getPartitionAttributeStats)
                    .filter(PartitionAttributeStats::isReadOnly)
                    .map(attr -> new NodeRankByPartitionAttributes(localEndpoint, attr))
                    .ifPresent(set::add);
        }

        return Optional.ofNullable(set.pollLast());
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
