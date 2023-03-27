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
                    .map(fsStats -> new NodeRankByPartitionAttributes(nodeEndpoint, fsStats))
                    .ifPresent(set::add);
        }

        return Optional.ofNullable(set.pollLast());
    }

    public Optional<NodeRankByPartitionAttributes> healedServer(ClusterState clusterState) {
        ImmutableList<String> unresponsiveNodes = clusterState.getUnresponsiveNodes();
        log.info("Cluster state: {}", clusterState);
        log.info("clusterState.getLocalEndpoint() is {} but unresponsive nodes: {}", clusterState.getLocalEndpoint(), unresponsiveNodes);
        if (unresponsiveNodes.contains(clusterState.getLocalEndpoint())) {
            Optional<FileSystemStats> maybeFileSystem = getFileSystemStats(clusterState);

            return maybeFileSystem
                    .filter(fsStats -> fsStats.getPartitionAttributeStats().isWritable())
                    // !!! We DO NOT check that, the batch processor is in OK status, otherwise we will NEVER be able to
                    // heal the node, because the batch processor must be in the ERROR state until "reset" operation
                    // will be triggered by the "heal" step
                    //.filter(fsStats -> fsStats.getBatchProcessorStats().isOk())
                    .map(fileSystem -> new NodeRankByPartitionAttributes(clusterState.getLocalEndpoint(), fileSystem));
        } else {
            return Optional.empty();
        }
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
