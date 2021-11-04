package org.corfudb.infrastructure.management;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.protocols.wireprotocol.NodeState;
import org.corfudb.protocols.wireprotocol.failuredetector.FileSystemStats;
import org.corfudb.protocols.wireprotocol.failuredetector.FileSystemStats.ResourceQuotaStats;
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
        //find a node which has no resource quota
        NavigableSet<NodeRankByResourceQuota> set = new TreeSet<>();
        for (NodeState node : clusterState.getNodes().values()) {
            node.getFileSystem().ifPresent(fsStats -> {
                if (!fsStats.getQuota().isExceeded()) {
                    return;
                }

                NodeRankByResourceQuota quota = new NodeRankByResourceQuota(
                        node.getConnectivity().getEndpoint(),
                        fsStats.getQuota()
                );
                set.add(quota);
            });
        }

        return Optional.ofNullable(set.pollLast());
    }

    public Optional<NodeRankByResourceQuota> healedServer(ClusterState clusterState) {

        if (!clusterState.getLocalNode().isPresent()) {
            log.trace("Can't find a node to heal, cluster state doesn't contain local node");
            return Optional.empty();
        }

        NodeState localNodeState = clusterState.getLocalNode().get();

        if (!localNodeState.getFileSystem().isPresent()) {
            log.trace("Can't find a node to heal, missing file system stats");
            return Optional.empty();
        }

        FileSystemStats fileSystem = localNodeState.getFileSystem().get();

        if (fileSystem.getQuota().isExceeded()) {
            log.trace("No node to heal, quota is exceeded");
            return Optional.empty();
        }

        NodeRankByResourceQuota resourceQuota = new NodeRankByResourceQuota(
                clusterState.getLocalEndpoint(),
                fileSystem.getQuota()
        );

        return Optional.of(resourceQuota);
    }
}
