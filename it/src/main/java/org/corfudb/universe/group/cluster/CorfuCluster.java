package org.corfudb.universe.group.cluster;

import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters.CorfuRuntimeParametersBuilder;
import org.corfudb.universe.group.Group.GroupParams;
import org.corfudb.universe.node.Node;
import org.corfudb.universe.node.NodeException;
import org.corfudb.universe.node.client.LocalCorfuClient;
import org.corfudb.universe.node.server.CorfuServer;

/**
 * Provides a Corfu specific cluster of servers
 */
public interface CorfuCluster<T extends Node, G extends GroupParams> extends Cluster<T, G> {

    /**
     * Provides a corfu client running on local machine
     *
     * @return local corfu client
     */
    LocalCorfuClient getLocalCorfuClient();

    /**
     * Provides a corfu client running on local machine with the given metrics port open
     *
     * @return local corfu client
     */
    LocalCorfuClient getLocalCorfuClient(CorfuRuntimeParametersBuilder runtimeParametersBuilder);

    /**
     * Find a corfu server by index in the cluster:
     * - get all corfu servers in the cluster
     * - skip particular number of servers in the cluster according to index offset
     * - extract first server from the list
     *
     * @param index corfu server position
     * @return a corfu server
     */
    default CorfuServer getServerByIndex(int index) {
        return nodes()
                .values()
                .stream()
                .skip(index)
                .findFirst()
                .map(CorfuServer.class::cast)
                .orElseThrow(() -> new NodeException("Corfu server not found by index: " + index));
    }

    /**
     * To find the first corfu server in a cluster:
     * - get all corfu servers in the cluster
     * - extract first element in the list
     *
     * @return the first corfu server
     */
    default CorfuServer getFirstServer() {
        return getServerByIndex(0);
    }
}
