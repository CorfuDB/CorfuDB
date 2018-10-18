package org.corfudb.universe.group.cluster;

import org.corfudb.universe.node.client.LocalCorfuClient;

/**
 * Provides a Corfu specific cluster of servers
 */
public interface CorfuCluster extends Cluster {

    /**
     * Provides a corfu client running on local machine
     * @return local corfu client
     */
    LocalCorfuClient getLocalCorfuClient();

}
