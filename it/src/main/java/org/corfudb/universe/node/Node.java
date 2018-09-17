package org.corfudb.universe.node;

import java.time.Duration;

/**
 * Represent nodes within {@link org.corfudb.universe.service.Service}s of {@link org.corfudb.universe.cluster.Cluster}
 */
public interface Node {

    /**
     * Deploys a specific node into the cluster. Note that Node is immutable and changes to state will lead to returning
     * a new immutable instance of the node.
     *
     * @return a new instance of node with the new state.
     * @throws NodeException thrown when can not deploy {@link Node}
     */
    Node deploy();

    /**
     * Stops a {@link Node} gracefully within the timeout provided to this method.
     *
     * @param timeout a limit within which the method attempts to gracefully stop the node.
     * @throws NodeException thrown in case of unsuccessful stop.
     */
    void stop(Duration timeout);

    /**
     * Kills a {@link Node} immediately.
     *
     * @throws NodeException thrown in case of unsuccessful kill.
     */
    void kill();

    NodeParams getParams();

    /**
     * Common interface for the configurations of different implementation of {@link Node}.
     */
    interface NodeParams {
        String getName();
        NodeType getNodeType();
    }

    enum NodeType {
        CORFU_SERVER, CORFU_CLIENT
    }
}
