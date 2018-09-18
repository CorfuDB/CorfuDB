package org.corfudb.universe.node;

import org.corfudb.universe.group.Group;
import org.corfudb.universe.universe.Universe;

import java.time.Duration;

/**
 * Represent nodes within {@link Group}s of {@link Universe}
 */
public interface Node {

    /**
     * Deploys a specific node into the {@link Universe}.
     *
     * @return current instance of the {@link Node} with the new state.
     * @throws NodeException thrown when can not deploy {@link Node}
     */
    Node deploy();

    /**
     * Stops a {@link Node} gracefully within the timeout provided to this method.
     *
     * @param timeout a limit within which the method attempts to gracefully stop the {@link Node}.
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
