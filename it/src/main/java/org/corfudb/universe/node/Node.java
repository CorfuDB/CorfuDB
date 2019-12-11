package org.corfudb.universe.node;

import com.google.common.collect.ComparisonChain;
import org.corfudb.universe.group.Group;
import org.corfudb.universe.universe.Universe;

import java.time.Duration;
import java.util.Set;

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

    /**
     * Destroy a {@link Node} completely.
     *
     * @throws NodeException thrown in case of unsuccessful destroy.
     */
    void destroy();

    NodeParams getParams();

    /**
     * Common interface for the configurations of different implementation of {@link Node}.
     */
    interface NodeParams extends Comparable<NodeParams> {
        String getName();

        default int compareTo(NodeParams other) {
            return ComparisonChain.start()
                    .compare(this.getName(), other.getName())
                    .compare(this.getNodeType(), other.getNodeType())
                    .result();
        }

        Set<Integer> getPorts();

        NodeType getNodeType();
    }

    enum NodeType {
        CORFU_SERVER, CORFU_CLIENT, METRICS_SERVER, SHELL_NODE
    }
}
