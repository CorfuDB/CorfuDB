package org.corfudb.universe.group;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.corfudb.universe.node.Node;
import org.corfudb.universe.node.Node.NodeType;
import org.corfudb.universe.universe.Universe;

import java.time.Duration;

import static org.corfudb.universe.node.Node.NodeParams;

/**
 * This provides an abstraction for a group of {@link Node}s that come together to provide a logical service.
 * <p>
 * The following are the main functionalities provided by this class:
 * <p>
 * DEPLOY: deploys a {@link Group} representing a collection of {@link Node}-s using
 * the provided configuration in {@link GroupParams}
 * STOP: stops a {@link Group} gracefully within the provided timeout
 * KILL: kills a {@link Group} immediately
 */
public interface Group {

    /**
     * Deploy the {@link Group} into the {@link Universe}.
     *
     * @return current instance of deployed {@link Group}
     */
    Group deploy();

    /**
     * Stop the {@link Group} by stopping all individual {@link Node}-s of the group.
     * Must happened within the limit of provided timeout.
     *
     * @param timeout allowed time to gracefully stop the {@link Group}
     */
    void stop(Duration timeout);

    /**
     * Kill the {@link Group} immediately by killing all the {@link Node}-s of the group.
     * Kill - means stop or interrupt immediately
     */
    void kill();

    /**
     * Destroy the {@link Group} immediately by destroying all the {@link Node}-s of the group.
     * Destroy - means kill and clean up the node directory which contains the application itself and could contain
     * config files, db files and so on.
     */
    void destroy();

    Node add(NodeParams nodeParams);

    /**
     * Provides {@link GroupParams} used for configuring a {@link Group}
     *
     * @return a Group parameters
     */
    <T extends GroupParams> T getParams();

    /**
     * Provide the nodes that the {@link Group} is composed of.
     *
     * @return an {@link ImmutableList} of {@link Node}s.
     */
    <T extends Node> ImmutableMap<String, T> nodes();

    <T extends Node> T getNode(String nodeName);

    interface GroupParams {
        String getName();

        <T extends NodeParams> ImmutableList<T> getNodesParams();

        NodeType getNodeType();
    }
}
