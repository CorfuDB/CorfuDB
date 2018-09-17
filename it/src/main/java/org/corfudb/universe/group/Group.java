package org.corfudb.universe.service;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.corfudb.universe.node.Node;
import org.corfudb.universe.node.Node.NodeType;
import org.corfudb.universe.util.ClassUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.corfudb.universe.node.Node.NodeParams;

/**
 * This provides an interface as an abstraction for a logical service that groups a list of {@link Node}s of the same
 * type.
 * <p>
 * The following are the main functionalities provided by this class: *
 * <p>
 * DEPLOY: deploys a group representing a collection nodes using the provided configuration in {@link GroupParams}
 * STOP: stops a group gracefully within the provided timeout
 * KILL: kills a group immediately
 */
public interface Group {

    /**
     * Deploy the group into cluster.
     *
     * @return new instance of deployed service
     */
    Group deploy();

    /**
     * Stop the group by stopping all individual nodes of the group. The must happened within within the limit of
     * provided timeout.
     *
     * @param timeout allowed time to gracefully stop the group
     */
    void stop(Duration timeout);

    /**
     * Kill the group immediately by killing all the nodes of the group.
     */
    void kill();

    <T extends NodeParams> Group add(T nodeParams);

    /**
     * Provides {@link GroupParams} used for configuring a {@link Group}
     *
     * @return a Group parameters
     */
    <T extends NodeParams> GroupParams<T> getParams();

    /**
     * Provide the nodes that the {@link Group} is composed of.
     *
     * @return an {@link ImmutableList} of {@link Node}s.
     */
    ImmutableMap<String, ? extends Node> nodes();

    <T extends Node> T getNode(String nodeName);

    @AllArgsConstructor
    @Builder
    @EqualsAndHashCode
    class GroupParams<T extends NodeParams> {
        @Getter
        private final String name;
        @Default
        private final List<T> nodes = new ArrayList<>();
        @Getter
        private final NodeType nodeType;

        public ImmutableList<T> getNodeParams(){
            return ImmutableList.copyOf(nodes);
        }

        public synchronized <R extends NodeParams> GroupParams<T> add(R nodeParams){
            nodes.add(ClassUtils.cast(nodeParams));
            return this;
        }
    }
}
