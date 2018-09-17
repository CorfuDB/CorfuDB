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
 * DEPLOY: deploys a service representing a collection nodes using the provided configuration in {@link ServiceParams}
 * STOP: stops a service gracefully within the provided timeout
 * KILL: kills a service immediately
 */
public interface Service {

    /**
     * Deploy the service into cluster. Note that deploy creates a new immutable instance of service. In other words,
     * changing the state of service will lead to creation of a new instance of {@link Service}
     *
     * @return new instance of deployed service
     */
    Service deploy();

    /**
     * Stop the service by stopping all individual nodes of the service. The must happend within within the limit of
     * provided timeout.
     *
     * @param timeout allowed time to gracefully stop the service
     */
    void stop(Duration timeout);

    /**
     * Kill the service immediately by killing all the nodes of the service.
     */
    void kill();

    <T extends NodeParams> Service add(T nodeParams);

    /**
     * Provides {@link ServiceParams} used for configuring a {@link Service}
     *
     * @return a Service parameters
     */
    <T extends NodeParams> ServiceParams<T> getParams();

    /**
     * Provide the nodes that the {@link Service} is composed of.
     *
     * @return an {@link ImmutableList} of {@link Node}s.
     */
    ImmutableMap<String, ? extends Node> nodes();

    <T extends Node> T getNode(String nodeName);

    @AllArgsConstructor
    @Builder
    @EqualsAndHashCode
    class ServiceParams<T extends NodeParams> {
        @Getter
        private final String name;
        @Default
        private final List<T> nodes = new ArrayList<>();
        @Getter
        private final NodeType nodeType;

        public ImmutableList<T> getNodeParams(){
            return ImmutableList.copyOf(nodes);
        }

        public synchronized <R extends NodeParams> ServiceParams<T> add(R nodeParams){
            nodes.add(ClassUtils.cast(nodeParams));
            return this;
        }
    }
}
