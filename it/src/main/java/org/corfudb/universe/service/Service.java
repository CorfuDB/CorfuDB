package org.corfudb.universe.service;

import com.google.common.collect.ImmutableList;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.corfudb.universe.node.Node;

import java.time.Duration;

import static org.corfudb.universe.node.CorfuServer.ServerParams;
import static org.corfudb.universe.node.Node.NodeParams;

/**
 * This provides an interface as an abstraction for a logical service that groups a list of {@link Node}s of the same
 * type.
 * <p>
 * The followings are the main functionalities provided by this class: *
 * <p>
 * DEPLOY: deploys a service representing a collection nodes using the provided configuration in {@link ServiceParams}
 * STOP: stops a service gracefully within the provided timeout
 * KILL: kills a service immediately
 * UNLINK: unlinks a node from the service by removing it from the collection of nodes
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

    /**
     * Unlink the node from the service by removing the node.
     *
     * @param node
     */
    void unlink(Node node);

    /**
     * Provides {@link ServiceParams} used for configuring a {@link Service}
     *
     * @return a Service parameters
     */
    ServiceParams<ServerParams> getParams();

    /**
     * Provide the nodes that the {@link Service} is composed of.
     *
     * @return an {@link ImmutableList} of {@link Node}s.
     */
    <T extends Node> ImmutableList<T> nodes();

    default <T extends Node> ImmutableList<T> nodes(Class<T> nodeType) {
        return nodes();
    }

    @AllArgsConstructor
    @Getter
    @Builder
    @EqualsAndHashCode
    class ServiceParams<T extends NodeParams> {
        private final String name;
        private final ImmutableList<T> nodes;
    }
}
