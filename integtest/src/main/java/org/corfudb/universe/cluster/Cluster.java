package org.corfudb.universe.cluster;

import com.google.common.collect.ImmutableMap;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.corfudb.universe.node.CorfuClient;
import org.corfudb.universe.node.CorfuServer;
import org.corfudb.universe.node.Node;
import org.corfudb.universe.service.Service;
import org.corfudb.universe.service.Service.ServiceParams;

import java.time.Duration;
import java.util.UUID;

import static lombok.Builder.Default;
import static lombok.EqualsAndHashCode.Exclude;
import static org.corfudb.universe.node.Node.NodeParams;

/**
 * A Cluster represents a common notion of a cluster of nodes. The architecture of a cluster is composed of collections
 * of {@link Service}s and {@link Node}s.
 * Each instance of service in the cluster is composed of a collection of {@link Node}s which are a subset of the
 * entire nodes included in the cluster.
 * Cluster configuration is provided by an instance of {@link ClusterParams}.
 * <p>
 * The followings are the main functionalities provided by this class:
 * DEPLOY: create a cluster according to cluster parameters.
 * SHUTDOWN: shutdown a cluster by stopping the nodes, and shutdown the network.
 */
public interface Cluster {

    /**
     * Create a cluster according to the desired state mentioned by {@link ClusterParams}
     *
     * @return an immutable instance of applied change in cluster
     * @throws ClusterException
     */
    Cluster deploy() throws ClusterException;

    /**
     * Shutdown the entire cluster by shutting down all the services in cluster
     *
     * @throws ClusterException
     */
    void shutdown() throws ClusterException;

    /**
     * Returns an instance of {@link ClusterParams} representing the configuration for the cluster.
     *
     * @return an instance of {@link ClusterParams}
     */
    ClusterParams getClusterParams();

    /**
     * Returns an {@link ImmutableMap} of {@link Service}s contained in the cluster.
     *
     * @return services in the cluster
     */
    ImmutableMap<String, Service> services();

    Service getService(String serviceName);

    @Getter
    @Builder
    @EqualsAndHashCode
    class ClusterParams {
        private static final int TIMEOUT_IN_SECONDS = 5;

        @Default
        private final String networkName = "CorfuNet" + UUID.randomUUID().toString();
        @Default
        private final ImmutableMap<String, ServiceParams<? extends NodeParams>> services = ImmutableMap.of();

        @Exclude
        @Default
        private final Duration timeout = Duration.ofSeconds(TIMEOUT_IN_SECONDS);

        /**
         * Returns the configuration of a particular service at given index
         *
         * @param name service name
         * @param <T>  service type: {@link CorfuServer} / {@link CorfuClient}
         * @return an instance of {@link ServiceParams} representing particular type of service
         */
        public <T extends NodeParams> ServiceParams<T> getService(String name, Class<T> nodeType) {
            return getService(name);
        }

        public <T extends NodeParams> ServiceParams<T> getService(String name) {
            ServiceParams<?> p = services.get(name);
            return (ServiceParams<T>) p;
        }
    }
}