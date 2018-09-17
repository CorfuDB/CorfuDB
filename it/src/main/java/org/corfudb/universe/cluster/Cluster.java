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
import org.corfudb.universe.util.ClassUtils;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

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
 * The following are the main functionalities provided by this class:
 * DEPLOY: create a cluster according to cluster parameters.
 * SHUTDOWN: shutdown a cluster.
 * Depending on the underlying deployment this might translate into stopping the nodes and/or shutting down the network.
 */
public interface Cluster {

    /**
     * Create a cluster according to the desired state mentioned by {@link ClusterParams}
     *
     * @return an immutable instance of applied change in cluster
     * @throws ClusterException
     */
    Cluster deploy();

    /**
     * Shutdown the entire cluster by shutting down all the services in cluster
     *
     * @throws ClusterException
     */
    void shutdown();

    <T extends ServiceParams<?>> Cluster add(T serviceParams);

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

    @Builder(toBuilder = true)
    @EqualsAndHashCode
    class ClusterParams {
        private static final int TIMEOUT_IN_SECONDS = 5;
        private static final String NETWORK_PREFIX = "CorfuNet";

        @Getter
        @Default
        private final String networkName = NETWORK_PREFIX + UUID.randomUUID().toString();
        @Default
        private final ConcurrentMap<String, ServiceParams<? extends NodeParams>> services = new ConcurrentHashMap<>();

        /**
         * Cluster timeout to wait until an action is completed.
         * For instance: stop a service, stop a node, shutdown cluster
         */
        @Getter
        @Exclude
        @Default
        private final Duration timeout = Duration.ofSeconds(TIMEOUT_IN_SECONDS);

        /**
         * Returns the configuration of a particular service by the name
         *
         * @param name service name
         * @param <T>  node type: {@link CorfuServer} / {@link CorfuClient}
         * @return an instance of {@link ServiceParams} representing particular type of service
         */
        public <T extends NodeParams> ServiceParams<T> getServiceParams(String name, Class<T> nodeType) {
            return getServiceParams(name);
        }

        public <T extends NodeParams> ServiceParams<T> getServiceParams(String name) {
            ServiceParams<?> p = services.get(name);
            return ClassUtils.cast(p);
        }

        public ClusterParams add(ServiceParams<? extends NodeParams> serviceParams){
            services.put(serviceParams.getName(), serviceParams);
            return this;
        }

        public ImmutableMap<String, ServiceParams<? extends NodeParams>> getServices() {
            return ImmutableMap.copyOf(services);
        }
    }
}