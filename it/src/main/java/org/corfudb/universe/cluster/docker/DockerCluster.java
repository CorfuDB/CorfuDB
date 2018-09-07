package org.corfudb.universe.cluster.docker;

import com.google.common.collect.ImmutableMap;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.messages.NetworkConfig;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.universe.cluster.Cluster;
import org.corfudb.universe.cluster.ClusterException;
import org.corfudb.universe.service.DockerService;
import org.corfudb.universe.service.Service;
import org.corfudb.universe.service.Service.ServiceParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;

import static org.corfudb.universe.node.CorfuServer.ServerParams;

/**
 * Represents Docker implementation of a {@link Cluster}.
 */
@Slf4j
public class DockerCluster implements Cluster {
    private static final FakeDns FAKE_DNS = FakeDns.getInstance().install();

    @Getter
    private final ClusterParams clusterParams;

    private final DockerClient docker;
    private final DockerNetwork network = new DockerNetwork();

    private final ImmutableMap<String, Service> services;
    private final Optional<String> clusterId;
    private final Thread shutdownHook;

    @Builder
    public DockerCluster(ClusterParams clusterParams, DockerClient docker, ImmutableMap<String, Service> services,
                         Optional<String> clusterId) {
        this.clusterParams = clusterParams;
        this.docker = docker;
        this.services = services;
        this.clusterId = clusterId;

        shutdownHook = new Thread(this::shutdown);
        Runtime.getRuntime().addShutdownHook(shutdownHook);
    }


    /**
     * Deploy a cluster according to provided cluster parameter, docker client, docker network, and other cluster
     * components.
     * The instances of this class are immutable. In other word, when the state of an instance is changed a new
     * immutable instance is provided.
     *
     * @return New immutable instance of docker cluster would be returned.
     * @throws ClusterException this exception will be thrown if deploying a cluster is not successful
     */
    @Override
    public DockerCluster deploy() {
        log.info("Deploying cluster");

        if (!clusterId.isPresent()) {
            network.setup();
        }

        Map<String, Service> servicesSnapshot = createAndDeployServices();

        Optional<String> newClusterId = clusterId;
        if (!newClusterId.isPresent()) {
            newClusterId = Optional.of(UUID.randomUUID().toString());
        }

        DockerCluster cluster = DockerCluster.builder()
                .clusterParams(clusterParams)
                .docker(docker)
                .services(ImmutableMap.copyOf(servicesSnapshot))
                .clusterId(newClusterId)
                .build();

        Runtime.getRuntime().removeShutdownHook(shutdownHook);

        return cluster;
    }

    @Override
    public void shutdown() {
        log.info("Shutdown docker cluster: {}", clusterId);

        // gracefully stop all services
        services.values().forEach(service -> {
            try {
                service.stop(clusterParams.getTimeout());
            } catch (Exception ex) {
                log.info("Can't stop service: {}", service.getParams().getName());
            }
        });

        // Kill all docker containers
        clusterParams.getServices().keySet().forEach(serviceName -> {
            ServiceParams<ServerParams> serviceParams = clusterParams.getService(serviceName);

            serviceParams.getNodes().forEach(serverParams -> {
                try {
                    docker.killContainer(serverParams.getGenericName());
                } catch (Exception e) {
                    log.debug("Can't kill container. In general it should be killed already. Container: {}",
                            serverParams.getGenericName());
                }
            });

        });

        try {
            network.shutdown();
        } catch (ClusterException e) {
            log.debug("Can't stop docker network. Already stopped");
        }
    }

    @Override
    public ImmutableMap<String, Service> services() {
        return services;
    }

    @Override
    public Service getService(String serviceName) {
        return services.get(serviceName);
    }

    private Map<String, Service> createAndDeployServices() {
        Map<String, Service> servicesSnapshot = new HashMap<>();

        for (String serviceName : clusterParams.getServices().keySet()) {
            DockerService service = DockerService.builder()
                    .clusterParams(clusterParams)
                    .params(clusterParams.getService(serviceName))
                    .docker(docker)
                    .build();

            service.getParams().getNodes().forEach(node ->
                    FAKE_DNS.addForwardResolution(node.getGenericName(), InetAddress.getLoopbackAddress())
            );

            service = service.deploy();

            servicesSnapshot.put(serviceName, service);
        }

        return servicesSnapshot;
    }

    private class DockerNetwork {
        private final Logger log = LoggerFactory.getLogger(DockerNetwork.class);

        /**
         * Sets up a docker network.
         *
         * @throws ClusterException will be thrown if cannot set up a docker network
         */
        void setup() {
            log.info("Setup network: {}", clusterParams.getNetworkName());
            NetworkConfig networkConfig = NetworkConfig.builder()
                    .checkDuplicate(true)
                    .attachable(true)
                    .name(clusterParams.getNetworkName())
                    .build();

            try {
                docker.createNetwork(networkConfig);
            } catch (Exception e) {
                throw new ClusterException("Cannot setup docker network.", e);
            }
        }

        /**
         * Shuts down a docker network.
         *
         * @throws ClusterException will be thrown if cannot shut up a docker network
         */
        void shutdown() {
            log.info("Shutdown network: {}", clusterParams.getNetworkName());
            try {
                docker.removeNetwork(clusterParams.getNetworkName());
            } catch (Exception e) {
                final String err = String.format("Cannot shutdown docker network: %s.", clusterParams.getNetworkName());
                throw new ClusterException(err, e);
            }
        }
    }
}
