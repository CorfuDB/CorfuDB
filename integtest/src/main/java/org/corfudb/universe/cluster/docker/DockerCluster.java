package org.corfudb.universe.cluster.docker;

import com.google.common.collect.ImmutableList;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.exceptions.DockerException;
import com.spotify.docker.client.messages.NetworkConfig;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.universe.cluster.Cluster;
import org.corfudb.universe.cluster.ClusterException;
import org.corfudb.universe.service.DockerService;
import org.corfudb.universe.service.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static lombok.Builder.Default;

/**
 * Represents Docker implementation of a {@link Cluster}.
 */
@Slf4j
@Builder
@AllArgsConstructor
public class DockerCluster implements Cluster {

    @Getter
    private final ClusterParams clusterParams;
    private final DockerClient docker;
    private final DockerNetwork network = new DockerNetwork();
    @Default
    private final ImmutableList<Service> services = ImmutableList.of();

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
    public DockerCluster deploy() throws ClusterException {
        network.setup();

        List<Service> servicesSnapshot = createAndDeployServices();

        return DockerCluster.builder()
                .clusterParams(clusterParams)
                .docker(docker)
                .services(ImmutableList.copyOf(servicesSnapshot))
                .build();
    }

    @Override
    public void shutdown() throws ClusterException {
        services.forEach(service -> service.stop(clusterParams.getTimeout()));
        network.shutdown();
    }

    @Override
    public ImmutableList<Service> getServices() {
        return services;
    }

    private List<Service> createAndDeployServices() {
        List<Service> servicesSnapshot = new ArrayList<>();

        for (String serviceName : clusterParams.getServices().keySet()) {
            DockerService service = DockerService.builder()
                    .clusterParams(clusterParams)
                    .params(clusterParams.getService(serviceName))
                    .docker(docker)
                    .build();

            service = service.deploy();

            servicesSnapshot.add(service);
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
        void setup() throws ClusterException {
            log.info("Setup network: {}", clusterParams.getNetworkName());
            NetworkConfig networkConfig = NetworkConfig.builder()
                    .checkDuplicate(true)
                    .attachable(true)
                    .name(clusterParams.getNetworkName())
                    .build();

            try {
                docker.createNetwork(networkConfig);
            } catch (DockerException | InterruptedException e) {
                throw new ClusterException("Cannot setup docker network.", e);
            }
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    shutdown();
                } catch (ClusterException e) {
                    log.debug("Can't shutdown network: {}.", clusterParams.getNetworkName());
                }
            }));
        }

        /**
         * Shuts down a docker network.
         *
         * @throws ClusterException will be thrown if cannot shut up a docker network
         */
        void shutdown() throws ClusterException {
            log.info("Shutdown network: {}", clusterParams.getNetworkName());
            try {
                docker.removeNetwork(clusterParams.getNetworkName());
            } catch (DockerException | InterruptedException e) {
                throw new ClusterException(
                        String.format("Cannot shutdown docker network: %s.", clusterParams.getNetworkName()),
                        e
                );
            }
        }
    }
}
