package org.corfudb.universe.cluster.docker;

import com.google.common.collect.ImmutableMap;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.messages.NetworkConfig;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.universe.cluster.Cluster;
import org.corfudb.universe.cluster.ClusterException;
import org.corfudb.universe.service.DockerGroup;
import org.corfudb.universe.service.Group;
import org.corfudb.universe.service.Group.GroupParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.corfudb.universe.node.CorfuServer.ServerParams;

/**
 * Represents Docker implementation of a {@link Cluster}.
 */
@Slf4j
public class DockerCluster implements Cluster {
    /**
     * Docker parameter --network=host doesn't work in mac machines,
     * FakeDns is used to solve the issue, it resolves a dns record (which is a node name) to loopback interface always.
     * See Readme.md
     */
    private static final FakeDns FAKE_DNS = FakeDns.getInstance().install();

    private final AtomicReference<ClusterParams> clusterParams = new AtomicReference<>();

    private final DockerClient docker;
    private final DockerNetwork network = new DockerNetwork();

    private final ConcurrentMap<String, Group> services = new ConcurrentHashMap<>();
    private final String clusterId;
    private final AtomicBoolean initialized = new AtomicBoolean();

    @Builder
    public DockerCluster(ClusterParams clusterParams, DockerClient docker) {
        this.clusterParams.set(clusterParams);
        this.docker = docker;
        this.clusterId = UUID.randomUUID().toString();

        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
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

        if (!initialized.get()) {
            network.setup();
            initialized.set(true);
        }

        Map<String, Group> servicesSnapshot = createAndDeployServices();
        services.putAll(servicesSnapshot);

        return this;
    }

    @Override
    public void shutdown() {
        log.info("Shutdown docker cluster: {}", clusterId);

        // gracefully stop all services
        services.values().forEach(service -> {
            try {
                service.stop(clusterParams.get().getTimeout());
            } catch (Exception ex) {
                log.info("Can't stop service: {}", service.getParams().getName());
            }
        });

        // Kill all docker containers
        clusterParams.get().getServices().keySet().forEach(serviceName -> {
            GroupParams<ServerParams> groupParams = clusterParams.get().getServiceParams(serviceName);

            groupParams.getNodeParams().forEach(serverParams -> {
                try {
                    docker.killContainer(serverParams.getName());
                } catch (Exception e) {
                    log.debug(
                            "Can't kill container. In general it should be killed already. Container: {}",
                            serverParams.getName()
                    );
                }
            });

        });

        try {
            network.shutdown();
        } catch (ClusterException e) {
            log.debug("Can't stop docker network. Network name: {}", clusterParams.get().getNetworkName());
        }
    }

    @Override
    public <T extends GroupParams<?>> Cluster add(T serviceParams) {
        clusterParams.get().add(serviceParams);
        deployDockerService(serviceParams);
        return this;
    }

    @Override
    public ClusterParams getClusterParams() {
        return clusterParams.get();
    }

    @Override
    public ImmutableMap<String, Group> services() {
        return ImmutableMap.copyOf(services);
    }


    @Override
    public Group getService(String serviceName) {
        return services.get(serviceName);
    }

    private Map<String, Group> createAndDeployServices() {
        Map<String, Group> servicesSnapshot = new HashMap<>();

        ClusterParams clusterConfig = clusterParams.get();
        for (String serviceName : clusterConfig.getServices().keySet()) {
            DockerGroup service = deployDockerService(clusterConfig.getServiceParams(serviceName));

            servicesSnapshot.put(serviceName, service);
        }

        return servicesSnapshot;
    }

    private DockerGroup deployDockerService(GroupParams<?> groupParams) {
        switch (groupParams.getNodeType()) {
            case CORFU_SERVER:
                groupParams.getNodeParams().forEach(node ->
                        FAKE_DNS.addForwardResolution(node.getName(), InetAddress.getLoopbackAddress())
                );

                DockerGroup service = DockerGroup.builder()
                        .clusterParams(clusterParams.get())
                        .params(groupParams)
                        .docker(docker)
                        .build();

                service.deploy();
                return service;
            case CORFU_CLIENT:
                throw new ClusterException("Not implemented corfu client. Group config: " + groupParams);
            default:
                throw new ClusterException("Unknown node type");
        }
    }

    private class DockerNetwork {
        private final Logger log = LoggerFactory.getLogger(DockerNetwork.class);

        /**
         * Sets up a docker network.
         *
         * @throws ClusterException will be thrown if cannot set up a docker network
         */
        void setup() {
            String networkName = clusterParams.get().getNetworkName();
            log.info("Setup network: {}", networkName);
            NetworkConfig networkConfig = NetworkConfig.builder()
                    .checkDuplicate(true)
                    .attachable(true)
                    .name(networkName)
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
            String networkName = clusterParams.get().getNetworkName();
            log.info("Shutdown network: {}", networkName);
            try {
                docker.removeNetwork(networkName);
            } catch (Exception e) {
                final String err = String.format("Cannot shutdown docker network: %s.", networkName);
                throw new ClusterException(err, e);
            }
        }
    }
}
