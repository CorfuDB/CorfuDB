package org.corfudb.universe.universe.docker;

import com.google.common.collect.ImmutableMap;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.messages.NetworkConfig;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.universe.group.DockerCorfuCluster;
import org.corfudb.universe.group.Group;
import org.corfudb.universe.group.Group.GroupParams;
import org.corfudb.universe.universe.Universe;
import org.corfudb.universe.universe.UniverseException;
import org.corfudb.universe.util.ClassUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Represents Docker implementation of a {@link Universe}.
 */
@Slf4j
public class DockerUniverse implements Universe {
    /**
     * Docker parameter --network=host doesn't work in mac machines,
     * FakeDns is used to solve the issue, it resolves a dns record (which is a node name) to loopback address always.
     * See Readme.md
     */
    private static final FakeDns FAKE_DNS = FakeDns.getInstance().install();

    private final AtomicReference<UniverseParams> universeParams = new AtomicReference<>();

    private final DockerClient docker;
    private final DockerNetwork network = new DockerNetwork();

    private final ConcurrentMap<String, Group> groups = new ConcurrentHashMap<>();
    private final String clusterId;
    private final AtomicBoolean initialized = new AtomicBoolean();

    @Builder
    public DockerUniverse(UniverseParams universeParams, DockerClient docker) {
        this.universeParams.set(universeParams);
        this.docker = docker;
        this.clusterId = UUID.randomUUID().toString();

        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
    }


    /**
     * Deploy a {@link Universe} according to provided parameter, docker client, docker network, and other components.
     * The instances of this class are immutable. In other word, when the state of an instance is changed a new
     * immutable instance is provided.
     *
     * @return Current instance of a docker {@link Universe} would be returned.
     * @throws UniverseException this exception will be thrown if deploying a {@link Universe} is not successful
     */
    @Override
    public DockerUniverse deploy() {
        log.info("Deploying universe: {}", universeParams);

        if (!initialized.get()) {
            network.setup();
            initialized.set(true);
        }

        createAndDeployGroups();

        return this;
    }

    @Override
    public void shutdown() {
        log.info("Shutdown docker universe: {}", clusterId);

        // gracefully stop all groups
        groups.values().forEach(group -> {
            try {
                group.stop(universeParams.get().getTimeout());
            } catch (Exception ex) {
                log.info("Can't stop group: {}", group.getParams().getName());
            }
        });

        // Kill all docker containers
        universeParams.get().getGroups().keySet().forEach(groupName -> {
            GroupParams groupParams = universeParams.get().getGroupParams(groupName, GroupParams.class);

            groupParams.getNodesParams().forEach(serverParams -> {
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
        } catch (UniverseException e) {
            log.debug("Can't stop docker network. Network name: {}", universeParams.get().getNetworkName());
        }
    }

    @Override
    public Universe add(GroupParams groupParams) {
        universeParams.get().add(groupParams);
        deployDockerCluster(groupParams);
        return this;
    }

    @Override
    public UniverseParams getUniverseParams() {
        return universeParams.get();
    }

    @Override
    public ImmutableMap<String, Group> groups() {
        return ImmutableMap.copyOf(groups);
    }

    @Override
    public <T extends Group> T getGroup(String groupName) {
        return ClassUtils.cast(groups.get(groupName));
    }

    private void createAndDeployGroups() {
        UniverseParams clusterConfig = universeParams.get();
        for (String groupName : clusterConfig.getGroups().keySet()) {
            deployDockerCluster(clusterConfig.getGroupParams(groupName, GroupParams.class));
        }
    }

    private void deployDockerCluster(GroupParams groupParams) {
        switch (groupParams.getNodeType()) {
            case CORFU_SERVER:
                groupParams.getNodesParams().forEach(node ->
                        FAKE_DNS.addForwardResolution(node.getName(), InetAddress.getLoopbackAddress())
                );

                DockerCorfuCluster cluster = DockerCorfuCluster.builder()
                        .universeParams(universeParams.get())
                        .params(ClassUtils.cast(groupParams))
                        .docker(docker)
                        .build();

                cluster.deploy();

                groups.put(groupParams.getName(), cluster);
                break;
            case CORFU_CLIENT:
                throw new UniverseException("Not implemented corfu client. Group config: " + groupParams);
            default:
                throw new UniverseException("Unknown node type");
        }
    }

    private class DockerNetwork {
        private final Logger log = LoggerFactory.getLogger(DockerNetwork.class);

        /**
         * Sets up a docker network.
         *
         * @throws UniverseException will be thrown if cannot set up a docker network
         */
        void setup() {
            String networkName = universeParams.get().getNetworkName();
            log.info("Setup network: {}", networkName);
            NetworkConfig networkConfig = NetworkConfig.builder()
                    .checkDuplicate(true)
                    .attachable(true)
                    .name(networkName)
                    .build();

            try {
                docker.createNetwork(networkConfig);
            } catch (Exception e) {
                throw new UniverseException("Cannot setup docker network.", e);
            }
        }

        /**
         * Shuts down a docker network.
         *
         * @throws UniverseException will be thrown if cannot shut up a docker network
         */
        void shutdown() {
            String networkName = universeParams.get().getNetworkName();
            log.info("Shutdown network: {}", networkName);
            try {
                docker.removeNetwork(networkName);
            } catch (Exception e) {
                final String err = String.format("Cannot shutdown docker network: %s.", networkName);
                throw new UniverseException(err, e);
            }
        }
    }
}
