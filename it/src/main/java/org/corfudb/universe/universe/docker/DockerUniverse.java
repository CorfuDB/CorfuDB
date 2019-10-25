package org.corfudb.universe.universe.docker;

import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.messages.NetworkConfig;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.util.ClassUtils;
import org.corfudb.universe.group.Group.GroupParams;
import org.corfudb.universe.group.cluster.Cluster;
import org.corfudb.universe.group.cluster.docker.DockerCorfuCluster;
import org.corfudb.universe.group.cluster.docker.DockerSupportCluster;
import org.corfudb.universe.logging.LoggingParams;
import org.corfudb.universe.node.Node.NodeParams;
import org.corfudb.universe.universe.AbstractUniverse;
import org.corfudb.universe.universe.Universe;
import org.corfudb.universe.universe.UniverseException;
import org.corfudb.universe.universe.UniverseParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Represents Docker implementation of a {@link Universe}.
 */
@Slf4j
public class DockerUniverse extends AbstractUniverse<NodeParams, UniverseParams> {
    /**
     * Docker parameter --network=host doesn't work in mac machines,
     * FakeDns is used to solve the issue, it resolves a dns record (which is a node name) to loopback address always.
     * See Readme.md
     */
    private static final FakeDns FAKE_DNS = FakeDns.getInstance().install();
    private final DockerClient docker;
    private final DockerNetwork network = new DockerNetwork();
    private final AtomicBoolean initialized = new AtomicBoolean();
    private final LoggingParams loggingParams;
    private final AtomicBoolean destroyed = new AtomicBoolean();

    @Builder
    public DockerUniverse(UniverseParams universeParams, DockerClient docker, LoggingParams loggingParams) {
        super(universeParams);
        this.docker = docker;
        this.loggingParams = loggingParams;

        init();
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

        deployGroups();

        return this;
    }

    @Override
    public void shutdown() {
        log.info("Shutdown docker universe: {}", universeId.toString());

        if (!universeParams.isCleanUpEnabled()) {
            log.info("Shutdown is disabled");
            return;
        }

        if (destroyed.getAndSet(true)) {
            log.warn("Docker universe already destroyed");
            return;
        }

        shutdownGroups();

        // Remove docker network
        try {
            network.shutdown();
        } catch (UniverseException e) {
            log.debug("Can't remove docker network: {}", universeParams.getNetworkName());
        }
    }

    @Override
    public Universe add(GroupParams groupParams) {
        universeParams.add(groupParams);
        buildGroup(groupParams).deploy();
        return this;
    }

    @Override
    protected Cluster buildGroup(GroupParams<NodeParams> groupParams) {

        switch (groupParams.getType()) {

            case CORFU_CLUSTER:
                groupParams.getNodesParams().forEach(node ->
                        FAKE_DNS.addForwardResolution(node.getName(), InetAddress.getLoopbackAddress())
                );

                return DockerCorfuCluster.builder()
                        .universeParams(universeParams)
                        .params(ClassUtils.cast(groupParams))
                        .loggingParams(loggingParams)
                        .docker(docker)
                        .build();
            case SUPPORT_CLUSTER:
                groupParams.getNodesParams().forEach(node ->
                        FAKE_DNS.addForwardResolution(node.getName(), InetAddress.getLoopbackAddress())
                );

                return DockerSupportCluster.builder()
                        .universeParams(universeParams)
                        .supportParams(ClassUtils.cast(groupParams))
                        .loggingParams(loggingParams)
                        .docker(docker)
                        .build();
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
            String networkName = universeParams.getNetworkName();
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
            String networkName = universeParams.getNetworkName();
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
