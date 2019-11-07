package org.corfudb.universe.node.server.docker;

import static com.spotify.docker.client.DockerClient.LogsParam;

import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.DockerClient.ListImagesParam;
import com.spotify.docker.client.LogStream;
import com.spotify.docker.client.exceptions.DockerException;
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.ContainerCreation;
import com.spotify.docker.client.messages.ContainerInfo;
import com.spotify.docker.client.messages.HostConfig;
import com.spotify.docker.client.messages.Image;
import com.spotify.docker.client.messages.PortBinding;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.corfudb.universe.group.cluster.CorfuClusterParams;
import org.corfudb.universe.logging.LoggingParams;
import org.corfudb.universe.node.Node;
import org.corfudb.universe.node.NodeException;
import org.corfudb.universe.node.server.AbstractCorfuServer;
import org.corfudb.universe.node.server.CorfuServer;
import org.corfudb.universe.node.server.CorfuServerParams;
import org.corfudb.universe.universe.UniverseParams;
import org.corfudb.universe.util.DockerManager;
import org.corfudb.universe.util.IpTablesUtil;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Implements a docker instance representing a {@link CorfuServer}.
 */
@Slf4j
public class DockerCorfuServer extends AbstractCorfuServer<CorfuServerParams, UniverseParams> {
    private static final String ALL_NETWORK_INTERFACES = "0.0.0.0";

    @NonNull
    private final DockerClient docker;

    @NonNull
    private final DockerManager dockerManager;

    @NonNull
    private final LoggingParams loggingParams;
    @NonNull
    private final CorfuClusterParams clusterParams;
    private final AtomicReference<String> ipAddress = new AtomicReference<>();
    private final AtomicBoolean destroyed = new AtomicBoolean();

    @Builder
    public DockerCorfuServer(
            DockerClient docker, CorfuServerParams params, UniverseParams universeParams,
            CorfuClusterParams clusterParams, LoggingParams loggingParams,
            DockerManager dockerManager) {
        super(params, universeParams);
        this.docker = docker;
        this.loggingParams = loggingParams;
        this.clusterParams = clusterParams;
        this.dockerManager = dockerManager;
    }

    /**
     * Deploys a Corfu server / docker container
     */
    @Override
    public DockerCorfuServer deploy() {
        log.info("Deploying the Corfu server. Docker container: {}", params.getName());

        deployContainer();

        return this;
    }

    /**
     * This method attempts to gracefully stop the Corfu server. After timeout, it will kill the Corfu server.
     *
     * @param timeout a duration after which the stop will kill the server
     * @throws NodeException this exception will be thrown if the server cannot be stopped.
     */
    @Override
    public void stop(Duration timeout) {
        log.info("Stopping the Corfu server. Docker container: {}", params.getName());
        dockerManager.stop(params.getName(), timeout);
    }

    /**
     * Immediately kill the Corfu server.
     *
     * @throws NodeException this exception will be thrown if the server can not be killed.
     */
    @Override
    public void kill() {
        log.info("Killing the Corfu server. Docker container: {}", params.getName());
        dockerManager.kill(params.getName());
    }

    /**
     * Immediately kill and remove the docker container
     *
     * @throws NodeException this exception will be thrown if the server can not be killed.
     */
    @Override
    public void destroy() {
        log.info("Destroying the Corfu server. Docker container: {}", params.getName());

        if (destroyed.getAndSet(true)) {
            log.debug("Already destroyed: {}", params.getName());
            return;
        }

        collectLogs();
        dockerManager.destroy(params.getName());
    }

    /**
     * Symmetrically disconnect the server from the cluster.
     * This partitions the container from all the others.
     * The test runtime can still connect to this server.
     *
     * @throws NodeException this exception will be thrown if the server can not be disconnected
     */
    @Override
    public void disconnect() {
        log.info("Disconnecting the docker server: {} from the cluster ", params.getName());

        clusterParams.getNodesParams()
                .stream()
                .filter(neighbourServer -> !neighbourServer.equals(params))
                .forEach(neighbourServer -> {
                    try {
                        ContainerInfo server = docker.inspectContainer(neighbourServer.getName());
                        String neighbourIp = server.networkSettings().networks().values().asList().get(0).ipAddress();

                        if (StringUtils.isEmpty(neighbourIp)) {
                            throw new NodeException("Empty ip address. Container: " + neighbourServer.getName());
                        }

                        // iptables -A INPUT -s $neighbourIp -j DROP
                        dockerManager.execCommand(params.getName(),
                                "iptables", "-A", "INPUT", "-s", neighbourIp, "-j", "DROP");
                        // iptables -A OUTPUT -d $neighbourIp -j DROP
                        dockerManager.execCommand(params.getName(),
                                "iptables", "-A", "OUTPUT", "-d", neighbourIp, "-j", "DROP");
                    } catch (DockerException | InterruptedException ex) {
                        List<String> clusterNodes = clusterParams.getClusterNodes();
                        throw new NodeException("Can't disconnect container: " + params.getName() +
                                " from docker network. Corfu cluster: " + clusterNodes, ex);
                    }
                });
    }

    /**
     * Symmetrically disconnect a server from a list of other servers,
     * which creates a partial partition.
     *
     * @param servers List of servers to disconnect from.
     * @throws NodeException this exception will be thrown if the server can not be disconnected
     */
    @Override
    public void disconnect(List<CorfuServer> servers) {
        log.info("Disconnecting the docker server: {} from specified servers: {}",
                params.getName(), servers);

        servers.stream()
                .filter(neighbourServer -> !neighbourServer.getParams().equals(params))
                .forEach(neighbourServer -> {
                    try {
                        dockerManager.execCommand(params.getName(),
                                IpTablesUtil.dropInput(neighbourServer.getIpAddress()));
                        dockerManager.execCommand(params.getName(),
                                IpTablesUtil.dropOutput(neighbourServer.getIpAddress()));
                    } catch (DockerException | InterruptedException ex) {
                        throw new NodeException("Can't disconnect container: " + params.getName() +
                                " from server: " + neighbourServer.getParams().getName(), ex);
                    }
                });
    }

    /**
     * Pause the container from docker network
     *
     * @throws NodeException this exception will be thrown if the server can not be paused
     */
    @Override
    public void pause() {
        log.info("Pausing the Corfu server: {}", params.getName());
        dockerManager.pause(params.getName());
    }

    /**
     * Start a {@link Node}
     *
     * @throws NodeException this exception will be thrown if the server can not be started
     */
    @Override
    public void start() {
        log.info("Starting the corfu server: {}", params.getName());
        dockerManager.start(params.getName());
    }

    /**
     * Restart a {@link Node}
     *
     * @throws NodeException this exception will be thrown if the server can not be restarted
     */
    @Override
    public void restart() {
        log.info("Restarting the corfu server: {}", params.getName());
        dockerManager.restart(params.getName());
    }

    /**
     * Reconnect a server to the cluster
     *
     * @throws NodeException this exception will be thrown if the node can not be reconnected
     */
    @Override
    public void reconnect() {
        log.info("Reconnecting the docker server: {} to the network.", params.getName());

        try {
            dockerManager.execCommand(params.getName(), IpTablesUtil.cleanInput());
            dockerManager.execCommand(params.getName(), IpTablesUtil.cleanOutput());
        } catch (DockerException | InterruptedException e) {
            throw new NodeException("Can't reconnect container to docker network " + params.getName(), e);
        }
    }

    /**
     * Reconnect a server to a list of servers.
     *
     * @param servers List of servers to reconnect.
     */
    @Override
    public void reconnect(List<CorfuServer> servers) {
        log.info("Reconnecting the docker server: {} to specified servers: {}",
                params.getName(), servers);

        servers.stream()
                .filter(neighbourServer -> !neighbourServer.getParams().equals(params))
                .forEach(neighbourServer -> {
                    try {
                        dockerManager.execCommand(params.getName(),
                                IpTablesUtil.revertDropInput(neighbourServer.getIpAddress()));
                        dockerManager.execCommand(params.getName(),
                                IpTablesUtil.revertDropOutput(neighbourServer.getIpAddress()));
                    } catch (DockerException | InterruptedException ex) {
                        throw new NodeException("Can't reconnect container: " + params.getName() +
                                " to server: " + neighbourServer.getParams().getName(), ex);
                    }
                });
    }

    /**
     * Resume a {@link CorfuServer}
     *
     * @throws NodeException this exception will be thrown if the node can not be resumed
     */
    @Override
    public void resume() {
        log.info("Resuming the corfu server: {}", params.getName());
        dockerManager.resume(params.getName());
    }

    @Override
    public String getIpAddress() {
        return ipAddress.get();
    }

    /**
     * Deploy and start docker container, expose ports, connect to a network
     *
     * @return docker container id
     */
    private String deployContainer() {
        ContainerConfig containerConfig = buildContainerConfig();

        String id;
        try {
            ListImagesParam corfuImageQuery = ListImagesParam
                    .byName(params.getDockerImageNameFullName());

            List<Image> corfuImages = docker.listImages(corfuImageQuery);
            if (corfuImages.isEmpty()) {
                docker.pull(params.getDockerImageNameFullName());
            }

            ContainerCreation container = docker.createContainer(containerConfig, params.getName());
            id = container.id();

            dockerManager.addShutdownHook(params.getName());

            docker.disconnectFromNetwork(id, "bridge");
            docker.connectToNetwork(id, docker.inspectNetwork(universeParams.getNetworkName()).id());

            dockerManager.start(id);

            String ipAddr = docker.inspectContainer(id)
                    .networkSettings()
                    .networks()
                    .values()
                    .asList()
                    .get(0)
                    .ipAddress();

            if (StringUtils.isEmpty(ipAddr)) {
                throw new NodeException("Empty Ip address for container: " + params.getName());
            }

            ipAddress.set(ipAddr);
        } catch (InterruptedException | DockerException e) {
            throw new NodeException("Can't start a container", e);
        }

        return id;
    }

    private ContainerConfig buildContainerConfig() {
        // Bind ports
        List<String> ports = params.getPorts().stream()
                .map(Objects::toString).collect(Collectors.toList());
        Map<String, List<PortBinding>> portBindings = new HashMap<>();
        for (String port : ports) {
            List<PortBinding> hostPorts = new ArrayList<>();
            hostPorts.add(PortBinding.of(ALL_NETWORK_INTERFACES, port));
            portBindings.put(port, hostPorts);
        }

        HostConfig.Builder hostConfigBuilder = HostConfig.builder();
        params.getContainerResources()
                .ifPresent(limits -> hostConfigBuilder.memory(limits.getMemory()));

        HostConfig hostConfig = hostConfigBuilder
                .privileged(true)
                .portBindings(portBindings)
                .build();

        // Compose command line for starting Corfu
        String cmdLine = new StringBuilder()
                .append("mkdir -p " + params.getStreamLogDir())
                .append(" && ")
                .append("java -cp *.jar ")
                .append(org.corfudb.infrastructure.CorfuServer.class.getCanonicalName())
                .append(" ")
                .append(getCommandLineParams())
                .toString();

        return ContainerConfig.builder()
                .hostConfig(hostConfig)
                .image(params.getDockerImageNameFullName())
                .hostname(params.getName())
                .exposedPorts(ports.toArray(new String[0]))
                .cmd("sh", "-c", cmdLine)
                .build();
    }

    /**
     * Collect logs from container and write to the log directory
     */
    private void collectLogs() {
        if (!loggingParams.isEnabled()) {
            log.debug("Logging is disabled");
            return;
        }

        File serverLogDir = loggingParams.getServerLogDir().toFile();
        if (!serverLogDir.exists() && serverLogDir.mkdirs()) {
            log.info("Created new corfu log directory at {}.", serverLogDir);
        }

        log.debug("Collect logs for: {}", params.getName());

        try (LogStream stream = docker.logs(params.getName(), LogsParam.stdout(), LogsParam.stderr())) {
            String logs = stream.readFully();

            if (StringUtils.isEmpty(logs)) {
                log.warn("Empty logs from container: {}", params.getName());
            }

            Path filePathObj = loggingParams.getServerLogDir().resolve(params.getName() + ".log");
            Files.write(filePathObj, logs.getBytes(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (InterruptedException | DockerException | IOException e) {
            log.error("Can't collect logs from container: {}", params.getName(), e);
        }
    }

    @Override
    public String getNetworkInterface() {
        return params.getName();
    }
}
