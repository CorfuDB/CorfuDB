package org.corfudb.universe.node.server.docker;

import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.DockerClient.ExecCreateParam;
import com.spotify.docker.client.LogStream;
import com.spotify.docker.client.exceptions.DockerException;
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.ContainerCreation;
import com.spotify.docker.client.messages.ContainerInfo;
import com.spotify.docker.client.messages.ExecCreation;
import com.spotify.docker.client.messages.HostConfig;
import com.spotify.docker.client.messages.IpamConfig;
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
import org.corfudb.universe.node.stress.Stress;
import org.corfudb.universe.universe.UniverseParams;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.spotify.docker.client.DockerClient.LogsParam;

/**
 * Implements a docker instance representing a {@link CorfuServer}.
 */
@Slf4j
public class DockerCorfuServer extends AbstractCorfuServer<CorfuServerParams, UniverseParams> {
    private static final String IMAGE_NAME = "corfu-server:" + getAppVersion();
    private static final String ALL_NETWORK_INTERFACES = "0.0.0.0";

    @NonNull
    private final DockerClient docker;
    @NonNull
    private final LoggingParams loggingParams;
    @NonNull
    private final CorfuClusterParams clusterParams;
    private final AtomicReference<String> ipAddress = new AtomicReference<>();
    private final AtomicBoolean destroyed = new AtomicBoolean();

    @Builder
    public DockerCorfuServer(DockerClient docker, CorfuServerParams params, UniverseParams universeParams,
                             CorfuClusterParams clusterParams, LoggingParams loggingParams) {
        super(params, universeParams);
        this.docker = docker;
        this.loggingParams = loggingParams;
        this.clusterParams = clusterParams;
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

        try {
            ContainerInfo container = docker.inspectContainer(params.getName());
            if (!container.state().running() && !container.state().paused()) {
                log.warn("The container `{}` is already stopped", container.name());
                return;
            }
            docker.stopContainer(params.getName(), (int) timeout.getSeconds());
        } catch (DockerException | InterruptedException e) {
            throw new NodeException("Can't stop Corfu server: " + params.getName(), e);
        }
    }

    /**
     * Immediately kill the Corfu server.
     *
     * @throws NodeException this exception will be thrown if the server can not be killed.
     */
    @Override
    public void kill() {
        log.info("Killing the Corfu server. Docker container: {}", params.getName());

        try {
            ContainerInfo container = docker.inspectContainer(params.getName());

            if (!container.state().running() && !container.state().paused()) {
                log.warn("The container `{}` is not running", container.name());
                return;
            }
            docker.killContainer(params.getName());
        } catch (DockerException | InterruptedException ex) {
            throw new NodeException("Can't kill Corfu server: " + params.getName(), ex);
        }
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

        try {
            kill();
        } catch (NodeException ex) {
            log.warn("Can't kill container: {}", params.getName());
        }

        collectLogs();

        try {
            docker.removeContainer(params.getName());
        } catch (DockerException | InterruptedException ex) {
            throw new NodeException("Can't destroy Corfu server. Already deleted. Container: " + params.getName(), ex);
        }
    }

    @Override
    public Stress getStress() {
        throw new UnsupportedOperationException("Not implemented");
    }

    /**
     * Disconnect the container from docker network. This disconnects partitions from other containers.
     * The test runtime can still connect to this. This causes a case of partial partitions.
     *
     * @throws NodeException this exception will be thrown if the server can not be disconnected
     */
    @Override
    public void disconnect() {
        log.info("Disconnecting the server from docker network. Docker container: {}", params.getName());

        clusterParams.getNodesParams().forEach(neighbourServer -> {
            if (neighbourServer.equals(params)){
                return;
            }

            try {
                String networkName = universeParams.getNetworkName();
                IpamConfig ipamConfig = docker.inspectNetwork(networkName).ipam().config().get(0);
                String gateway = ipamConfig.gateway();

                ContainerInfo server = docker.inspectContainer(neighbourServer.getName());
                String neighbourhoodIp = server.networkSettings().networks().values().asList().get(0).ipAddress();

                if (StringUtils.isEmpty(neighbourhoodIp)){
                    throw new NodeException("Empty ip address. Container: " + neighbourServer.getName());
                }

                // iptables -A INPUT -s $gateway -j ACCEPT
                execCommand("iptables", "-A", "INPUT", "-s", gateway, "-j", "ACCEPT");
                // iptables -A INPUT -s $subnet -j DROP
                execCommand("iptables", "-A", "INPUT", "-s", neighbourhoodIp, "-j", "DROP");
                // iptables -A OUTPUT -d $gateway -j ACCEPT
                execCommand("iptables", "-A", "OUTPUT", "-d", gateway, "-j", "ACCEPT");
                // iptables -A OUTPUT -d $subnet -j DROP
                execCommand("iptables", "-A", "OUTPUT", "-d", neighbourhoodIp, "-j", "DROP");
            } catch (DockerException | InterruptedException ex) {
                throw new NodeException("Can't disconnect container from docker network " + params.getName(), ex);
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

        try {
            ContainerInfo container = docker.inspectContainer(params.getName());
            if (!container.state().running()) {
                log.warn("The container `{}` is not running", container.name());
                return;
            }
            docker.pauseContainer(params.getName());
        } catch (DockerException | InterruptedException ex) {
            throw new NodeException("Can't pause container " + params.getName(), ex);
        }
    }

    /**
     * Start a {@link Node}
     *
     * @throws NodeException this exception will be thrown if the server can not be started
     */
    @Override
    public void start() {
        log.info("Starting the corfu server: {}", params.getName());

        try {
            ContainerInfo container = docker.inspectContainer(params.getName());
            if (container.state().running() || container.state().paused()) {
                log.warn("The container `{}` already running, should stop before start", container.name());
                return;
            }
            docker.startContainer(params.getName());
        } catch (DockerException | InterruptedException ex) {
            throw new NodeException("Can't start container " + params.getName(), ex);
        }
    }

    /**
     * Restart a {@link Node}
     *
     * @throws NodeException this exception will be thrown if the server can not be restarted
     */
    @Override
    public void restart() {
        log.info("Restarting the corfu server: {}", params.getName());

        try {
            ContainerInfo container = docker.inspectContainer(params.getName());
            if (container.state().running() || container.state().paused()) {
                log.warn("The container `{}` already running, should stop before restart", container.name());
                return;
            }
            docker.restartContainer(params.getName());
        } catch (DockerException | InterruptedException ex) {
            throw new NodeException("Can't restart container " + params.getName(), ex);
        }
    }

    /**
     * Reconnect a {@link Node} to the network
     *
     * @throws NodeException this exception will be thrown if the node can not be reconnected
     */
    @Override
    public void reconnect() {
        log.info("Reconnecting the corfu server to the network. Docker container: {}", params.getName());

        try {
            execCommand(IpTablesUtil.cleanInput());
            execCommand(IpTablesUtil.cleanOutput());
        } catch (DockerException | InterruptedException e) {
            throw new NodeException("Can't reconnect container to docker network " + params.getName(), e);
        }
    }

    /**
     * Resume a {@link CorfuServer}
     *
     * @throws NodeException this exception will be thrown if the node can not be resumed
     */
    @Override
    public void resume() {
        log.info("Resuming the corfu server: {}", params.getName());

        try {
            ContainerInfo container = docker.inspectContainer(params.getName());
            if (!container.state().paused()) {
                log.warn("The container `{}` is not paused, should pause before resuming", container.name());
                return;
            }
            docker.unpauseContainer(params.getName());
        } catch (DockerException | InterruptedException ex) {
            throw new NodeException("Can't resume container " + params.getName(), ex);
        }
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
            ContainerCreation container = docker.createContainer(containerConfig, params.getName());
            id = container.id();

            addShutdownHook();

            docker.disconnectFromNetwork(id, "bridge");
            docker.connectToNetwork(id, docker.inspectNetwork(universeParams.getNetworkName()).id());

            docker.startContainer(id);

            String ipAddr = docker.inspectContainer(id)
                    .networkSettings().networks()
                    .values().asList().get(0)
                    .ipAddress();

            if (StringUtils.isEmpty(ipAddr)){
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
        String[] ports = {String.valueOf(params.getPort())};
        Map<String, List<PortBinding>> portBindings = new HashMap<>();
        for (String port : ports) {
            List<PortBinding> hostPorts = new ArrayList<>();
            hostPorts.add(PortBinding.of(ALL_NETWORK_INTERFACES, port));
            portBindings.put(port, hostPorts);
        }

        HostConfig hostConfig = HostConfig.builder()
                .privileged(true)
                .portBindings(portBindings)
                .build();

        // Compose command line for starting Corfu
        String cmdLine = new StringBuilder()
                .append("mkdir -p " + params.getStreamLogDir())
                .append(" && ")
                .append("java -cp *.jar org.corfudb.infrastructure.CorfuServer ")
                .append(getCommandLineParams())
                .toString();

        return ContainerConfig.builder()
                .hostConfig(hostConfig)
                .image(IMAGE_NAME)
                .hostname(params.getName())
                .exposedPorts(ports)
                .cmd("sh", "-c", cmdLine)
                .build();
    }

    private void addShutdownHook() {
        // Just in case a test failed and didn't kill the container
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                destroy();
            } catch (Exception e) {
                log.debug("Corfu server shutdown hook. Can't kill container: {}", params.getName());
            }
        }));
    }

    /**
     * Run `docker exec` on a container
     */
    private void execCommand(String... command) throws DockerException, InterruptedException {
        log.info("Executing docker command: {}", String.join(" ", command));

        ExecCreation execCreation = docker.execCreate(
                params.getName(),
                command,
                ExecCreateParam.attachStdout(),
                ExecCreateParam.attachStderr()
        );

        docker.execStart(execCreation.id());
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
