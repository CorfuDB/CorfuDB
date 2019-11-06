package org.corfudb.universe.node.server.docker;

import com.google.common.collect.ImmutableMap;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.exceptions.DockerException;
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.ContainerCreation;
import com.spotify.docker.client.messages.HostConfig;
import com.spotify.docker.client.messages.IpamConfig;
import com.spotify.docker.client.messages.PortBinding;
import groovy.util.logging.Log4j;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NonNull;
import org.apache.commons.lang.StringUtils;
import org.corfudb.universe.group.cluster.SupportClusterParams;
import org.corfudb.universe.node.NodeException;
import org.corfudb.universe.node.server.SupportServer;
import org.corfudb.universe.node.server.SupportServerParams;
import org.corfudb.universe.universe.UniverseParams;
import org.corfudb.universe.util.DockerManager;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@Log4j
@Builder
public class DockerSupportServer<N extends SupportServerParams> implements SupportServer {
    private static final String ALL_NETWORK_INTERFACES = "0.0.0.0";
    private static final String LINUX_OS = "linux";

    private static final Map<NodeType, String> IMAGE_NAME = ImmutableMap.<NodeType, String>builder()
            .put(NodeType.METRICS_SERVER, "prom/prometheus")
            .put(NodeType.SHELL_NODE, "ubuntu")
            .build();

    private static final Map<NodeType, String> CMD = ImmutableMap.<NodeType, String>builder()
            .put(NodeType.SHELL_NODE, "bash")
            .build();

    @Default
    @Getter
    @NonNull
    private final String prometheusConfigPath = "/etc/prometheus/prometheus.yml";

    @Getter
    @NonNull
    protected final N params;

    @NonNull
    @Getter
    protected final UniverseParams universeParams;

    @NonNull
    private final DockerClient docker;

    @NonNull
    private final DockerManager dockerManager;

    @NonNull
    private final SupportClusterParams clusterParams;

    @NonNull
    @Default
    private final List<File> openedFiles = new ArrayList<>();

    @NonNull
    private final AtomicReference<String> ipAddress = new AtomicReference<>();

    @Override
    public SupportServer deploy() {
        deployContainer();

        return this;
    }

    private ContainerConfig buildContainerConfig() {
        List<String> ports = params.getPorts()
                .stream()
                .map(Objects::toString)
                .collect(Collectors.toList());
        Map<String, List<PortBinding>> portBindings = new HashMap<>();
        for (String port : ports) {
            List<PortBinding> hostPorts = new ArrayList<>();
            hostPorts.add(PortBinding.of(ALL_NETWORK_INTERFACES, port));
            portBindings.put(port, hostPorts);
        }

        HostConfig.Bind configurationFile = HostConfig.Bind.builder()
                .from(createConfiguration(params.getMetricPorts()))
                .to(prometheusConfigPath)
                .build();

        HostConfig hostConfig = HostConfig.builder()
                .privileged(true)
                .binds(configurationFile)
                .portBindings(portBindings)
                .build();

        ContainerConfig.Builder builder = ContainerConfig.builder();
        builder.hostConfig(hostConfig)
                .exposedPorts(ports.toArray(new String[0]))
                .image(IMAGE_NAME.get(getParams().getNodeType()));

        return builder.build();
    }

    private String createConfiguration(Set<Integer> metricsPorts) {
        try {
            String corfuRuntimeIp = "host.docker.internal";
            if (System.getProperty("os.name").compareToIgnoreCase(LINUX_OS) == 0) {
                corfuRuntimeIp = docker
                        .inspectNetwork(universeParams.getNetworkName())
                        .ipam()
                        .config()
                        .stream()
                        .findFirst()
                        .map(IpamConfig::gateway)
                        .orElseThrow(() -> new NodeException("Ip address not found"));
            }
            File tempConfiguration = File.createTempFile("prometheus", ".yml");

            try (BufferedWriter writer = new BufferedWriter(new FileWriter(tempConfiguration))) {
                writer.write(PrometheusConfig.getConfig(corfuRuntimeIp, metricsPorts));
                writer.flush();
            }
            openedFiles.add(tempConfiguration);
            return tempConfiguration.getAbsolutePath();
        } catch (Exception e) {
            throw new NodeException(e);
        }
    }

    private String deployContainer() {
        ContainerConfig containerConfig = buildContainerConfig();

        String id;
        try {
            ContainerCreation container = docker.createContainer(containerConfig, params.getName());
            id = container.id();

            dockerManager.addShutdownHook(clusterParams.getName());

            docker.disconnectFromNetwork(id, "bridge");
            docker.connectToNetwork(id, docker.inspectNetwork(universeParams.getNetworkName()).id());

            docker.startContainer(id);

            String ipAddr = docker.inspectContainer(id)
                    .networkSettings().networks()
                    .values().asList().get(0)
                    .ipAddress();

            if (StringUtils.isEmpty(ipAddr)) {
                throw new NodeException("Empty Ip address for container: " + clusterParams.getName());
            }

            ipAddress.set(ipAddr);
        } catch (InterruptedException | DockerException e) {
            throw new NodeException("Can't start a container", e);
        }

        return id;
    }

    /**
     * This method attempts to gracefully stop the Corfu server. After timeout, it will kill the Corfu server.
     *
     * @param timeout a duration after which the stop will kill the server
     * @throws NodeException this exception will be thrown if the server cannot be stopped.
     */
    @Override
    public void stop(Duration timeout) {
        dockerManager.stop(params.getName(), timeout);
        openedFiles.forEach(File::delete);
    }

    /**
     * Immediately kill the Corfu server.
     *
     * @throws NodeException this exception will be thrown if the server can not be killed.
     */
    @Override
    public void kill() {
        dockerManager.kill(params.getName());
        openedFiles.forEach(File::delete);
    }

    /**
     * Immediately kill and remove the docker container
     *
     * @throws NodeException this exception will be thrown if the server can not be killed.
     */
    @Override
    public void destroy() {
        dockerManager.destroy(params.getName());
        openedFiles.forEach(File::delete);
    }

}
