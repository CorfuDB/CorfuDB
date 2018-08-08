package org.corfudb.integtest;

import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.LogStream;
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.ContainerCreation;
import com.spotify.docker.client.messages.HostConfig;
import com.spotify.docker.client.messages.PortBinding;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Allows to manage dockerized corfu servers.
 */
@Slf4j
public class CorfuServerDockerized {
    private final CorfuServerParams params;
    private final DockerClient docker;
    private boolean started = false;
    private String containerId;
    private final String networkName;

    public CorfuServerDockerized(CorfuServerParams params, DockerClient docker, String networkName) {
        this.params = params;
        this.docker = docker;
        this.networkName = networkName;
    }

    /**
     * Start corfu server in a docker container
     *
     * @throws Exception docker error
     */
    public void start() throws Exception {
        if (started) {
            throw new IllegalStateException("Corfu server already started. Parameters: " + params);
        }
        started = true;

        String containerId = startContainer();
        this.containerId = containerId;
    }

    /**
     * Start docker container, expose ports, connect to a network
     * @return docker container id
     * @throws Exception docker error
     */
    private String startContainer() throws Exception {
        String[] ports = {String.valueOf(params.getPort())};
        Map<String, List<PortBinding>> portBindings = new HashMap<>();
        for (String port : ports) {
            List<PortBinding> hostPorts = new ArrayList<>();
            hostPorts.add(PortBinding.of("0.0.0.0", port));
            portBindings.put(port, hostPorts);
        }

        HostConfig hostConfig = HostConfig.builder()
                .portBindings(portBindings)
                .autoRemove(true)
                .build();

        String cmdLine = String.format(
                "java -cp *.jar org.corfudb.infrastructure.CorfuServer %s",
                params.getCommandLineParams()
        );

        final ContainerConfig containerConfig = ContainerConfig.builder()
                .hostConfig(hostConfig)
                .image("corfu-it:" + params.getAppVersion())
                .hostname(params.getHostName())
                .exposedPorts(ports)
                .cmd("sh", "-c", cmdLine)
                .build();

        final ContainerCreation creation = docker.createContainer(containerConfig, params.getHostName());
        final String containerId = creation.id();

        docker.connectToNetwork(containerId, docker.inspectNetwork(networkName).id());
        docker.startContainer(containerId);

        //Just in case if a test failed and didn't kill the container
        Runtime.getRuntime().addShutdownHook(new Thread(this::killContainer));
        return containerId;
    }

    /**
     * Kill a docker container, ignore errors if the container already killed
     */
    public void killContainer() {
        if (!started) {
            throw new IllegalStateException("Can't stop container - is not running");
        }

        try {
            docker.killContainer(containerId);
        } catch (Exception ex) {
            log.error("Can't kill container. Already killed. Id: {}", containerId);
        }
    }

    /**
     * Print container logs
     * @param containerId docker container id
     * @throws Exception docker exception
     */
    private void logs(String containerId) throws Exception {
        final String logs;
        try (LogStream stream = docker.logs(containerId, DockerClient.LogsParam.stdout(), DockerClient.LogsParam.stderr())) {
            logs = stream.readFully();
            log.info(logs);
        }
    }

    public String getIpAddress() throws Exception {
        return docker.inspectContainer(containerId).networkSettings().ipAddress();
    }
}
