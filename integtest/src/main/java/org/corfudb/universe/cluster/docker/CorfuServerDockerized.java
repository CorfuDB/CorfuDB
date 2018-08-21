package org.corfudb.universe.cluster.docker;

import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.LogStream;
import com.spotify.docker.client.exceptions.DockerException;
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.ContainerCreation;
import com.spotify.docker.client.messages.HostConfig;
import com.spotify.docker.client.messages.PortBinding;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.maven.model.Model;
import org.apache.maven.model.io.xpp3.MavenXpp3Reader;
import org.codehaus.plexus.util.xml.pull.XmlPullParserException;
import org.corfudb.universe.cluster.ClusterException;
import org.corfudb.universe.node.CorfuServer;
import org.corfudb.universe.node.NodeException;

import java.io.FileReader;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static lombok.Builder.Default;
import static org.corfudb.universe.cluster.Cluster.ClusterParams;

/**
 * Implements a docker instance representing a Corfu CorfuServer.
 */
@Slf4j
@Builder
public class CorfuServerDockerized implements CorfuServer {

    @Getter
    private final ServerParams params;
    private final DockerClient docker;
    private final ClusterParams clusterParams;
    @Default
    private final Optional<String> containerId = Optional.empty();

    /**
     * Deploys a Corfu server / docker container
     */
    @Override
    public CorfuServerDockerized deploy() throws NodeException {
        log.info("Deploying the Corfu server. {}", params.getGenericName());

        if(containerId.isPresent()){
            throw new IllegalStateException("Corfu server already started. Parameters: " + params);
        }

        String id = deployContainer();

        CorfuServerDockerized server = CorfuServerDockerized.builder()
                .clusterParams(clusterParams)
                .params(params)
                .docker(docker)
                .containerId(Optional.of(id))
                .build();

        server.addShutdownHook();
        return server;
    }

    /**
     * This method attempts to gracefully stops the Corfu server. After timeout, it will kill the Corfu server.
     *
     * @param timeout a duration after which the stop will kill the server
     * @throws NodeException this exception will be thrown if the server can not be stopped.
     */
    @Override
    public void stop(Duration timeout) throws NodeException {
        log.info("Stopping the Corfu server. {}", params.getGenericName());

        containerId.orElseThrow(() -> new NodeException("Can't STOP container. Invalid docker container id"));
        containerId.ifPresent(id -> {
            try {
                docker.stopContainer(id, (int) timeout.getSeconds());
            } catch (DockerException | InterruptedException e) {
                throw new NodeException("Can't stop Corfu server", e);
            }
        });
    }

    /**
     * Immediately kill the Corfu server.
     *
     * @throws NodeException this exception will be thrown if the server can not be killed.
     */
    @Override
    public void kill() throws NodeException {
        log.info("Killing the Corfu server. {}", params.getGenericName());

        containerId.orElseThrow(() -> new NodeException("Can't KILL container. Invalid docker container id"));
        containerId.ifPresent(id -> {
            try {
                docker.killContainer(id);
            } catch (DockerException | InterruptedException ex) {
                throw new NodeException("Can't kill Corfu server", ex);
            }
        });
    }

    /**
     * Deploy and start docker container, expose ports, connect to a network
     *
     * @return docker container id
     */
    private String deployContainer() throws NodeException {
        ContainerConfig containerConfig = buildContainerConfig();

        String id;
        try {
            ContainerCreation creation = docker.createContainer(containerConfig, params.getGenericName());
            id = creation.id();
            docker.connectToNetwork(id, docker.inspectNetwork(clusterParams.getNetworkName()).id());

            docker.startContainer(id);
        } catch (InterruptedException | DockerException e) {
            throw new NodeException("Can't start a container", e);
        }

        return id;
    }

    private ContainerConfig buildContainerConfig() throws NodeException {
        // Bind ports
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

        // Compose command line for starting Corfu
        String cmdLine = String.format(
                "java -cp *.jar org.corfudb.infrastructure.CorfuServer %s",
                getCommandLineParams()
        );

        try {
            return ContainerConfig.builder()
                    .hostConfig(hostConfig)
                    .image("corfu-server:" + getAppVersion())
                    .hostname(params.getGenericName())
                    .exposedPorts(ports)
                    .cmd("sh", "-c", cmdLine)
                    .build();
        } catch (XmlPullParserException | IOException e) {
            throw new NodeException("Can't parse application version", e);
        }
    }

    private void addShutdownHook(){
        //Just in case if a test failed and didn't kill the container
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                kill();
            } catch (NodeException ex) {
                log.debug("Can't kill container. Already killed. Id: {}", containerId);
            }
        }));
    }

    private String getAppVersion() throws IOException, XmlPullParserException {
        MavenXpp3Reader reader = new MavenXpp3Reader();
        Model model = reader.read(new FileReader("pom.xml"));
        return model.getParent().getVersion();
    }

    /**
     * This method create a command line string for starting Corfu server
     *
     * @return command line parameters
     */
    private String getCommandLineParams() {
        StringBuilder cmd = new StringBuilder();
        cmd.append("-a ").append("0.0.0.0");

        switch (params.getPersistence()){
            case DISK:
                if(StringUtils.isEmpty(params.getLogDir())){
                    throw new ClusterException("Invalid log dir in disk persistence mode");
                }
                cmd.append(" -l ").append(params.getLogDir());
                break;
            case MEMORY:
                cmd.append(" -m");
                break;
        }

        if (params.getMode() == Mode.SINGLE) {
            cmd.append(" -s");
        }

        cmd.append(" -d ").append(params.getLogLevel().toString()).append(" ");

        cmd.append(params.getPort());

        String cmdLineParams = cmd.toString();
        log.trace("Command line parameters: {}", cmdLineParams);

        return cmdLineParams;
    }

    /**
     * Print container logs.
     *
     * @param containerId docker container id
     * @throws Exception docker exception
     */
    private void logs(String containerId) throws DockerException, InterruptedException {
        final String logs;
        try (LogStream stream = docker.logs(containerId,
                DockerClient.LogsParam.stdout(),
                DockerClient.LogsParam.stderr())) {
            logs = stream.readFully();
            log.info(logs);
        }
    }
}
