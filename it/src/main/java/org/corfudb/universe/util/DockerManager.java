package org.corfudb.universe.util;

import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.exceptions.DockerException;
import com.spotify.docker.client.messages.ContainerInfo;
import com.spotify.docker.client.messages.ExecCreation;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.universe.node.NodeException;

import java.time.Duration;

/**
 * Manages docker containers
 */
@Builder
@Slf4j
public class DockerManager {

    @NonNull
    private final DockerClient docker;

    /**
     * This method attempts to gracefully stop a container and kill it after timeout.
     *
     * @param timeout a duration after which the stop will kill the container
     * @throws NodeException this exception will be thrown if a container cannot be stopped.
     */
    public void stop(String containerName, Duration timeout) {
        log.info("Stopping the Corfu server. Docker container: {}", containerName);

        try {
            ContainerInfo container = docker.inspectContainer(containerName);
            if (!container.state().running() && !container.state().paused()) {
                log.warn("The container `{}` is already stopped", container.name());
                return;
            }
            docker.stopContainer(containerName, (int) timeout.getSeconds());
        } catch (DockerException | InterruptedException e) {
            throw new NodeException("Can't stop Corfu server: " + containerName, e);
        }
    }

    /**
     * Immediately kill a docker container.
     *
     * @throws NodeException this exception will be thrown if the container can not be killed.
     */
    public void kill(String containerName) {
        log.info("Killing docker container: {}", containerName);

        try {
            ContainerInfo container = docker.inspectContainer(containerName);

            if (!container.state().running() && !container.state().paused()) {
                log.warn("The container `{}` is not running", container.name());
                return;
            }
            docker.killContainer(containerName);
        } catch (DockerException | InterruptedException ex) {
            throw new NodeException("Can't kill Corfu server: " + containerName, ex);
        }
    }

    /**
     * Immediately kill and remove a docker container
     *
     * @throws NodeException this exception will be thrown if the container can not be killed.
     */
    public void destroy(String containerName) {
        log.info("Destroying docker container: {}", containerName);

        try {
            kill(containerName);
        } catch (NodeException ex) {
            log.warn("Can't kill container: {}", containerName);
        }

        try {
            docker.removeContainer(containerName);
        } catch (DockerException | InterruptedException ex) {
            throw new NodeException("Can't destroy Corfu server. Already deleted. Container: " + containerName, ex);
        }
    }

    /**
     * Pause a container from the docker network
     *
     * @throws NodeException this exception will be thrown if the container can not be paused
     */
    public void pause(String containerName) {
        log.info("Pausing container: {}", containerName);

        try {
            ContainerInfo container = docker.inspectContainer(containerName);
            if (!container.state().running()) {
                log.warn("The container `{}` is not running", container.name());
                return;
            }
            docker.pauseContainer(containerName);
        } catch (DockerException | InterruptedException ex) {
            throw new NodeException("Can't pause container " + containerName, ex);
        }
    }

    /**
     * Start a docker container
     *
     * @throws NodeException this exception will be thrown if the container can not be started
     */
    public void start(String containerName) {
        log.info("Starting docker container: {}", containerName);

        try {
            ContainerInfo container = docker.inspectContainer(containerName);
            if (container.state().running() || container.state().paused()) {
                log.warn("The container `{}` already running, should stop before start", container.name());
                return;
            }
            docker.startContainer(containerName);
        } catch (DockerException | InterruptedException ex) {
            throw new NodeException("Can't start container " + containerName, ex);
        }
    }

    /**
     * Restart a docker container
     *
     * @throws NodeException this exception will be thrown if the container can not be restarted
     */
    public void restart(String containerName) {
        log.info("Restarting the corfu server: {}", containerName);

        try {
            ContainerInfo container = docker.inspectContainer(containerName);
            if (container.state().restarting()) {
                log.warn("The container `{}` is already restarting", container.name());
                return;
            }
            docker.restartContainer(containerName);
        } catch (DockerException | InterruptedException ex) {
            throw new NodeException("Can't restart container " + containerName, ex);
        }
    }

    /**
     * Resume a docker container
     *
     * @throws NodeException this exception will be thrown if the container can not be resumed
     */
    public void resume(String containerName) {
        log.info("Resuming docker container: {}", containerName);

        try {
            ContainerInfo container = docker.inspectContainer(containerName);
            if (!container.state().paused()) {
                log.warn("The container `{}` is not paused, should pause before resuming", container.name());
                return;
            }
            docker.unpauseContainer(containerName);
        } catch (DockerException | InterruptedException ex) {
            throw new NodeException("Can't resume container " + containerName, ex);
        }
    }

    /**
     * Run `docker exec` on a container
     */
    public void execCommand(String containerName, String... command) throws DockerException, InterruptedException {
        log.info("Executing docker command: {}", String.join(" ", command));

        ExecCreation execCreation = docker.execCreate(
                containerName,
                command,
                DockerClient.ExecCreateParam.attachStdout(),
                DockerClient.ExecCreateParam.attachStderr()
        );

        docker.execStart(execCreation.id());
    }

    public void addShutdownHook(String containerName) {
        // Just in case a test failed and didn't kill the container
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                destroy(containerName);
            } catch (Exception e) {
                log.debug("Corfu server shutdown hook. Can't kill container: {}", containerName);
            }
        }));
    }
}
