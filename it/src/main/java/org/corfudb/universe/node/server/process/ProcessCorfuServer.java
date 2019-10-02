package org.corfudb.universe.node.server.process;

import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.universe.node.NodeException;
import org.corfudb.universe.node.server.AbstractCorfuServer;
import org.corfudb.universe.node.server.CorfuServer;
import org.corfudb.universe.node.server.CorfuServerParams;
import org.corfudb.universe.universe.UniverseParams;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

/**
 * Implements a {@link CorfuServer} instance that is running on a host machine.
 */
@Slf4j
public class ProcessCorfuServer extends AbstractCorfuServer<CorfuServerParams, UniverseParams> {
    private static final Path TMP_DIR = Paths.get(System.getProperty("java.io.tmpdir"));

    @NonNull
    private final Path infrastructureJar;

    @NonNull
    private final String ipAddress;

    private final ExecutionHelper commandHelper = ExecutionHelper.getInstance();

    private final Path corfuDir;
    private final Path serverDir;
    private final Path dbDir;
    private final Path serverJar;
    private final Path serverJarRelativePath;

    @Builder
    public ProcessCorfuServer(
            CorfuServerParams params, UniverseParams universeParams, String version) {
        super(params, universeParams, version);
        this.ipAddress = getIpAddress();
        this.infrastructureJar = Paths.get(
                "infrastructure",
                "target",
                String.format("infrastructure-%s-shaded.jar", version)
        );

        try {
            corfuDir = Files.createDirectories(TMP_DIR.resolve("corfu"));
            serverDir = corfuDir.resolve(params.getName());
            dbDir = corfuDir.resolve(params.getStreamLogDir());

            serverJarRelativePath = Paths.get(params.getName(), "corfu-server.jar");
            serverJar = corfuDir.resolve(serverJarRelativePath);

            Files.createDirectories(serverDir);
            Files.createDirectories(dbDir);
        } catch (IOException e) {
            throw new NodeException("Can't create directory", e);
        }
    }

    /**
     * Deploys a Corfu server on the VM as specified, including the following steps:
     * a) Copy the corfu jar file under the working directory to the VM
     * b) Run that jar file using java on the VM
     */
    @Override
    public CorfuServer deploy() {
        commandHelper.copyFile(infrastructureJar, serverJar);
        start();
        return this;
    }

    /**
     * Symmetrically disconnect the server from the cluster,
     * which creates a complete partition.
     */
    @Override
    public void disconnect() {
        throw new UnsupportedOperationException("Not supported");
    }

    /**
     * Symmetrically disconnect a server from a list of other servers,
     * which creates a partial partition.
     *
     * @param servers List of servers to disconnect from
     */
    @Override
    public void disconnect(List<CorfuServer> servers) {
        throw new UnsupportedOperationException("Not supported");
    }

    /**
     * Pause the {@link CorfuServer} process on the VM
     */
    @Override
    public void pause() {
        log.info("Pausing the Corfu server: {}", params.getName());

        StringBuilder cmd = new StringBuilder()
                .append("ps -ef")
                .append(" | ")
                .append("grep -v grep")
                .append(" | ")
                .append(String.format("grep \"%s\"", params.getName()))
                .append(" | ")
                .append("awk '{print $2}'")
                .append(" | ")
                .append("xargs kill -STOP");

        executeCommand(Optional.empty(), cmd.toString());
    }

    /**
     * Start a {@link CorfuServer} process on the VM
     */
    @Override
    public void start() {
        Path corfuLog = corfuDir.resolve("corfu.log");

        // Compose command line for starting Corfu
        StringBuilder cmdLine = new StringBuilder();
        cmdLine.append("java -cp ");
        cmdLine.append(serverJarRelativePath);
        cmdLine.append(" ");
        cmdLine.append(org.corfudb.infrastructure.CorfuServer.class.getName());
        cmdLine.append(" ");
        cmdLine.append(getCommandLineParams());
        cmdLine.append(" > ");
        cmdLine.append(corfuLog);
        cmdLine.append(" 2>&1 &");

        executeCommand(Optional.of(corfuDir), cmdLine.toString());
    }

    /**
     * Restart the {@link CorfuServer} process on the VM
     */
    @Override
    public void restart() {
        stop(params.getStopTimeout());
        start();
    }

    /**
     * Reconnect a server to the cluster
     */
    @Override
    public void reconnect() {
        throw new UnsupportedOperationException("Not supported");
    }

    /**
     * Reconnect a server to a list of servers.
     */
    @Override
    public void reconnect(List<CorfuServer> servers) {
        throw new UnsupportedOperationException("Not supported");
    }

    /**
     * Resume a {@link CorfuServer}
     */
    @Override
    public void resume() {
        log.info("Resuming the corfu server: {}", params.getName());

        StringBuilder cmd = new StringBuilder()
                .append("ps -ef")
                .append(" | ")
                .append("grep -v grep")
                .append(" | ")
                .append(String.format("grep \"%s\"", params.getName()))
                .append(" | ")
                .append("awk '{print $2}'")
                .append(" | ")
                .append("xargs kill -CONT");
        executeCommand(Optional.empty(), cmd.toString());
    }

    /**
     * Executes a certain command on the VM.
     */
    private void executeCommand(Optional<Path> workDir, String cmdLine) {
        try {
            commandHelper.executeCommand(workDir, cmdLine);
        } catch (IOException e) {
            throw new NodeException("Execution error. Cmd: " + cmdLine, e);
        }
    }

    /**
     * @return the IpAddress of this VM.
     */
    @Override
    public String getIpAddress() {
        return "127.0.0.1";
    }

    /**
     * @param timeout a limit within which the method attempts to gracefully stop the {@link CorfuServer}.
     */
    @Override
    public void stop(Duration timeout) {
        log.info("Stop corfu server. Params: {}", params);

        try {
            StringBuilder cmd = new StringBuilder()
                    .append("ps -ef")
                    .append(" | ")
                    .append("grep -v grep")
                    .append(" | ")
                    .append(String.format("grep \"%s\"", params.getName()))
                    .append(" | ")
                    .append("awk '{print $2}'")
                    .append(" | ")
                    .append("xargs kill -15");

            executeCommand(Optional.empty(), cmd.toString());
        } catch (Exception e) {
            String err = String.format("Can't STOP corfu: %s. Process not found", params.getName());
            throw new NodeException(err, e);
        }
    }

    /**
     * Kill the {@link CorfuServer} process on the VM directly.
     */
    @Override
    public void kill() {
        log.info("Kill the corfu server. Params: {}", params);
        try {
            StringBuilder cmd = new StringBuilder()
                    .append("ps -ef")
                    .append(" | ")
                    .append("grep -v grep")
                    .append(" | ")
                    .append(String.format("grep \"%s\"", params.getName()))
                    .append(" | ")
                    .append("awk '{print $2}'")
                    .append(" | ")
                    .append("xargs kill -9");

            executeCommand(Optional.empty(), cmd.toString());
        } catch (Exception e) {
            String err = String.format("Can't KILL corfu: %s. Process not found, ip: %s",
                    params.getName(), ipAddress
            );
            throw new NodeException(err, e);
        }
    }

    /**
     * Destroy the {@link CorfuServer} by killing the process and removing the files
     *
     * @throws NodeException this exception will be thrown if the server can not be destroyed.
     */
    @Override
    public void destroy() {
        log.info("Destroy node: {}", params.getName());
        kill();
        try {
            removeAppDir();
        } catch (Exception e) {
            throw new NodeException("Can't clean corfu directories", e);
        }
    }

    /**
     * Remove corfu server application dir.
     * AppDir is a directory that contains corfu-infrastructure jar file and could have log files, stream-log files and
     * so on, whatever used by the application.
     */
    private void removeAppDir() {
        executeCommand(Optional.empty(), String.format("rm -rf %s", serverDir));
    }

    @Override
    public String getNetworkInterface() {
        return ipAddress;
    }
}
