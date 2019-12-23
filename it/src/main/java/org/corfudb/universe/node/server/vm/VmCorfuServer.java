package org.corfudb.universe.node.server.vm;

import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.universe.group.cluster.vm.RemoteOperationHelper;
import org.corfudb.universe.node.NodeException;
import org.corfudb.universe.node.server.AbstractCorfuServer;
import org.corfudb.universe.node.server.CorfuServer;
import org.corfudb.universe.node.server.process.CorfuProcessManager;
import org.corfudb.universe.node.stress.vm.VmStress;
import org.corfudb.universe.universe.vm.ApplianceManager.VmManager;
import org.corfudb.universe.universe.vm.VmUniverseParams;
import org.corfudb.universe.util.IpAddress;
import org.corfudb.universe.util.IpTablesUtil;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;

/**
 * Implements a {@link CorfuServer} instance that is running on VM.
 */
@Slf4j
public class VmCorfuServer extends AbstractCorfuServer<VmCorfuServerParams, VmUniverseParams> {

    @NonNull
    private final VmManager vm;

    @NonNull
    private final IpAddress ipAddress;

    @Getter
    @NonNull
    private final RemoteOperationHelper remoteOperationHelper;

    @NonNull
    private final VmStress stress;

    @NonNull
    private final CorfuProcessManager processManager;

    @Builder
    public VmCorfuServer(
            VmCorfuServerParams params, VmManager vm, VmUniverseParams universeParams,
            VmStress stress, RemoteOperationHelper remoteOperationHelper) {
        super(params, universeParams);
        this.vm = vm;
        this.ipAddress = getIpAddress();
        this.stress = stress;
        this.remoteOperationHelper = remoteOperationHelper;

        Path corfuDir = Paths.get("~");
        this.processManager = new CorfuProcessManager(corfuDir, params);
    }

    /**
     * Deploys a Corfu server on the VM as specified, including the following steps:
     * a) Copy the corfu jar file under the working directory to the VM
     * b) Run that jar file using java on the VM
     */
    @Override
    public CorfuServer deploy() {
        executeCommand(processManager.createServerDirCommand());
        executeCommand(processManager.createStreamLogDirCommand());

        remoteOperationHelper.copyFile(
                params.getInfrastructureJar(),
                processManager.getServerJar()
        );

        start();

        return this;
    }

    /**
     * Symmetrically disconnect the server from the cluster,
     * which creates a complete partition.
     */
    @Override
    public void disconnect() {
        log.info("Disconnecting the VM server: {} from the network.", params.getVmName());

        universeParams.getVmIpAddresses().values().stream()
                .filter(addr -> !addr.equals(getIpAddress()))
                .forEach(addr -> {
                    executeSudoCommand(String.join(" ", IpTablesUtil.dropInput(addr)));
                    executeSudoCommand(String.join(" ", IpTablesUtil.dropOutput(addr)));
                });
    }

    /**
     * Symmetrically disconnect a server from a list of other servers,
     * which creates a partial partition.
     *
     * @param servers List of servers to disconnect from
     */
    @Override
    public void disconnect(List<CorfuServer> servers) {
        log.info("Disconnecting the VM server: {} from the specified servers: {}",
                params.getName(), servers);

        servers.stream()
                .filter(s -> !s.getParams().equals(params))
                .forEach(s -> {
                    executeSudoCommand(String.join(" ", IpTablesUtil.dropInput(s.getIpAddress())));
                    executeSudoCommand(String.join(" ", IpTablesUtil.dropOutput(s.getIpAddress())));
                });
    }

    /**
     * Pause the {@link CorfuServer} process on the VM
     */
    @Override
    public void pause() {
        log.info("Pausing the VM Corfu server: {}", params.getName());

        executeCommand(processManager.pauseCommand());
    }

    /**
     * Start a {@link CorfuServer} process on the VM
     */
    @Override
    public void start() {
        // Compose command line for starting Corfu
        String cmd = String.format(
                "sh -c '%s'",
                processManager.startCommand(getCommandLineParams())
        );
        executeCommand(cmd);
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
        log.info("Reconnecting the VM server: {} to the cluster.", params.getVmName());

        executeSudoCommand(String.join(" ", IpTablesUtil.cleanInput()));
        executeSudoCommand(String.join(" ", IpTablesUtil.cleanOutput()));
    }

    @Override
    public void execute(String command) {
        executeCommand(command);
    }

    /**
     * Reconnect a server to a list of servers.
     */
    @Override
    public void reconnect(List<CorfuServer> servers) {
        log.info("Reconnecting the VM server: {} to specified servers: {}",
                params.getName(), servers);

        servers.stream()
                .filter(s -> !s.getParams().equals(params))
                .forEach(s -> {
                    executeSudoCommand(String.join(" ", IpTablesUtil.revertDropInput(s.getIpAddress())));
                    executeSudoCommand(String.join(" ", IpTablesUtil.revertDropOutput(s.getIpAddress())));
                });
    }

    /**
     * Resume a {@link CorfuServer}
     */
    @Override
    public void resume() {
        log.info("Resuming the corfu server: {}", params.getName());
        executeCommand(processManager.resumeCommand());
    }

    /**
     * Executes a certain command on the VM.
     */
    private void executeCommand(String cmdLine) {
        remoteOperationHelper.executeCommand(cmdLine);
    }

    /**
     * Executes a certain Sudo command on the VM.
     */
    private void executeSudoCommand(String cmdLine) {
        remoteOperationHelper.executeSudoCommand(cmdLine);
    }

    /**
     * @return the IpAddress of this VM.
     */
    @Override
    public IpAddress getIpAddress() {
        return vm.getResolvedIpAddress();
    }

    /**
     * @param timeout a limit within which the method attempts to gracefully stop the {@link CorfuServer}.
     */
    @Override
    public void stop(Duration timeout) {
        log.info("Stop corfu server on vm: {}, params: {}", params.getVmName(), params);

        try {
            executeCommand(processManager.stopCommand());
        } catch (Exception e) {
            String err = String.format("Can't STOP corfu: %s. Process not found on vm: %s, ip: %s",
                    params.getName(), params.getVmName(), ipAddress
            );
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
            executeCommand(processManager.killCommand());
        } catch (Exception e) {
            String err = String.format("Can't KILL corfu: %s. Process not found on vm: %s, ip: %s",
                    params.getName(), params.getVmName(), ipAddress
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
            executeSudoCommand(IpTablesUtil.cleanAll());
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
        executeCommand(processManager.removeServerDirCommand());
    }

    @Override
    public IpAddress getNetworkInterface() {
        return ipAddress;
    }
}
