package org.corfudb.universe.node.server.vm;

import com.jcraft.jsch.JSchException;
import com.vmware.vim25.GuestInfo;
import com.vmware.vim25.mo.VirtualMachine;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.universe.group.cluster.vm.RemoteOperationHelper;
import org.corfudb.universe.node.Node;
import org.corfudb.universe.node.NodeException;
import org.corfudb.universe.node.server.AbstractCorfuServer;
import org.corfudb.universe.node.server.CorfuServer;
import org.corfudb.universe.node.stress.Stress;
import org.corfudb.universe.node.stress.vm.VmStress;
import org.corfudb.universe.universe.vm.VmUniverseParams;
import org.corfudb.universe.util.IpTablesUtil;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;

/**
 * Implements a {@link CorfuServer} instance that is running on VM.
 */
@Slf4j
public class VmCorfuServer extends AbstractCorfuServer<VmCorfuServerParams, VmUniverseParams> {
    private static final String CORFU_INFRASTRUCTURE_JAR = String.format(
            "./target/corfu/infrastructure-%s-shaded.jar", getAppVersion()
    );

    @NonNull
    private final VirtualMachine vm;
    @NonNull
    private final String ipAddress;
    private final RemoteOperationHelper commandHelper;
    @NonNull
    private final VmStress stress;

    @Builder
    public VmCorfuServer(VmCorfuServerParams params, VirtualMachine vm, VmUniverseParams universeParams, VmStress stress) {
        super(params, universeParams);
        this.vm = vm;
        this.ipAddress = getIpAddress();
        this.stress = stress;
        commandHelper = RemoteOperationHelper.getInstance();
    }

    /**
     * Deploys a Corfu server on the VM as specified, including the following steps:
     * a) Copy the corfu jar file under the working directory to the VM
     * b) Run that jar file using java on the VM
     */
    @Override
    public CorfuServer deploy() {
        executeCommand("mkdir -p ./" + params.getName());
        executeCommand("mkdir -p ./" + params.getStreamLogDir());

        commandHelper.copyFile(
                ipAddress,
                universeParams.getVmUserName(),
                universeParams.getVmPassword(),
                CORFU_INFRASTRUCTURE_JAR,
                "./" + params.getName() + "/corfu-server.jar"
        );

        start();

        return this;
    }

    /**
     * Disconnect the VM from the vSphere network
     */
    @Override
    public void disconnect() {
        log.info("Disconnecting the server from the network. VM: {}", params.getVmName());

        universeParams.getVmIpAddresses().values().stream()
                .filter(addr -> !addr.equals(getIpAddress()))
                .forEach(addr -> {
                    executeSudoCommand(IpTablesUtil.dropInput(addr));
                    executeSudoCommand(IpTablesUtil.dropOutput(addr));
                });
    }

    /**
     * Pause the {@link CorfuServer} process on the VM
     */
    @Override
    public void pause() {
        log.info("Pausing the Corfu server: {}", params.getName());

        String cmd = "ps -ef | grep -v grep | grep \"corfudb\" | awk '{print $2}' | xargs kill -STOP";
        executeCommand(cmd);
    }

    /**
     * Start a {@link CorfuServer} process on the VM
     */
    @Override
    public void start() {
        // Compose command line for starting Corfu
        StringBuilder cmdLine = new StringBuilder();
        cmdLine.append("sh -c 'nohup java -cp ./" + params.getName() + "/*.jar org.corfudb.infrastructure.CorfuServer ");
        cmdLine.append(getCommandLineParams());
        cmdLine.append(" > /tmp/corfu.log 2>&1 &'");

        executeCommand(cmdLine.toString());
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
     * Reconnect a {@link Node} to the vSphere network
     */
    @Override
    public void reconnect() {
        log.info("Reconnecting the corfu server to the network. VM: {}", params.getVmName());

        executeSudoCommand(String.join(" ", IpTablesUtil.cleanInput()));
        executeSudoCommand(String.join(" ", IpTablesUtil.cleanOutput()));
    }

    /**
     * Resume a {@link CorfuServer}
     */
    @Override
    public void resume() {
        log.info("Resuming the corfu server: {}", params.getName());

        String cmd = "ps -ef | grep -v grep | grep \"corfudb\" | awk '{print $2}' | xargs kill -CONT";
        executeCommand(cmd);
    }

    /**
     * Executes a certain command on the VM.
     */
    private void executeCommand(String cmdLine) {
        String ipAddress = getIpAddress();

        commandHelper.executeCommand(ipAddress,
                universeParams.getVmUserName(),
                universeParams.getVmPassword(),
                cmdLine
        );
    }

    /**
     * Executes a certain Sudo command on the VM.
     */
    private void executeSudoCommand(String cmdLine) {
        String ipAddress = getIpAddress();

        try {
            commandHelper.executeSudoCommand(ipAddress,
                    universeParams.getVmUserName(),
                    universeParams.getVmPassword(),
                    "sudo " + cmdLine
            );
        } catch (JSchException | IOException e) {
            throw new NodeException("Can't execute sudo command: " + cmdLine, e);
        }
    }

    /**
     * @return the IpAddress of this VM.
     */
    @Override
    public String getIpAddress() {
        GuestInfo guest = vm.getGuest();
        return guest.getIpAddress();
    }

    /**
     * @param timeout a limit within which the method attempts to gracefully stop the {@link CorfuServer}.
     */
    @Override
    public void stop(Duration timeout) {
        log.info("Stop corfu server on vm: {}, params: {}", params.getVmName(), params);

        try {
            executeCommand("ps -ef | grep -v grep | grep \"corfudb\" | awk '{print $2}' | xargs kill -15");
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
            executeCommand("ps -ef | grep -v grep | grep \"corfudb\" | awk '{print $2}' | xargs kill -9");
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

    @Override
    public Stress getStress() {
        return stress;
    }

    /**
     * Remove corfu server application dir.
     * AppDir is a directory that contains corfu-infrastructure jar file and could have log files, stream-log files and
     * so on, whatever used by the application.
     */
    private void removeAppDir() {
        executeCommand(String.format("rm -rf ./%s", params.getName()));
    }

    @Override
    public String getNetworkInterface() {
        return ipAddress;
    }
}
