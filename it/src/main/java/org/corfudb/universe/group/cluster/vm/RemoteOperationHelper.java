package org.corfudb.universe.group.cluster.vm;

import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.taskdefs.optional.ssh.SSHExec;
import org.apache.tools.ant.taskdefs.optional.ssh.Scp;
import org.corfudb.universe.universe.vm.VmUniverseParams.Credentials;
import org.corfudb.universe.util.IpAddress;

import java.nio.file.Path;


/**
 * Provides the helper functions that do operations (copy file/execute command) on a remote machine.
 */
@Slf4j
@Builder
public class RemoteOperationHelper {
    private static final Project PROJECT = new Project();

    @NonNull
    private final Credentials credentials;

    @NonNull
    private final IpAddress ipAddress;

    /**
     * Copy a file from local computer to a remote computer
     *
     * @param localFile local file
     * @param remoteDir remote directory
     */
    public void copyFile(Path localFile, Path remoteDir) {
        Scp scp = new Scp();

        scp.setLocalFile(localFile.toString());

        String remoteDirUrl = String.format(
                "%s:%s@%s:%s",
                credentials.getUsername(), credentials.getPassword(), ipAddress, remoteDir
        );
        scp.setTodir(remoteDirUrl);

        scp.setProject(PROJECT);
        scp.setTrust(true);
        log.info("Copying {} to {} on {}", localFile, remoteDir, ipAddress);
        scp.execute();
    }

    /**
     * Execute a shell command on a remote vm
     *
     * @param command shell command
     */
    public void executeCommand(String command) {
        SSHExec sshExec = new SSHExec();

        sshExec.setUsername(credentials.getUsername());
        sshExec.setPassword(credentials.getPassword());
        sshExec.setHost(ipAddress.getIp());
        sshExec.setCommand(command);
        sshExec.setProject(PROJECT);
        sshExec.setTrust(true);
        log.info("Executing command: {}, on {}", command, ipAddress);
        sshExec.execute();
    }

    /**
     * Execute a shell command in sudo mode on a remote vm
     *
     * @param command shell command
     */
    public void executeSudoCommand(String command) {
        String cmdLine = String.format(
                "echo %s | sudo -S -p '' %s",
                credentials.getPassword(), command
        );
        executeCommand(cmdLine);
    }
}
