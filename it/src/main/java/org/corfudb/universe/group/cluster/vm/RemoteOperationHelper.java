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
import java.util.UUID;


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
        scp.setProject(PROJECT);
        scp.setTrust(true);

        scp.setLocalFile(localFile.toString());

        String remoteDirUrl = String.format(
                "%s:%s@%s:%s",
                credentials.getUsername(), credentials.getPassword(), ipAddress, remoteDir
        );
        scp.setTodir(remoteDirUrl);

        log.info("Copying {} to {} on {}", localFile, remoteDir, ipAddress);
        scp.execute();
    }

    /**
     * Download a file from a remote computer to the local computer
     *
     * @param localPath local path
     * @param remotePath remote path
     */
    public void downloadFile(Path localPath, Path remotePath) {
        Scp scp = new Scp();
        scp.setProject(PROJECT);
        scp.setTrust(true);

        String remoteFileUrl = String.format(
                "%s:%s@%s:%s",
                credentials.getUsername(), credentials.getPassword(), ipAddress, remotePath
        );

        scp.setRemoteFile(remoteFileUrl);
        scp.setLocalTofile(localPath.toString());

        log.info("Downloading {} to {} from {} to local host", remotePath, localPath, ipAddress);
        scp.execute();
    }

    /**
     * Execute a shell command on a remote vm
     *
     * @param command shell command
     */
    public String executeCommand(String command) {
        String commandId = "universe-framework-command-" + UUID.randomUUID().toString();

        SSHExec sshExec = new SSHExec();

        sshExec.setUsername(credentials.getUsername());
        sshExec.setPassword(credentials.getPassword());
        sshExec.setHost(ipAddress.getIp());
        sshExec.setCommand(command);
        sshExec.setProject(PROJECT);
        sshExec.setTrust(true);

        sshExec.setOutputproperty(commandId);

        log.info("Executing command: {}, on {}", command, ipAddress);
        sshExec.execute();

        return PROJECT.getProperty(commandId);
    }

    /**
     * Execute a shell command in sudo mode on a remote vm
     *
     * @param command shell command
     */
    public String executeSudoCommand(String command) {
        String cmdLine = String.format(
                "echo %s | sudo -S -p '' %s",
                credentials.getPassword(), command
        );
        return executeCommand(cmdLine);
    }
}
