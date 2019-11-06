package org.corfudb.universe.group.cluster.vm;

import lombok.extern.slf4j.Slf4j;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.taskdefs.optional.ssh.SSHExec;
import org.apache.tools.ant.taskdefs.optional.ssh.Scp;
import org.corfudb.universe.universe.vm.VmUniverseParams.Credentials;

import java.nio.file.Path;


/**
 * Provides the helper functions that do operations (copy file/execute command) on a remote machine.
 */
@Slf4j
public class RemoteOperationHelper {
    private static final Project PROJECT = new Project();
    private static final RemoteOperationHelper INSTANCE = new RemoteOperationHelper();

    private RemoteOperationHelper() {
        //prevent creating class instances
    }

    public static RemoteOperationHelper getInstance() {
        return INSTANCE;
    }

    /**
     * Copy a file from local computer to a remote computer
     *
     * @param vmIpAddress remote computer ip address
     * @param localFile   local file
     * @param remoteDir   remote directory
     */
    public void copyFile(
            String vmIpAddress, Credentials credentials, Path localFile, Path remoteDir) {
        Scp scp = new Scp();

        scp.setLocalFile(localFile.toString());

        String remoteDirUrl = String.format(
                "%s:%s@%s:%s",
                credentials.getUsername(), credentials.getPassword(), vmIpAddress, remoteDir
        );
        scp.setTodir(remoteDirUrl);

        scp.setProject(PROJECT);
        scp.setTrust(true);
        log.info("Copying {} to {} on {}", localFile, remoteDir, vmIpAddress);
        scp.execute();
    }

    /**
     * Execute a shell command on a remote vm
     *
     * @param vmIpAddress remote vm ip address
     * @param credentials user credentials
     * @param command     shell command
     */
    public void executeCommand(String vmIpAddress, Credentials credentials, String command) {
        SSHExec sshExec = new SSHExec();

        sshExec.setUsername(credentials.getUsername());
        sshExec.setPassword(credentials.getPassword());
        sshExec.setHost(vmIpAddress);
        sshExec.setCommand(command);
        sshExec.setProject(PROJECT);
        sshExec.setTrust(true);
        log.info("Executing command: {}, on {}", command, vmIpAddress);
        sshExec.execute();
    }

    /**
     * Execute a shell command in sudo mode on a remote vm
     *
     * @param vmIpAddress remote vm ip address
     * @param credentials user credentials
     * @param command     shell command
     */
    public void executeSudoCommand(String vmIpAddress, Credentials credentials, String command) {
        String cmdLine = String.format(
                "echo %s | sudo -S -p '' %s",
                credentials.getPassword(), command
        );
        executeCommand(vmIpAddress, credentials, cmdLine);
    }
}
