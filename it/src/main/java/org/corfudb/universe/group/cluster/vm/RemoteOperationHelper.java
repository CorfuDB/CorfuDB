package org.corfudb.universe.group.cluster.vm;

import lombok.extern.slf4j.Slf4j;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.taskdefs.optional.ssh.SSHExec;
import org.apache.tools.ant.taskdefs.optional.ssh.Scp;

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
     * @param userName    remote user
     * @param password    remote password
     * @param localFile   local file
     * @param remoteDir   remote directory
     */
    public void copyFile(
            String vmIpAddress, String userName, String password, Path localFile, Path remoteDir) {
        Scp scp = new Scp();

        scp.setLocalFile(localFile.toString());
        scp.setTodir(userName + ":" + password + "@" + vmIpAddress + ":" + remoteDir);
        scp.setProject(PROJECT);
        scp.setTrust(true);
        log.info("Copying {} to {} on {}", localFile, remoteDir, vmIpAddress);
        scp.execute();
    }

    /**
     * Execute a shell command on a remote vm
     *
     * @param vmIpAddress remote vm ip address
     * @param userName    user name
     * @param password    password
     * @param command     shell command
     */
    public void executeCommand(
            String vmIpAddress, String userName, String password, String command) {
        SSHExec sshExec = new SSHExec();

        sshExec.setUsername(userName);
        sshExec.setPassword(password);
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
     * @param userName    user name
     * @param password    password
     * @param command     shell command
     */
    public void executeSudoCommand(
            String vmIpAddress, String userName, String password, String command) {
        executeCommand(
                vmIpAddress, userName, password,
                String.format("echo %s | sudo -S -p '' %s", password, command)
        );
    }
}
