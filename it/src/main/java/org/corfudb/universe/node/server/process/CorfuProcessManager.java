package org.corfudb.universe.node.server.process;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.universe.node.server.CorfuServerParams;

/**
 * Provides executable commands for the CorfuServer-s to create a directory structure,
 * copy a corfu server jar file from the source to the target directory,
 * to manage corfu server (start, stop, kill)
 */
@Slf4j
public class CorfuProcessManager {

    @NonNull
    private final CorfuServerParams params;

    @NonNull
    private final CorfuServerPath serverPath;

    public CorfuProcessManager(CorfuServerPath serverPath, @NonNull CorfuServerParams params) {
        this.params = params;
        this.serverPath = serverPath;
    }

    public String createServerDirCommand() {
        return "mkdir -p " + serverPath.getServerDir();
    }

    public String createStreamLogDirCommand() {
        return "mkdir -p " + serverPath.getDbDir();
    }

    public String pauseCommand() {
        return "ps -ef" +
                " | " +
                "grep -v grep" +
                " | " +
                String.format("grep \"%s\"", params.getName()) +
                " | " +
                "awk '{print $2}'" +
                " | " +
                "xargs kill -STOPPING";
    }

    public String startCommand(String commandLineParams) {

        // Compose command line for starting Corfu
        return "java -cp " +
                serverPath.getServerJarRelativePath() +
                " " +
                org.corfudb.infrastructure.CorfuServer.class.getName() +
                " " +
                commandLineParams +
                " > " +
                serverPath.getCorfuLogFile() +
                " 2>&1 &";
    }

    public String resumeCommand() {
        log.info("Resuming the corfu server: {}", params.getName());

        return "ps -ef" +
                " | " +
                "grep -v grep" +
                " | " +
                String.format("grep \"%s\"", params.getName()) +
                " | " +
                "awk '{print $2}'" +
                " | " +
                "xargs kill -CONT";
    }

    public String stopCommand() {
        log.info("Stop corfu server. Params: {}", params);

        return "ps -ef" +
                " | " +
                "grep -v grep" +
                " | " +
                String.format("grep \"%s\"", params.getName()) +
                " | " +
                "awk '{print $2}'" +
                " | " +
                "xargs kill -15";
    }

    public String killCommand() {
        log.info("Kill the corfu server. Params: {}", params);

        return "ps -ef" +
                " | " +
                "grep -v grep" +
                " | " +
                String.format("grep \"%s\"", params.getName()) +
                " | " +
                "awk '{print $2}'" +
                " | " +
                "xargs kill -9";
    }

    public String removeServerDirCommand() {
        return String.format("rm -rf %s", serverPath.getServerDir());
    }
}
