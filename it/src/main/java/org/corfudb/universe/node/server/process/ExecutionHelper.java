package org.corfudb.universe.node.server.process;

import lombok.extern.slf4j.Slf4j;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.taskdefs.Copy;
import org.apache.tools.ant.taskdefs.Execute;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;


/**
 * Provides the helper functions that do operations (copy file/execute command) on a remote machine.
 */
@Slf4j
public class ExecutionHelper {
    private static final Project PROJECT = new Project();
    private static final ExecutionHelper INSTANCE = new ExecutionHelper();

    private ExecutionHelper() {
        //prevent creating class instances
    }

    public static ExecutionHelper getInstance() {
        return INSTANCE;
    }

    /**
     * Copy a file
     *
     * @param srcFile   source file
     * @param targetFile target directory
     */
    public void copyFile(Path srcFile, Path targetFile) {
        Copy copy = new Copy();

        copy.setFile(srcFile.toFile());
        copy.setTofile(targetFile.toFile());
        copy.setProject(PROJECT);
        log.info("Copying {} to {}", srcFile, targetFile);
        copy.execute();
    }

    /**
     * Execute a shell command
     *
     * @param command shell command
     */
    public void executeCommand(Optional<Path> workDir, String command) throws IOException {
        Execute exec = new Execute();

        exec.setCommandline(new String[]{"sh", "-c", command});
        exec.setAntRun(PROJECT);
        workDir.ifPresent(wd -> exec.setWorkingDirectory(wd.toFile()));
        log.info("Executing command: {}, workDir: {}", command, workDir);
        exec.execute();
    }
}
