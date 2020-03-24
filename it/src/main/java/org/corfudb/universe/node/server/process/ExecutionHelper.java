package org.corfudb.universe.node.server.process;

import lombok.extern.slf4j.Slf4j;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.taskdefs.Copy;
import org.apache.tools.ant.taskdefs.Execute;
import org.apache.tools.ant.taskdefs.PumpStreamHandler;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Optional;


/**
 * Provides the helper functions that do operations (copy file/execute command) on a LOCAL machine.
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
     * The utility copies the contents of the source_file to the target_file.
     *
     * @param srcFile    source file
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
    public String executeCommand(Optional<Path> workDir, String command) throws IOException {
        Execute exec = new Execute();

        exec.setCommandline(new String[]{"sh", "-c", command});
        exec.setAntRun(PROJECT);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        exec.setStreamHandler(new PumpStreamHandler(out, out));
        workDir.ifPresent(wd -> exec.setWorkingDirectory(wd.toFile()));
        log.info("Executing command: {}, workDir: {}", command, workDir);
        exec.execute();

        return out.toString(StandardCharsets.UTF_8.name());
    }
}
