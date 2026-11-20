package org.corfudb.infrastructure;

import org.apache.commons.io.IOUtils;
import org.corfudb.runtime.DistributedCheckpointer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class InvokeCheckpointingJvm implements InvokeCheckpointing {

    private static final int MAX_COMPACTION_RETRIES = 8;
    private final ServerContext serverContext;
    private volatile Process checkpointerProcess;
    private final Logger log;
    private volatile boolean isInvoked;

    public InvokeCheckpointingJvm(ServerContext serverContext) {
        this.serverContext = serverContext;
        this.log = LoggerFactory.getLogger("compactor-leader");
    }

    @Override
    public void invokeCheckpointing() {
        for (int i = 1; i <= MAX_COMPACTION_RETRIES; i++) {
            try {
                if (!serverContext.getCompactorScriptPath().isPresent() || !serverContext.getCompactorConfig().isPresent()) {
                    log.warn("Compactor client runner script or config file not found");
                    return;
                }

                String compactorScriptPath = serverContext.getCompactorScriptPath().get();
                String compactorConfigPath = serverContext.getCompactorConfig().get();

                String hostName = serverContext.getNodeLocator().getHost();
                String port = Integer.toString(serverContext.getNodeLocator().getPort());
                log.info("invokeCheckpointing: hostName: {}, port: {}", hostName, port);

                if (isRunning()) {
                    shutdown();
                }

                ProcessBuilder pb = new ProcessBuilder(compactorScriptPath, "--hostname", hostName, "--port",
                        port, "--compactorConfig", compactorConfigPath, "--startCheckpointing=true");
                pb.redirectOutput(ProcessBuilder.Redirect.PIPE);
                pb.redirectError(ProcessBuilder.Redirect.PIPE);
                this.checkpointerProcess = pb.start();
                this.isInvoked = true;
                log.info("Triggered compactor jvm");

                Thread commandLineThread = new Thread(() -> {
                    try {
                        String err = IOUtils.toString(checkpointerProcess.getErrorStream(), StandardCharsets.UTF_8);
                        if (err.length() > 0) {
                            log.error("Error occurred after invoking compactor jvm: {}", err);
                        }
                    } catch (IOException ex) {
                        log.error("Exception occurred while getting ErrorStream: ", ex);
                    }
                });
                commandLineThread.setDaemon(true);
                commandLineThread.start();

                return;
            } catch (RuntimeException re) {
                if (DistributedCheckpointer.isCriticalRuntimeException(re, i, MAX_COMPACTION_RETRIES)) {
                    break;
                }
            } catch (IOException io) {
                log.error("Encountered IOException due to : {}. StackTrace: {}", io.getMessage(), io.getStackTrace());
            }
        }
    }

    @Override
    public boolean isRunning() {
        return this.checkpointerProcess != null && this.checkpointerProcess.isAlive();
    }

    @Override
    public boolean isInvoked() {
        return this.isInvoked;
    }

    @Override
    public void shutdown() {
        if (isRunning()) {
            this.checkpointerProcess.destroy();
            log.info("Shutting down existing checkpointer jvm ");
        }
        this.isInvoked = false;
    }
}
