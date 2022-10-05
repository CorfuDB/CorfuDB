package org.corfudb.infrastructure;

import org.corfudb.runtime.DistributedCheckpointer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class InvokeCheckpointingJvm implements InvokeCheckpointing {

    private static final int MAX_COMPACTION_RETRIES = 8;
    private final ServerContext serverContext;
    private volatile Process checkpointerProcess;
    private final Logger syslog;
    private volatile boolean isInvoked;

    public InvokeCheckpointingJvm(ServerContext serverContext) {
        this.serverContext = serverContext;
        syslog = LoggerFactory.getLogger("syslog");
    }

    @Override
    public void invokeCheckpointing() {
        for (int i = 1; i <= MAX_COMPACTION_RETRIES; i++) {
            try {
                if (!serverContext.getCompactorScriptPath().isPresent() || !serverContext.getCompactorConfig().isPresent()) {
                    syslog.warn("Compactor client runner script or config file not found");
                    return;
                }

                String compactorScriptPath = serverContext.getCompactorScriptPath().get();
                String compactorConfigPath = serverContext.getCompactorConfig().get();
                List<String> endpoint = Arrays.asList(serverContext.getLocalEndpoint().split(":"));
                String hostName = endpoint.get(0);
                String port = endpoint.get(1);

                if (isRunning()) {
                    shutdown();
                }

                ProcessBuilder pb = new ProcessBuilder(compactorScriptPath, "--hostname", hostName, "--port",
                        port, "--compactorConfig", compactorConfigPath, "--startCheckpointing=true");
                pb.redirectOutput(ProcessBuilder.Redirect.PIPE);
                pb.redirectError(ProcessBuilder.Redirect.PIPE);
                this.checkpointerProcess = pb.start();
                this.isInvoked = true;
                syslog.info("Triggered compactor jvm");
                return;
            } catch (RuntimeException re) {
                if (DistributedCheckpointer.isCriticalRuntimeException(re, i, MAX_COMPACTION_RETRIES)) {
                    break;
                }
            } catch (IOException io) {
                syslog.error("Encountered IOException due to : {}. StackTrace: {}", io.getMessage(), io.getStackTrace());
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
        }
        this.isInvoked = false;
    }
}
