package org.corfudb.infrastructure;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.exceptions.NetworkException;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
public class InvokeCheckpointingJvm implements IInvokeCheckpointing {

    private static final long CONN_RETRY_DELAY_MILLISEC = 500;
    private final ServerContext serverContext;
    private volatile Process checkpointerProcess;

    public InvokeCheckpointingJvm(ServerContext serverContext) {
        this.serverContext = serverContext;
    }

    @Override
    public void invokeCheckpointing() {
        int MAX_COMPACTION_RETRIES = 8;
        for (int i = 1; i <= MAX_COMPACTION_RETRIES; i++) {
            try {
                String compactorScriptPath = (String) serverContext.getCompactorScriptPath();
                String compactorConfigPath = (String) serverContext.getCompactorConfig();

                log.info("Script path: {}, configPath: {}", compactorScriptPath, compactorConfigPath);
                List<String> endpoint = Arrays.asList(serverContext.getLocalEndpoint().split(":"));
                String hostName = endpoint.get(0);
                String port = endpoint.get(1);

                if (this.checkpointerProcess != null && this.checkpointerProcess.isAlive()) {
                    this.checkpointerProcess.destroy();
                }

                ProcessBuilder pb = new ProcessBuilder(compactorScriptPath, "--hostname", hostName, "--port",
                        port, "--compactorConfig", compactorConfigPath);
                pb.inheritIO();
                this.checkpointerProcess = pb.start();
                log.info("runCompactionOrchestrator: started the process");
                this.checkpointerProcess.waitFor();
                this.checkpointerProcess = null;

                log.debug("runCompactionOrchestrator: successfully finished a cycle");
                break;

            } catch (RuntimeException re) {
                log.trace("runCompactionOrchestrator: encountered an exception on attempt {}/{}.",
                        i, MAX_COMPACTION_RETRIES, re);

                if (i >= MAX_COMPACTION_RETRIES) {
                    log.error("runCompactionOrchestrator: retry exhausted.", re);
                    break;
                }

                if (re instanceof NetworkException || re.getCause() instanceof TimeoutException) {
                    try {
                        TimeUnit.MILLISECONDS.sleep(CONN_RETRY_DELAY_MILLISEC);
                    } catch (InterruptedException e) {
                        log.error("Interrupted in network retry delay sleep");
                        break;
                    }
                }
            } catch (Throwable t) {
                log.error("runCompactionOrchestrator: encountered unexpected exception", t);
                log.error("StackTrace: {}", t.getStackTrace());
            }
        }
    }

    @Override
    public void shutdown() {
        if (this.checkpointerProcess != null && this.checkpointerProcess.isAlive()) {
            this.checkpointerProcess.destroy();
            this.checkpointerProcess = null;
        }
    }
}
