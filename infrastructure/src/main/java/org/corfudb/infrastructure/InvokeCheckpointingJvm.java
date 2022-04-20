package org.corfudb.infrastructure;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.exceptions.NetworkException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class InvokeCheckpointingJvm implements IInvokeCheckpointing {

    private static final long CONN_RETRY_DELAY_MILLISEC = 500;
    private final ServerContext serverContext;
    private volatile Process checkpointerProcess;
    private Logger syslog;

    public InvokeCheckpointingJvm(ServerContext serverContext) {
        this.serverContext = serverContext;
        syslog = LoggerFactory.getLogger("SYSLOG");
    }

    @Override
    public void invokeCheckpointing() {
        int MAX_COMPACTION_RETRIES = 8;
        for (int i = 1; i <= MAX_COMPACTION_RETRIES; i++) {
            try {
                String compactorScriptPath = (String) serverContext.getCompactorScriptPath();
                String compactorConfigPath = (String) serverContext.getCompactorConfig();

                syslog.info("Script path: {}, configPath: {}", compactorScriptPath, compactorConfigPath);
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
                syslog.info("runCompactionOrchestrator: started the process");
                break;
            } catch (RuntimeException re) {
                syslog.trace("runCompactionOrchestrator: encountered an exception on attempt {}/{}.",
                        i, MAX_COMPACTION_RETRIES, re);

                if (i >= MAX_COMPACTION_RETRIES) {
                    syslog.error("runCompactionOrchestrator: retry exhausted.", re);
                    break;
                }

                if (re instanceof NetworkException || re.getCause() instanceof TimeoutException) {
                    try {
                        TimeUnit.MILLISECONDS.sleep(CONN_RETRY_DELAY_MILLISEC);
                    } catch (InterruptedException e) {
                        syslog.error("Interrupted in network retry delay sleep");
                        break;
                    }
                }
            } catch (Throwable t) {
                syslog.error("runCompactionOrchestrator: encountered unexpected exception", t);
                syslog.error("StackTrace: {}", t.getStackTrace());
            }
        }
    }

    @Override
    public boolean isRunning() {
        return (this.checkpointerProcess != null && this.checkpointerProcess.isAlive());
    }

    @Override
    public void shutdown() {
        if (this.checkpointerProcess != null && this.checkpointerProcess.isAlive()) {
            this.checkpointerProcess.destroy();
            this.checkpointerProcess = null;
        }
    }
}
