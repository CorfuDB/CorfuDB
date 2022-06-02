package org.corfudb.infrastructure;

import org.corfudb.runtime.exceptions.NetworkException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class InvokeCheckpointingJvm implements IInvokeCheckpointing {

    private static final long CONN_RETRY_DELAY_MILLISEC = 500;
    private static final int MAX_COMPACTION_RETRIES = 8;
    private final ServerContext serverContext;
    private volatile Process checkpointerProcess;
    private Logger syslog;
    private boolean isInvoked;

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
                        port, "--compactorConfig", compactorConfigPath);
                pb.inheritIO();
                this.checkpointerProcess = pb.start();
                this.isInvoked = true;
                syslog.info("Triggered the compaction jvm");
                break;
            } catch (RuntimeException re) {
                syslog.trace("Encountered an exception on attempt {}/{}.",
                        i, MAX_COMPACTION_RETRIES, re);

                if (i >= MAX_COMPACTION_RETRIES) {
                    syslog.error("Retry exhausted.", re);
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
                syslog.error("Encountered unexpected exception", t);
                syslog.error("StackTrace: {}", t.getStackTrace());
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
