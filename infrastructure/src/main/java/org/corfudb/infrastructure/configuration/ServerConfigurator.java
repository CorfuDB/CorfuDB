package org.corfudb.infrastructure.configuration;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.BatchProcessor;
import org.corfudb.infrastructure.LogUnitServer;
import org.corfudb.infrastructure.LogUnitServer.LogUnitParameters;
import org.corfudb.infrastructure.LogUnitServerCache;
import org.corfudb.infrastructure.ManagementAgent;
import org.corfudb.infrastructure.ManagementServer;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.ServerThreadFactory;
import org.corfudb.infrastructure.log.InMemoryStreamLog;
import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.infrastructure.log.StreamLogCompaction;
import org.corfudb.infrastructure.log.StreamLogFiles;
import org.corfudb.infrastructure.management.ClusterStateContext;
import org.corfudb.infrastructure.management.FailureDetector;
import org.corfudb.infrastructure.orchestrator.Orchestrator;
import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters;
import org.corfudb.runtime.exceptions.UnreachableClusterException;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.Utils;
import org.corfudb.util.concurrent.SingletonResource;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.corfudb.infrastructure.management.ClusterStateContext.HeartbeatCounter;

/**
 * This class is used to configure and create the corfu servers that share the same resources.
 */
@Slf4j
public class ServerConfigurator {
    // Global server context
    @Getter
    private final ServerContext context;

    // Log Unit related configuration
    private final long streamCompactionInitialDelay = 10L;
    private final long streamCompactionPeriod = 45L;
    private final TimeUnit streamCompactionUnits = TimeUnit.MINUTES;
    private StreamLog streamLog;
    private LogUnitParameters params;
    private BatchProcessor batchProcessor;
    private LogUnitServerCache logUnitServerCache;
    private StreamLogCompaction streamLogCompaction;
    private LogUnitServer logUnitServer;

    // Management Server related configuration

    /**
     * The number of tries to be made to execute any RPC request before the runtime gives up and
     * invokes the systemDownHandler.
     * This is set to 60  based on the fact that the sleep duration between RPC retries is
     * defaulted to 1 second in the Runtime parameters. This gives the Runtime a total of 1 minute
     * to make progress. Else the ongoing task is aborted.
     */
    private final int systemDownHandlerTriggerLimit = 60;
    private ManagementAgent managementAgent;
    private CorfuRuntime managementServerRuntime;
    private HeartbeatCounter heartbeatCounter;
    private ClusterStateContext clusterStateContext;

    public ServerConfigurator(ServerContext context) {
        this.context = context;
        this.params = LogUnitParameters.parse(context);
    }

    private StreamLog getStreamLog() {
        if (streamLog != null) {
            return streamLog;
        }
        if (params.isMemoryMode()) {
            log.warn("Log unit opened in-memory mode (Maximum size={}). "
                    + "This should be run for testing purposes only. "
                    + "If you exceed the maximum size of the unit, old entries will be "
                    + "AUTOMATICALLY trimmed. "
                    + "The unit WILL LOSE ALL DATA if it exits.", Utils
                    .convertToByteStringRepresentation(params.getMaxCacheSize()));
            streamLog = new InMemoryStreamLog();
        } else {
            streamLog = new StreamLogFiles(getContext(), params.isNoVerify());
        }

        return streamLog;
    }


    private BatchProcessor getBatchProcessor() {
        if (batchProcessor != null) {
            return batchProcessor;
        }
        batchProcessor = new BatchProcessor(getStreamLog(), getContext().getServerEpoch(),
                !params.isNoSync());
        return batchProcessor;
    }

    private StreamLogCompaction getStreamLogCompaction() {
        if (streamLogCompaction != null) {
            return streamLogCompaction;
        }
        streamLogCompaction = new StreamLogCompaction(getStreamLog(),
                streamCompactionInitialDelay, streamCompactionPeriod,
                streamCompactionUnits, ServerContext.SHUTDOWN_TIMER);
        return streamLogCompaction;
    }


    private LogUnitServerCache getLogUnitServerCache() {
        if (logUnitServerCache != null) {
            return logUnitServerCache;
        }
        logUnitServerCache = new LogUnitServerCache(params, getStreamLog());
        return logUnitServerCache;
    }

    public LogUnitServer getLogUnitServer() {
        if (logUnitServer != null) {
            return logUnitServer;
        }

        ExecutorService executor = Executors.newFixedThreadPool(getContext()
                        .getLogunitThreadCount(),
                new ServerThreadFactory("LogUnit-",
                        new ServerThreadFactory.ExceptionHandler()));

        logUnitServer = LogUnitServer.builder()
                .serverContext(getContext())
                .config(params)
                .executor(executor)
                .streamLog(getStreamLog())
                .dataCache(getLogUnitServerCache())
                .batchWriter(getBatchProcessor())
                .logCleaner(getStreamLogCompaction())
                .build();
        return logUnitServer;
    }

    private CorfuRuntime getManagementServerRuntime() {
        if (managementServerRuntime != null) {
            return managementServerRuntime;
        }
        CorfuRuntimeParameters corfuParams = getContext()
                .getManagementRuntimeParameters();
        corfuParams.setSystemDownHandlerTriggerLimit(systemDownHandlerTriggerLimit);
        managementServerRuntime = CorfuRuntime.fromParameters(corfuParams);
        return managementServerRuntime;
    }

    private CorfuRuntime connect(CorfuRuntime runtime) {
        Layout managementLayout = getContext().copyManagementLayout();
        if (managementLayout != null) {
            managementLayout.getLayoutServers().forEach(runtime::addLayoutServer);
        }
        runtime.connect();

        Runnable runtimeSystemDownHandler = () -> {
            log.warn("ManagementServer: Runtime stalled. Invoking systemDownHandler after {} "
                    + "unsuccessful tries.", runtime.getParameters().getSystemDownHandlerTriggerLimit());
            throw new UnreachableClusterException("Runtime stalled. Invoking systemDownHandler after "
                    + runtime.getParameters().getSystemDownHandlerTriggerLimit() + " unsuccessful tries.");
        };

        log.info("Corfu Runtime connected successfully");
        runtime.getParameters().setSystemDownHandler(runtimeSystemDownHandler);
        return runtime;
    }

    private SingletonResource<CorfuRuntime> getSingletonResourceRuntime() {
        return SingletonResource.withInitial(() -> connect(getManagementServerRuntime()));
    }

    private HeartbeatCounter getHeartbeatCounter() {
        if (heartbeatCounter != null) {
            return heartbeatCounter;
        }
        heartbeatCounter = new HeartbeatCounter();
        return heartbeatCounter;
    }

    private ClusterStateContext getClusterStateContext() {
        if (clusterStateContext != null) {
            return clusterStateContext;
        }

        ClusterState defaultView = ClusterState.builder()
                .localEndpoint(getContext().getLocalEndpoint())
                .nodes(ImmutableMap.of())
                .unresponsiveNodes(ImmutableList.of())
                .build();
        clusterStateContext = ClusterStateContext.builder()
                .counter(getHeartbeatCounter())
                .clusterView(new AtomicReference<>(defaultView))
                .build();
        return clusterStateContext;
    }

    private ManagementAgent getManagementAgent() {
        if (managementAgent != null) {
            return managementAgent;
        }

        FailureDetector failureDetector = new FailureDetector(getHeartbeatCounter(),
                getContext().getLocalEndpoint());

        managementAgent = new ManagementAgent(
                getSingletonResourceRuntime(),
                getContext(),
                getClusterStateContext(),
                failureDetector,
                getContext().copyManagementLayout());
        return managementAgent;
    }

    private Orchestrator getOrchestrator() {
        return new Orchestrator(
                getSingletonResourceRuntime(), getContext(), getStreamLog());
    }


    public ManagementServer getManagementServer() {

        ExecutorService executor = Executors.newFixedThreadPool(
                getContext().getManagementServerThreadCount(),
                new ServerThreadFactory("management-",
                        new ServerThreadFactory.ExceptionHandler()));

        ExecutorService heartbeatThread = Executors.newSingleThreadExecutor(
                new ServerThreadFactory("heartbeat-",
                        new ServerThreadFactory.ExceptionHandler()));

        return ManagementServer.builder()
                .serverContext(getContext())
                .executor(executor)
                .heartbeatThread(heartbeatThread)
                .failureHandlerPolicy(getContext().getFailureHandlerPolicy())
                .corfuRuntime(getSingletonResourceRuntime())
                .clusterContext(getClusterStateContext())
                .managementAgent(getManagementAgent())
                .orchestrator(getOrchestrator())
                .build();
    }

}
