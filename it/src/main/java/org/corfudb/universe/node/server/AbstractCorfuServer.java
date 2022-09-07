package org.corfudb.universe.node.server;

import com.google.common.collect.ImmutableSortedSet;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.universe.logging.LoggingParams;
import org.corfudb.universe.node.client.LocalCorfuClient;
import org.corfudb.universe.universe.UniverseParams;

@Slf4j
@Getter
public abstract class AbstractCorfuServer<T extends CorfuServerParams, U extends UniverseParams>
        implements CorfuServer {

    @NonNull
    protected final T params;

    @NonNull
    protected final U universeParams;

    @NonNull
    protected final LoggingParams loggingParams;

    protected AbstractCorfuServer(@NonNull T params, @NonNull U universeParams,
                                  @NonNull LoggingParams loggingParams) {
        this.params = params;
        this.universeParams = universeParams;
        this.loggingParams = loggingParams;
    }


    /**
     * This method creates a command line string for starting Corfu server
     *
     * @return command line parameters
     */
    protected String getCommandLineParams() {
        StringBuilder cmd = new StringBuilder();
        cmd.append("-a ").append(getNetworkInterface());

        switch (params.getPersistence()) {
            case DISK:
                cmd.append(" -l ").append(params.getStreamLogDir());
                break;
            case MEMORY:
                cmd.append(" -m");
                break;
            default:
                throw new IllegalStateException("Unknown persistence mode");
        }

        if (params.getMode() == Mode.SINGLE) {
            cmd.append(" -s");
        }

        int healthPort = params.getHealthPort();
        cmd.append(" --health-port=").append(healthPort);

        cmd.append(" --log-size-quota-percentage=").append(params.getLogSizeQuotaPercentage()).append(" ");

        cmd.append(" -d ").append(params.getLogLevel().toString()).append(" ");

        cmd.append(params.getPort());

        String cmdLineParams = cmd.toString();
        log.info("Corfu server. Command line parameters: {}", cmdLineParams);

        return cmdLineParams;
    }

    @Override
    public LocalCorfuClient getLocalCorfuClient() {
        return LocalCorfuClient.builder()
                .serverEndpoints(ImmutableSortedSet.of(getEndpoint()))
                .corfuRuntimeParams(CorfuRuntime.CorfuRuntimeParameters.builder())
                .build()
                .deploy();
    }

    @Override
    public int compareTo(CorfuServer other) {
        return Integer.compare(getParams().getPort(), other.getParams().getPort());
    }
}
