package org.corfudb.universe.node.client;

import com.google.common.collect.ImmutableSortedSet;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters.CorfuRuntimeParametersBuilder;
import org.corfudb.runtime.collections.ICorfuTable;
import org.corfudb.runtime.collections.PersistentCorfuTable;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.ManagementView;
import org.corfudb.runtime.view.ObjectsView;
import org.corfudb.util.NodeLocator;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

import static org.corfudb.runtime.CorfuRuntime.fromParameters;

/**
 * Provides Corfu client (utility class) used in the local machine
 * (in current process) which is basically a wrapper of CorfuRuntime.
 */
@Slf4j
public class LocalCorfuClient implements CorfuClient {
    private final CorfuRuntime runtime;

    @Getter
    private final ClientParams params;

    @Getter
    private final ImmutableSortedSet<String> serverEndpoints;

    @Builder
    public LocalCorfuClient(
            ClientParams params, ImmutableSortedSet<String> serverEndpoints,
            CorfuRuntimeParametersBuilder corfuRuntimeParams) {

        this.params = params;
        this.serverEndpoints = serverEndpoints;

        List<NodeLocator> layoutServers = serverEndpoints.stream()
                .sorted()
                .map(NodeLocator::parseString)
                .collect(Collectors.toList());

        corfuRuntimeParams
                .layoutServers(layoutServers)
                .systemDownHandler(this::systemDownHandler);

        this.runtime = fromParameters(corfuRuntimeParams.build());
    }

    /**
     * Connect corfu runtime to the server
     *
     * @return LocalCorfuClient
     */
    @Override
    public LocalCorfuClient deploy() {
        connect();
        return this;
    }

    /**
     * Shutdown corfu runtime
     *
     * @param timeout a limit within which the method attempts to gracefully stop the client (not used for a client).
     */
    @Override
    public void stop(Duration timeout) {
        runtime.shutdown();
    }

    /**
     * Shutdown corfu runtime
     */
    @Override
    public void kill() {
        runtime.shutdown();
    }

    /**
     * Shutdown corfu runtime
     */
    @Override
    public void destroy() {
        runtime.shutdown();
    }

    @Override
    public <K, V> ICorfuTable<K, V> createDefaultCorfuTable(String streamName) {
        return runtime.getObjectsView()
                .build()
                .setTypeToken(PersistentCorfuTable.<K, V>getTypeToken())
                .setStreamName(streamName)
                .open();
    }

    @Override
    public void connect() {
        runtime.connect();
    }

    @Override
    public CorfuRuntime getRuntime() {
        return runtime;
    }

    @Override
    public Layout getLayout() {
        return runtime.getLayoutView().getLayout();
    }

    @Override
    public ObjectsView getObjectsView() {
        return runtime.getObjectsView();
    }

    @Override
    public ManagementView getManagementView() {
        return runtime.getManagementView();
    }

    @Override
    public void invalidateLayout() {
        runtime.invalidateLayout();
    }

    @Override
    public void shutdown() {
        runtime.shutdown();
    }
}
