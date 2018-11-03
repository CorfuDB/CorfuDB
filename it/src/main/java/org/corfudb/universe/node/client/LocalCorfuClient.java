package org.corfudb.universe.node.client;

import com.google.common.collect.ImmutableSortedSet;
import com.google.common.reflect.TypeToken;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.ManagementView;
import org.corfudb.runtime.view.ObjectsView;
import org.corfudb.universe.node.stress.Stress;
import org.corfudb.util.NodeLocator;

import java.time.Duration;

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
    private final ImmutableSortedSet<NodeLocator> serverEndpoints;

    @Builder
    public LocalCorfuClient(ClientParams params, ImmutableSortedSet<NodeLocator> serverEndpoints) {
        this.params = params;
        this.serverEndpoints = serverEndpoints;

        CorfuRuntimeParameters runtimeParams = CorfuRuntimeParameters
                .builder()
                .layoutServers(serverEndpoints)
                .systemDownHandler(this::systemDownHandler)
                .build();

        this.runtime = fromParameters(runtimeParams);
    }

    /**
     * Connect corfu runtime to the server
     * @return
     */
    @Override
    public LocalCorfuClient deploy() {
        connect();
        return this;
    }

    /**
     * Shutdown corfu runtime
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
    public Stress getStress() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public <K, V> CorfuTable<K, V> createDefaultCorfuTable(String streamName) {
        return runtime.getObjectsView()
                .build()
                .setTypeToken(new TypeToken<CorfuTable<K, V>>() {})
                .setStreamName(streamName)
                .open();
    }

    @Override
    public void connect() {
        runtime.connect();
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
}
