package org.corfudb.universe.node;

import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.ManagementView;
import org.corfudb.runtime.view.ObjectsView;
import org.corfudb.universe.node.CorfuServer.ServerParams;
import org.corfudb.util.NodeLocator;

import java.time.Duration;
import java.util.Collections;

import static org.corfudb.runtime.CorfuRuntime.fromParameters;

@Slf4j
public class LocalCorfuClient implements CorfuClient {
    private final CorfuRuntime runtime;
    @Getter
    private final ClientParams params;
    @Getter
    private final ServerParams serverParams;

    @Builder
    public LocalCorfuClient(ClientParams params, ServerParams serverParams) {
        this.params = params;
        this.serverParams = serverParams;

        NodeLocator node = NodeLocator
                .builder()
                .protocol(NodeLocator.Protocol.TCP)
                .host(serverParams.getName())
                .port(serverParams.getPort())
                .build();

        CorfuRuntime.CorfuRuntimeParameters runtimeParams = CorfuRuntime.CorfuRuntimeParameters
                .builder()
                .layoutServers(Collections.singletonList(node))
                .build();

        this.runtime = fromParameters(runtimeParams);
    }

    @Override
    public LocalCorfuClient deploy() {
        connect();
        return this;
    }

    public boolean add(CorfuServer server) {
        log.debug("Add node: {}", server.getParams());

        //FIXME fix it corfu runtime and remove this code then
        if (serverParams.equals(server.getParams())) {
            log.warn("Can't add itself into the corfu cluster. Server params: {}", serverParams);
            return false;
        }

        ServerParams clusterNodeParams = server.getParams();
        runtime.getManagementView().addNode(
                clusterNodeParams.getEndpoint(),
                clusterNodeParams.getWorkflowNumRetry(),
                clusterNodeParams.getTimeout(),
                clusterNodeParams.getPollPeriod()
        );

        return true;
    }

    public boolean remove(CorfuServer server) {
        log.debug("Remove node: {}", server.getParams());

        if (serverParams.equals(server.getParams())) {
            log.warn("Can't add itself into the corfu cluster. Server params: {}", serverParams);
            return false;
        }

        ServerParams clusterNodeParams = server.getParams();
        runtime.getManagementView().removeNode(
                clusterNodeParams.getEndpoint(),
                clusterNodeParams.getWorkflowNumRetry(),
                clusterNodeParams.getTimeout(),
                clusterNodeParams.getPollPeriod()
        );

        return true;
    }

    public Layout getLayout() {
        return runtime.getLayoutView().getLayout();
    }

    public ObjectsView getObjectsView(){
        return runtime.getObjectsView();
    }

    private void connect() {
        runtime.connect();
    }

    @Override
    public void stop(Duration timeout) {
        //Nothing to stop
    }

    @Override
    public void kill() {
        //Nothing to kill
    }

    public ManagementView getManagementView() {
        return runtime.getManagementView();
    }

    public void invalidateLayout() {
        runtime.invalidateLayout();
    }
}
