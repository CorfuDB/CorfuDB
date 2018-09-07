package org.corfudb.universe.service;

import com.google.common.collect.ImmutableList;
import com.vmware.vim25.mo.*;
import lombok.Builder;
import lombok.Getter;
import org.corfudb.universe.cluster.vm.CorfuServerOnAppliance;
import org.corfudb.universe.node.Node;
import org.corfudb.universe.node.CorfuServer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static lombok.Builder.Default;
import static org.corfudb.universe.cluster.vm.VmCluster.VmClusterParams;
import static org.corfudb.universe.node.CorfuServer.ServerParams;

/**
 * Provides VM implementation of {@link Service}.
 */
@Builder
public class VMService implements Service {
    @Default
    private final ImmutableList<Node> nodes = ImmutableList.of();
    private final VirtualMachine appliance;
    @Getter
    private final ServiceParams<ServerParams> params;
    private final VmClusterParams clusterParams;

    @Override
    public VMService deploy() {
        List<Node> nodesSnapshot = new ArrayList<>();
        for (ServerParams serverParam : params.getNodes()) {
            CorfuServer node = CorfuServerOnAppliance.builder()
                    .clusterParams(clusterParams)
                    .params(serverParam)
                    .appliance(appliance)
                    .build();

            node = node.deploy();
            nodesSnapshot.add(node);
        }

        return VMService
                .builder()
                .clusterParams(clusterParams)
                .params(params)
                .nodes(ImmutableList.copyOf(nodesSnapshot))
                .appliance(appliance)
                .build();
    }

    @Override
    public void stop(Duration timeout) {
        nodes.forEach(n -> n.stop(timeout));
    }

    @Override
    public void kill() {
        nodes.forEach(Node::kill);
    }

    @Override
    public void unlink(Node node) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public ImmutableList<Node> nodes() {
        return nodes;
    }
}
