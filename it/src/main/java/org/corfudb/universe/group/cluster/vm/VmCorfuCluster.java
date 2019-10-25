package org.corfudb.universe.group.cluster.vm;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedSet;
import com.vmware.vim25.mo.VirtualMachine;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.BootstrapUtil;
import org.corfudb.runtime.view.Layout;
import org.corfudb.universe.group.cluster.AbstractCorfuCluster;
import org.corfudb.universe.group.cluster.CorfuCluster;
import org.corfudb.universe.group.cluster.CorfuClusterParams;
import org.corfudb.universe.node.Node;
import org.corfudb.universe.node.Node.NodeParams;
import org.corfudb.universe.node.server.CorfuServerParams;
import org.corfudb.universe.node.server.vm.VmCorfuServer;
import org.corfudb.universe.node.server.vm.VmCorfuServerParams;
import org.corfudb.universe.node.stress.vm.VmStress;
import org.corfudb.universe.universe.vm.VmUniverseParams;
import org.corfudb.common.util.ClassUtils;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Provides VM implementation of a {@link CorfuCluster}.
 */
@Slf4j
public class VmCorfuCluster extends AbstractCorfuCluster<VmUniverseParams> {
    private final ImmutableMap<String, VirtualMachine> vms;

    @Builder
    protected VmCorfuCluster(CorfuClusterParams corfuClusterParams,
                             VmUniverseParams universeParams,
                             ImmutableMap<String, VirtualMachine> vms) {
        super(corfuClusterParams, universeParams);
        this.vms = vms;
    }


    /**
     * Deploys a Corfu server node according to the provided parameter.
     *
     * @return an instance of {@link Node}
     */
    @Override
    protected Node buildServer(CorfuServerParams nodeParams) {
        log.info("Deploy corfu server: {}", nodeParams);
        VmCorfuServerParams params = getVmServerParams(nodeParams);

        VirtualMachine vm = vms.get(params.getVmName());

        VmStress stress = VmStress.builder()
                .params(params)
                .universeParams(universeParams)
                .vm(vm)
                .build();

        return VmCorfuServer.builder()
                .universeParams(universeParams)
                .params(params)
                .vm(vm)
                .stress(stress)
                .build();
    }

    @Override
    protected ImmutableSortedSet<String> getClusterLayoutServers() {
        return ImmutableSortedSet.copyOf(buildLayout().getLayoutServers());
    }

    @Override
    public void bootstrap() {
        Layout layout = buildLayout();
        log.info("Bootstrap corfu cluster. Cluster: {}. layout: {}", params.getName(), layout.asJSONString());

        BootstrapUtil.bootstrap(layout, params.getBootStrapRetries(), params.getRetryDuration());
    }

    /**
     * @return an instance of {@link Layout} that is built from the existing parameters.
     */
    private Layout buildLayout() {
        long epoch = 0;
        UUID clusterId = UUID.randomUUID();

        List<String> servers = params.getNodesParams()
                .stream()
                .map(this::getVmServerParams)
                .map(vmParams -> vms.get(vmParams.getVmName()).getGuest().getIpAddress() + ":" + vmParams.getPort())
                .collect(Collectors.toList());

        Layout.LayoutSegment segment = new Layout.LayoutSegment(
                Layout.ReplicationMode.CHAIN_REPLICATION,
                0L,
                -1L,
                Collections.singletonList(new Layout.LayoutStripe(servers))
        );
        return new Layout(servers, servers, Collections.singletonList(segment), epoch, clusterId);
    }

    private VmCorfuServerParams getVmServerParams(NodeParams serverParams) {
        return ClassUtils.cast(serverParams, VmCorfuServerParams.class);
    }
}
