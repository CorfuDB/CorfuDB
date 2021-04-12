package org.corfudb.universe.group.cluster.vm;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedSet;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.util.ClassUtils;
import org.corfudb.runtime.BootstrapUtil;
import org.corfudb.runtime.view.Layout;
import org.corfudb.universe.group.cluster.AbstractCorfuCluster;
import org.corfudb.universe.group.cluster.CorfuCluster;
import org.corfudb.universe.group.cluster.CorfuClusterParams;
import org.corfudb.universe.logging.LoggingParams;
import org.corfudb.universe.node.Node;
import org.corfudb.universe.node.Node.NodeParams;
import org.corfudb.universe.node.server.vm.VmCorfuServer;
import org.corfudb.universe.node.server.vm.VmCorfuServerParams;
import org.corfudb.universe.node.server.vm.VmCorfuServerParams.VmName;
import org.corfudb.universe.universe.vm.VmManager;
import org.corfudb.universe.universe.vm.VmUniverseParams;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Provides VM implementation of a {@link CorfuCluster}.
 */
@Slf4j
public class VmCorfuCluster extends AbstractCorfuCluster<VmCorfuServerParams, VmUniverseParams> {
    private final ImmutableMap<VmName, VmManager> vms;

    @Builder
    protected VmCorfuCluster(CorfuClusterParams<VmCorfuServerParams> corfuClusterParams,
                             VmUniverseParams universeParams, ImmutableMap<VmName, VmManager> vms,
                             @NonNull LoggingParams loggingParams) {
        super(corfuClusterParams, universeParams, loggingParams);
        this.vms = vms;

        init();
    }

    /**
     * Deploys a Corfu server node according to the provided parameter.
     *
     * @return an instance of {@link Node}
     */
    @Override
    protected Node buildServer(VmCorfuServerParams nodeParams) {
        log.info("Deploy corfu server: {}", nodeParams);
        VmCorfuServerParams params = getVmServerParams(nodeParams);

        VmManager vmManager = vms.get(params.getVmName());

        RemoteOperationHelper commandHelper = RemoteOperationHelper.builder()
                .ipAddress(vmManager.getResolvedIpAddress())
                .credentials(universeParams.getCredentials().getVmCredentials())
                .build();

        return VmCorfuServer.builder()
                .universeParams(universeParams)
                .params(params)
                .vmManager(vmManager)
                .remoteOperationHelper(commandHelper)
                .loggingParams(loggingParams)
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
                .map(vmParams -> vms.get(vmParams.getVmName()).getResolvedIpAddress() + ":" + vmParams.getPort())
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
