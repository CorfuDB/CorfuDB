package org.corfudb.universe.group.cluster.docker;

import com.google.common.collect.ImmutableList;
import com.spotify.docker.client.DockerClient;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.BootstrapUtil;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.Layout.LayoutSegment;
import org.corfudb.universe.group.cluster.AbstractCorfuCluster;
import org.corfudb.universe.group.cluster.CorfuClusterParams;
import org.corfudb.universe.logging.LoggingParams;
import org.corfudb.universe.node.server.CorfuServer;
import org.corfudb.universe.node.server.CorfuServerParams;
import org.corfudb.universe.node.server.docker.DockerCorfuServer;
import org.corfudb.universe.universe.UniverseParams;
import org.corfudb.universe.group.cluster.CorfuCluster;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Provides Docker implementation of {@link CorfuCluster}.
 */
@Slf4j
public class DockerCorfuCluster extends AbstractCorfuCluster<CorfuClusterParams, UniverseParams> {
    @NonNull
    private final DockerClient docker;
    @NonNull
    private final LoggingParams loggingParams;

    @Builder
    public DockerCorfuCluster(DockerClient docker, CorfuClusterParams params, UniverseParams universeParams,
                              LoggingParams loggingParams) {
        super(params, universeParams);
        this.docker = docker;
        this.loggingParams = loggingParams;
    }

    @Override
    protected CorfuServer buildCorfuServer(CorfuServerParams nodeParams) {

        return DockerCorfuServer.builder()
                .universeParams(universeParams)
                .clusterParams(params)
                .params(nodeParams)
                .loggingParams(loggingParams)
                .docker(docker)
                .build();
    }

    @Override
    protected ImmutableList<String> getClusterLayoutServers() {
        List<String> servers = nodes.values()
                .stream()
                .map(CorfuServer::getEndpoint)
                .collect(Collectors.toList());

        return ImmutableList.copyOf(servers);
    }

    @Override
    public void bootstrap() {
        Layout layout = getLayout();
        log.info("Bootstrap docker corfu cluster. Cluster: {}. layout: {}", params.getName(), layout.asJSONString());

        BootstrapUtil.bootstrap(layout, params.getBootStrapRetries(), params.getRetryTimeout());
    }

    private Layout getLayout() {
        long epoch = 0;
        UUID clusterId = UUID.randomUUID();
        List<String> servers = getClusterLayoutServers();

        LayoutSegment segment = new LayoutSegment(
                Layout.ReplicationMode.CHAIN_REPLICATION,
                0L,
                -1L,
                Collections.singletonList(new Layout.LayoutStripe(servers))
        );
        return new Layout(servers, servers, Collections.singletonList(segment), epoch, clusterId);
    }
}
